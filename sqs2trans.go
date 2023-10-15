package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
	"golang.org/x/sync/semaphore"
)

type S3 struct {
	Bucket struct {
		Name string
	}
	Object struct {
		Key  string
		Size int64
	}
}

type Record struct {
	EventVersion string
	EventSource  string
	AwsRegion    string
	EventTime    string
	EventName    string
	S3           S3
}

type Message struct {
	Records []Record
	Event   string
}

// CompareBucket and send SQS message, not checking Head file (no TransferMetadata)
func compareBucket(from, to BInfo, sqsSvc *sqs.SQS) error {
	var wg sync.WaitGroup
	var ignoreList []*string
	var fromList, toList []*s3.Object
	var err error
	var jobList []Message

	wg.Add(3)
	go func() {
		defer wg.Done()
		ignoreList = getIgnoreList()
		if err != nil {
			log.Fatalln(err)
		}
	}()
	go func() {
		defer wg.Done()
		fromList, err = getS3ObjectList(from)
		if err != nil {
			log.Fatalln(err)
		}
	}()
	go func() {
		defer wg.Done()
		toList, err = getS3ObjectList(to)
		if err != nil {
			log.Fatalln(err)
		}
	}()
	wg.Wait()
	// Compare each objects's name and size, pick up the delta
	jobList, sizeCount = compareS3Objects(fromList, toList, ignoreList, from, to)
	objectCount = int64(len(jobList))

	// sqsSvc 如果nil就是不发SQS，只统计和写log
	if sqsSvc != nil {
		log.Printf("Found %d jobs to send SQS\n", objectCount)
		wg.Add(1)
		go func() {
			// Send SQS Message in batch
			defer wg.Done()
			log.Printf("Uploading jobs to SQS queue: %s\n", cfg.SQSUrl)
			err = sendSQS(jobList, sqsSvc)
			if err != nil {
				log.Println("Failed to send SQS messages", err)
			}
		}()
	}

	// Write jobList to file
	if cfg.JobListPath != "" {
		wg.Add(1)
		go func() {
			// Write Messages to file
			defer wg.Done()
			log.Println("Writing SQS messages to file", cfg.JobListPath)
			err = writeJobListFile(jobList, cfg.JobListPath)
			if err != nil {
				log.Println("Failed to write SQS messages to file", err)
			}
		}()
	}
	wg.Wait()
	return nil
}

// Compare S3 Objects, return delta list
func compareS3Objects(fromList, toList []*s3.Object, ignoreList []*string, from, to BInfo) ([]Message, int64) {
	var listSizeCount = int64(0)
	delta := make([]Message, 0)
	fromMap := make(map[string]*s3.Object)
	toMap := make(map[string]*s3.Object)

	for _, obj := range fromList {
		if !isIgnored(*obj.Key, ignoreList) {
			fromMap[*obj.Key] = obj
		}
	}
	for _, obj := range toList {
		toMap[*obj.Key] = obj
	}

	// 只生成在From有而To没有的或Size不同的
	for key, fromObj := range fromMap {
		// 根据源和目标的 prefix 创建目标 key
		toKey := path.Join(to.prefix, strings.TrimPrefix(key, from.prefix))

		toObj, ok := toMap[toKey]
		if !ok || *toObj.Size != *fromObj.Size {
			records := []Record{
				{
					EventVersion: "2.1",
					EventSource:  "aws:s3",
					AwsRegion:    from.region,                     // You may need to update this
					EventTime:    time.Now().Format(time.RFC3339), // Use current time
					EventName:    "ObjectCreated:Put",             // Assume object creation
					S3: S3{
						Bucket: struct {
							Name string
						}{
							Name: from.bucket,
						},
						Object: struct {
							Key  string
							Size int64
						}{
							Key:  key,
							Size: *fromObj.Size,
						},
					},
				},
			}
			msg := Message{Records: records}
			delta = append(delta, msg)
			listSizeCount += *fromObj.Size
		}
	}
	return delta, listSizeCount
}

// Send SQS Message in batch with concurrency goroutines
func sendSQS(jobList []Message, sqsSvc *sqs.SQS) error {
	var sqsBatch int
	var sqsMessage []*sqs.SendMessageBatchRequestEntry
	var wg sync.WaitGroup
	BatchSize := 10

	// Create a buffered channel to hold the jobs 要并发写SQS，所以用channel做buffer
	jobs := make(chan []*sqs.SendMessageBatchRequestEntry, cfg.NumWorkers)

	// Start the workers concurrency cfg.NumWorkers
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go sendSQSWorker(i, jobs, &wg, sqsSvc)
	}

	// Send SQS Message in batch
	for i, job := range jobList {
		jobJSON, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %v", err)
		}
		sqsMessage = append(sqsMessage, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprint(i)),
			MessageBody: aws.String(string(jobJSON)),
		})
		sqsBatch++

		if sqsBatch == BatchSize || i == len(jobList)-1 {
			// Copy sqsMessage to prevent data race
			sqsMessageCopy := make([]*sqs.SendMessageBatchRequestEntry, len(sqsMessage))
			copy(sqsMessageCopy, sqsMessage)

			// Send a job to the workers
			jobs <- sqsMessageCopy

			sqsBatch = 0
			sqsMessage = sqsMessage[:0]
		}
	}

	close(jobs) // close the jobs channel
	wg.Wait()
	log.Printf("Complete upload job to queue: %s\n", cfg.SQSUrl)
	return nil
}

func sendSQSWorker(id int, jobs <-chan []*sqs.SendMessageBatchRequestEntry, wg *sync.WaitGroup, sqsSvc *sqs.SQS) {
	defer wg.Done()
	var file *os.File
	var err error
	var logPath string

	// Prepare SQS sent log for writing a file, it is for backup
	if cfg.SQSSentLogName != "" {
		// Create SQS sent log file
		dateTimePrefix := time.Now().Format("20060102150405")
		logPath = fmt.Sprintf("%s-%s-sqs-sent-%d.log", cfg.SQSSentLogName, dateTimePrefix, id)
		file, err = os.Create(logPath)
		if err != nil {
			log.Printf("Failed to create SQS sent log file: %v\n", err)
			return
		}
		defer file.Close()
	}

	for job := range jobs {

		// Send Message to SQS
		_, err := sqsSvc.SendMessageBatch(&sqs.SendMessageBatchInput{
			QueueUrl: aws.String(cfg.SQSUrl),
			Entries:  job,
		})
		if err != nil {
			log.Printf("Worker %d: Failed to send sqs message: %v; JobList: %v\n", id, err, job)
			continue
		}

		// Write SQS sent log to file for backup
		if cfg.SQSSentLogName != "" {
			for _, entry := range job {
				var messageBody map[string]interface{}
				err := json.Unmarshal([]byte(*entry.MessageBody), &messageBody)
				if err != nil {
					log.Printf("Worker %d: Failed to unmarshal MessageBody: %v\n", id, err)
					continue
				}
				messageBodyJson, err := json.Marshal(messageBody)
				if err != nil {
					log.Printf("Worker %d: Failed to marshal MessageBody to JSON: %v\n", id, err)
					continue
				}
				_, err = file.WriteString(string(messageBodyJson) + "\n")
				if err != nil {
					log.Printf("Worker %d: Failed to write SQS sent log: %v\n", id, err)
					continue
				}
			}
		}
	}
	log.Printf("Worker %d: Complete upload job to queue\n", id)
	if cfg.SQSSentLogName != "" {
		log.Printf("Worker %d: Complete write SQS sent log to file: %s\n", id, logPath)
	}
}

func writeJobListFile(jobList []Message, path string) error {

	// Check if the file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// If not, create the file
		file, err := os.Create(path)
		if err != nil {
			fmt.Println("Error creating file: ", err)
			return err
		}
		file.Close()
	}
	// Open the file in append mode
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file: ", err)
		return err
	}
	defer file.Close()

	for _, job := range jobList {
		jobJSON, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %v", err)
		}
		_, err = file.WriteString(string(jobJSON) + "\n")
		if err != nil {
			return err
		}
	}
	log.Println("Complete writing job list to file", path)

	return nil
}

func consumeSQS(sqsSvc *sqs.SQS) error {
	var wgsqs sync.WaitGroup                                // 用于等待所有worker完成(适配s3tos3Action)
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers)) // 并发量为NumWorkers的信号量(适配s3tos3Action)
	ignoreList := getIgnoreList()
	for i := 0; i < cfg.NumWorkers; i++ {
		wgsqs.Add(1)
		go getSQSWorker(i, semFile, &wgsqs, sqsSvc, ignoreList)
	}
	wgsqs.Wait()
	return nil
}

func getSQSWorker(i int, semFile *semaphore.Weighted, wgsqs *sync.WaitGroup, sqsSvc *sqs.SQS, ignoreList []*string) {
	defer wgsqs.Done()
	sqsBatch := aws.Int64(10)
	var wg sync.WaitGroup

	for {
		resp, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueUrl:            &cfg.SQSUrl,
			MaxNumberOfMessages: sqsBatch,
		})
		if err != nil {
			log.Printf("Worker %d: Failed to get SQS. Wait for 5 seconds. ERR: %v\n", i, err)
			time.Sleep(time.Duration(cfg.RetryDelay) * time.Second)
			continue
		}

		if len(resp.Messages) == 0 {
			log.Printf("Worker %d: No message in queue available, wait...", i)
			time.Sleep(60 * time.Second)
			continue
		}
		log.Printf("Worker %d: Received %d messages\n", i, len(resp.Messages))
		// 对Batch Message中的每个Record进行处理
		for _, message := range resp.Messages {
			var msg Message
			var transferErr error
			transferErr = json.Unmarshal([]byte(*message.Body), &msg)
			if transferErr != nil {
				log.Printf("Worker %d: Failed to parse SQS message. ERR: %v\n", i, err)
				continue
			}

			for _, record := range msg.Records {
				// 根据源和目标的 prefix 创建目标 key
				// Decode the key from URL format 避免中间出现 + 号的情况
				var decodedKey string
				decodedKey, transferErr = url.QueryUnescape(record.S3.Object.Key)
				if transferErr != nil {
					log.Printf("Failed to decode key: %v. ERR: %v\n", record.S3.Object.Key, err)
					continue // TODO: 这里跳出去之后会跑到SQS Del去了
				}

				// ignore list
				if isIgnored(decodedKey, ignoreList) {
					log.Printf("Skipping ignored key in ignoreList %s\n", decodedKey)
					continue
				}

				toKey := path.Join(to.prefix, strings.TrimPrefix(decodedKey, from.prefix))
				fileInfo := FileInfo{
					FromKey:    decodedKey,
					FromBucket: record.S3.Bucket.Name,
					Size:       record.S3.Object.Size,
					ToBucket:   to.bucket,
					ToKey:      toKey,
				}

				// Transfer object
				if strings.HasPrefix(record.EventName, "ObjectCreated:") {
					targetObjectList := make([]*s3.Object, 0) // 按照SQS消息来传输，将忽略targetObjectList
					multipartUploadsList := make([]*s3.MultipartUpload, 0)
					if fileInfo.Size >= cfg.ResumableThreshold {
						multipartUploadsList, _ = getMultipartUploadList(to.svc, fileInfo.ToBucket, fileInfo.ToKey) // 查当前key是否有未完成的Multipart Upload
					}
					transferErr = s3tos3Action(from, to, fileInfo, semFile, &wg, multipartUploadsList, targetObjectList)
					wg.Wait()
					if transferErr != nil {
						log.Printf("Worker %d: Failed to transfer object: %v\n", i, err)
						continue // TODO: 这里跳出去之后会跑到SQS Del去了
					}
				}
				// Delete object
				if strings.HasPrefix(record.EventName, "ObjectRemoved:") {
					transferErr = delObjcet(to.svc, fileInfo.ToBucket, fileInfo.ToKey)
				}
			}

			// Skip processing for "s3:TestEvent"
			if msg.Event == "s3:TestEvent" {
				fmt.Println("Skipping Test Event")
			}
			// Delete SQS message
			if transferErr == nil {
				err = delSQS(message, sqsSvc)
				if err != nil {
					log.Printf("Worker %d: Failed to delete SQS message: %v\n", i, err)
					continue
				}
			}
		}
	}
}

func delSQS(message *sqs.Message, sqsSvc *sqs.SQS) error {
	_, err := sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &cfg.SQSUrl,
		ReceiptHandle: message.ReceiptHandle,
	})
	if err != nil {
		return err
	}
	return nil
}

func delObjcet(svc *s3.S3, bucket, key string) error {
	_, err := svc.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return err
	}
	return nil
}

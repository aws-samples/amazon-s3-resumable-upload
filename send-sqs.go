package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CompareBucket and send SQS message, not checking Head file (no TransferMetadata)
func compareBucket(from, to BInfo) error {
	var wg sync.WaitGroup
	var ignoreList []*string
	var fromList, toList []*s3.Object
	var err error
	var jobList []FileInfo

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
	log.Printf("Found %d jobs to send SQS\n", objectCount)

	wg.Add(2)
	go func() {
		// Send SQS Message in batch
		defer wg.Done()
		log.Printf("Uploading jobs to SQS queue: %s\n", cfg.SQSUrl)
		err = sendSQS(jobList)
		if err != nil {
			log.Println("Failed to send SQS messages", err)
		}
	}()

	go func() {
		// Write Messages to file
		defer wg.Done()
		log.Println("Writing SQS messages to file", cfg.JobListPath)
		err = writeJobListFile(jobList, cfg.JobListPath)
		if err != nil {
			log.Println("Failed to write SQS messages to file", err)
		}
	}()
	wg.Wait()
	return nil
}

// Compare S3 Objects, return delta list
func compareS3Objects(fromList, toList []*s3.Object, ignoreList []*string, from, to BInfo) ([]FileInfo, int64) {
	var listSizeCount = int64(0)
	delta := make([]FileInfo, 0)
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
			delta = append(delta, FileInfo{
				FromBucket: from.bucket,
				FromKey:    key,
				ToBucket:   to.bucket,
				ToKey:      toKey,
				Size:       *fromObj.Size,
			})
			listSizeCount += *fromObj.Size
		}
	}
	return delta, listSizeCount
}

// Send SQS Message in batch with concurrency goroutines
func sendSQS(jobList []FileInfo) error {
	var sqsBatch int
	var sqsMessage []*sqs.SendMessageBatchRequestEntry
	var wg sync.WaitGroup
	BatchSize := 10

	// Create a buffered channel to hold the jobs
	jobs := make(chan []*sqs.SendMessageBatchRequestEntry, cfg.NumWorkers)

	// Start the workers concurrency cfg.NumWorkers
	for i := 0; i < cfg.NumWorkers; i++ {
		wg.Add(1)
		go sendSQSWorker(i, jobs, &wg, sqsSvc)
	}

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

func writeJobListFile(jobList []FileInfo, path string) error {
	if path != "" {
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
	}
	return nil
}

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CompareBucket and send SQS message, not checking Head file (no TransferMetadata)
func compareBucket(from, to BInfo) error {
	var wg sync.WaitGroup
	var ignoreList []string
	var fromList, toList []*s3.Object
	var err error
	var jobList []FileInfo

	wg.Add(3)
	go func() {
		defer wg.Done()
		ignoreList, err = getIgnoreList()
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

	// Send SQS Message in batch
	err = sendSQS(jobList)
	if err != nil {
		log.Println("Failed to send SQS messages", err)
	}
	// Send SQS Message in batch
	return nil
}

func getIgnoreList() ([]string, error) {
	ignoreListPath := "ignore_list.txt"
	log.Printf("Checking ignore files list in %s\n", ignoreListPath)
	ignoreList := []string{}

	_, err := os.Stat(ignoreListPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No ignore files list in %s\n", ignoreListPath)
		} else {
			log.Println(err)
		}
	} else {
		file, err := os.Open(ignoreListPath)
		if err != nil {
			log.Println(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			prefix := strings.TrimPrefix(scanner.Text(), "/")
			ignoreList = append(ignoreList, prefix)
		}
		if err := scanner.Err(); err != nil {
			log.Println(err)
		}
		log.Printf("Found ignore files list Length: %d, in %s", len(ignoreList), ignoreListPath)
	}
	return ignoreList, nil
}

// Compare S3 Objects, return delta list
func compareS3Objects(fromList, toList []*s3.Object, ignoreList []string, from, to BInfo) ([]FileInfo, int64) {
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

func isIgnored(key string, ignoreList []string) bool {
	for _, prefix := range ignoreList {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}
	return false
}

// Send SQS Message in batch,
// TODO: 如果Object数量特别大，可以将这里改为并发；还可以在源List的时候就逐个Compare并投入channel中，然后多个SQS Sender并发处理channel中的SQS数据
func sendSQS(jobList []FileInfo) error {
	var sqsBatch int
	var sqsMessage []*sqs.SendMessageBatchRequestEntry
	log.Printf("Start uploading jobs to queue: %s\n", cfg.SQSUrl)

	for i, job := range jobList {
		jobJSON, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal job: %v", err)
		}
		sqsMessage = append(sqsMessage, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprint(i)),
			MessageBody: aws.String(string(jobJSON)),
		})
		sqsBatch += 1

		if sqsBatch == 10 || i == len(jobList)-1 {
			_, err := sqsSvc.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueUrl: aws.String(cfg.SQSUrl),
				Entries:  sqsMessage,
			})

			if err != nil {
				log.Printf("Failed to send sqs message: %v, %v\n", sqsMessage, err)
				return err
			}
			sqsBatch = 0
			sqsMessage = sqsMessage[:0]
		}
	}
	log.Printf("Complete upload job to queue: %s\n", cfg.SQSUrl)
	return nil
}

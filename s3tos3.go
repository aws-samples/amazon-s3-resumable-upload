package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"log"
	"mime"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/sync/semaphore"
)

func s3tos3(from, to BInfo) error {
	ignoreList := getIgnoreList()
	var wg sync.WaitGroup
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers)) // 并发量为NumWorkers的信号量 for file

	targetObjectList := make([]*s3.Object, 0)
	var err error
	if cfg.ListTarget && !cfg.SkipCompare {
		targetObjectList, err = getS3ObjectList(to) // 获取目标 S3 桶中的文件列表
		if err != nil {
			return err
		}
	}

	multipartUploadsList, _ := getMultipartUploadList(to.svc, to.bucket, to.prefix)

	// 遍历源S3
	inputListSource := &s3.ListObjectsV2Input{
		Bucket: aws.String(from.bucket),
		Prefix: aws.String(from.prefix),
	}
	if from.requestPayer {
		inputListSource.RequestPayer = aws.String("requester")
	}
	log.Printf("Listing srouce s3://%s\n", path.Join(from.bucket, from.prefix))
	err = from.svc.ListObjectsV2Pages(inputListSource, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range page.Contents {
			// Skip if the object is a directory
			if strings.HasSuffix(*item.Key, "/") {
				log.Println("...Skipping directory", *item.Key)
				continue
			}

			// Skip if key in ignoreList
			if isIgnored(*item.Key, ignoreList) {
				log.Println("...Skiping ignored key in ignoreList", *item.Key)
			}

			var combinedKey string
			if *item.Key != from.prefix {
				combinedKey = path.Join(to.prefix, strings.TrimPrefix(*item.Key, from.prefix))
				combinedKey = strings.TrimPrefix(combinedKey, "/")
			} else {
				combinedKey = path.Join(to.prefix, path.Base(*item.Key))
			}
			contentType := mime.TypeByExtension(filepath.Ext(*item.Key))
			fileInfo := FileInfo{
				FromBucket: from.bucket,
				FromKey:    *item.Key,
				ToBucket:   to.bucket,
				ToKey:      combinedKey,
				Size:       *item.Size,
				Others:     MetaStruct{ContentType: &contentType},
			}
			err = s3tos3Action(from, to, fileInfo, semFile, &wg, multipartUploadsList, targetObjectList)
			if err != nil {
				log.Println("Failed to s3tos3Action", err)
				return false
			}
		}
		return true
	})
	if err != nil {
		log.Println("Failed to list objects", err)
		return err
	}
	wg.Wait()
	return err
}

func s3tos3Action(from, to BInfo, fileInfo FileInfo, semFile *semaphore.Weighted, wg *sync.WaitGroup, multipartUploadsList []*s3.MultipartUpload, targetObjectList []*s3.Object) error {
	if cfg.TransferMetadata {
		err := getMetadata(from, &fileInfo)
		if err != nil {
			return err
		}
	}

	// Check file exist on S3 Bucket and get uploadId
	uploadId, err := getUploadId(to.svc, fileInfo, multipartUploadsList, targetObjectList)
	if err != nil {
		return err
	}
	if uploadId == "NEXT" {
		log.Printf("...File exists and same size. Skipping target. s3://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
		return nil
	}

	semFile.Acquire(context.Background(), 1) //从线程信号池中获取，没有线程可用了就阻塞等待
	atomic.AddInt32(&runningGoroutines, 1)   //线程计数
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer semFile.Release(1)                      //释放线程信号池
		defer atomic.AddInt32(&runningGoroutines, -1) //线程计数

		if fileInfo.Size < cfg.ResumableThreshold {
			err := transferSmall(from, to, fileInfo)
			if err != nil {
				log.Println("Failed to transferSmall", err)
				return
			}
		} else {
			// >= ResumableThreshold
			log.Printf("   Start to transfer (>= ResumableThreshold) s3://%s, runningGoroutines: %d\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), runningGoroutines)
			err := transferMultipart(from, to, uploadId, fileInfo)
			if err != nil {
				log.Println("Failed to multipartProccess", err)
				return
			}
		}
		log.Printf("***Successfully transfered s3://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
		atomic.AddInt64(&objectCount, 1)
		atomic.AddInt64(&sizeCount, fileInfo.Size)
	}()
	return nil
}

func transferSmall(from, to BInfo, fileInfo FileInfo) error {
	log.Printf("   Start to download (< ResumableThreshold) from %s/%s, runningGoroutines: %d\n", fileInfo.FromBucket, fileInfo.FromKey, runningGoroutines)
	buff := &aws.WriteAtBuffer{}
	inputDownload := &s3.GetObjectInput{
		Bucket: aws.String(fileInfo.FromBucket),
		Key:    aws.String(fileInfo.FromKey),
	}
	if from.requestPayer {
		inputDownload.RequestPayer = aws.String("requester")
	}
	_, err := from.downloader.Download(buff, inputDownload)
	if err != nil {
		log.Println("Error download from", from.url, err)
		return err
	}

	md5Hash := md5.Sum(buff.Bytes())
	md5Str := base64.StdEncoding.EncodeToString(md5Hash[:])
	log.Printf("   Start to upload (< ResumableThreshold) to %s/%s, runningGoroutines: %d\n", fileInfo.ToBucket, fileInfo.ToKey, runningGoroutines)
	inputUpload := &s3manager.UploadInput{
		Bucket:     aws.String(fileInfo.ToBucket),
		Key:        aws.String(fileInfo.ToKey),
		Body:       bytes.NewReader(buff.Bytes()),
		ContentMD5: aws.String(md5Str),
	}
	if to.storageClass != "" {
		inputUpload.StorageClass = aws.String(to.storageClass)
	}
	if to.ACL != "" {
		inputUpload.ACL = aws.String(to.ACL)
	}

	if fileInfo.Others.ContentType != nil && *fileInfo.Others.ContentType != "" {
		inputUpload.ContentType = fileInfo.Others.ContentType
	}
	if cfg.TransferMetadata {
		inputUpload.Metadata = fileInfo.Others.Metadata
		inputUpload.ContentEncoding = fileInfo.Others.ContentEncoding
		inputUpload.ContentLanguage = fileInfo.Others.ContentLanguage
		inputUpload.CacheControl = fileInfo.Others.CacheControl
		inputUpload.ContentDisposition = fileInfo.Others.ContentDisposition
	}
	_, err = to.uploader.Upload(inputUpload)
	if err != nil {
		log.Println("Error upload to", to.url, err)
		return err
	}
	return nil
}

func transferPart(from, to BInfo, partInfo PartInfo, wg *sync.WaitGroup, sem *semaphore.Weighted, uploadId string, partnumberList *[]PartInfo, partnumberListMutex *sync.Mutex) error {
	defer wg.Done()
	defer sem.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)

	// Download part S3 API Call
	buffer, err := downloadPartAction(from.svc, partInfo)
	if err != nil {
		return err
	}
	// Upload part S3 API Call
	err = uploadPartAction(buffer, partInfo, to.svc, uploadId, partnumberList, partnumberListMutex)
	if err != nil {
		return err
	}
	return nil
}

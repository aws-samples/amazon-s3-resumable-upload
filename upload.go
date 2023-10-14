package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"errors"
	"io"
	"log"
	"mime"
	"os"
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

func startUpload(from, to BInfo) error {
	var wg sync.WaitGroup
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers)) // 并发量为numWorkers的信号量 for file

	// List Target S3
	targetObjectList := make([]*s3.Object, 0)
	var err error
	if cfg.ListTarget && !cfg.SkipCompare {
		log.Printf("Listing target s3://%s\n", path.Join(to.bucket, to.prefix))
		targetObjectList, err = getS3ObjectList(to) // 获取目标 S3 桶中的文件列表
		if err != nil {
			return err
		}
		log.Printf("There are %d objects already in target s3://%s\n", len(targetObjectList), path.Join(to.bucket, to.prefix))
	}

	// Listing multipart uploads ID
	multipartUploadsList, err := getMultipartUploadList(to.svc, to.bucket, to.prefix)
	if err != nil {
		return err
	}
	log.Printf("There are %d multipart uploads ID already in target s3://%s\n", len(multipartUploadsList), path.Join(to.bucket, to.prefix))

	// Walk through local path for uploading
	err = filepath.Walk(from.url, func(thispath string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Failed to access path", thispath, err)
			return err
		}

		// Skip if the path is a directory
		if info.IsDir() {
			return nil
		}
		combinedKey := path.Join(to.prefix, filepath.ToSlash(strings.TrimPrefix(thispath, filepath.Dir(from.url))))
		contentType := mime.TypeByExtension(filepath.Ext(thispath))
		fileInfo := FileInfo{
			ToKey:    combinedKey,
			ToBucket: to.bucket,
			Size:     info.Size(),
			Others:   MetaStruct{ContentType: &contentType},
		}

		// Check file exist on S3 Bucket and get uploadId
		uploadId, err := getUploadId(to.svc, fileInfo, multipartUploadsList, targetObjectList)
		if err != nil {
			return err
		}
		if uploadId == "NEXT" {
			log.Printf("...File already exists. Skipping... s3://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
			return nil
		}

		semFile.Acquire(context.Background(), 1) //从线程信号池中获取，没有线程可用了就阻塞等待
		atomic.AddInt32(&runningGoroutines, 1)   //线程计数
		wg.Add(1)
		go func(thispath string, info os.FileInfo, uploadId string) {
			defer wg.Done()
			defer semFile.Release(1)
			defer atomic.AddInt32(&runningGoroutines, -1)

			fileInfo.File, err = os.Open(thispath)
			if err != nil {
				log.Println("Failed to open file", thispath, err)
				return
			}
			defer fileInfo.File.Close()

			if info.Size() < cfg.ResumableThreshold {
				log.Printf("   Start to upload (< ResumableThreshold): %s to s3://%s, runningGoroutines: %d\n", thispath, path.Join(fileInfo.ToBucket, fileInfo.ToKey), runningGoroutines)
				err := uploadSmall(fileInfo, thispath, info, uploadId)
				if err != nil {
					log.Printf("Failed to uploadSmall: s3://%s, %v\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), err)
					return
				}
				// Success upload
			} else {
				// info.Size() >= ResumableThreshold Use multipart upload for large files resumable upload
				log.Printf("   Start to upload (>= ResumableThreshold): %s to s3://%s, runningGoroutines: %d\n", thispath, path.Join(fileInfo.ToBucket, fileInfo.ToKey), runningGoroutines)
				err := transferMultipart(from, to, uploadId, fileInfo)
				if err != nil {
					log.Printf("Failed to multipartProccess: s3://%s, %v\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), err)
					return
				}
			}
			log.Printf("***Successfully uploaded: s3://%s\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey))
			atomic.AddInt64(&objectCount, 1)
			atomic.AddInt64(&sizeCount, info.Size())
		}(thispath, info, uploadId)
		return nil
	})
	wg.Wait()
	if err != nil {
		log.Println("Failed to walk directory", err)
		return err
	}
	return err
}

func uploadSmall(fileInfo FileInfo, thispath string, info os.FileInfo, uploadId string) error {
	buff, err := io.ReadAll(fileInfo.File)
	if err != nil {
		log.Println("Failed to read file", thispath, err)
		return err
	}

	md5Hash := md5.Sum(buff)
	md5Str := base64.StdEncoding.EncodeToString(md5Hash[:])

	inputUpload := &s3manager.UploadInput{
		Bucket:     aws.String(fileInfo.ToBucket),
		Key:        aws.String(fileInfo.ToKey),
		Body:       bytes.NewReader(buff),
		ContentMD5: aws.String(md5Str),
	}
	if to.storageClass != "" {
		inputUpload.StorageClass = aws.String(to.storageClass)
	}
	if to.ACL != "" {
		inputUpload.ACL = aws.String(to.ACL)
	}
	if *fileInfo.Others.ContentType != "" {
		inputUpload.ContentType = fileInfo.Others.ContentType
	}
	_, err = to.uploader.Upload(inputUpload)
	if err != nil {
		log.Printf("Failed to upload file s3://%s, err: %v\n", path.Join(fileInfo.ToBucket, fileInfo.ToKey), err)
		return err
	}

	return nil
}

func transferMultipart(from, to BInfo, uploadId string, fileInfo FileInfo) error {
	semPart := semaphore.NewWeighted(int64(cfg.NumWorkers * 2)) // 并发量为numWorkers的信号量 for parts
	var partnumberList []PartInfo
	var partnumberListMutex sync.Mutex
	var fileMutex sync.Mutex
	var err error

	if uploadId == "" {
		inputCreate := &s3.CreateMultipartUploadInput{
			Bucket: aws.String(fileInfo.ToBucket),
			Key:    aws.String(fileInfo.ToKey),
		}
		if to.storageClass != "" {
			inputCreate.StorageClass = aws.String(to.storageClass)
		}
		if to.ACL != "" {
			inputCreate.ACL = aws.String(to.ACL)
		}
		if *fileInfo.Others.ContentType != "" {
			inputCreate.ContentType = fileInfo.Others.ContentType
		}
		if cfg.TransferMetadata {
			inputCreate.Metadata = fileInfo.Others.Metadata
			inputCreate.ContentEncoding = fileInfo.Others.ContentEncoding
			inputCreate.CacheControl = fileInfo.Others.CacheControl
			inputCreate.ContentLanguage = fileInfo.Others.ContentLanguage
			inputCreate.ContentDisposition = fileInfo.Others.ContentDisposition
		}
		resp, err := to.svc.CreateMultipartUpload(inputCreate)
		if err != nil {
			log.Println("Failed to create multipart upload", fileInfo.ToBucket, fileInfo.ToKey, err)
			return err
		}
		uploadId = *resp.UploadId
	} else {
		partnumberList, err = checkPartnumberList(to.svc, fileInfo.ToBucket, fileInfo.ToKey, uploadId)
		if err != nil {
			log.Println("Failed to get part number list", fileInfo.ToBucket, fileInfo.ToKey, err)
			_ = partnumberList
			return err
		}
	}

	indexList, chunkSizeAuto := split(fileInfo, chunkSize)

	var wg2 sync.WaitGroup
	for i, offset := range indexList {
		// 检查i是否在partnumberList里面
		found := false
		for _, value := range partnumberList {
			if int64(i+1) == value.PartNumber {
				found = true
				break
			}
		}
		// 如果已经在partnumberList则跳过到下一个i
		if found {
			continue
		}

		// 不在partnumberList，上传该part
		size := chunkSizeAuto
		if offset+chunkSizeAuto > fileInfo.Size { // 如果是最后一个分片则给实际size
			size = fileInfo.Size - offset
		}
		partInfo := PartInfo{
			ToKey:      fileInfo.ToKey,
			ToBucket:   fileInfo.ToBucket,
			FromKey:    fileInfo.FromKey,
			FromBucket: fileInfo.FromBucket,
			PartNumber: int64(i + 1),
			Size:       size,
			Offset:     offset,
			TotalParts: int64(len(indexList)),
		}

		semPart.Acquire(context.Background(), 1) //从线程池中获取，没有线程可用了就阻塞等待
		atomic.AddInt32(&runningGoroutines, 1)   //线程计数
		wg2.Add(1)
		if fileInfo.File != nil {
			go uploadPart(to.svc, partInfo, &wg2, semPart, uploadId, &partnumberList, &partnumberListMutex, fileInfo.File, &fileMutex)
		} else {
			// s3tos3 part
			go transferPart(from, to, partInfo, &wg2, semPart, uploadId, &partnumberList, &partnumberListMutex)
		}

	}
	wg2.Wait()
	if len(indexList) == len(partnumberList) {
		err := completeUpload(to.svc, uploadId, fileInfo.ToBucket, fileInfo.ToKey, &partnumberList)
		if err != nil {
			log.Println("Failed to complete upload", err, fileInfo.ToBucket, fileInfo.ToKey)
			return err
		}
		// Success complete upload
	} else {
		log.Println("Failed to complete upload, len(indexList) != len(partnumberList)", fileInfo.ToBucket, fileInfo.ToKey, len(indexList), len(partnumberList))
		return errors.New("failed to complete upload, len(indexList) != len(partnumberList)")
	}
	return nil
}

func uploadPart(svc *s3.S3, partInfo PartInfo, wg *sync.WaitGroup, sem *semaphore.Weighted, uploadId string, partnumberList *[]PartInfo, partnumberListMutex *sync.Mutex, file *os.File, fileMutex *sync.Mutex) error {
	defer wg.Done()
	defer sem.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)
	log.Printf("-->Uploading s3://%s, part:%d/%d, runningGoroutines: %d\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)

	// 创建一个分片大小的缓冲区
	fileMutex.Lock()
	file.Seek(partInfo.Offset, 0) // 定位到需要读取的文件部分
	buffer := make([]byte, partInfo.Size)
	_, err := io.ReadFull(file, buffer)
	if err != nil {
		log.Println("Failed to read full buffer from file", partInfo.ToBucket, partInfo.ToKey, partInfo.PartNumber, err)
		return err
	}
	fileMutex.Unlock()

	// Upload part S3 API Call
	err = uploadPartAction(buffer, partInfo, svc, uploadId, partnumberList, partnumberListMutex)
	if err != nil {
		return err
	}
	return nil
}

func uploadPartAction(buff []byte, partInfo PartInfo, svc *s3.S3, uploadId string, partnumberList *[]PartInfo, partnumberListMutex *sync.Mutex) error {
	log.Printf("-->Uploading s3://%s, part:%d/%d\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts)
	// 计算分片数据的MD5哈希值
	md5Hash := md5.Sum(buff)
	md5Str := base64.StdEncoding.EncodeToString(md5Hash[:])

	// 上传分片
	result, err := svc.UploadPart(&s3.UploadPartInput{
		Bucket:        aws.String(partInfo.ToBucket),
		Key:           aws.String(partInfo.ToKey),
		PartNumber:    aws.Int64(int64(partInfo.PartNumber)),
		UploadId:      aws.String(uploadId),
		Body:          bytes.NewReader(buff),
		ContentLength: aws.Int64(int64(partInfo.Size)),
		ContentMD5:    aws.String(md5Str),
	})
	if err != nil {
		log.Printf("Failed to upload part s3://%s, part:%d/%d, err: %v\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts, err)
		return nil
	}
	partnumberListMutex.Lock()
	*partnumberList = append(*partnumberList, PartInfo{
		PartNumber: partInfo.PartNumber,
		Etag:       *result.ETag,
	})
	partnumberListMutex.Unlock()
	log.Printf("===Uploaded part s3://%s, part:%d/%d\n", path.Join(partInfo.ToBucket, partInfo.ToKey), partInfo.PartNumber, partInfo.TotalParts)
	return nil
}

func completeUpload(svc *s3.S3, uploadId, bucket, key string, partnumberList *[]PartInfo) error {
	completedParts := []*s3.CompletedPart{}
	var i int64
	for i = 1; i <= int64(len(*partnumberList)); i++ {
		for _, partNumber := range *partnumberList {
			if i == partNumber.PartNumber {
				completedParts = append(completedParts, &s3.CompletedPart{
					ETag:       &partNumber.Etag,
					PartNumber: &partNumber.PartNumber,
				})
				break
			}
		}
	}

	_, err := svc.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	return err
}

package main

import (
	"context"
	"fmt"
	"log"
	"path"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/semaphore"
)

func checkPartnumberList(svc *s3.S3, bucket, key, uploadId string) ([]PartInfo, error) {
	var partNumbers []PartInfo
	var partNumbersPrint []int64
	err := svc.ListPartsPages(&s3.ListPartsInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
	}, func(page *s3.ListPartsOutput, lastPage bool) bool {
		for _, part := range page.Parts {
			partNumbers = append(partNumbers, PartInfo{
				PartNumber: *part.PartNumber,
				Etag:       *part.ETag,
			})
			partNumbersPrint = append(partNumbersPrint, *part.PartNumber)
		}
		return !lastPage
	})
	log.Printf("   Exist %d parts on s3://%s :%v\n", len(partNumbersPrint), path.Join(bucket, key), partNumbersPrint)
	return partNumbers, err
}

func checkFileExistHead(svc *s3.S3, tofileInfo FileInfo, multipartUploadsList []*s3.MultipartUpload) (string, error) {
	exist, err := headFile(svc, tofileInfo)
	if err != nil {
		return "", err
	}
	if exist {
		return "NEXT", nil
	}

	// 找不到文件，或文件不一致，且要重新传的，查是否有MultipartUpload ID
	uploadId, err := checkMultipartUploadId(tofileInfo, multipartUploadsList)

	return uploadId, err
}

func compareMetaStructs(meta1, meta2 MetaStruct) bool {
	if aws.StringValue(meta1.ContentType) != aws.StringValue(meta2.ContentType) ||
		aws.StringValue(meta1.ContentLanguage) != aws.StringValue(meta2.ContentLanguage) ||
		aws.StringValue(meta1.ContentEncoding) != aws.StringValue(meta2.ContentEncoding) ||
		aws.StringValue(meta1.CacheControl) != aws.StringValue(meta2.CacheControl) ||
		aws.StringValue(meta1.ContentDisposition) != aws.StringValue(meta2.ContentDisposition) {
		return false
	}
	if len(meta1.Metadata) != len(meta2.Metadata) {
		return false
	}
	for k, v := range meta1.Metadata {
		if v2, ok := meta2.Metadata[k]; !ok || aws.StringValue(v) != aws.StringValue(v2) {
			return false
		}
	}
	return true
}

func headFile(svc *s3.S3, tofileInfo FileInfo) (bool, error) {
	log.Printf("   Call HEAD to compare target s3://%s\n", path.Join(tofileInfo.ToBucket, tofileInfo.ToKey))
	input := &s3.HeadObjectInput{
		Bucket: aws.String(tofileInfo.ToBucket),
		Key:    aws.String(tofileInfo.ToKey),
	}
	result, err := svc.HeadObject(input)
	// If Not Exist
	if err != nil {
		if aerr, ok := err.(awserr.RequestFailure); ok {
			if aerr.StatusCode() == 404 {
				return false, nil
			}
		}
		return false, err
	}
	// If Exist check size
	if *result.ContentLength == tofileInfo.Size {
		// If Exist and need to check metadata
		if cfg.TransferMetadata {
			log.Printf("   Comparing metadata of target s3://%s\n", path.Join(tofileInfo.ToBucket, tofileInfo.ToKey))
			resultStruct := MetaStruct{
				Metadata:           result.Metadata,
				ContentType:        result.ContentType,
				ContentLanguage:    result.ContentLanguage,
				ContentEncoding:    result.ContentEncoding,
				CacheControl:       result.CacheControl,
				ContentDisposition: result.ContentDisposition,
			}

			if !compareMetaStructs(resultStruct, tofileInfo.Others) {
				log.Printf("...Metadata not match, upload target s3://%s\n", path.Join(tofileInfo.ToBucket, tofileInfo.ToKey))
				return false, nil
			}

		}
		return true, nil
	}
	return false, nil
}

func checkFileExistList(tofileInfo FileInfo, targetObjectList []*s3.Object, multipartUploadsList []*s3.MultipartUpload) (string, error) {
	for _, f := range targetObjectList {
		if *f.Key == tofileInfo.ToKey && *f.Size == tofileInfo.Size {
			return "NEXT", nil // 文件完全相同
		}
	}

	// 找不到文件，或文件不一致，且要重新传的，查是否有MultipartUpload ID
	uploadId, err := checkMultipartUploadId(tofileInfo, multipartUploadsList)
	return uploadId, err
}

func checkMultipartUploadId(tofileInfo FileInfo, multipartUploadsList []*s3.MultipartUpload) (string, error) {
	if tofileInfo.Size < cfg.ResumableThreshold {
		return "", nil // 文件小于ResumableThreshold，不需要分片
	}
	// 查所有相同Key的ID给keyIDList
	var keyIDList []*s3.MultipartUpload
	for _, u := range multipartUploadsList {
		if *u.Key == tofileInfo.ToKey {
			keyIDList = append(keyIDList, u)
		}
	}

	// 如果找不到上传过的MultipartUpload，则从头开始传
	if len(keyIDList) == 0 {
		return "", nil
	}

	// 对同一个Key的不同MultipartUpload ID排序找出时间最晚的值
	var latestUpload *s3.MultipartUpload
	for _, u := range keyIDList {
		if latestUpload == nil || u.Initiated.After(*latestUpload.Initiated) {
			latestUpload = u
		}
	}

	return *latestUpload.UploadId, nil
}

func getUploadId(svc *s3.S3, fileInfo FileInfo, multipartUploadsList []*s3.MultipartUpload, targetObjectList []*s3.Object) (string, error) {
	var uploadId string
	var err error
	if !cfg.SkipCompare { // 设置不做Compare了就不对比目的对象，直接覆盖
		if cfg.TransferMetadata || !cfg.ListTarget { // 要传metadata就必须用Head方式去获取对比；不ListTarget也是逐个Head去对比
			uploadId, err = checkFileExistHead(svc, fileInfo, multipartUploadsList)
			if err != nil {
				log.Printf("failed to checkFileExistHead, %v", err)
				return "", err
			}
		} else if cfg.ListTarget && !cfg.TransferMetadata { // 不要metadata就用list方式去获取对比(如果设置了ListTraget True)
			uploadId, err = checkFileExistList(fileInfo, targetObjectList, multipartUploadsList)
			if err != nil {
				log.Printf("failed to checkFileExistList, %v", err)
				return "", err
			}
		}
	}
	return uploadId, nil
}

func getMultipartUploadList(svc *s3.S3, bucket string, prefix string) ([]*s3.MultipartUpload, error) {
	log.Printf("Listing multipart uploads ID in target s3://%s\n", path.Join(bucket, prefix))
	var uploads []*s3.MultipartUpload
	err := svc.ListMultipartUploadsPages(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListMultipartUploadsOutput, lastPage bool) bool {
		uploads = append(uploads, page.Uploads...)
		return true // return false to stop pagination
	})

	if err != nil {
		return nil, err
	}

	return uploads, nil
}

func getS3ObjectList(b BInfo) ([]*s3.Object, error) {
	log.Printf("Listing s3://%s\n", path.Join(b.bucket, b.prefix))
	var s3Objects []*s3.Object
	var mu sync.Mutex
	var wg sync.WaitGroup
	var sem = semaphore.NewWeighted(int64(cfg.NumWorkers * 4))

	concurrencyListObjects(b.svc, b.bucket, b.prefix, sem, &s3Objects, &mu, &wg)
	wg.Wait()

	return s3Objects, nil
}

func concurrencyListObjects(svc *s3.S3, bucket, prefix string, sem *semaphore.Weighted,
	s3Objects *[]*s3.Object, mu *sync.Mutex, wg *sync.WaitGroup) {
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	err := svc.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			mu.Lock()
			*s3Objects = append(*s3Objects, page.Contents...)
			mu.Unlock()
			// TODO: workMode = listToSQS

			for _, commonPrefix := range page.CommonPrefixes {
				wg.Add(1)
				go func(p string) {
					defer sem.Release(1)
					defer wg.Done()
					sem.Acquire(context.Background(), 1)                           // 要放go func里面，因为上级线程需要继续运行下去
					concurrencyListObjects(svc, bucket, p, sem, s3Objects, mu, wg) //每个Prefix递归并发新线程
				}(*commonPrefix.Prefix)
			}
			return !lastPage
		})
	if err != nil {
		fmt.Printf("Error listing s3 objects: %v", err)
	}
}

func getMetadata(b BInfo, fileInfo *FileInfo) error {
	log.Printf("-->Get metadata s3://%s\n", path.Join(fileInfo.FromBucket, fileInfo.FromKey))
	headResp, err := b.svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(fileInfo.FromBucket),
		Key:    aws.String(fileInfo.FromKey),
	})
	if err != nil {
		log.Printf("failed to get object metadata, %v", err)
	}

	fileInfo.Others = MetaStruct{
		Metadata:           headResp.Metadata,
		ContentType:        headResp.ContentType,
		ContentLanguage:    headResp.ContentLanguage,
		ContentEncoding:    headResp.ContentEncoding,
		CacheControl:       headResp.CacheControl,
		ContentDisposition: headResp.ContentDisposition,
	}
	return nil
}

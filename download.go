package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/semaphore"
)

func downloadPartAction(svc *s3.S3, partInfo PartInfo) ([]byte, error) {
	log.Printf("-->Downloading part s3://%s %d/%d, runningGoroutines: %d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)
	input := &s3.GetObjectInput{
		Bucket: &partInfo.FromBucket,
		Key:    &partInfo.FromKey,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", partInfo.Offset, partInfo.Offset+partInfo.Size-1)),
	}
	if from.requestPayer {
		input.RequestPayer = aws.String("requester")
	}
	resp, err := svc.GetObject(input)
	if err != nil {
		log.Println("Failed to download part", partInfo.FromBucket, partInfo.FromKey, partInfo.PartNumber, err)
		return nil, err
	}
	defer resp.Body.Close()
	buffer, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("Failed to read from response body:", partInfo.FromBucket, partInfo.FromKey, partInfo.PartNumber, err)
		return nil, err
	}
	return buffer, nil
}

func downloadPart(svc *s3.S3, partInfo PartInfo, file *os.File, wg *sync.WaitGroup, semPart *semaphore.Weighted) error {
	defer wg.Done()
	defer semPart.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)

	// Download part S3 API Call
	buffer, err := downloadPartAction(svc, partInfo)
	if err != nil {
		return err
	}
	// Write the part to file
	if _, err := file.WriteAt(buffer, partInfo.Offset); err != nil {
		log.Println("Failed to write to file", partInfo.FromBucket, partInfo.FromKey, partInfo.PartNumber, err)
		return err
	}

	// Record the download part
	recordDownloadPart(partInfo)
	log.Printf("===Downloaded part s3://%s part:%d/%d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts)
	return nil
}

func startDownload(from, to BInfo) error {

	var wg sync.WaitGroup
	semFile := semaphore.NewWeighted(int64(cfg.NumWorkers))     // 并发量为NumWorkers的信号量 for file
	semPart := semaphore.NewWeighted(int64(cfg.NumWorkers * 2)) // 并发量为NumWorkers的信号量 for parts

	err := from.svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(from.bucket),
		Prefix: aws.String(from.prefix),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range page.Contents {
			// Skip if the object is a directory
			if strings.HasSuffix(*item.Key, "/") {
				log.Println("...Skiping directory", *item.Key)
				continue
			}
			var combinedKey string
			if *item.Key != from.prefix {
				// 只带上Prefix以内的目录结构
				combinedKey = strings.TrimPrefix(*item.Key, from.prefix)
			} else {
				// 单个文件的时候存在*item.Key == from.prefix的情况
				combinedKey = filepath.Base(*item.Key)
			}

			localPath := filepath.Join(to.url, combinedKey)
			// Check if file already exists and is the same size
			info, err := os.Stat(localPath)
			if !cfg.SkipCompare {
				if err == nil && info.Size() == *item.Size {
					log.Println("...File exists and same size, skipping", localPath)
					continue
				} else if err != nil && !os.IsNotExist(err) {
					log.Println("Failed to stat file", localPath, err)
					continue
				}
			}

			// Create necessary directories
			thisdir := filepath.Dir(localPath)
			if err := os.MkdirAll(thisdir, 0755); err != nil {
				log.Println("Failed to create directories:", localPath, err)
				continue
			}

			semFile.Acquire(context.Background(), 1) //从线程信号池中获取，没有线程可用了就阻塞等待
			atomic.AddInt32(&runningGoroutines, 1)   //线程计数
			wg.Add(1)
			go func(item *s3.Object) {
				defer wg.Done()
				defer semFile.Release(1)
				defer atomic.AddInt32(&runningGoroutines, -1)

				if *item.Size < cfg.ResumableThreshold {
					log.Println("   Start to download (<ResumableThreshold):", localPath, "runningGoroutines:", runningGoroutines)
					file, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Println("Failed to create file", localPath, err)
						return
					}
					defer file.Close()

					// Use s3manager for small files 不做断点续传
					// s3manager.Downloader 实例会并发地下载 S3 对象的多个部分，每个部分都通过一个单独的 goroutine 下载
					input := &s3.GetObjectInput{
						Bucket: aws.String(from.bucket),
						Key:    aws.String(*item.Key),
					}
					if from.requestPayer {
						input.RequestPayer = aws.String("requester")
					}
					_, err = from.downloader.Download(file, input)
					if err != nil {
						log.Println("Failed to download file", localPath, err)
						return
					}
				} else {
					// Use multipart resumable download for large files
					log.Println("   Start to download (>=ResumableThreshold):", localPath, "runningGoroutines:", runningGoroutines)
					multipart_download_finished := false
					file, err := os.OpenFile(localPath+".s3tmp", os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						log.Println("Failed to create s3tmp file", localPath, err)
						return
					}
					defer func() {
						file.Close() // 确保在 file close之后再执行rename
						if multipart_download_finished {
							// 检查文件是否存在, 如果文件存在，重命名为 localPath
							if _, err := os.Stat(localPath + ".s3tmp"); err == nil {
								//
								if err := os.Rename(localPath+".s3tmp", localPath); err != nil {
									log.Println(err, localPath)
								}
							} else if !os.IsNotExist(err) {
								log.Println(err, localPath)
							} // 如果文件不存在，跳过
						}
					}()

					fileInfo := FileInfo{
						FromKey:    *item.Key,
						FromBucket: from.bucket,
						Size:       *item.Size,
						File:       file,
					}
					indexList, chunkSize := split(fileInfo, chunkSize)
					partnumberList, _ := getDownloadedParts(fileInfo)
					if len(partnumberList) != 0 {
						log.Printf("Exist %d/%d parts on local path: %s, %v\n", len(partnumberList), len(indexList), localPath+".s3tmp", partnumberList)
					}
					var wg2 sync.WaitGroup
					for i, offset := range indexList {
						if !contains(partnumberList, i+1) {
							size := chunkSize
							if offset+chunkSize > fileInfo.Size {
								size = fileInfo.Size - offset
							}
							partInfo := PartInfo{
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
							go downloadPart(from.svc, partInfo, fileInfo.File, &wg2, semPart)
						}
					}
					// Clean up download part records, statstic counts
					wg2.Wait()
					deleteDownloadParts(fileInfo)
					multipart_download_finished = true
				}
				log.Println("***Successfully downloaded:", localPath)
				atomic.AddInt64(&objectCount, 1)
				atomic.AddInt64(&sizeCount, *item.Size)
			}(item)
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

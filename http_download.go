package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

func startHttpDownload(from, to BInfo) error {
	var err error
	semPart := semaphore.NewWeighted(int64(cfg.NumWorkers)) // 并发量为NumWorkers的信号量 for parts

	URL, err := url.Parse(from.url)
	if err != nil {
		log.Fatalf("Invalid HTTP URL: %s, %v\n", from.url, err)
		return err
	}
	from.bucket = strings.Split(URL.Host, ".")[0]
	fullPrefix := strings.TrimSuffix(strings.TrimPrefix(URL.Path, "/"), "/")
	fileName := filepath.Base(fullPrefix)
	localPath := filepath.Join(to.url, fileName)
	log.Println("   Start to https download", localPath)
	multipart_download_finished := false

	// Create necessary directories
	thisdir := filepath.Dir(localPath)
	if err := os.MkdirAll(thisdir, 0755); err != nil {
		log.Println("Failed to create directories:", localPath, err)
		return err
	}

	file, err := os.OpenFile(localPath+".s3tmp", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Failed to create s3tmp file", localPath, err)
		return err
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

	// Get the file size
	req, err := http.NewRequest("GET", from.url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status code while GET file size: %d", resp.StatusCode)
	}
	fileSizeStr := resp.Header.Get("Content-Range")
	if fileSizeStr == "" {
		return fmt.Errorf("missing Content-Range header while GET file size")
	}
	parts := strings.Split(fileSizeStr, "/")
	if len(parts) != 2 {
		return fmt.Errorf("invalid Content-Range header format while GET file size")
	}

	fileSize, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return err
	}

	// list parts numbers
	fileInfo := FileInfo{
		FromKey:    fullPrefix,
		FromBucket: from.bucket,
		Size:       fileSize,
		File:       file,
	}
	indexList, chunkSizeAuto := split(fileInfo, cfg.ChunkSize)
	partnumberList, _ := getDownloadedParts(fileInfo)
	if len(partnumberList) != 0 {
		log.Printf("Exist %d/%d parts on local path: %s, %v\n", len(partnumberList), len(indexList), localPath+".s3tmp", partnumberList)
	}

	// Follow indexList to download parts
	var wg2 sync.WaitGroup
	for i, offset := range indexList {
		if !contains(partnumberList, i+1) {
			size := chunkSizeAuto
			if offset+chunkSizeAuto > fileInfo.Size {
				size = fileInfo.Size - offset
			}
			partInfo := PartInfo{
				FromBucket: from.bucket,
				FromKey:    fullPrefix,
				URL:        from.url,
				PartNumber: int64(i + 1),
				Size:       size,
				Offset:     offset,
				TotalParts: int64(len(indexList)),
			}

			semPart.Acquire(context.Background(), 1) //从线程池中获取，没有线程可用了就阻塞等待
			atomic.AddInt32(&runningGoroutines, 1)   //线程计数
			wg2.Add(1)
			go downloadHttpChunk(partInfo, fileInfo.File, &wg2, semPart)
		}
	}
	// Clean up download part records, statstic counts
	wg2.Wait()
	deleteDownloadParts(fileInfo)
	multipart_download_finished = true

	return nil
}

func downloadHttpChunk(partInfo PartInfo, file *os.File, wg *sync.WaitGroup, semPart *semaphore.Weighted) error {
	defer wg.Done()
	defer semPart.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)

	// Download part HTTP API Call
	buffer, err := downloadHttpChunkAction(partInfo)
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

func downloadHttpChunkAction(partInfo PartInfo) ([]byte, error) {
	log.Printf("-->Downloading part s3://%s %d/%d, runningGoroutines: %d\n", path.Join(partInfo.FromBucket, partInfo.FromKey), partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)

	req, err := http.NewRequest("GET", from.url, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		return nil, err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", partInfo.Offset, partInfo.Offset+partInfo.Size-1))

	retryRoundTripper := &RetryRoundTripper{
		Proxied: http.DefaultTransport,
		Retries: 3,               // Set the desired number of retries
		Delay:   time.Second * 5, // Set the desired delay between retries
	}
	client := &http.Client{
		Transport: retryRoundTripper,
	}

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error downloading chunk:", err)
		return nil, err
	}
	defer resp.Body.Close()

	buffer := make([]byte, partInfo.Size)
	_, err = io.ReadFull(resp.Body, buffer)
	if err != nil {
		fmt.Println("Error reading chunk:", err)
		return nil, err
	}

	return buffer, nil
}

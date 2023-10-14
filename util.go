package main

import (
	"fmt"
	"log"
	"math"
	"time"
)

func split(fileInfo FileInfo, chunkSize int64) (indexList []int64, actualChunkSize int64) {
	partNumber := int64(1)
	indexList = []int64{0}

	if int64(math.Ceil(float64(fileInfo.Size)/float64(chunkSize))) > 10000 {
		chunkSize = fileInfo.Size/10000 + 1024 // 对于大于10000分片的大文件，自动调整Chunksize
	}

	for chunkSize*partNumber < fileInfo.Size { // 如果刚好是"="，则无需再分下一part，所以这里不能用"<="
		indexList = append(indexList, chunkSize*partNumber)
		partNumber += 1
	}
	return indexList, chunkSize
}

func withRetries(fn RetryFunc) error {
	var err error
	for i := 0; i < cfg.MaxRetries; i++ {
		err = fn()
		if err == nil {
			break
		}
		log.Println("Failed to execute function: ", err, ". Retrying...")
		time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
	}
	return err
}

func contains(s []int, e int) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

func ByteCountSI(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%dBytes", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cBytes", float64(b)/float64(div), "KMGTPE"[exp])
}

// 用GO轻量多线程并发列出S3 Bucket的对象数量和总Size，生成Key列表，相比单线程快数十倍，适合用于List大型S3 Bucket。例如一个3千万对象的Bucket单线程需要列90分钟，该代码只需要1分16秒（cpu16核, 64线程）
// 核心原理：列出当前目录中的一层子目录（prefix），然后递归并发多线程分别去列每一个子目录，每个二级子目录再并发多线程去列，并用numWorkers控制并发总数量；使用缓冲区来写入文件，列出的所有Key列表输出到output.txt中；
// Usage: 在s3-list.go文件所在的目录下运行
// sudo yum install -y go
// go mod init s3-list
// go mod tidy
// go run s3-list.go -bucket=commoncrawl -prefix=cc -numWorkers=64 -output=output.txt
// 不写 -prefix= 参数则默认为空，即List整个Bucket
// 默认并发 -numWorkers=64
// 如果 -output=“” 则不写output文件，只统计数量和Size，默认路径output.txt

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"golang.org/x/sync/semaphore"
)

var (
	s3Bucket          string
	s3Prefix          string
	objectCount       int64          //总object数量
	sizeCount         int64          //总size
	runningGoroutines int32          // 当前正在运行的 goroutine 的数量
	numWorkers        int            // 控制 goroutine 总量
	wg                sync.WaitGroup // 用以等完所有goroutine
	output            string         // 输出文件名
	outFile           *os.File
	writer            *bufio.Writer //写入缓冲区
	mu                sync.Mutex    //写入锁
)

func init() {
	flag.StringVar(&s3Bucket, "bucket", "commoncrawl", "The S3 bucket to list, default is demo bucket s3://commoncrawl.")
	flag.StringVar(&s3Prefix, "prefix", "", "The S3 bucket prefix to list")
	flag.StringVar(&output, "output", "output.txt", "S3 Keys list file path")
	flag.IntVar(&numWorkers, "numWorkers", 64, "The number of workers to concurrently list objects")

	flag.Parse()

	if len(s3Prefix) > 0 && s3Prefix[0] == '/' {
		s3Prefix = s3Prefix[1:]
	}
	fmt.Printf("List and count: s3://%s/%s\n", s3Bucket, s3Prefix)
}

func main() {
	startTime := time.Now()

	// 创建一个新的文件用于输出
	if output != "" {
		var err error
		outFile, err = os.Create(output)
		if err != nil {
			fmt.Printf("Error creating output file: %v", err)
			return
		}
	}
	defer outFile.Close()             // 确保在程序结束时关闭文件
	writer = bufio.NewWriter(outFile) // 创建一个缓冲写入器
	defer writer.Flush()              // 确保在程序结束时刷新缓冲区

	svc := getS3sess()
	if svc == nil {
		fmt.Println("Failed to create S3 session. Exiting.")
		return
	}
	sem := semaphore.NewWeighted(int64(numWorkers)) // 并发量为numWorkers的信号量
	listObjects(svc, s3Prefix, sem)
	wg.Wait()

	fmt.Printf("\n\nTotalObjects:%d, TotalSizes:%s(%d), s3://%s/%s, OutputFile:%s\nThe program ran for %v\n", objectCount, ByteCountSI(sizeCount), sizeCount, s3Bucket, s3Prefix, output, time.Now().Sub(startTime))
}

func getS3sess() *s3.S3 {
	// Call GetBucketLocation to determine the bucket's region.
	tempS3 := s3.New(session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // Default to us-east-1
	})))
	result, err := tempS3.GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(s3Bucket),
	})
	if err != nil {
		fmt.Println("Failed to get bucket location,", err)
		return nil
	}
	var region string
	if result.LocationConstraint == nil {
		region = "us-east-1" // Default bucket's region is us-east-1
	} else {
		region = aws.StringValue(result.LocationConstraint)
	}
	fmt.Printf("S3 Bucket Region: %s \n", region)
	sess := session.Must(session.NewSession(&aws.Config{
		Region:     aws.String(region),
		MaxRetries: aws.Int(3),
	}))
	svc := s3.New(sess)
	return svc
}

func listObjects(svc *s3.S3, prefix string, sem *semaphore.Weighted) {
	params := &s3.ListObjectsV2Input{
		Bucket:    aws.String(s3Bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String("/"),
	}

	err := svc.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			var pageKeys []string
			for _, content := range page.Contents {
				atomic.AddInt64(&objectCount, 1)
				atomic.AddInt64(&sizeCount, *content.Size)
				pageKeys = append(pageKeys, *content.Key)
			}
			if len(pageKeys) > 0 && output != "" {
				mu.Lock()                                                         // 加锁
				_, err := writer.WriteString(strings.Join(pageKeys, "\n") + "\n") // 使用缓冲写入器写key到文件
				mu.Unlock()                                                       // 解锁
				if err != nil {
					fmt.Printf("Error writing to output file: %v", err)
				}
			}

			for _, commonPrefix := range page.CommonPrefixes {
				wg.Add(1)
				go func(p string) {
					defer wg.Done()
					sem.Acquire(context.Background(), 1)   //从线程池中获取，没有线程可用了就阻塞等待
					atomic.AddInt32(&runningGoroutines, 1) //线程计数
					listObjects(svc, p, sem)               //每个Prefix递归并发新线程
					sem.Release(1)
					atomic.AddInt32(&runningGoroutines, -1)
				}(*commonPrefix.Prefix)
			}

			fmt.Printf("Objects: %d, Sizes: %s, Concurrent: %d\n", objectCount, ByteCountSI(sizeCount), runningGoroutines)

			// 返回 true 继续分页，返回 false 停止分页
			return !lastPage
		})

	if err != nil {
		fmt.Printf("Error listing s3 objects: %v", err)
	}
}

func ByteCountSI(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(b)/float64(div), "kMGTPE"[exp])
}

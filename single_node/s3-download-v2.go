// 多线程并发下载S3，支持断点续传，边List边下载
// Usage: 在s3-download.go文件所在的目录下运行
// sudo yum install -y go
// go mod init s3-list
// 如果在中国区，则通过go代理来下载依赖包，所以需要再运行一句：go env -w GOPROXY=https://goproxy.cn,direct
// go mod tidy
// go build s3-download-v2.go
// ./s3-download-v2 -bucket=your_bucket -prefix=your_prefix -downloadDir=your_download_directory -profileName=your_aws_profile_name
// 使用 ./s3-download-v2 -h 获取更多帮助信息

package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sync/semaphore"
)

const (
	chunkSize                  = 10 * 1024 * 1024   // Multipart 分片大小
	multipartDownloadThreshold = 50 * 1024 * 1024   // 走 Multipart 流程的门槛，小于该值则直接下载，不用Multipart，效率更高
	dbPath                     = "./s3-download.db" // 自动创建已经下载的分片状态记录数据库
	maxRetries                 = 10                 // 最大重试次数
	httpTimeout                = 180                // S3 http 超时时间(秒)
)

var (
	bucket            string
	prefix            string
	objectCount       int64  //总object数量
	sizeCount         int64  //总size
	runningGoroutines int32  // 当前正在运行的 goroutine 的数量
	numWorkers        int    // 控制 goroutine 总量
	downloadDir       string //your_download_directory
	profileName       string //aws_profile_name in ~/.aws/credentials
)

type FileInfo struct {
	Key    string
	Bucket string
	Size   int64
}

type PartInfo struct {
	Key        string
	Bucket     string
	Size       int64
	PartNumber int
	TotalParts int
	Offset     int64
}

type RetryFunc func() error

func init() {
	flag.StringVar(&bucket, "bucket", "", "The S3 bucket to download")
	flag.StringVar(&prefix, "prefix", "", "The S3 bucket prefix to download, default is all prefix in the bucket")
	flag.StringVar(&downloadDir, "downloadDir", "./", "Local file path")
	flag.IntVar(&numWorkers, "numWorkers", 4, "Concurrent files and concurrent parts, actual goroutines are up to numWorkers^2, recommend numWorkers <= vCPU number")
	flag.StringVar(&profileName, "profileName", "default", "The AWS profile in ~/.aws/credentials")

	flag.Parse()

	if len(prefix) > 0 && prefix[0] == '/' {
		prefix = prefix[1:]
	}
	fmt.Printf("Transfering s3://%s/%s\n", bucket, prefix)
}

func main() {
	startTime := time.Now()

	sess := getS3sess(bucket, profileName)
	if sess == nil {
		os.Exit(0)
	}
	svc := s3.New(sess)

	var wg sync.WaitGroup
	semFile := semaphore.NewWeighted(int64(numWorkers)) // 并发量为numWorkers的信号量 for file
	semPart := semaphore.NewWeighted(int64(numWorkers)) // 并发量为numWorkers的信号量 for parts

	err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, item := range page.Contents {
			semFile.Acquire(context.Background(), 1) //从线程信号池中获取，没有线程可用了就阻塞等待
			atomic.AddInt32(&runningGoroutines, 1)   //线程计数
			wg.Add(1)
			go func(item *s3.Object) {
				defer wg.Done()
				defer semFile.Release(1)
				defer atomic.AddInt32(&runningGoroutines, -1)

				// Create necessary directories
				localPath := filepath.Join(downloadDir, *item.Key)
				mydir := filepath.Dir(localPath)
				if err := os.MkdirAll(mydir, 0755); err != nil {
					fmt.Println("Failed to create directories:", localPath, err)
					return
				}

				// Skip if the object is a directory
				if strings.HasSuffix(*item.Key, "/") {
					fmt.Println("Skipping directory", *item.Key)
					return
				}

				// Check if file already exists and is the same size
				info, err := os.Stat(localPath)
				if err == nil && info.Size() == *item.Size {
					fmt.Println("...File exists and same size, skipping", localPath)
					return
				} else if err != nil && !os.IsNotExist(err) {
					fmt.Println("Failed to stat file", localPath, err)
					return
				}

				fmt.Println("Start to download:", localPath, "CurrentThreads:", runningGoroutines)

				if *item.Size < multipartDownloadThreshold {
					file, err := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						fmt.Println("Failed to create file", localPath, err)
						return
					}
					defer file.Close()

					// Use s3manager for small files
					downloader := s3manager.NewDownloader(sess)
					_, err = downloader.Download(file,
						&s3.GetObjectInput{
							Bucket: aws.String(bucket),
							Key:    aws.String(*item.Key),
						})
					if err != nil {
						fmt.Println("Failed to download file", localPath, err)
						return
					}
				} else {
					// Use multipart download for large files
					multipart_download_finished := false
					file, err := os.OpenFile(localPath+".s3tmp", os.O_CREATE|os.O_WRONLY, 0644)
					if err != nil {
						fmt.Println("Failed to create s3tmp file", localPath, err)
						return
					}
					defer func() {
						file.Close() // 确保在 file close之后再执行rename
						if multipart_download_finished {
							// 检查文件是否存在, 如果文件存在，重命名为 localPath
							if _, err := os.Stat(localPath + ".s3tmp"); err == nil {
								//
								if err := os.Rename(localPath+".s3tmp", localPath); err != nil {
									fmt.Println(err, localPath)
								}
							} else if !os.IsNotExist(err) {
								fmt.Println(err, localPath)
							} // 如果文件不存在，跳过
						}
					}()

					fileInfo := FileInfo{
						Key:    *item.Key,
						Bucket: bucket,
						Size:   *item.Size,
					}
					indexList, chunkSize := split(fileInfo, chunkSize)
					partnumberList, _ := getDownloadedParts(fileInfo)
					var wg2 sync.WaitGroup
					for i, offset := range indexList {
						if !contains(partnumberList, i+1) {
							size := chunkSize
							if offset+chunkSize > fileInfo.Size {
								size = fileInfo.Size - offset
							}
							partInfo := PartInfo{
								Key:        fileInfo.Key,
								Bucket:     fileInfo.Bucket,
								PartNumber: i + 1,
								Size:       size,
								Offset:     offset,
								TotalParts: len(indexList),
							}

							semPart.Acquire(context.Background(), 1) //从线程池中获取，没有线程可用了就阻塞等待
							atomic.AddInt32(&runningGoroutines, 1)   //线程计数
							wg2.Add(1)
							go downloadPart(svc, partInfo, file, &wg2, semPart)
						}
					}
					// Clean up download part records, statstic counts
					wg2.Wait()
					deleteDownloadParts(fileInfo)
					multipart_download_finished = true
				}
				fmt.Println("***Successfully downloaded:", localPath)
				atomic.AddInt64(&objectCount, 1)
				atomic.AddInt64(&sizeCount, *item.Size)
			}(item)
		}
		return true
	})
	if err != nil {
		fmt.Println("Failed to list objects", err)
		return
	}
	wg.Wait()

	fmt.Printf("\n\nTotalObjects:%d, TotalSizes:%s(%d), s3://%s/%s, downloadDir:%s\nThe program ran for %v\n", objectCount, ByteCountSI(sizeCount), sizeCount, bucket, prefix, downloadDir, time.Now().Sub(startTime))
}

func getS3sess(bucket string, profileName string) *session.Session {
	// Call GetBucketLocation to determine the bucket's region.
	tempS3sess, err := session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String("us-east-1"), MaxRetries: aws.Int(maxRetries)}, // Default to us-east-1
		Profile:           profileName,
		SharedConfigState: session.SharedConfigEnable, //~/.aws/目录下，文件名为config或者credentials
	})
	if err != nil {
		fmt.Println("Failed to create session with profile:", profileName, err)
		return nil
	}

	result, err := s3.New(tempS3sess).GetBucketLocation(&s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		fmt.Println("Failed to get bucket location:", bucket, err)
		return nil
	}
	var region string
	if result.LocationConstraint == nil {
		region = "us-east-1" // Default bucket's region is us-east-1
	} else {
		region = aws.StringValue(result.LocationConstraint)
	}

	fmt.Printf("S3 Bucket Region: %s \n", region)

	// 创建具有超时的 http 客户端cd ~
	client := &http.Client{Timeout: time.Second * httpTimeout}
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:     aws.String(region),
			MaxRetries: aws.Int(maxRetries),
			HTTPClient: client, // 使用自定义的 http 客户端
		},
		Profile:           profileName,
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		fmt.Println("Failed to create session with profile:", profileName, err)
		return nil
	}
	return sess
}

func downloadPart(svc *s3.S3, partInfo PartInfo, file *os.File, wg *sync.WaitGroup, semPart *semaphore.Weighted) {
	defer wg.Done()
	defer semPart.Release(1)
	defer atomic.AddInt32(&runningGoroutines, -1)

	fmt.Printf("-->Downloading part:%s %d/%d, CurrentThreads: %d\n", partInfo.Key, partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)
	input := &s3.GetObjectInput{
		Bucket: &partInfo.Bucket,
		Key:    &partInfo.Key,
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", partInfo.Offset, partInfo.Offset+partInfo.Size-1)),
	}
	resp, err := svc.GetObject(input)
	if err != nil {
		fmt.Println("Failed to download part", partInfo.Key, partInfo.PartNumber, err)
		return
	}
	defer resp.Body.Close()

	// Write the part to file
	if _, err := file.WriteAt(readAll(resp.Body), partInfo.Offset); err != nil {
		fmt.Println("Failed to write to file", partInfo.Key, partInfo.PartNumber, err)
		return
	}

	// Record the download part
	recordDownloadPart(partInfo)
	fmt.Printf("-->Finished part:%s %d/%d, CurrentThreads: %d\n", partInfo.Key, partInfo.PartNumber, partInfo.TotalParts, runningGoroutines)
}

func readAll(r io.Reader) []byte {
	buf := new(strings.Builder)
	_, _ = io.Copy(buf, r)
	return []byte(buf.String())
}

func split(fileInfo FileInfo, chunkSize int64) (indexList []int64, actualChunkSize int64) {
	partNumber := int64(1)
	indexList = []int64{0}

	if int64(math.Ceil(float64(fileInfo.Size)/float64(chunkSize)))+1 > 10000 {
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
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			break
		}
		fmt.Println("Failed to execute function: ", err, ". Retrying...")
		time.Sleep(time.Duration(int64(math.Pow(2, float64(i)))) * time.Second)
	}
	return err
}

func getDatabase() (*sql.DB, error) {
	var database *sql.DB
	var err error
	err = withRetries(func() error {
		database, err = sql.Open("sqlite3", dbPath)
		if err != nil {
			fmt.Println("Failed to connect to sqlite3", err)
			return err
		}
		statement, err := database.Prepare("CREATE TABLE IF NOT EXISTS download (ID TEXT PRIMARY KEY, key TEXT, bucket TEXT, part INT)")
		if err != nil {
			fmt.Println("Failed to prepare getDatabase statement: ", err)
			return err
		}
		_, err = statement.Exec()
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return database, nil
}

func recordDownloadPart(partInfo PartInfo) {
	err := withRetries(func() error {
		database, err := getDatabase()
		if err != nil {
			fmt.Println("Failed to get sqlite3 database", err)
			return err
		}
		defer database.Close()
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		statement, err := database.Prepare("INSERT INTO download (ID, key, bucket, part) VALUES (?, ?, ?, ?)")
		if err != nil {
			fmt.Println("Failed to prepare recordDownloadPart statement: ", err)
			return err
		}
		_, execErr := statement.Exec(uuid, partInfo.Key, partInfo.Bucket, partInfo.PartNumber)
		if execErr != nil {
			fmt.Println("Failed to execute recordDownloadPart statement: ", execErr, ". Retrying...")
		}
		return execErr
	})
	if err != nil {
		fmt.Println("Failed to execute recordDownloadPart statement after retries: ", err)
		return
	}
}

func getDownloadedParts(fileInfo FileInfo) ([]int, error) {
	var partnumberList []int
	err := withRetries(func() error {
		database, err := getDatabase()
		if err != nil {
			fmt.Println("Failed to get sqlite3 database", err)
			return err
		}
		defer database.Close()
		partnumberList = []int{}
		rows, err := database.Query("SELECT part FROM download WHERE key = ? AND bucket = ?", fileInfo.Key, fileInfo.Bucket)
		if err != nil {
			fmt.Println("Failed to prepare getDownloadedParts statement: ", err)
			return err
		}
		defer rows.Close()
		var part int
		for rows.Next() {
			err := rows.Scan(&part)
			if err != nil {
				fmt.Println("Failed to scan row: ", err)
				return err
			}
			partnumberList = append(partnumberList, part)
		}
		if err = rows.Err(); err != nil {
			fmt.Println("Rows iteration error: ", err)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return partnumberList, nil
}

func deleteDownloadParts(fileInfo FileInfo) error {
	err := withRetries(func() error {
		database, err := getDatabase()
		if err != nil {
			fmt.Println("Failed to get sqlite3 database: ", err)
			return err
		}
		defer database.Close()
		statement, err := database.Prepare("DELETE FROM download WHERE key = ? AND bucket = ?")
		if err != nil {
			fmt.Println("Failed to prepare deleteDownloadParts statement: ", err)
			return err
		}
		_, err = statement.Exec(fileInfo.Key, fileInfo.Bucket)
		if err != nil {
			fmt.Println("Failed to execute deleteDownloadParts statement: ", err)
			return err
		}
		return nil
	})
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

package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
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

func getIgnoreList() []*string {
	log.Printf("Checking ignore files list in %s\n", cfg.IgnoreListPath)
	ignoreList := []*string{}

	_, err := os.Stat(cfg.IgnoreListPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("No ignore list in path %s\n", cfg.IgnoreListPath)
		} else {
			log.Println(err)
		}
	} else {
		file, err := os.Open(cfg.IgnoreListPath)
		if err != nil {
			log.Println(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			prefix := strings.TrimPrefix(scanner.Text(), "/")
			ignoreList = append(ignoreList, &prefix)
		}
		if err := scanner.Err(); err != nil {
			log.Println(err)
		}
		log.Printf("Found ignore files list with prefix Length: %d, in %s", len(ignoreList), cfg.IgnoreListPath)
	}
	return ignoreList
}

func isIgnored(key string, ignoreList []*string) bool {
	for _, prefix := range ignoreList {
		if strings.HasPrefix(key, *prefix) {
			return true
		}
	}
	return false
}

func getDatabase() (*sql.DB, error) {
	var database *sql.DB
	var err error
	err = withRetries(func() error {
		database, err = sql.Open("sqlite3", cfg.DBPath)
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
		_, execErr := statement.Exec(uuid, partInfo.FromKey, partInfo.FromBucket, partInfo.PartNumber)
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
		rows, err := database.Query("SELECT part FROM download WHERE key = ? AND bucket = ? ORDER BY part ASC", fileInfo.FromKey, fileInfo.FromBucket)
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
		_, err = statement.Exec(fileInfo.FromKey, fileInfo.FromBucket)
		if err != nil {
			fmt.Println("Failed to execute deleteDownloadParts statement: ", err)
			return err
		}
		return nil
	})
	return err
}

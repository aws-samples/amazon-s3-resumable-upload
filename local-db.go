package main

import (
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

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

package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"
)

func main() {
	fileOpen, err := os.Open("news.csv")
	if err != nil {
		log.Fatal("Tidak dapat membuka file:", err)
	}
	defer fileOpen.Close()

	// Create a new csv file just detik
	fileCreate, err := os.Create("detiknews.csv")
	if err != nil {
		log.Fatal("Tidak dapat membuat file:", err)
	}
	defer fileCreate.Close()

	csvCleaning := NewCsvCleaning()
	err = csvCleaning.ProcessFile(fileOpen, fileCreate)
	if err != nil {
		log.Fatal("Gagal memproses file:", err)
	}

	log.Println("File berhasil dibuat")
}

const (
	batchSize   = 1000
	workerCount = 100
)

type CsvCleaning struct {
	wg       sync.WaitGroup
	jobsChan chan [2]string
}

func NewCsvCleaning() *CsvCleaning {
	return &CsvCleaning{
		jobsChan: make(chan [2]string, batchSize),
	}
}

func (c *CsvCleaning) ProcessFile(readCSV *os.File, createdCSV *os.File) error {
	reader := csv.NewReader(readCSV)
	//skip header
	_, err := reader.Read()
	if err != nil {
		return err
	}
	writer := csv.NewWriter(createdCSV)
	defer writer.Flush()
	writer.Write([]string{"Judul", "Link"})
	// Start workers
	for i := 0; i < workerCount; i++ {
		go c.worker(writer, i)
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// Skip records where the first column is not "detik news"
		if len(record) > 0 && record[0] != "detik news" {
			continue
		}

		data := [2]string{}
		for i, v := range record {
			if i == 2 || i == 3 {
				data[i-2] = v // Adjust index to fit into the data slice
			}
		}

		c.wg.Add(1)
		c.jobsChan <- data
	}

	close(c.jobsChan)
	c.wg.Wait()

	return nil
}

func (c *CsvCleaning) worker(writer *csv.Writer, id int) {
	counter := 0
	for data := range c.jobsChan {
		writer.Write(data[:])
		log.Printf("Title: %s, Link: %s\n berhasil dibuat", data[0], data[1])
		counter++
		c.wg.Done()
	}
}

package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
)

// OffsetWriter takes a topic, partition and offset and saves the offest
type OffsetWriter interface {
	WriteOffset(topic string, partition int32, offset int64) error
	ReadOffset(topic string, partition int32) (int64, error)
}

// NewCSVOffsetWriter takes a filepath and returns a CSVOffsetWriter
func NewCSVOffsetWriter(filepath string) (*CSVOffsetWriter, error) {
	return &CSVOffsetWriter{sync.Mutex{}, filepath}, nil
}

// CSVOffsetWriter stores the data as a CSV
type CSVOffsetWriter struct {
	mu       sync.Mutex
	filepath string
}

// ReadOffset looks up the offest given a topic and partition
func (ow *CSVOffsetWriter) ReadOffset(topic string, partition int32) (int64, error) {
	ow.mu.Lock()
	m, err := ow.read()
	ow.mu.Unlock()
	if err != nil {
		return 0, err
	}

	if offset, exists := m[topic][partition]; exists {
		return offset, nil
	}

	return 0, fmt.Errorf("no offset saved")
}

// WriteOffset implements OffsetWriter
func (ow *CSVOffsetWriter) WriteOffset(topic string, partition int32, offset int64) error {
	ow.mu.Lock()
	defer ow.mu.Unlock()

	m, err := ow.read()
	if err != nil {
		return err
	}

	if _, exists := m[topic]; !exists {
		m[topic] = make(map[int32]int64)
	}

	m[topic][partition] = offset

	if err := ow.write(m); err != nil {
		return err
	}

	return nil
}

func (ow *CSVOffsetWriter) read() (map[string]map[int32]int64, error) {
	m := make(map[string]map[int32]int64)

	file, err := os.OpenFile(ow.filepath, os.O_RDWR|os.O_CREATE, 0775)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	for _, row := range records {
		// row => topic, partition, offset

		topic := row[0]
		partition, err := strconv.Atoi(row[1])
		if err != nil {
			return nil, err
		}

		offset, err := strconv.Atoi(row[2])
		if err != nil {
			return nil, err
		}

		if _, exists := m[topic]; !exists {
			m[topic] = make(map[int32]int64)
		}

		m[topic][int32(partition)] = int64(offset)
	}

	return m, nil
}

func (ow *CSVOffsetWriter) write(m map[string]map[int32]int64) error {
	var records [][]string

	for topic, sm := range m {
		for partition, offest := range sm {
			records = append(records, []string{topic, strconv.Itoa(int(partition)), strconv.Itoa(int(offest))})
		}
	}

	file, err := os.OpenFile(ow.filepath, os.O_RDWR|os.O_CREATE, 0775)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	if err := writer.WriteAll(records); err != nil {
		return err
	}

	return nil
}

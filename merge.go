package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	totalReadBufferSize = 1024 * 1024 * 500
	writeBufferSize     = 1024 * 1024 * 100
	includeTimestamp    = true
	includeOutputName   = true
)

var (
	// TODO: Buffer is used only for output and not shared between goroutines. So we could remove the sync.Pool and use a slice directly as a buffer.
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 4096) // typical line size
		},
	}
)

func MergeLogs(basePath string) error {
	// Find files to process
	files, err := ListFiles(basePath)
	if err != nil {
		return fmt.Errorf("failed to detect files: %v", err)
	}
	return mergeFiles(basePath, files)
}

func mergeFiles(basePath string, files []string) error {
	var (
		outputNames = make(map[string]string)
		scanners    = make(map[string]*bufio.Scanner)
		fileHandles = make(map[string]*os.File)
	)
	openFilesDuration, err := MeasureDuration(func() error {
		for _, file := range files {
			relativePath, err := filepath.Rel(basePath, file)
			if err != nil {
				return fmt.Errorf("failed to calculate relative path for file %s: %v", file, err)
			}
			f, err := os.Open(file)
			if err != nil {
				return fmt.Errorf("failed to open file %s: %v", file, err)
			}
			outputNames[file] = relativePath
			scanners[file] = bufio.NewScanner(bufio.NewReaderSize(f, totalReadBufferSize/len(files)))
			fileHandles[file] = f
		}
		return nil
	})
	OpenFilesDuration = int64(openFilesDuration)
	if err != nil {
		return err
	}

	defer func() {
		for _, f := range fileHandles {
			_ = f.Close()
		}
	}()

	// TODO: Consider simplifying metric collection codes like: err := AddMetric(&MetricName, func() error { ... })
	mergeScannersDuration, err := MeasureDuration(func() error {
		MergeScanners(files, outputNames, scanners)
		return nil
	})
	MergeScannersDuration = int64(mergeScannersDuration)
	return nil
}

func MergeScanners(sourceNames []string, outputNames map[string]string, scanners map[string]*bufio.Scanner) {
	writer := bufio.NewWriterSize(os.Stdout, writeBufferSize)
	defer writer.Flush()

	// Initialize heap
	h := &MinHeap{}
	heap.Init(h)

	// Populate heap with the first entry from each file
	for _, sourceName := range sourceNames {
		scanner := scanners[sourceName]
		entry := ParseLine(sourceName, scanner)
		if entry != nil {
			heap.Push(h, entry)
		}
	}

	// Calculate max output name length
	maxOutputNameLen := 0
	for _, outputName := range outputNames {
		if len(outputName) > maxOutputNameLen {
			maxOutputNameLen = len(outputName)
		}
	}

	// Merge logs
	for h.Len() > 0 {
		current := heap.Pop(h).(*LogLine)

		// TODO: Eliminate aggregatedLines by directly writing to the output buffer

		// Aggregate lines until finding a timestamped line from the same source
		var aggregatedLines []string
		sourceName := current.SourceName
		scanner := scanners[sourceName]
		next := ParseLine(sourceName, scanner)
		for next != nil && next.Timestamp == noTimestamp {
			aggregatedLines = append(aggregatedLines, next.RawLine)
			next = ParseLine(sourceName, scanner)
		}

		outputName := outputNames[sourceName]

		writeOut(writer, current.Timestamp, maxOutputNameLen, outputName, current.RawLine)
		for _, line := range aggregatedLines {
			writeOut(writer, noTimestamp, maxOutputNameLen, outputName, line)
		}

		// Put the current line to the heap
		if next != nil {
			heap.Push(h, next)
		}
	}
}

func writeOut(writer *bufio.Writer, timestamp time.Time, maxOutputNameLen int, outputName string, logLine string) {
	writeLineDuration, _ := MeasureDuration(func() error {
		buf := bufferPool.Get().([]byte)
		buf = buf[:0] // reset buffer

		if includeTimestamp {
			// Handle timestamp
			bufStart := len(buf)
			if timestamp != noTimestamp {
				// RFC3339 is always 25 bytes or less
				buf = timestamp.AppendFormat(buf, time.RFC3339)
				// Pad to 25 characters
				for i := len(buf); i < 25; i++ {
					buf = append(buf, ' ')
				}
			} else {
				// No timestamp case - just add 25 spaces
				buf = append(buf, "                         "...)
			}

			// Add space after timestamp
			buf = append(buf, ' ')
			BytesWrittenForTimestamps += int64(len(buf) - bufStart)
		}

		if includeOutputName {
			// Add output name
			bufStart := len(buf)
			buf = append(buf, outputName...)

			// Pad output name
			for i := len(outputName); i < maxOutputNameLen; i++ {
				buf = append(buf, ' ')
			}

			// Add separator
			buf = append(buf, ' ', '-', ' ')
			BytesWrittenForOutputNames += int64(len(buf) - bufStart)
		}

		// Add log line and newline
		// FIXME: Maybe rest of the line could be bigger than the buffer. It must be chunked
		bufStart := len(buf)
		buf = append(buf, logLine...)
		buf = append(buf, '\n')
		BytesWrittenForRawLines += int64(len(buf) - bufStart)

		// Single write operation
		nn, err := writer.Write(buf)
		BytesWritten += int64(nn)
		bufferPool.Put(buf)

		if err != nil {
			return fmt.Errorf("failed to write to output: %v", err)
		}
		return nil
	})
	WriteLineDuration += int64(writeLineDuration)
}

package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	totalReadBufferSize = 1024 * 1024 * 500
	writeBufferSize     = 1024 * 1024 * 100
)

func mergeLogs(basePath string) error {
	// Find files to process
	files, err := listFiles(basePath)
	if err != nil {
		return fmt.Errorf("failed to detect files: %v", err)
	}
	return mergeFiles(basePath, files)
}

func mergeFiles(basePath string, files []string) error {
	// Open all files and create scanners
	var (
		outputNames = make(map[string]string)
		scanners    = make(map[string]*bufio.Scanner)
		fileHandles = make(map[string]*os.File)
	)
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
	defer func() {
		for _, f := range fileHandles {
			_ = f.Close()
		}
	}()

	printDuration(fmt.Sprintf("Merge %d files", len(files)), func() {
		MergeScanners(files, outputNames, scanners)
	})
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
		entry := parseLine(sourceName, scanner)
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

		// Aggregate lines until finding a timestamped line from the same source
		var aggregatedLines []string
		sourceName := current.SourceName
		scanner := scanners[sourceName]
		next := parseLine(sourceName, scanner)
		for next != nil && next.Timestamp == noTimestamp {
			aggregatedLines = append(aggregatedLines, next.RawLine)
			next = parseLine(sourceName, scanner)
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
	// Preallocate a buffer to avoid multiple small writes
	// Initial size: 25 (timestamp) + 1 (space) + maxOutputNameLen + 3 ( | ) + len(logLine) + 1 (\n)
	bufSize := 30 + maxOutputNameLen + len(logLine)
	buf := make([]byte, 0, bufSize)

	// Handle timestamp
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

	// Add output name
	buf = append(buf, outputName...)

	// Pad output name
	for i := len(outputName); i < maxOutputNameLen; i++ {
		buf = append(buf, ' ')
	}

	// Add separator
	buf = append(buf, ' ', '|', ' ')

	// Add log line and newline
	buf = append(buf, logLine...)
	buf = append(buf, '\n')

	// Single write operation
	writer.Write(buf)
}

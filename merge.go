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
			return make([]byte, 0, 100*1024)
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
	startOfOpenFiles := time.Now()

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
	OpenFilesDuration = MeasureSince(startOfOpenFiles)

	startOfMergeScanners := time.Now()
	MergeScanners(files, outputNames, scanners)
	MergeScannersDuration = MeasureSince(startOfMergeScanners)

	return nil
}

func MergeScanners(sourceNames []string, outputNames map[string]string, scanners map[string]*bufio.Scanner) {
	startOfNewWriter := time.Now()
	writer := bufio.NewWriterSize(os.Stdout, writeBufferSize)
	defer writer.Flush()
	NewWriterDuration = MeasureSince(startOfNewWriter)

	// Calculate max output name length
	startOfMaxOutputNameLenCalc := time.Now()
	maxOutputNameLen := 0
	for _, outputName := range outputNames {
		if len(outputName) > maxOutputNameLen {
			maxOutputNameLen = len(outputName)
		}
	}
	MaxOutputNameLenCalcDuration = MeasureSince(startOfMaxOutputNameLenCalc)

	// Initialize heap
	startOfHeapInit := time.Now()
	h := &MinHeap{}
	heap.Init(h)
	HeapInitDuration = MeasureSince(startOfHeapInit)

	// Populate heap with the first entry from each file
	startOfHeapPopulate := time.Now()
	for _, sourceName := range sourceNames {
		scanner := scanners[sourceName]
		startOfParseLine := time.Now()
		entry := ParseLine(sourceName, scanner)
		PopulateParseLineDuration += MeasureSince(startOfParseLine)
		if entry != nil {
			startOfHeapPush := time.Now()
			heap.Push(h, entry)
			PopulateHeapPushDuration += MeasureSince(startOfHeapPush)
		}
	}
	HeapPopulateDuration = MeasureSince(startOfHeapPopulate)

	// Merge logs
	startOfMergeLoop := time.Now()
	for h.Len() > 0 {
		startOfHeapPop := time.Now()
		current := heap.Pop(h).(*LogLine)
		HeapPopDuration += MeasureSince(startOfHeapPop)

		startOfWriteFirstLine := time.Now()
		sourceName := current.SourceName
		outputName := outputNames[sourceName]
		scanner := scanners[sourceName]
		writeOut(writer, current.Timestamp, maxOutputNameLen, outputName, current.RawLine)
		WriteFirstLineDuration += MeasureSince(startOfWriteFirstLine)

		// Aggregate lines until finding a timestamped line from the same source
		startOfWriteNextLines := time.Now()
		next := ParseLine(sourceName, scanner)
		for next != nil && next.Timestamp == noTimestamp {
			writeOut(writer, noTimestamp, maxOutputNameLen, outputName, next.RawLine)
			next = ParseLine(sourceName, scanner)
		}
		WriteNextLinesDuration += MeasureSince(startOfWriteNextLines)

		// Put the current line to the heap
		if next != nil {
			startOfHeapPush := time.Now()
			heap.Push(h, next)
			InnerHeapPushDuration += MeasureSince(startOfHeapPush)
		}
		InnerMergeLoopDuration += MeasureSince(startOfHeapPop)
	}
	MergeLoopDuration = MeasureSince(startOfMergeLoop)
}

func writeOut(writer *bufio.Writer, timestamp time.Time, maxOutputNameLen int, outputName string, logLine string) {
	startOfWriteLine := time.Now()

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
	bufStart := len(buf)
	buf = append(buf, logLine...)
	buf = append(buf, '\n')
	BytesWrittenForRawLines += int64(len(buf) - bufStart)

	// Single write operation
	nn, err := writer.Write(buf)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to write to output: %v\n", err)
	}
	BytesWritten += int64(nn)
	bufferPool.Put(buf)

	WriteLineDuration += MeasureSince(startOfWriteLine)
}

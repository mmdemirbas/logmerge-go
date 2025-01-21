package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	// sync.Pool gives better performance than using a direct slice due to reduced GC pressure and better cache locality
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 100*1024)
		},
	}
)

func MergeFiles(inputPath string, outputPath string) error {
	// Find files to process
	files, err := ListFiles(inputPath)
	if err != nil {
		return fmt.Errorf("failed to detect files: %v", err)
	}

	startOfOpenFiles := MeasureStart("OpenFiles")
	readers := make([]*FileReader, len(files))

	for i, file := range files {
		sourceName := ""
		// TODO: Consider measuring overhead of each features separately (overheadOfWriteSourceNames etc)
		if writeSourceNames {
			rel, err := filepath.Rel(inputPath, file)
			if err != nil {
				return fmt.Errorf("failed to calculate relative path for file %s: %v", file, err)
			}
			sourceName = rel
		}

		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("failed to open file %s: %v", file, err)
		}

		reader := NewFileReader(f, sourceName, timestampSearchPrefixLen)
		readers[i] = reader
		defer reader.Close()
	}
	OpenFilesDuration = MeasureSince(startOfOpenFiles)

	// Calculate max output name length
	startOfMaxOutputNameLenCalc := MeasureStart("MaxOutputNameLenCalc")
	if writeSourceNames {
		maxOutputNameLen := 0
		for _, reader := range readers {
			if maxOutputNameLen < len(reader.SourceName) {
				maxOutputNameLen = len(reader.SourceName)
			}
		}
		// pad output names to max length
		for _, reader := range readers {
			reader.SourceName = fmt.Sprintf("%-*s - ", maxOutputNameLen, reader.SourceName)
		}
	}
	MaxOutputNameLenCalcDuration = MeasureSince(startOfMaxOutputNameLenCalc)

	baseWriter := os.Stdout
	if outputPath != "" {
		f, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %v", err)
		}
		defer f.Close()
		baseWriter = f
	}

	startOfMergeScanners := MeasureStart("MergeFileReaders")
	MergeFileReaders(readers, baseWriter)
	MergeScannersDuration = MeasureSince(startOfMergeScanners)

	return nil
}

func MergeFileReaders(readers []*FileReader, baseWriter io.Writer) {
	startOfNewWriter := MeasureStart("NewWriter")
	writer := bufio.NewWriterSize(baseWriter, outputBufferSize)
	defer writer.Flush()
	NewWriterDuration = MeasureSince(startOfNewWriter)

	// Initialize heap
	startOfHeapInit := MeasureStart("HeapInit")
	h := &MinHeap{}
	heap.Init(h)
	HeapInitDuration = MeasureSince(startOfHeapInit)

	for _, reader := range readers {
		ExpectedBytesToRead += int64(reader.FileSize)
	}

	// Populate heap with the first entry from each file
	startOfHeapPopulate := MeasureStart("HeapPopulate")
	for _, reader := range readers {
		entry, err := ReadLinePrefix(reader)
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "failed to read line prefix from %s: %v\n", reader.SourceName, err)
			continue
		}
		if entry != nil {
			heap.Push(h, entry)
			HeapPushCount++
		}
	}
	HeapPopulateDuration = MeasureSince(startOfHeapPopulate)

	// Merge logs
	startOfMergeLoop := MeasureStart("MergeLoop")
	for h.Len() > 0 {
		startOfHeapPop := MeasureStart("HeapPop")
		current := heap.Pop(h).(*LinePrefix)
		nextInHeap := h.Peek()
		HeapPopDuration += MeasureSince(startOfHeapPop)
		HeapPopCount++

		source := current.Source
		untilTimestamp := noTimestamp
		if nextInHeap != nil {
			untilTimestamp = nextInHeap.Timestamp
		}

		startOfInnerReadWrite := MeasureStart("InnerReadWrite")
		writeLine(writer, current.Timestamp, source)
		successiveLineCount := 1

		// Aggregate lines until finding a timestamped line from the same source
		next, err := ReadLinePrefix(source)
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "failed to read line prefix from %s: %v\n", source.SourceName, err)
		} else {
			for next != nil && !next.Timestamp.After(untilTimestamp) {
				if !next.Timestamp.Equal(noTimestamp) {
					MaxSuccessiveLineCount = max(MaxSuccessiveLineCount, int64(successiveLineCount))
					UpdateBucketCount(successiveLineCount, SuccessiveLineCountBucketLevels, SuccessiveLineCountBucketValues)
					successiveLineCount = 0
				}
				writeLine(writer, next.Timestamp, source)
				successiveLineCount++
				next, err = ReadLinePrefix(source)
				if err != nil {
					//goland:noinspection GoUnhandledErrorResult
					fmt.Fprintf(os.Stderr, "failed to read line prefix from %s: %v\n", source.SourceName, err)
					break
				}
			}
		}
		InnerReadWriteDuration += MeasureSince(startOfInnerReadWrite)
		MaxSuccessiveLineCount = max(MaxSuccessiveLineCount, int64(successiveLineCount))
		UpdateBucketCount(successiveLineCount, SuccessiveLineCountBucketLevels, SuccessiveLineCountBucketValues)

		// Put the current line to the heap
		if next != nil {
			startOfHeapPush := MeasureStart("HeapPush")
			heap.Push(h, next)
			InnerHeapPushDuration += MeasureSince(startOfHeapPush)
			HeapPushCount++
		}
	}
	MergeLoopDuration = MeasureSince(startOfMergeLoop)
}

func writeLine(writer *bufio.Writer, timestamp time.Time, reader *FileReader) {
	startOfWriteLine := MeasureStart("WriteLinePartial")

	buf := bufferPool.Get().([]byte)
	buf = buf[:0] // reset buffer

	if writeTimestamp {
		// Handle timestamp
		bufStart := len(buf)
		if timestamp != noTimestamp {
			// RFC3339Nanos is always 35 bytes or less
			startOfAppendFormat := MeasureStart("AppendFormat")
			buf = timestamp.AppendFormat(buf, time.RFC3339Nano)
			AppendFormatDuration += MeasureSince(startOfAppendFormat)
			// Pad to 35 characters
			for i := len(buf); i < 35; i++ {
				buf = append(buf, ' ')
			}
		} else {
			// No timestamp case - just add 35 spaces
			buf = append(buf, "                                   "...)
		}

		// Add space after timestamp
		buf = append(buf, ' ')
		BytesWrittenForTimestamps += int64(len(buf) - bufStart)
	}

	if writeSourceNames {
		// Add output name
		bufStart := len(buf)
		buf = append(buf, reader.SourceName...)
		BytesWrittenForOutputNames += int64(len(buf) - bufStart)
	}

	_, err := writer.Write(buf)
	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "failed to write metadata to output: %v\n", err)
	}
	bufferPool.Put(buf)

	// Write rest of the line including the new line character
	beforeWriteRawLine := MeasureStart("WriteRawLine")
	reader.WriteLine(writer)
	WriteRawDataDuration += MeasureSince(beforeWriteRawLine)
	WriteLineDuration += MeasureSince(startOfWriteLine)
}

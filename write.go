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
	space35 = []byte("                                   ") // exactly 35 spaces
)

func MergeFiles(inputPath string, outputPath string) error {
	// Find files to process
	startOfListFiles := MeasureStart("ListFiles")
	files, err := ListFiles(inputPath)
	ListFilesDuration = MeasureSince(startOfListFiles)

	MatchedFiles = files
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

		reader := NewFileReader(f, sourceName, readerBufferSize)
		readers[i] = reader
		defer reader.Close()
	}
	OpenFilesDuration = MeasureSince(startOfOpenFiles)

	// Calculate max output name length
	startOfMaxOutputNameLenCalc := MeasureStart("CalcMaxOutputNameLen")
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

	startOfMerge := MeasureStart("CreateOutputFile")
	baseWriter := os.Stdout
	if outputPath != "" {
		f, err := os.Create(outputPath)
		if err != nil {
			return fmt.Errorf("failed to create output file: %v", err)
		}
		defer f.Close()
		baseWriter = f
	}
	MeasureSince(startOfMerge)

	startOfMergeScanners := MeasureStart("MergeFileReaders")
	MergeFileReaders(readers, baseWriter)
	MergeScannersDuration = MeasureSince(startOfMergeScanners)

	return nil
}

func MergeFileReaders(readers []*FileReader, baseWriter io.Writer) {
	startOfNewWriter := MeasureStart("NewWriter")
	writer := bufio.NewWriterSize(baseWriter, writerBufferSize)
	defer writer.Flush()
	NewWriterDuration = MeasureSince(startOfNewWriter)

	// Initialize heap
	startOfHeapInit := MeasureStart("HeapInit")
	h := &MinHeap{}
	heap.Init(h)
	HeapInitDuration = MeasureSince(startOfHeapInit)

	startOfCalcExpectedByteCount := MeasureStart("CalcExpectedByteCount")
	for _, reader := range readers {
		ExpectedBytesToRead += int64(reader.FileSize)
	}
	MeasureSince(startOfCalcExpectedByteCount)

	// Populate heap with the first entry from each file
	startOfHeapPopulate := MeasureStart("HeapPopulate")
	for _, reader := range readers {
		startOfReadLinePrefix := MeasureStart("ReadLinePrefix")
		entry, err := ReadLinePrefix(reader)
		MeasureSince(startOfReadLinePrefix)

		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "failed to read line prefix from %s: %v\n", reader.SourceName, err)
			continue
		}
		if entry != nil {
			startOfHeapPush := MeasureStart("HeapPush")
			heap.Push(h, entry)
			MeasureSince(startOfHeapPush)
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

		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		startOfInnerReadWrite := MeasureStart("InnerReadWrite")
		HeapPopCount++
		source := current.Source
		untilTimestamp := noTimestamp
		if nextInHeap != nil {
			untilTimestamp = nextInHeap.Timestamp
		}
		startOfWriteLine := MeasureStart("WriteLine")
		writeLine(writer, current.Timestamp, source)
		successiveLineCount := 1
		MeasureSince(startOfWriteLine)

		// Aggregate lines until finding a timestamped line from the same source
		startOfReadLinePrefix := MeasureStart("ReadLinePrefix")
		next, err := ReadLinePrefix(source)
		MeasureSince(startOfReadLinePrefix)

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

				startOfWriteLine = MeasureStart("WriteLine")
				writeLine(writer, next.Timestamp, source)
				successiveLineCount++
				MeasureSince(startOfWriteLine)

				startOfReadLinePrefix = MeasureStart("ReadLinePrefix")
				next, err = ReadLinePrefix(source)
				MeasureSince(startOfReadLinePrefix)

				if err != nil {
					//goland:noinspection GoUnhandledErrorResult
					fmt.Fprintf(os.Stderr, "failed to read line prefix from %s: %v\n", source.SourceName, err)
					break
				}
			}
		}
		MaxSuccessiveLineCount = max(MaxSuccessiveLineCount, int64(successiveLineCount))
		UpdateBucketCount(successiveLineCount, SuccessiveLineCountBucketLevels, SuccessiveLineCountBucketValues)
		InnerReadWriteDuration += MeasureSince(startOfInnerReadWrite)

		// Put the current line to the heap
		startOfHeapPush := MeasureStart("HeapPush")
		if next != nil {
			heap.Push(h, next)
			HeapPushCount++
		}
		InnerHeapPushDuration += MeasureSince(startOfHeapPush)
	}
	MergeLoopDuration = MeasureSince(startOfMergeLoop)
}

func writeLine(writer *bufio.Writer, timestamp time.Time, reader *FileReader) {
	startOfWriteOverhead := MeasureStart("WriteOverhead")
	buf := bufferPool.Get().([]byte)
	buf = buf[:0] // reset buffer

	startOfWriteTimestamp := MeasureStart("WriteTimestamp")
	if writeTimestamp {
		// Handle timestamp
		bufStart := len(buf)
		if timestamp != noTimestamp {
			// TODO: Consider optimizing time formatting
			buf = timestamp.AppendFormat(buf, time.RFC3339Nano)
			if delta := 35 - (len(buf) - bufStart); delta > 0 {
				buf = append(buf, space35[:delta]...)
			}
		} else {
			buf = append(buf, space35...)
		}

		// Add space after timestamp
		buf = append(buf, ' ')
		BytesWrittenForTimestamps += int64(len(buf) - bufStart)
	}
	MeasureSince(startOfWriteTimestamp)

	startOfWriteSourceNames := MeasureStart("WriteSourceNames")
	if writeSourceNames {
		// Add output name
		bufStart := len(buf)
		buf = append(buf, reader.SourceName...)
		BytesWrittenForOutputNames += int64(len(buf) - bufStart)
	}
	MeasureSince(startOfWriteSourceNames)

	startOfFlush := MeasureStart("Flush")
	_, err := writer.Write(buf)
	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "failed to write metadata to output: %v\n", err)
	}
	bufferPool.Put(buf)
	MeasureSince(startOfFlush)
	WriteOverheadDuration += MeasureSince(startOfWriteOverhead)

	// Write rest of the line including the new line character
	beforeWriteRawData := MeasureStart("WriteRawData")
	reader.WriteLine(writer)
	WriteRawDataDuration += MeasureSince(beforeWriteRawData)
}

package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

var (
	space36 = []byte("                                    ")
)

func MergeFiles(inputPath string, stdout *os.File) error {
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

		reader, err := NewFileReader(f, sourceName, readerBufferSize)
		if err != nil {
			return fmt.Errorf("failed to create reader for file %s: %v", file, err)
		}
		readers[i] = reader
		defer reader.Close()
	}
	MeasureSince(startOfOpenFiles)

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
	MeasureSince(startOfMaxOutputNameLenCalc)

	startOfMergeScanners := MeasureStart("MergeFileReaders")
	err = MergeFileReaders(readers, stdout)
	ProcessDuration = MeasureSince(startOfMergeScanners)

	return err
}

func MergeFileReaders(readers []*FileReader, stdout io.Writer) error {
	startOfNewWriter := MeasureStart("NewWriter")
	writer := bufio.NewWriterSize(stdout, writerBufferSize)
	defer writer.Flush()
	MeasureSince(startOfNewWriter)

	// Initialize heap
	startOfHeapInit := MeasureStart("HeapInit")
	h := &MinHeap{}
	heap.Init(h)
	MeasureSince(startOfHeapInit)

	// Populate heap with the first entry from each file
	startOfHeapPopulate := MeasureStart("HeapPopulate")
	for _, reader := range readers {
		startOfReadLinePrefix := MeasureStart("ReadLinePrefix")
		entry, err := ReadLinePrefix(reader)
		MeasureSince(startOfReadLinePrefix)

		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			return fmt.Errorf("failed to read line prefix from %s: %v", reader.SourceName, err)
		}
		if entry != nil {
			startOfHeapPush := MeasureStart("HeapPush")
			heap.Push(h, entry)
			MeasureSince(startOfHeapPush)
			HeapPushCount++
		}
	}
	MeasureSince(startOfHeapPopulate)

	// Merge logs
	startOfMergeLoop := MeasureStart("MergeLoop")
	for h.Len() > 0 {
		startOfHeapPop := MeasureStart("HeapPop")
		current := heap.Pop(h).(*LinePrefix)
		nextInHeap := h.Peek()
		MeasureSince(startOfHeapPop)

		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		startOfInnerReadWrite := MeasureStart("InnerReadWrite")
		HeapPopCount++
		source := current.Source
		untilTimestamp := noTimestamp
		if nextInHeap != nil {
			untilTimestamp = nextInHeap.Timestamp
		}
		startOfWriteLine := MeasureStart("WriteLine")
		err := writeLine(writer, current.Timestamp, source)
		successiveLineCount := 1
		MeasureSince(startOfWriteLine)

		if err != nil {
			return fmt.Errorf("failed to write line: %v", err)
		}

		// Aggregate lines until finding a timestamped line from the same source
		startOfReadLinePrefix := MeasureStart("ReadLinePrefix")
		next, err := ReadLinePrefix(source)
		MeasureSince(startOfReadLinePrefix)

		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			return fmt.Errorf("failed to read line prefix from %s: %v", source.SourceName, err)
		}
		for next != nil && !next.Timestamp.After(untilTimestamp) {
			if !next.Timestamp.Equal(noTimestamp) {
				MaxSuccessiveLineCount = max(MaxSuccessiveLineCount, int64(successiveLineCount))
				UpdateBucketCount(successiveLineCount, SuccessiveLineCountBucketLevels, SuccessiveLineCountBucketValues)
				successiveLineCount = 0
			}

			startOfWriteLine = MeasureStart("WriteLine")
			err = writeLine(writer, next.Timestamp, source)
			successiveLineCount++
			MeasureSince(startOfWriteLine)

			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			startOfReadLinePrefix = MeasureStart("ReadLinePrefix")
			next, err = ReadLinePrefix(source)
			MeasureSince(startOfReadLinePrefix)

			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				return fmt.Errorf("failed to read line prefix from %s: %v", source.SourceName, err)
			}
		}

		MaxSuccessiveLineCount = max(MaxSuccessiveLineCount, int64(successiveLineCount))
		UpdateBucketCount(successiveLineCount, SuccessiveLineCountBucketLevels, SuccessiveLineCountBucketValues)
		MeasureSince(startOfInnerReadWrite)

		// Put the current line to the heap
		startOfHeapPush := MeasureStart("HeapPush")
		if next != nil {
			heap.Push(h, next)
			HeapPushCount++
		}
		MeasureSince(startOfHeapPush)
	}
	MeasureSince(startOfMergeLoop)
	return nil
}

func writeLine(writer *bufio.Writer, timestamp time.Time, reader *FileReader) error {
	startOfWriteTimestamp := MeasureStart("WriteTimestamp")
	if writeTimestamp {
		if timestamp == noTimestamp {
			startOfNoSourceNamePadding := MeasureStart("NoSourceNamePadding")
			n, err := writer.Write(space36)
			MeasureSince(startOfNoSourceNamePadding)

			BytesWrittenForTimestamps += int64(n)
			if err != nil {
				return fmt.Errorf("failed to write timestamp padding: %v", err)
			}
		} else {
			// TODO: Consider optimizing time formatting
			startOfAppendFormat := MeasureStart("AppendFormat")
			n, err := writer.WriteString(timestamp.Format(time.RFC3339Nano))
			MeasureSince(startOfAppendFormat)

			BytesWrittenForTimestamps += int64(n)
			if err != nil {
				return fmt.Errorf("failed to write timestamp: %v", err)
			}

			startOfAppendFormatPadding := MeasureStart("AppendFormatPadding")
			if delta := 35 - n; delta > 0 {
				n, err = writer.Write(space36[:delta])
				BytesWrittenForTimestamps += int64(n)
				if err != nil {
					return fmt.Errorf("failed to write timestamp padding: %v", err)
				}
			}
			MeasureSince(startOfAppendFormatPadding)
		}
	}
	TotalWriteOutputDuration += MeasureSince(startOfWriteTimestamp)

	startOfWriteSourceNames := MeasureStart("WriteSourceNames")
	if writeSourceNames {
		n, err := writer.WriteString(reader.SourceName)
		BytesWrittenForOutputNames += int64(n)
		if err != nil {
			return fmt.Errorf("failed to write source name: %v", err)
		}
	}
	TotalWriteOutputDuration += MeasureSince(startOfWriteSourceNames)

	// Write rest of the line including the new line character
	beforeWriteRawData := MeasureStart("WriteRawData")
	err := reader.WriteLine(writer)
	MeasureSince(beforeWriteRawData)

	return err
}

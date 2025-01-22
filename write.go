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
	startTime := MeasureStart("ListFiles")
	files, err := ListFiles(inputPath)
	ListFilesDuration = MeasureSince(startTime)

	MatchedFiles = files
	if err != nil {
		return fmt.Errorf("failed to detect files: %v", err)
	}

	startTime = MeasureStart("OpenFiles")
	readers := make([]*FileReader, len(files))
	for i, file := range files {
		sourceName := ""
		// TODO: Consider measuring overhead of each features separately (overheadOfWriteSourceNames etc)
		if writeSourceNamesPerLine || writeSourceNamesPerBlock {
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
	MeasureSince(startTime)

	if writeSourceNamesPerLine {
		startTime = MeasureStart("SetSourceNameForLine")
		maxSourceNameLen := 0
		for _, reader := range readers {
			sourceNameLen := len(reader.SourceName)
			if maxSourceNameLen < sourceNameLen {
				maxSourceNameLen = sourceNameLen
			}
		}
		// pad source names to max length
		for _, reader := range readers {
			reader.SourceNameForLine = fmt.Sprintf("%-*s - ", maxSourceNameLen, reader.SourceName)
		}
		MeasureSince(startTime)
	}
	if writeSourceNamesPerBlock {
		startTime = MeasureStart("SetSourceNameForBlock")
		for _, reader := range readers {
			reader.SourceNameForBlock = fmt.Sprintf("\n--- %s ---\n", reader.SourceName)
		}
		MeasureSince(startTime)
	}

	startTime = MeasureStart("MergeFileReaders")
	err = MergeFileReaders(readers, stdout)
	ProcessDuration = MeasureSince(startTime)

	return err
}

func MergeFileReaders(readers []*FileReader, stdout io.Writer) error {
	startTime := MeasureStart("NewWriter")
	writer := bufio.NewWriterSize(stdout, writerBufferSize)
	defer writer.Flush()
	MeasureSince(startTime)

	// Initialize heap
	startTime = MeasureStart("HeapInit")
	h := &MinHeap{}
	heap.Init(h)
	MeasureSince(startTime)

	// Populate heap with the first entry from each file
	startTime = MeasureStart("HeapPopulate")
	for _, reader := range readers {
		startTime := MeasureStart("ReadLinePrefix")
		entry, err := ReadLinePrefix(reader)
		MeasureSince(startTime)

		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
		}
		if entry != nil {
			startTime := MeasureStart("HeapPush")
			heap.Push(h, entry)
			MeasureSince(startTime)
			HeapPushCount++
		}
	}
	MeasureSince(startTime)

	// Merge logs
	startTime = MeasureStart("MergeLoop")
	for h.Len() > 0 {
		startTime := MeasureStart("HeapPop")
		current := heap.Pop(h).(*LinePrefix)
		nextInHeap := h.Peek()
		MeasureSince(startTime)

		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		startTimeInnerReadWrite := MeasureStart("InnerReadWrite")
		HeapPopCount++
		source := current.Source
		untilTimestamp := noTimestamp
		if nextInHeap != nil {
			untilTimestamp = nextInHeap.Timestamp
		}

		if writeSourceNamesPerBlock {
			startTime := MeasureStart("WriteSourceNamePerBlock")
			n, err := writer.WriteString(source.SourceNameForBlock)
			BytesWrittenForSourceNamePerBlock += int64(n)
			MeasureSince(startTime)
			if err != nil {
				return fmt.Errorf("failed to write source name: %v", err)
			}
		}

		startTime = MeasureStart("WriteLine")
		err := writeLine(writer, current.Timestamp, source)
		successiveLineCount := 1
		MeasureSince(startTime)

		if err != nil {
			return fmt.Errorf("failed to write line: %v", err)
		}

		// Aggregate lines until finding a timestamped line from the same source
		startTime = MeasureStart("ReadLinePrefix")
		next, err := ReadLinePrefix(source)
		MeasureSince(startTime)

		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			return fmt.Errorf("failed to read line prefix from %s: %v", source.File.Name(), err)
		}

		for next != nil && !next.Timestamp.After(untilTimestamp) {
			if next.Timestamp != noTimestamp {
				// Timestamp changed
				SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
				successiveLineCount = 0
			}

			startTime = MeasureStart("WriteLine")
			err = writeLine(writer, next.Timestamp, source)
			successiveLineCount++
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			startTime = MeasureStart("ReadLinePrefix")
			next, err = ReadLinePrefix(source)
			MeasureSince(startTime)

			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				return fmt.Errorf("failed to read line prefix from %s: %v", source.File.Name(), err)
			}
		}

		SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
		MeasureSince(startTimeInnerReadWrite)

		// Put the current line to the heap
		if next != nil {
			startTime = MeasureStart("HeapPush")
			heap.Push(h, next)
			HeapPushCount++
			MeasureSince(startTime)
		}
	}
	MeasureSince(startTime)
	return nil
}

func writeLine(writer *bufio.Writer, timestamp time.Time, reader *FileReader) error {
	if writeTimestamp {
		startTime := MeasureStart("WriteTimestamp")
		if timestamp == noTimestamp {
			n, err := writer.Write(space36)
			BytesWrittenForTimestamps += int64(n)
			if err != nil {
				return fmt.Errorf("failed to write timestamp padding: %v", err)
			}
		} else {
			// TODO: Consider optimizing time formatting
			startTime := MeasureStart("AppendFormat")
			n, err := writer.WriteString(timestamp.Format(time.RFC3339Nano))
			MeasureSince(startTime)

			BytesWrittenForTimestamps += int64(n)
			if err != nil {
				return fmt.Errorf("failed to write timestamp: %v", err)
			}

			if delta := 35 - n; delta > 0 {
				startTime = MeasureStart("AppendFormatPadding")
				n, err = writer.Write(space36[:delta])
				BytesWrittenForTimestamps += int64(n)
				if err != nil {
					return fmt.Errorf("failed to write timestamp padding: %v", err)
				}
				MeasureSince(startTime)
			}
		}
		WriteOutputMetric.MeasureSince(startTime)
	}
	if writeSourceNamesPerLine {
		startTime := MeasureStart("WriteSourceNamePerLine")
		n, err := writer.WriteString(reader.SourceNameForLine)
		BytesWrittenForSourceNamePerLine += int64(n)
		if err != nil {
			return fmt.Errorf("failed to write source name: %v", err)
		}
		WriteOutputMetric.MeasureSince(startTime)
	}

	// Write rest of the line including the new line character
	startTime := MeasureStart("WriteRawData")
	err := reader.WriteLine(writer)
	MeasureSince(startTime)

	return err
}

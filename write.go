package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var (
	space36 = []byte("                                    ")
)

func MergeFiles(inputPath string) error {
	startTime := MeasureStart("ListFiles")
	files, err := ListFiles(inputPath)
	ListFilesDuration = MeasureSince(startTime)

	MatchedFiles = files
	if err != nil {
		return fmt.Errorf("failed to detect files: %v", err)
	}

	readers := make([]*InputFile, len(files))
	for i, file := range files {
		sourceName := ""
		// TODO: Consider measuring overhead of each features separately (overheadOfWriteSourceNames etc)
		if WriteSourceNamesPerBlock || WriteSourceNamesPerLine {
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

		reader, err := NewInputFile(f, sourceName, ReaderBufferSize)
		if err != nil {
			return fmt.Errorf("failed to create reader for file %s: %v", file, err)
		}
		readers[i] = reader
		defer reader.Close()
	}

	if WriteSourceNamesPerBlock {
		for _, reader := range readers {
			reader.SourceNamePerBlock = fmt.Sprintf("\n--- %s ---\n", reader.SourceName)
		}
	}

	if WriteSourceNamesPerLine {
		maxSourceNameLen := 0
		for _, reader := range readers {
			sourceNameLen := len(reader.SourceName)
			if maxSourceNameLen < sourceNameLen {
				maxSourceNameLen = sourceNameLen
			}
		}
		// pad source names to max length
		for _, reader := range readers {
			reader.SourceNamePerLine = fmt.Sprintf("%-*s - ", maxSourceNameLen, reader.SourceName)
		}
	}

	return MergeFileReaders(readers)
}

func MergeFileReaders(readers []*InputFile) error {
	writer := bufio.NewWriterSize(Stdout, WriterBufferSize)
	defer writer.Flush()

	// Initialize heap
	h := &MinHeap{}
	heap.Init(h)

	// Populate heap with the first entry from each file
	startTime := MeasureStart("HeapPopulate")
	for _, reader := range readers {
		startTime := MeasureStart("ReadTimestamp")
		timestamp, err := ReadTimestamp(reader)
		MeasureSince(startTime)

		if err != nil {
			return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
		}
		if timestamp != nil {
			startTime = MeasureStart("HeapPush")
			reader.CurrentTimestamp = *timestamp
			heap.Push(h, reader)
			HeapPushMetric.MeasureSince(startTime)
		}
	}
	MeasureSince(startTime)

	// Merge logs
	startTime = MeasureStart("MergeLoop")
	for h.Len() > 0 {
		startTime := MeasureStart("HeapPop")
		reader := heap.Pop(h).(*InputFile)
		HeapPopMetric.MeasureSince(startTime)

		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		processedLineCount := 0

		// Skip lines until finding an eligible line
		timestamp := &reader.CurrentTimestamp
		for timestamp != nil && timestamp.Before(MinTimestamp) {
			processedLineCount++
			err := reader.SkipLine()
			if err != nil {
				return fmt.Errorf("failed to skip line from %s: %v", reader.File.Name(), err)
			}

			startTime = MeasureStart("ReadTimestamp")
			newTimestamp, err := ReadTimestamp(reader)
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
			}
			if newTimestamp == nil || *newTimestamp != noTimestamp {
				timestamp = newTimestamp
			}
		}

		if timestamp == nil || timestamp.After(MaxTimestamp) {
			// File is done
			continue
		}

		var effectiveMaxTimestamp time.Time
		nextReader := h.Peek()
		if nextReader != nil && nextReader.CurrentTimestamp.Before(MaxTimestamp) {
			effectiveMaxTimestamp = nextReader.CurrentTimestamp
		} else {
			effectiveMaxTimestamp = MaxTimestamp
		}

		shouldWriteSourceName := WriteSourceNamesPerBlock
		successiveLineCount := 0

		// Write lines until reaching the known bigger timestamp or a skip-line or the end of the file
		for timestamp != nil && !timestamp.After(effectiveMaxTimestamp) {
			if shouldWriteSourceName {
				shouldWriteSourceName = false
				startTime = MeasureStart("WriteSourceNamePerBlock")
				n, err := writer.WriteString(reader.SourceNamePerBlock)
				BytesWrittenForSourceNamePerBlock += int64(n)
				MeasureSince(startTime)
				if err != nil {
					return fmt.Errorf("failed to write source name: %v", err)
				}
			}

			var timestampToWrite *time.Time
			if successiveLineCount != 0 {
				timestampToWrite = &noTimestamp
			} else {
				timestampToWrite = timestamp
			}

			startTime = MeasureStart("WriteLine")
			err := writeLine(writer, timestampToWrite, reader)
			successiveLineCount++
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			// Aggregate lines until finding a timestamped line from the same source
			startTime = MeasureStart("ReadTimestamp")
			newTimestamp, err := ReadTimestamp(reader)
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
			}

			if newTimestamp == nil || *newTimestamp != noTimestamp {
				// Timestamp changed
				timestamp = newTimestamp
				processedLineCount += successiveLineCount
				SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
				successiveLineCount = 0
			}
		}
		LinesRead += int64(processedLineCount)

		if timestamp != nil && !timestamp.After(MaxTimestamp) {
			// Put the current line to the heap
			startTime = MeasureStart("HeapPush")
			reader.CurrentTimestamp = *timestamp
			heap.Push(h, reader)
			HeapPushMetric.MeasureSince(startTime)
		}
	}
	MeasureSince(startTime)
	return nil
}

func writeLine(writer *bufio.Writer, timestamp *time.Time, reader *InputFile) error {
	if WriteTimestampPerLine {
		startTime := MeasureStart("WriteTimestamp")
		if *timestamp == noTimestamp {
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
				if err != nil {
					return fmt.Errorf("failed to write timestamp padding: %v", err)
				}
				MeasureSince(startTime)
				BytesWrittenForTimestamps += int64(n)
			}
		}
		WriteOutputMetric.MeasureSince(startTime)
	}
	if WriteSourceNamesPerLine {
		startTime := MeasureStart("WriteSourceNamePerLine")
		n, err := writer.WriteString(reader.SourceNamePerLine)
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

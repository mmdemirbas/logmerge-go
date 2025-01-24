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
	space30 = []byte("                              ")
)

func MergeFiles(inputPath string) error {
	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "MergeFiles: Recovered from panic: %v\n", r)
		}
	}()

	files, err := ListFiles(inputPath)

	MatchedFiles = files
	if err != nil {
		return fmt.Errorf("failed to detect files: %v", err)
	}

	perFileBufferSize := max(TimestampSearchEndIndex, BufferSizeForRead/len(files))

	readers := make([]*InputFile, len(files))
	readers = readers[:0]

	for _, file := range files {
		sourceName := ""
		// TODO: Consider measuring overhead of each features separately (overheadOfWriteSourceNames etc)
		if WriteSourceNamesPerBlock || WriteSourceNamesPerLine {
			rel, err := filepath.Rel(inputPath, file)
			if err != nil {
				return fmt.Errorf("failed to calculate relative path for file %s: %v", file, err)
			}
			sourceName = rel
			alias, ok := SourceNameAliases[sourceName]
			if ok {
				sourceName = alias
			}
		}

		f, err := os.Open(file)
		if err != nil {
			fmt.Fprintf(Stderr, "failed to open file %s: %v", file, err)
			continue
		}

		reader, err := NewInputFile(f, sourceName, perFileBufferSize)
		if err != nil {
			return fmt.Errorf("failed to create reader for file %s: %v", file, err)
		}
		readers = append(readers, reader)
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

	writer := bufio.NewWriterSize(Stdout, BufferSizeForWrite)
	defer writer.Flush()

	startTime := MeasureStart("HeapInit")
	// Initialize heap
	h := &MinHeap{}
	heap.Init(h)

	// Populate heap with the first entry from each file
	for _, reader := range readers {
		startTime := MeasureStart("UpdateTimestamp")
		err := UpdateTimestamp(reader)
		MeasureSince(startTime)

		if err != nil {
			return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
		}
		if reader.TimestampParsed {
			startTime = MeasureStart("HeapPush")
			heap.Push(h, reader)
			HeapPushMetric.MeasureSince(startTime)
		}
	}
	MeasureSince(startTime)

	// Print progress
	go func() {
		// Print progress only if it takes some time
		time.Sleep(3 * time.Second)

		totalCount := len(readers)
		totalSize := 0

		for _, reader := range readers {
			totalSize += reader.FileSize
		}

		totalCount100 := float64(totalCount) / 100
		totalSize100 := float64(totalSize) / 100

		totalCountString := count(int64(totalCount))
		totalSizeString := bytes(int64(totalSize))

		fmt.Fprintf(Stderr, "\n")

		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			completedCount := 0
			completedSize := 0
			for _, reader := range readers {
				if reader.Done {
					completedSize += reader.FileSize
					completedCount++
				} else {
					completedSize += reader.BytesRead
				}
			}
			fmt.Fprintf(Stderr, "Progress: %.2f %% of data (%12s / %12s) - %.2f of files (%12s / %12s)\r",
				float64(completedSize)/totalSize100, bytes(int64(completedSize)), totalSizeString,
				float64(completedCount)/totalCount100, count(int64(completedCount)), totalCountString,
			)
		}
	}()

	lastPrintedSourceName := ""

	// Merge logs
	for h.Len() > 0 {
		startTime := MeasureStart("HeapPop")
		reader := heap.Pop(h).(*InputFile)
		nextReader := h.Peek()
		HeapPopMetric.MeasureSince(startTime)

		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		skippedLineCount := 0

		// Skip lines until finding an eligible line
		for reader.TimestampParsed && reader.Timestamp.Before(MinTimestamp) {
			skippedLineCount++
			err := reader.SkipLine()
			if err != nil {
				return fmt.Errorf("failed to skip line from %s: %v", reader.File.Name(), err)
			}

			startTime = MeasureStart("UpdateTimestamp")
			err = UpdateTimestamp(reader)
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
			}
		}

		var effectiveMaxTimestamp MyTime
		if nextReader != nil && nextReader.Timestamp.Before(MaxTimestamp) {
			effectiveMaxTimestamp = nextReader.Timestamp
		} else {
			effectiveMaxTimestamp = MaxTimestamp
		}

		shouldWriteSourceName := WriteSourceNamesPerBlock && lastPrintedSourceName != reader.SourceName
		successiveLineCount := 0
		processedLineCount := 0

		// Write lines until reaching the known bigger timestamp or a skip-line or the end of the file
		for reader.TimestampParsed && !reader.Timestamp.After(effectiveMaxTimestamp) {
			if shouldWriteSourceName {
				shouldWriteSourceName = false
				lastPrintedSourceName = reader.SourceName
				startTime = MeasureStart("WriteSourceNamePerBlock")
				n, err := writer.WriteString(reader.SourceNamePerBlock)
				BytesWrittenForSourceNamePerBlock += int64(n)
				MeasureSince(startTime)
				if err != nil {
					return fmt.Errorf("failed to write source name: %v", err)
				}
			}

			var timestampToWrite MyTime
			if successiveLineCount != 0 {
				timestampToWrite = noTimestamp
			} else {
				timestampToWrite = reader.Timestamp
			}

			startTime = MeasureStart("WriteLine")
			err := writeLine(writer, timestampToWrite, reader)
			successiveLineCount++
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			// Aggregate lines until finding a timestamped line from the same source
			startTime = MeasureStart("UpdateTimestamp")
			err = UpdateTimestamp(reader)
			MeasureSince(startTime)

			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
			}

			if !reader.TimestampParsed || reader.Timestamp != noTimestamp {
				// Timestamp changed or file ended
				processedLineCount += successiveLineCount
				SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
				successiveLineCount = 0
			}
		}

		LinesRead += int64(processedLineCount + skippedLineCount)
		LinesReadAndSkipped += int64(skippedLineCount)

		if reader.TimestampParsed && !reader.Timestamp.After(MaxTimestamp) {
			// Put the next entry to the heap
			startTime = MeasureStart("HeapPush")
			heap.Push(h, reader)
			HeapPushMetric.MeasureSince(startTime)
		} else {
			// Close the file
			err := reader.Close()
			if err != nil {
				fmt.Fprintf(Stderr, "failed to close file %s: %v", reader.File.Name(), err)
			}
			// Update metrics
			BytesRead += int64(reader.BytesRead)
			BytesNotRead += int64(reader.FileSize - reader.BytesRead)
			reader.Done = true
		}
	}
	return nil
}

func writeLine(writer *bufio.Writer, timestamp MyTime, reader *InputFile) error {
	if WriteTimestampPerLine {
		startTime := MeasureStart("WriteTimestamp")
		var toWrite []byte
		if timestamp == noTimestamp {
			toWrite = space30
		} else {
			toWrite = []byte(timestamp.String())
		}

		n, err := writer.Write(toWrite)
		if err != nil {
			if timestamp == noTimestamp {
				return fmt.Errorf("failed to write timestamp padding: %v", err)
			} else {
				return fmt.Errorf("failed to write timestamp: %v", err)
			}
		}
		BytesWrittenForTimestamps += int64(n)
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
	return reader.WriteLine(writer)
}

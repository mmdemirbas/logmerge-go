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

func MergeFiles(inputPath string, programStartTime time.Time) error {
	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "MergeFiles: Recovered from panic: %v\n", r)
		}
	}()

	files, err := ListFiles(inputPath)

	MatchedFiles = files
	if err != nil {
		return fmt.Errorf("failed to list files: %v", err)
	}

	perFileBufferSize := max(TimestampSearchEndIndex, BufferSizeForRead/len(files))

	readers := make([]*FileReader, len(files))
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
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "failed to open file %s: %v\n", file, err)
			continue
		}

		reader, err := NewInputFile(f, sourceName, perFileBufferSize)
		if err != nil {
			return fmt.Errorf("failed to create reader for file %s: %v", file, err)
		}
		readers = append(readers, reader)
		defer reader.Close()
	}

	if len(readers) == 0 {
		return fmt.Errorf("no files to merge")
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
	h := MinHeap(make([]*FileReader, 0, len(readers))) // Pre-allocate heap with the number of files
	h = h[:0]                                          // Reset the heap length to zero

	// Populate heap with the first entry from each file
	remainingReaderCount := 0
	for _, reader := range readers {
		startTime := MeasureStart("UpdateTimestamp")
		err = UpdateTimestamp(reader)
		MeasureSince(startTime)

		if err != nil {
			return fmt.Errorf("failed to read line prefix from %s: %v", reader.File.Name(), err)
		}
		if reader.TimestampParsed {
			h = append(h, reader)
			remainingReaderCount++
		} else {
			err = reader.Close()
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "failed to close file %s: %v\n", reader.File.Name(), err)
			}
			// Update metrics
			BytesRead += int64(reader.BytesRead)
			BytesNotRead += int64(reader.FileSize - reader.BytesRead)
			reader.Done = true
		}
	}
	heap.Init(&h)
	MeasureSince(startTime)

	// Print progress
	go func() {
		// TODO: Make printProgress params configurable (initial delay, interval, etc)
		// Print progress only if it takes some time
		time.Sleep(1 * time.Second)

		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(Stderr, "\n")

		ticker := time.NewTicker(1000 * time.Millisecond)
		for range ticker.C {
			printProgress(readers, programStartTime)
		}
	}()

	lastPrintedSourceName := ""

	startTime = MeasureStart("HeapPop")
	reader := heap.Pop(&h).(*FileReader)
	var nextReader *FileReader = nil
	if remainingReaderCount > 0 {
		nextReader = heap.Pop(&h).(*FileReader)
		remainingReaderCount--
		HeapPopMetric.CallCount++
	}
	HeapPopMetric.MeasureSince(startTime)

	// Merge logs
	for reader != nil {
		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		skippedLineCount := 0

		// Skip lines until finding an eligible line
		for reader.TimestampParsed && reader.Timestamp < MinTimestamp {
			skippedLineCount++
			err = reader.SkipLine()
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
		if nextReader != nil && nextReader.Timestamp < MaxTimestamp {
			effectiveMaxTimestamp = nextReader.Timestamp
		} else {
			effectiveMaxTimestamp = MaxTimestamp
		}

		shouldWriteSourceName := WriteSourceNamesPerBlock && lastPrintedSourceName != reader.SourceName // TODO: Source name comparison could be optimized by using a pointer or number code?
		successiveLineCount := 0
		blockLineCount := 0

		// Write lines until reaching the known bigger timestamp or a skip-line or the end of the file
		for reader.TimestampParsed && reader.Timestamp <= effectiveMaxTimestamp {
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
			if successiveLineCount == 0 {
				timestampToWrite = reader.Timestamp
			} else {
				timestampToWrite = noTimestamp
			}

			startTime = MeasureStart("WriteLine")
			err = writeLine(writer, timestampToWrite, reader)
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
				blockLineCount += successiveLineCount
				SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
				successiveLineCount = 0
			}
		}

		LinesRead += int64(blockLineCount + skippedLineCount)
		LinesReadAndSkipped += int64(skippedLineCount)
		SkippedLineCounts.UpdateBucketCount(skippedLineCount)
		BlockLineCounts.UpdateBucketCount(blockLineCount)

		if reader.TimestampParsed && reader.Timestamp <= MaxTimestamp {
			// Put the next entry to the heap
			startTime = MeasureStart("HeapPush")
			heap.Push(&h, reader)
			HeapPushMetric.MeasureSince(startTime)
		} else {
			// Close the file
			remainingReaderCount--
			err = reader.Close()
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "failed to close file %s: %v\n", reader.File.Name(), err)
			}
			// Update metrics
			BytesRead += int64(reader.BytesRead)
			BytesNotRead += int64(reader.FileSize - reader.BytesRead)
			reader.Done = true
		}

		reader = nextReader
		if remainingReaderCount > 0 {
			startTime = MeasureStart("HeapPop")
			nextReader = heap.Pop(&h).(*FileReader)
			HeapPopMetric.MeasureSince(startTime)
		} else {
			nextReader = nil
		}
	}
	printProgress(readers, programStartTime)
	return nil
}

func writeLine(writer *bufio.Writer, timestamp MyTime, reader *FileReader) error {
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

func printProgress(readers []*FileReader, programStartTime time.Time) {
	completedSize := 0
	completedCount := 0

	totalSize := 0
	totalCount := len(readers)

	for _, reader := range readers {
		if reader.Done {
			completedSize += reader.FileSize
			completedCount++
		} else {
			completedSize += reader.BytesRead
		}
		totalSize += reader.FileSize
	}

	totalSize = max(totalSize, 1)
	totalCount = max(totalCount, 1)

	elapsedTime := time.Since(programStartTime)

	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(os.Stderr, "Progress: %6.2f %% of data (%12s / %12s) - %6.2f %% of files (%5d / %5d) - Elapsed: %s\r",
		float64(completedSize)/(float64(totalSize)/100), bytes(int64(completedSize)), bytes(int64(totalSize)),
		float64(completedCount)/(float64(totalCount)/100), int64(completedCount), int64(totalCount),
		elapsedTime.Round(time.Millisecond).String(),
	)
}

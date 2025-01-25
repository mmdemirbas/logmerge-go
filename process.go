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

func ProcessFiles(inputPath string, programStartTime time.Time) error {
	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "ProcessFiles: Recovered from panic: %v\n", r)
		}
	}()

	fileList, err := ListFiles(inputPath)
	if err != nil {
		return fmt.Errorf("failed to list files: %v", err)
	}

	perFileBufferSize := max(TimestampSearchEndIndex, BufferSizeForRead/len(fileList))

	files := make([]*FileHandle, len(fileList))
	files = files[:0]

	for _, file := range fileList {
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

		file, err := NewFileHandle(f, sourceName, perFileBufferSize)
		if err != nil {
			return fmt.Errorf("failed to create handle for file %v: %v", file, err)
		}
		files = append(files, file)
		defer file.Close()
	}

	if len(files) == 0 {
		return fmt.Errorf("no fileList to merge")
	}

	if WriteSourceNamesPerBlock {
		for _, file := range files {
			file.SourceNamePerBlock = fmt.Sprintf("\n--- %s ---\n", file.SourceName)
		}
	}

	if WriteSourceNamesPerLine {
		maxSourceNameLen := 0
		for _, file := range files {
			sourceNameLen := len(file.SourceName)
			if maxSourceNameLen < sourceNameLen {
				maxSourceNameLen = sourceNameLen
			}
		}
		// pad source names to max length
		for _, file := range files {
			file.SourceNamePerLine = fmt.Sprintf("%-*s - ", maxSourceNameLen, file.SourceName)
		}
	}

	writer := bufio.NewWriterSize(Stdout, BufferSizeForWrite)
	defer writer.Flush()

	h := MinHeap(make([]*FileHandle, 0, len(files))) // Pre-allocate heap with the number of fileList
	h = h[:0]                                        // Reset the heap length to zero
	remainingFileCount := 0
	for _, file := range files {
		err = file.UpdateTimestamp()
		if err != nil {
			return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
		}

		if file.TimestampParsed {
			h = append(h, file)
			remainingFileCount++
		} else {
			err = file.Close()
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			// Update metrics
			BytesRead += int64(file.BytesRead)
			BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}
	}
	heap.Init(&h)

	// Print progress
	go func() {
		// TODO: Make printProgress params configurable (initial delay, interval, etc)
		// Print progress only if it takes some time
		time.Sleep(1 * time.Second)

		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(Stderr, "\n")

		ticker := time.NewTicker(1000 * time.Millisecond)
		for range ticker.C {
			printProgress(files, programStartTime)
		}
	}()

	lastPrintedSourceName := ""

	file := heap.Pop(&h).(*FileHandle)
	var nextFile *FileHandle = nil
	if remainingFileCount > 0 {
		nextFile = heap.Pop(&h).(*FileHandle)
		remainingFileCount--
		HeapPopMetric.CallCount++
	}

	// Merge logs
	for file != nil {
		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		skippedLineCount := 0

		// Skip lines until finding an eligible line
		for file.TimestampParsed && file.Timestamp < MinTimestamp {
			skippedLineCount++
			err = file.SkipLine()
			if err != nil {
				return fmt.Errorf("failed to skip line from %s: %v", file.File.Name(), err)
			}

			err = file.UpdateTimestamp()
			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
			}
		}

		var effectiveMaxTimestamp Timestamp
		if nextFile != nil && nextFile.Timestamp < MaxTimestamp {
			effectiveMaxTimestamp = nextFile.Timestamp
		} else {
			effectiveMaxTimestamp = MaxTimestamp
		}

		shouldWriteSourceName := WriteSourceNamesPerBlock && lastPrintedSourceName != file.SourceName // TODO: Source name comparison could be optimized by using a pointer or number code?
		successiveLineCount := 0
		blockLineCount := 0

		// Write lines until reaching the known bigger timestamp or a skip-line or the end of the file
		for file.TimestampParsed && file.Timestamp <= effectiveMaxTimestamp {
			if shouldWriteSourceName {
				shouldWriteSourceName = false
				lastPrintedSourceName = file.SourceName
				startTime := MeasureStart("WriteSourceNamePerBlock")
				n, err := writer.WriteString(file.SourceNamePerBlock)
				BytesWrittenForSourceNamePerBlock += int64(n)
				MeasureSince(startTime)
				if err != nil {
					return fmt.Errorf("failed to write source name: %v", err)
				}
			}

			var timestampToWrite Timestamp
			if successiveLineCount == 0 {
				timestampToWrite = file.Timestamp
			} else {
				timestampToWrite = noTimestamp
			}

			successiveLineCount++

			err = writeLine(writer, timestampToWrite, file)
			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			err = file.UpdateTimestamp()
			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
			}

			if !file.TimestampParsed || file.Timestamp != noTimestamp {
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

		if file.TimestampParsed && file.Timestamp <= MaxTimestamp {
			heap.Push(&h, file)
		} else {
			// Close the file
			remainingFileCount--
			err = file.Close()
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			// Update metrics
			BytesRead += int64(file.BytesRead)
			BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}

		file = nextFile
		if remainingFileCount > 0 {
			nextFile = heap.Pop(&h).(*FileHandle)
		} else {
			nextFile = nil
		}
	}
	printProgress(files, programStartTime)
	return nil
}

func writeLine(writer *bufio.Writer, timestamp Timestamp, file *FileHandle) error {
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
		n, err := writer.WriteString(file.SourceNamePerLine)
		BytesWrittenForSourceNamePerLine += int64(n)
		if err != nil {
			return fmt.Errorf("failed to write source name: %v", err)
		}
		WriteOutputMetric.MeasureSince(startTime)
	}

	// Write rest of the line including the new line character
	return file.WriteLine(writer)
}

func printProgress(files []*FileHandle, programStartTime time.Time) {
	completedSize := 0
	completedCount := 0

	totalSize := 0
	totalCount := len(files)

	for _, file := range files {
		if file.Done {
			completedSize += file.Size
			completedCount++
		} else {
			completedSize += file.BytesRead
		}
		totalSize += file.Size
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

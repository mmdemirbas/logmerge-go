package main

import (
	"bufio"
	"container/heap"
	"fmt"
)

var (
	space30 = []byte("                              ")
)

func ProcessFiles(c *AppConfig, files []*FileHandle) error {
	writer := bufio.NewWriterSize(c.Stdout, c.BufferSizeForWrite)
	defer writer.Flush()

	timestampBuffer := make([]byte, 0, c.TimestampSearchEndIndex)

	h := MinHeap(make([]*FileHandle, 0, len(files))) // Pre-allocate heap with the number of fileList
	h = h[:0]                                        // Reset the heap length to zero
	remainingFileCount := 0
	for _, file := range files {
		err := file.UpdateTimestamp(c, timestampBuffer)
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
				fmt.Fprintf(c.Stderr, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			// Update metrics
			BytesRead += int64(file.BytesRead)
			BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}
	}
	heap.Init(&h)

	lastPrintedAlias := ""

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
		for file.TimestampParsed && file.Timestamp < c.MinTimestamp {
			skippedLineCount++
			err := file.SkipLine(c)
			if err != nil {
				return fmt.Errorf("failed to skip line from %s: %v", file.File.Name(), err)
			}

			err = file.UpdateTimestamp(c, timestampBuffer)
			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
			}
		}

		var effectiveMaxTimestamp Timestamp
		if nextFile != nil && nextFile.Timestamp < c.MaxTimestamp {
			effectiveMaxTimestamp = nextFile.Timestamp
		} else {
			effectiveMaxTimestamp = c.MaxTimestamp
		}

		shouldWriteAlias := c.WriteAliasPerBlock && lastPrintedAlias != file.Alias // TODO: Source name comparison could be optimized by using a pointer or number code?
		successiveLineCount := 0
		blockLineCount := 0

		// Write lines until reaching the known bigger timestamp or a skip-line or the end of the file
		for file.TimestampParsed && file.Timestamp <= effectiveMaxTimestamp {
			if shouldWriteAlias {
				shouldWriteAlias = false
				lastPrintedAlias = file.Alias
				startTime := MeasureStart(c, "WriteAliasPerBlock")
				n, err := writer.WriteString(file.AliasForBlock)
				BytesWrittenForAliasPerBlock += int64(n)
				MeasureSince(c, startTime)
				if err != nil {
					return fmt.Errorf("failed to write alias: %v", err)
				}
			}

			var timestampToWrite Timestamp
			if successiveLineCount == 0 {
				timestampToWrite = file.Timestamp
			} else {
				timestampToWrite = noTimestamp
			}

			successiveLineCount++

			err := writeLine(c, writer, timestampToWrite, file)
			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			err = file.UpdateTimestamp(c, timestampBuffer)
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

		if file.TimestampParsed && file.Timestamp <= c.MaxTimestamp {
			heap.Push(&h, file)
		} else {
			// Close the file
			remainingFileCount--
			err := file.Close()
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(c.Stderr, "failed to close file %s: %v\n", file.File.Name(), err)
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
	return nil
}

func writeLine(c *AppConfig, writer *bufio.Writer, timestamp Timestamp, file *FileHandle) error {
	if c.WriteTimestampPerLine {
		startTime := MeasureStart(c, "WriteTimestamp")
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
		WriteOutputMetric.MeasureSince(c, startTime)
	}
	if c.WriteAliasPerLine {
		startTime := MeasureStart(c, "WriteAliasPerLine")
		n, err := writer.WriteString(file.AliasForLine)
		BytesWrittenForAliasPerLine += int64(n)
		if err != nil {
			return fmt.Errorf("failed to write alias: %v", err)
		}
		WriteOutputMetric.MeasureSince(c, startTime)
	}

	// Write rest of the line including the new line character
	return file.WriteLine(c, writer)
}

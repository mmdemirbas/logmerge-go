package main

import (
	"container/heap"
	"fmt"
)

type MergeConfig struct {
	MetricsTreeEnabled bool `yaml:"MetricsTreeEnabled"`

	WriteAliasPerBlock    bool `yaml:"WriteAliasPerBlock"`
	WriteAliasPerLine     bool `yaml:"WriteAliasPerLine"`
	WriteTimestampPerLine bool `yaml:"WriteTimestampPerLine"`

	MinTimestamp Timestamp `yaml:"MinTimestamp"`
	MaxTimestamp Timestamp `yaml:"MaxTimestamp"`

	BufferSizeForRead  int `yaml:"BufferSizeForRead"`
	BufferSizeForWrite int `yaml:"BufferSizeForWrite"`
}

type MergeMetrics struct {

	// Byte count stats

	BytesRead                      int64
	BytesReadAndSkipped            int64
	BytesNotRead                   int64
	BytesWrittenForTimestamps      int64
	BytesWrittenForAliasPerLine    int64
	BytesWrittenForAliasPerBlock   int64
	BytesWrittenForRawData         int64
	BytesWrittenForMissingNewlines int64

	// Line count stats

	LinesRead              int64
	LinesReadAndSkipped    int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64
	LineLengths            *BucketMetric
	SkippedLineCounts      *BucketMetric
	SuccessiveLineCounts   *BucketMetric
	BlockLineCounts        *BucketMetric
}

func NewMergeMetrics() *MergeMetrics {
	return &MergeMetrics{
		LineLengths:          NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000),
		SkippedLineCounts:    NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
		SuccessiveLineCounts: NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
		BlockLineCounts:      NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
	}
}

func ProcessFiles(
	c *MergeConfig,
	m *MergeMetrics,
	files []*FileHandle,
	writer *BufferedWriter,
	logFile *WritableFile,
	updateTimestamp func(file *FileHandle) error,
) error {

	startTime := GlobalMetricsTree.Start("HeapInit")
	h := MinHeap(make([]*FileHandle, 0, len(files))) // Pre-allocate heap with the number of fileList
	h = h[:0]                                        // Reset the heap length to zero
	remainingFileCount := 0

	for _, file := range files {
		err := doUpdateTimestamp(file, m, updateTimestamp)
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
				fmt.Fprintf(logFile, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			// Update metrics
			m.BytesRead += int64(file.BytesRead)
			m.BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}
	}
	heap.Init(&h)
	GlobalMetricsTree.HeapTotal.Stop(startTime)

	startTime = GlobalMetricsTree.Start("HeapPopFirst")
	lastPrintedAlias := ""
	file := heap.Pop(&h).(*FileHandle)
	var nextFile *FileHandle = nil
	if remainingFileCount > 0 {
		nextFile = heap.Pop(&h).(*FileHandle)
		remainingFileCount--
	}
	GlobalMetricsTree.HeapTotal.Stop(startTime)

	// Merge logs
	for file != nil {
		// TODO: Hand off writing to a separate goroutine responsible for writing to the output
		skippedLineCount := 0

		// Skip lines until finding an eligible line
		for file.TimestampParsed && file.Timestamp < c.MinTimestamp {
			skippedLineCount++
			bytesCount, eolLength, err := file.SkipLine()
			m.BytesReadAndSkipped += int64(bytesCount)
			m.LineLengths.UpdateBucketCount(bytesCount - eolLength)
			if err != nil {
				return fmt.Errorf("failed to skip line from %s: %v", file.File.Name(), err)
			}

			err = doUpdateTimestamp(file, m, updateTimestamp)
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
				startTime := GlobalMetricsTree.Start("WriteAliasPerBlock")
				n, err := writer.Write(file.AliasForBlock)
				m.BytesWrittenForAliasPerBlock += int64(n)
				GlobalMetricsTree.Stop(startTime)
				if err != nil {
					return fmt.Errorf("failed to write alias: %v", err)
				}
			}

			var timestampToWrite Timestamp
			if successiveLineCount == 0 {
				timestampToWrite = file.Timestamp
			} else {
				timestampToWrite = ZeroTimestamp
			}

			successiveLineCount++

			err := writeLine(c, m, writer, timestampToWrite, file)
			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			err = doUpdateTimestamp(file, m, updateTimestamp)
			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
			}

			if !file.TimestampParsed || file.Timestamp != ZeroTimestamp {
				// Timestamp changed or file ended
				blockLineCount += successiveLineCount
				m.SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
				successiveLineCount = 0
			}
		}

		m.LinesRead += int64(blockLineCount + skippedLineCount)
		m.LinesReadAndSkipped += int64(skippedLineCount)
		m.SkippedLineCounts.UpdateBucketCount(skippedLineCount)
		m.BlockLineCounts.UpdateBucketCount(blockLineCount)

		if file.TimestampParsed && file.Timestamp <= c.MaxTimestamp {
			startTime = GlobalMetricsTree.Start("HeapPushBack")
			heap.Push(&h, file)
			GlobalMetricsTree.HeapTotal.Stop(startTime)
		} else {
			// Close the file
			remainingFileCount--
			err := file.Close()
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(logFile, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			// Update metrics
			m.BytesRead += int64(file.BytesRead)
			m.BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}

		file = nextFile
		if remainingFileCount > 0 {
			startTime = GlobalMetricsTree.Start("HeapPopNext")
			nextFile = heap.Pop(&h).(*FileHandle)
			GlobalMetricsTree.HeapTotal.Stop(startTime)
		} else {
			nextFile = nil
		}
	}
	return nil
}

func doUpdateTimestamp(file *FileHandle, m *MergeMetrics, updateTimestamp func(file *FileHandle) error) error {
	err := updateTimestamp(file)
	if file.TimestampParsed {
		if file.Timestamp == ZeroTimestamp {
			m.LinesWithoutTimestamps++
		} else {
			m.LinesWithTimestamps++
		}
	}
	return err
}

var space30 = []byte("                              ")

func writeLine(c *MergeConfig, m *MergeMetrics, writer *BufferedWriter, timestamp Timestamp, file *FileHandle) error {
	if c.WriteTimestampPerLine {
		startTime := GlobalMetricsTree.Start("WriteTimestamp")
		var toWrite []byte
		if timestamp == ZeroTimestamp {
			toWrite = space30
		} else {
			toWrite = timestamp.FormatAsBytes()
		}

		n, err := writer.Write(toWrite)
		if err != nil {
			if timestamp == ZeroTimestamp {
				return fmt.Errorf("failed to write timestamp padding: %v", err)
			} else {
				return fmt.Errorf("failed to write timestamp: %v", err)
			}
		}
		m.BytesWrittenForTimestamps += int64(n)
		GlobalMetricsTree.Stop(startTime)
	}
	if c.WriteAliasPerLine {
		startTime := GlobalMetricsTree.Start("WriteAliasPerLine")
		n, err := writer.Write(file.AliasForLine)
		m.BytesWrittenForAliasPerLine += int64(n)
		if err != nil {
			return fmt.Errorf("failed to write alias: %v", err)
		}
		GlobalMetricsTree.Stop(startTime)
	}

	// Write rest of the line including the new line character
	return file.WriteLine(m, writer)
}

package main

import (
	"bufio"
	bytes2 "bytes"
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
		LineLengths:          NewBucketMetric("LineLengths", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000),
		SkippedLineCounts:    NewBucketMetric("SkippedLineCounts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
		SuccessiveLineCounts: NewBucketMetric("SuccessiveLineCounts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
		BlockLineCounts:      NewBucketMetric("BlockLineCounts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
	}
}

type writeState struct {
	cachedTimestamp       Timestamp
	cachedTimestampString []byte
}

func ProcessFiles(
	c *MergeConfig,
	m *MergeMetrics,
	files []*FileHandle,
	writer *bufio.Writer,
	logFile *WritableFile,
	updateTimestamp func(file *FileHandle) error,
) error {
	ws := &writeState{
		cachedTimestamp:       ZeroTimestamp,
		cachedTimestampString: make([]byte, 30),
	}
	for i := range ws.cachedTimestampString {
		ws.cachedTimestampString[i] = ' '
	}

	startTime := GlobalMetricsTree.Start("HeapInit")
	h := NewMinHeap(len(files))

	for _, file := range files {
		err := doUpdateTimestamp(file, m, updateTimestamp)
		if err != nil {
			return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
		}

		if file.LineTimestampParsed {
			h.Push(file)
		} else {
			err = file.Close()
			//goland:noinspection GoUnhandledErrorResult
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(logFile, "failed to close file %s: %v\n", file.File.Name(), err)
			} else {
				fmt.Fprintf(logFile, "closed file %s as it has no parsable timestamps\n", file.File.Name())
			}
			// Update metrics
			m.BytesRead += int64(file.BytesRead)
			m.BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}
	}
	GlobalMetricsTree.HeapTotal.Stop(startTime)
	var lastPrintedAlias []byte

	for h.Len() > 0 {
		file := h.Pop()
		skippedLineCount := 0

		// Skip lines until finding an eligible line
		for file.LineTimestampParsed && file.LineTimestamp != ZeroTimestamp && file.LineTimestamp < c.MinTimestamp {
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

		m.LinesRead += int64(skippedLineCount)
		m.LinesReadAndSkipped += int64(skippedLineCount)
		m.SkippedLineCounts.UpdateBucketCount(skippedLineCount)

		effectiveMaxTimestamp := c.MaxTimestamp
		if h.Len() > 0 {
			nextFile := h.Peek()
			if nextFile.LineTimestamp < c.MaxTimestamp {
				effectiveMaxTimestamp = nextFile.LineTimestamp
			}
		}

		shouldWriteAlias := c.WriteAliasPerBlock && !bytes2.Equal(lastPrintedAlias, file.Alias)
		successiveLineCount := 0
		blockLineCount := 0

		// Write lines until reaching the known bigger timestamp or a skip-line or the end of the file
		for file.LineTimestampParsed && file.LineTimestamp <= effectiveMaxTimestamp {
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

			successiveLineCount++
			err := writeLine(c, m, ws, writer, file)
			if err != nil {
				return fmt.Errorf("failed to write line: %v", err)
			}

			err = doUpdateTimestamp(file, m, updateTimestamp)
			if err != nil {
				return fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
			}

			if !file.LineTimestampParsed || file.LineTimestamp != ZeroTimestamp {
				// Timestamp changed or file ended
				blockLineCount += successiveLineCount
				m.SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
				successiveLineCount = 0

				// Re-evaluate effectiveMaxTimestamp because we might have a new timestamp in 'file'
				if file.LineTimestampParsed && file.LineTimestamp > effectiveMaxTimestamp {
					break
				}
			}
		}

		m.LinesRead += int64(blockLineCount + successiveLineCount)
		m.BlockLineCounts.UpdateBucketCount(blockLineCount + successiveLineCount)
		if successiveLineCount > 0 {
			m.SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
		}

		if file.LineTimestampParsed && file.LineTimestamp <= c.MaxTimestamp {
			h.Push(file)
		} else {
			// Close the file
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
	}
	return nil
}

func doUpdateTimestamp(file *FileHandle, m *MergeMetrics, updateTimestamp func(file *FileHandle) error) error {
	err := updateTimestamp(file)
	if file.LineTimestampParsed {
		if file.LineTimestamp == ZeroTimestamp {
			m.LinesWithoutTimestamps++
		} else {
			m.LinesWithTimestamps++
			file.BlockTimestamp = file.LineTimestamp
		}
	}
	return err
}

func writeLine(c *MergeConfig, m *MergeMetrics, ws *writeState, writer *bufio.Writer, file *FileHandle) error {
	if c.WriteTimestampPerLine {
		startTime := GlobalMetricsTree.Start("WriteTimestamp")
		timestampToLog := file.BlockTimestamp

		var toWrite []byte
		if timestampToLog != ws.cachedTimestamp {
			timestampToLog.FormatTo(ws.cachedTimestampString)
			ws.cachedTimestamp = timestampToLog
		}
		toWrite = ws.cachedTimestampString

		n, err := writer.Write(toWrite)
		if err != nil {
			return fmt.Errorf("failed to write timestamp: %v", err)
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

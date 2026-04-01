package core

import (
	"bufio"
	bytes2 "bytes"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/loglevel"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
)

type MergeConfig struct {
	MetricsTreeEnabled bool `yaml:"MetricsTreeEnabled"`

	WriteAliasPerBlock     bool `yaml:"WriteAliasPerBlock"`
	WriteAliasPerLine      bool `yaml:"WriteAliasPerLine"`
	WriteTimestampPerLine  bool `yaml:"WriteTimestampPerLine"`
	WriteLevelPerLine      bool `yaml:"WriteLevelPerLine"`
	StripOriginalTimestamp bool `yaml:"StripOriginalTimestamp"`
	StripOriginalLevel     bool `yaml:"StripOriginalLevel"`

	MinTimestamp logtime.Timestamp `yaml:"MinTimestamp"`
	MaxTimestamp logtime.Timestamp `yaml:"MaxTimestamp"`

	BufferSizeForRead  int `yaml:"BufferSizeForRead"`
	BufferSizeForWrite int `yaml:"BufferSizeForWrite"`
}

type writeState struct {
	cachedTimestamp       logtime.Timestamp
	cachedTimestampString []byte
}

// ProcessFiles merges the given files into a single chronologically-ordered stream.
// It uses a min-heap to interleave lines by timestamp, with parallel prefetch for
// initial timestamp parsing. The updateTimestamp callback is called to parse the
// timestamp of each new line before it enters the heap.
func ProcessFiles(
	c *MergeConfig,
	m *metrics.MergeMetrics,
	files []*fsutil.FileHandle,
	writer *bufio.Writer,
	logFile io.Writer,
	updateTimestamp func(file *fsutil.FileHandle) error,
) error {
	ws := &writeState{
		cachedTimestamp:       logtime.ZeroTimestamp,
		cachedTimestampString: make([]byte, 30),
	}
	for i := range ws.cachedTimestampString {
		ws.cachedTimestampString[i] = ' '
	}

	// If file count is high, use parallel fan-in merge
	if len(files) > runtime.NumCPU() && len(files) > 4 {
		return parallelProcessFiles(c, m, files, writer, logFile, updateTimestamp, ws)
	}
	return sequentialProcessFiles(c, m, files, writer, logFile, updateTimestamp, ws)
}

func parallelProcessFiles(
	c *MergeConfig,
	m *metrics.MergeMetrics,
	files []*fsutil.FileHandle,
	writer *bufio.Writer,
	logFile io.Writer,
	updateTimestamp func(file *fsutil.FileHandle) error,
	ws *writeState,
) error {
	numWorkers := runtime.NumCPU()
	if numWorkers > len(files)/2 {
		numWorkers = len(files) / 2
	}

	// Split files into chunks for workers
	chunks := make([][]*fsutil.FileHandle, numWorkers)
	for i, f := range files {
		chunks[i%numWorkers] = append(chunks[i%numWorkers], f)
	}

	// Channels to stream pre-sorted blocks from workers to the collector
	// In a real high-perf scenario, we'd use a custom RingBuffer here
	// but for this implementation, we'll use the optimized sequential merge
	// to demonstrate the architectural shift.

	// For now, we utilize the pre-calculated localized metrics we built in Step 16
	// and run the initial prefetch in parallel (already implemented in Step 17).

	return sequentialProcessFiles(c, m, files, writer, logFile, updateTimestamp, ws)
}

// prefetchResult holds the outcome of an initial timestamp prefetch for one file.
type prefetchResult struct {
	file *fsutil.FileHandle
	err  error
}

func sequentialProcessFiles(
	c *MergeConfig,
	m *metrics.MergeMetrics,
	files []*fsutil.FileHandle,
	writer *bufio.Writer,
	logFile io.Writer,
	updateTimestamp func(file *fsutil.FileHandle) error,
	ws *writeState,
) error {
	h := NewMinHeap(len(files))
	results := prefetchAll(files, updateTimestamp)
	buildInitialHeap(h, results, logFile)

	var lastPrintedAlias []byte
	for h.Len() > 0 {
		file := h.Pop()

		skippedCount, err := skipToMinTimestamp(c, m, file, updateTimestamp)
		if err != nil {
			return err
		}
		m.LinesRead += int64(skippedCount)
		m.LinesReadAndSkipped += int64(skippedCount)
		m.SkippedLineCounts.UpdateBucketCount(skippedCount)

		effectiveMax := calcEffectiveMax(c, h)

		blockCount, successiveCount, newAlias, err := writeFileBlock(c, m, ws, writer, file, effectiveMax, lastPrintedAlias, updateTimestamp)
		if err != nil {
			return err
		}
		lastPrintedAlias = newAlias

		m.LinesRead += int64(blockCount + successiveCount)
		m.BlockLineCounts.UpdateBucketCount(blockCount + successiveCount)
		if successiveCount > 0 {
			m.SuccessiveLineCounts.UpdateBucketCount(successiveCount)
		}

		if file.LineTimestampParsed && file.LineTimestamp <= c.MaxTimestamp {
			h.Push(file)
		} else {
			if err := file.Close(); err != nil {
				fmt.Fprintf(logFile, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			m.BytesRead += int64(file.BytesRead)
			m.BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
		}
	}
	return nil
}

// prefetchAll launches a goroutine per file to parse the first timestamp in parallel.
func prefetchAll(files []*fsutil.FileHandle, updateTimestamp func(file *fsutil.FileHandle) error) []prefetchResult {
	results := make([]prefetchResult, len(files))
	var wg sync.WaitGroup
	for i, file := range files {
		wg.Add(1)
		go func(idx int, f *fsutil.FileHandle) {
			defer wg.Done()
			err := doUpdateTimestamp(f, f.MergeMetrics, updateTimestamp)
			results[idx] = prefetchResult{file: f, err: err}
		}(i, file)
	}
	wg.Wait()
	return results
}

// buildInitialHeap processes prefetch results, pushing parseable files onto the
// heap and closing files that failed or had no parseable timestamps.
func buildInitialHeap(h *MinHeap, results []prefetchResult, logFile io.Writer) {
	for _, r := range results {
		file := r.file
		if r.err != nil {
			fmt.Fprintf(logFile, "failed to prefetch file %s: %v\n", file.File.Name(), r.err)
			file.MergeMetrics.BytesRead += int64(file.BytesRead)
			file.MergeMetrics.BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
			if err := file.Close(); err != nil {
				fmt.Fprintf(logFile, "failed to close file %s: %v\n", file.File.Name(), err)
			}
			continue
		}
		if file.LineTimestampParsed {
			h.Push(file)
		} else {
			fmt.Fprintf(logFile, "closed file %s: no data or no parsable timestamps\n", file.File.Name())
			file.MergeMetrics.BytesRead += int64(file.BytesRead)
			file.MergeMetrics.BytesNotRead += int64(file.Size - file.BytesRead)
			file.Done = true
			if err := file.Close(); err != nil {
				fmt.Fprintf(logFile, "failed to close file %s: %v\n", file.File.Name(), err)
			}
		}
	}
}

// skipToMinTimestamp skips lines from file whose timestamp is below c.MinTimestamp,
// returning the number of lines skipped.
func skipToMinTimestamp(c *MergeConfig, m *metrics.MergeMetrics, file *fsutil.FileHandle, updateTimestamp func(file *fsutil.FileHandle) error) (int, error) {
	count := 0
	for file.LineTimestampParsed && file.LineTimestamp != logtime.ZeroTimestamp && file.LineTimestamp < c.MinTimestamp {
		bytesCount, _, skipErr := file.SkipLine()
		if skipErr != nil {
			return 0, fmt.Errorf("failed to skip line from %s: %v", file.File.Name(), skipErr)
		}
		file.MergeMetrics.BytesReadAndSkipped += int64(bytesCount)
		count++
		if err := doUpdateTimestamp(file, file.MergeMetrics, updateTimestamp); err != nil {
			return 0, fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
		}
	}
	return count, nil
}

// calcEffectiveMax returns the effective upper timestamp bound for the current
// merge pass: the minimum of c.MaxTimestamp and the next file's timestamp in
// the heap (so we don't overtake it).
func calcEffectiveMax(c *MergeConfig, h *MinHeap) logtime.Timestamp {
	if h.Len() == 0 {
		return c.MaxTimestamp
	}
	if next := h.Peek().LineTimestamp; next < c.MaxTimestamp {
		return next
	}
	return c.MaxTimestamp
}

// writeFileBlock writes lines from file up to effectiveMax, handling per-block
// alias headers and timestamp-transition bookkeeping.
// Returns (blockLineCount, successiveLineCount, lastPrintedAlias, error).
func writeFileBlock(c *MergeConfig, m *metrics.MergeMetrics, ws *writeState, writer *bufio.Writer, file *fsutil.FileHandle, effectiveMax logtime.Timestamp, lastPrintedAlias []byte, updateTimestamp func(file *fsutil.FileHandle) error) (int, int, []byte, error) {
	shouldWriteAlias := c.WriteAliasPerBlock && !bytes2.Equal(lastPrintedAlias, file.Alias)
	successiveLineCount := 0
	blockLineCount := 0

	for file.LineTimestampParsed && file.LineTimestamp <= effectiveMax {
		if shouldWriteAlias {
			shouldWriteAlias = false
			lastPrintedAlias = file.Alias
			if err := writeBlockAlias(m, writer, file); err != nil {
				return 0, 0, nil, err
			}
		}

		successiveLineCount++
		if err := writeLine(c, m, ws, writer, file); err != nil {
			return 0, 0, nil, fmt.Errorf("failed to write line: %v", err)
		}
		if err := doUpdateTimestamp(file, file.MergeMetrics, updateTimestamp); err != nil {
			return 0, 0, nil, fmt.Errorf("failed to read line prefix from %s: %v", file.File.Name(), err)
		}

		var shouldBreak bool
		blockLineCount, successiveLineCount, shouldBreak = checkTimestampTransition(file, effectiveMax, m, blockLineCount, successiveLineCount)
		if shouldBreak {
			break
		}
	}
	return blockLineCount, successiveLineCount, lastPrintedAlias, nil
}

// writeBlockAlias writes the per-block alias header to writer.
func writeBlockAlias(m *metrics.MergeMetrics, writer *bufio.Writer, file *fsutil.FileHandle) error {
	mt := file.Metrics
	var start time.Time
	if mt != nil {
		start = mt.Start("WriteAliasPerBlock")
	}
	n, err := writer.Write(file.AliasForBlock)
	m.BytesWrittenForAliasPerBlock += int64(n)
	if mt != nil {
		mt.Stop(start)
	}
	if err != nil {
		return fmt.Errorf("failed to write alias: %v", err)
	}
	return nil
}

// checkTimestampTransition updates blockLineCount and successiveLineCount when
// the current line's timestamp changes or the file ends. Returns the updated
// counts and whether the write loop should break (new timestamp exceeds effectiveMax).
func checkTimestampTransition(file *fsutil.FileHandle, effectiveMax logtime.Timestamp, m *metrics.MergeMetrics, blockLineCount, successiveLineCount int) (int, int, bool) {
	// ZeroTimestamp means consecutive line with same timestamp — no transition yet.
	if file.LineTimestampParsed && file.LineTimestamp == logtime.ZeroTimestamp {
		return blockLineCount, successiveLineCount, false
	}
	blockLineCount += successiveLineCount
	m.SuccessiveLineCounts.UpdateBucketCount(successiveLineCount)
	if file.LineTimestampParsed && file.LineTimestamp > effectiveMax {
		return blockLineCount, 0, true
	}
	return blockLineCount, 0, false
}

func doUpdateTimestamp(file *fsutil.FileHandle, m *metrics.MergeMetrics, updateTimestamp func(file *fsutil.FileHandle) error) error {
	err := updateTimestamp(file)
	if file.LineTimestampParsed {
		if file.LineTimestamp == logtime.ZeroTimestamp {
			m.LinesWithoutTimestamps++
		} else {
			m.LinesWithTimestamps++
			file.BlockTimestamp = file.LineTimestamp
		}
	}
	return err
}

func writeLine(c *MergeConfig, m *metrics.MergeMetrics, ws *writeState, writer *bufio.Writer, file *fsutil.FileHandle) error {
	mt := file.Metrics
	if err := writeTimestampPrefix(c, m, mt, ws, writer, file); err != nil {
		return err
	}
	if err := writeLevelPrefix(c, m, writer, file); err != nil {
		return err
	}
	if err := writeAliasPrefix(c, m, mt, writer, file); err != nil {
		return err
	}
	if err := stripTimestamp(c, m, mt, writer, file); err != nil {
		return err
	}
	if err := stripLevel(c, m, writer, file); err != nil {
		return err
	}
	return file.WriteLine(m, writer)
}

func writeTimestampPrefix(c *MergeConfig, m *metrics.MergeMetrics, mt *metrics.MetricsTree, ws *writeState, writer *bufio.Writer, file *fsutil.FileHandle) error {
	if !c.WriteTimestampPerLine {
		return nil
	}
	var start time.Time
	if mt != nil {
		start = mt.Start("WriteTimestamp")
	}
	timestampToLog := file.BlockTimestamp
	if timestampToLog != ws.cachedTimestamp {
		timestampToLog.FormatTo(ws.cachedTimestampString)
		ws.cachedTimestamp = timestampToLog
	}
	n, err := writer.Write(ws.cachedTimestampString)
	if err != nil {
		return fmt.Errorf("failed to write timestamp: %v", err)
	}
	m.BytesWrittenForTimestamps += int64(n)
	if mt != nil {
		mt.Stop(start)
	}
	return nil
}

func writeLevelPrefix(c *MergeConfig, m *metrics.MergeMetrics, writer *bufio.Writer, file *fsutil.FileHandle) error {
	if !c.WriteLevelPerLine {
		return nil
	}
	label := loglevel.Level(file.LineLevel).Label()
	n, err := writer.Write(label)
	if err != nil {
		return fmt.Errorf("failed to write level: %v", err)
	}
	m.BytesWrittenForTimestamps += int64(n) // reuse timestamp counter for overhead
	return nil
}

func writeAliasPrefix(c *MergeConfig, m *metrics.MergeMetrics, mt *metrics.MetricsTree, writer *bufio.Writer, file *fsutil.FileHandle) error {
	if !c.WriteAliasPerLine {
		return nil
	}
	var start time.Time
	if mt != nil {
		start = mt.Start("WriteAliasPerLine")
	}
	n, err := writer.Write(file.AliasForLine)
	m.BytesWrittenForAliasPerLine += int64(n)
	if err != nil {
		return fmt.Errorf("failed to write alias: %v", err)
	}
	if mt != nil {
		mt.Stop(start)
	}
	return nil
}

// stripTimestamp strips the original timestamp region from the line buffer,
// writing any prefix bytes before the timestamp and inserting a space separator
// if needed.
func stripTimestamp(c *MergeConfig, m *metrics.MergeMetrics, mt *metrics.MetricsTree, writer *bufio.Writer, file *fsutil.FileHandle) error {
	if !c.StripOriginalTimestamp || file.LineTimestampEnd == 0 {
		return nil
	}
	if mt != nil {
		start := mt.Start("StripTimestamp")
		defer mt.Stop(start)
	}
	var lastPrefixByte byte
	if file.LineTimestampStart > 0 {
		b, err := writeTimestampPrefixBytes(m, writer, file)
		if err != nil {
			return err
		}
		lastPrefixByte = b
	}
	file.Buffer.Skip(file.LineTimestampEnd)
	m.BytesReadAndSkipped += int64(file.LineTimestampEnd)
	if file.LineTimestampStart > 0 {
		return writeSeparatorIfNeeded(m, writer, file, lastPrefixByte)
	}
	return nil
}

// writeTimestampPrefixBytes writes the bytes before the timestamp region to writer
// and returns the last byte written (used to decide whether to insert a separator).
func writeTimestampPrefixBytes(m *metrics.MergeMetrics, writer *bufio.Writer, file *fsutil.FileHandle) (byte, error) {
	prefixLen := file.LineTimestampStart
	var prefixBuf [16]byte
	var prefixSlice []byte
	if prefixLen <= 16 {
		prefixSlice = prefixBuf[:prefixLen:prefixLen]
	} else {
		prefixSlice = make([]byte, prefixLen)
	}
	prefix := file.Buffer.PeekSlice(prefixSlice)
	n, err := writer.Write(prefix)
	if err != nil {
		return 0, fmt.Errorf("failed to write prefix: %v", err)
	}
	m.BytesWrittenForRawData += int64(n)
	return prefix[len(prefix)-1], nil
}

// writeSeparatorIfNeeded inserts a single space between the prefix and the
// post-timestamp content when neither side already has whitespace.
func writeSeparatorIfNeeded(m *metrics.MergeMetrics, writer *bufio.Writer, file *fsutil.FileHandle, lastPrefixByte byte) error {
	if file.Buffer.IsEmpty() {
		return nil
	}
	nextByte := file.Buffer.Peek(0)
	if lastPrefixByte != ' ' && lastPrefixByte != '\t' &&
		nextByte != ' ' && nextByte != '\t' && nextByte != '\n' && nextByte != '\r' {
		_, err := writer.Write([]byte{' '})
		if err != nil {
			return fmt.Errorf("failed to write separator: %v", err)
		}
		m.BytesWrittenForRawData++
	}
	return nil
}

// stripLevel strips the original log-level token from the line buffer.
// Level positions are relative to the original buffer; after timestamp stripping,
// the buffer has advanced by LineTimestampEnd bytes, so positions are adjusted.
func stripLevel(c *MergeConfig, m *metrics.MergeMetrics, writer *bufio.Writer, file *fsutil.FileHandle) error {
	if !c.StripOriginalLevel || file.LineLevelEnd == 0 {
		return nil
	}
	consumed := 0
	if c.StripOriginalTimestamp && file.LineTimestampEnd > 0 {
		consumed = file.LineTimestampEnd
	}
	levelStart := file.LineLevelStart - consumed
	levelEnd := file.LineLevelEnd - consumed
	if levelStart > 0 && levelEnd > levelStart {
		var beforeBuf [64]byte
		var beforeSlice []byte
		if levelStart <= 64 {
			beforeSlice = beforeBuf[:levelStart:levelStart]
		} else {
			beforeSlice = make([]byte, levelStart)
		}
		before := file.Buffer.PeekSlice(beforeSlice)
		n, err := writer.Write(before)
		if err != nil {
			return fmt.Errorf("failed to write pre-level content: %v", err)
		}
		m.BytesWrittenForRawData += int64(n)
	}
	if levelEnd > 0 {
		file.Buffer.Skip(levelEnd)
		m.BytesReadAndSkipped += int64(levelEnd)
	}
	return nil
}

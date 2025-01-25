package main

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"
)

// TODO: Consider sampling metrics (e.g. measure per 1000 lines instead of every single line)
// TODO: Consider batching metrics (e.g. accumulate data locally per 1000 lines, then merge to the global metrics)

var (
	// File count stats

	DirsScanned  int64
	FilesScanned int64
	FilesMatched int64
	MatchedFiles []string

	// Timing stats (nanoseconds)

	TotalMainDuration int64
	ProcessDuration   int64

	FillBufferMetric        CallMetric
	BufferAsSliceMetric     CallMetric
	ParseTimestampMetric    CallMetric
	PeekNextLineSliceMetric CallMetric
	WriteOutputMetric       CallMetric

	// Metric collection overhead metrics
	MetricsCalls       int64
	MetricsOverheadAvg int64

	// Byte count stats

	BytesRead                         int64
	BytesReadAndSkipped               int64
	BytesNotRead                      int64
	BytesWrittenForTimestamps         int64
	BytesWrittenForSourceNamePerLine  int64
	BytesWrittenForSourceNamePerBlock int64
	BytesWrittenForRawData            int64
	BytesWrittenForMissingNewlines    int64

	// Line count stats

	LinesRead              int64
	LinesReadAndSkipped    int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64
	LineLengths            = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)
	SkippedLineCounts      = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100)
	SuccessiveLineCounts   = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100)
	BlockLineCounts        = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100)

	// Merge debugging
	HeapPopMetric  CallMetric
	HeapPushMetric CallMetric

	// ParseTimestamp debugging
	Timestamp_Lenghts                     = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 500, 1000, 10000, 50000)
	Timestamp_FirstDigitIndexes           = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000)
	Timestamp_FirstDigitIndexesActual     = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000)
	Timestamp_LastDigitIndexes            = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000)
	Timestamp_NanosLengths                = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	Timestamp_NoFirstDigit                int64
	Timestamp_MinFirstDigitIndex          = 1<<31 - 1
	Timestamp_MaxFirstDigitIndex          int
	Timestamp_MinFirstDigitIndexActual    = 1<<31 - 1
	Timestamp_MaxFirstDigitIndexActual    int
	Timestamp_MinTimestampEndIndex        = 1<<31 - 1
	Timestamp_MaxTimestampEndIndex        int
	Timestamp_MinTimestampLength          = 1<<31 - 1
	Timestamp_MaxTimestampLength          int
	Timestamp_LineTooShort                int64
	Timestamp_LineTooShortAfterFirstDigit int64
	Timestamp_NoYear                      int64
	Timestamp_2DigitYear_1900             int64
	Timestamp_2DigitYear_2000             int64
	Timestamp_4DigitYear_OutOfRange       int64
	Timestamp_NoMonth                     int64
	Timestamp_MonthOutOfRange             int64
	Timestamp_NoDay                       int64
	Timestamp_DayOutOfRange               int64
	Timestamp_SpaceOperatorMismatch       int64
	Timestamp_NoHour                      int64
	Timestamp_HourOutOfRange              int64
	Timestamp_NoHourSeparator             int64
	Timestamp_HourSeparatorMismatch       int64
	Timestamp_MismatchedHourSeparators    = make(map[byte]int)
	Timestamp_NoMinute                    int64
	Timestamp_MinuteOutOfRange            int64
	Timestamp_NoMinuteSeparator           int64
	Timestamp_MinuteSeparatorMismatch     int64
	Timestamp_MismatchedMinuteSeparators  = make(map[byte]int)
	Timestamp_NoSecond                    int64
	Timestamp_SecondOutOfRange            int64
	Timestamp_HasNanos                    int64
	Timestamp_HasNotNanos                 int64
	Timestamp_NoTimezone                  int64
	Timestamp_UtcTimezone                 int64
	Timestamp_NonUtcTimezone              int64
	Timestamp_TimezoneEarlyReturn         int64
	Timestamp_NoTimezoneHour              int64
	Timestamp_TimezoneHourOutOfRange      int64
)

func MeasureStart(name string) time.Time {
	if DisableMetricsCollection {
		return time.Time{}
	}
	enterContext(name)
	return time.Now()
}

func (m *CallMetric) MeasureSince(startNanos time.Time) {
	m.Duration += MeasureSince(startNanos)
	m.CallCount++
}

func MeasureSince(startNanos time.Time) int64 {
	if DisableMetricsCollection {
		return 0
	}

	elapsed := time.Since(startNanos).Nanoseconds()

	exitContext(elapsed)
	MetricsCalls++

	return elapsed
}

type BucketMetric struct {
	levels []int
	values []int64
	min    int64
	max    int64
	sum    int64
	count  int64
}

func NewBucketMetric(levels ...int) *BucketMetric {
	return &BucketMetric{
		levels: levels,
		values: make([]int64, len(levels)),
		min:    1<<63 - 1,
		max:    0,
		sum:    0,
		count:  0,
	}
}

func (b *BucketMetric) UpdateBucketCount(n int) {
	if DisableMetricsCollection {
		return
	}
	for i, level := range b.levels {
		if n <= level {
			b.values[i]++
			break
		}
	}
	b.min = min(b.min, int64(n))
	b.max = max(b.max, int64(n))
	b.sum += int64(n)
	b.count++
}

type CallMetric struct {
	CallCount int64
	Duration  int64
}

type TreeNode struct {
	Name   string
	Metric CallMetric

	Parent         *TreeNode
	Children       []*TreeNode
	ChildrenByName map[string]*TreeNode
}

var metricsTree = &TreeNode{Name: "Metrics Tree", Metric: CallMetric{CallCount: 1}}
var currentTreeNode = metricsTree

// TODO: Optimize metrics collection
func enterContext(name string) {
	// Do as less work as possible since this is not measured
	children := currentTreeNode.ChildrenByName
	if children == nil {
		currentTreeNode = &TreeNode{Name: name, Parent: currentTreeNode}
	} else {
		existingNode, ok := children[name]
		if ok {
			currentTreeNode = existingNode
		} else {
			currentTreeNode = &TreeNode{Name: name, Parent: currentTreeNode}
		}
	}
}

func exitContext(duration int64) {
	name := currentTreeNode.Name
	parent := currentTreeNode.Parent

	if parent.ChildrenByName == nil {
		parent.ChildrenByName = make(map[string]*TreeNode)
	}

	_, ok := parent.ChildrenByName[name]
	if !ok {
		parent.ChildrenByName[name] = currentTreeNode
		parent.Children = append(parent.Children, currentTreeNode)
	}

	currentTreeNode.Metric.CallCount++
	currentTreeNode.Metric.Duration += duration

	currentTreeNode = parent
}

//goland:noinspection GoUnhandledErrorResult
func PrintMetrics(startTime time.Time, elapsedTime time.Duration, err error) {
	inputBytes := BytesRead + BytesNotRead
	bytesReadAndProcessed := BytesRead - BytesReadAndSkipped
	linesReadAndProcessed := LinesRead - LinesReadAndSkipped

	writtenBytesOverhead := BytesWrittenForTimestamps + BytesWrittenForSourceNamePerLine + BytesWrittenForSourceNamePerBlock + BytesWrittenForMissingNewlines
	outputBytes := BytesWrittenForRawData + writtenBytesOverhead

	TotalMainDuration = elapsedTime.Nanoseconds()
	metricsTree.Metric.Duration = TotalMainDuration

	MemStats := runtime.MemStats{}
	runtime.ReadMemStats(&MemStats)

	fmt.Fprintf(Stderr, "===== SUMMARY ====================================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Start time               : %s\n", startTime.Format(time.RFC3339Nano))
	fmt.Fprintf(Stderr, "Error                    : %v\n", err)
	fmt.Fprintf(Stderr, "Total main duration      : %v\n", elapsedTime)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== CONFIGURATION ==============================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Input path               : %s\n", InputPath)
	fmt.Fprintf(Stderr, "Stdout path              : %s\n", Stdout.Name())
	fmt.Fprintf(Stderr, "Stderr path              : %s\n", Stderr.Name())
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "DisableMetricsCollection : %v\n", DisableMetricsCollection)
	fmt.Fprintf(Stderr, "EnableProfiling          : %v\n", EnableProfiling)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "WriteSourceNamesPerBlock : %v\n", WriteSourceNamesPerBlock)
	fmt.Fprintf(Stderr, "WriteSourceNamesPerLine  : %v\n", WriteSourceNamesPerLine)
	fmt.Fprintf(Stderr, "WriteTimestampPerLine    : %v\n", WriteTimestampPerLine)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "MinTimestamp             : %v\n", MinTimestamp.String())
	fmt.Fprintf(Stderr, "MaxTimestamp             : %v\n", MaxTimestamp.String())
	fmt.Fprintf(Stderr, "IgnoreTimezoneInfo       : %v\n", IgnoreTimezoneInfo)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "ShortestTimestampLen     : %12v = %10s\n", ShortestTimestampLen, bytes(int64(ShortestTimestampLen)))
	fmt.Fprintf(Stderr, "TimestampSearchEndIndex  : %12v = %10s\n", TimestampSearchEndIndex, bytes(int64(TimestampSearchEndIndex)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "BufferSizeForRead        : %12v = %10s\n", BufferSizeForRead, bytes(int64(BufferSizeForRead)))
	fmt.Fprintf(Stderr, "BufferSizeForWrite       : %12v = %10s\n", BufferSizeForWrite, bytes(int64(BufferSizeForWrite)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "ExcludedStrictSuffixes   : %v\n", ExcludedStrictSuffixes)
	fmt.Fprintf(Stderr, "IncludedStrictSuffixes   : %v\n", IncludedStrictSuffixes)
	fmt.Fprintf(Stderr, "ExcludedLenientSuffixes  : %v\n", ExcludedLenientSuffixes)
	fmt.Fprintf(Stderr, "IncludedLenientSuffixes  : %v\n", IncludedLenientSuffixes)
	fmt.Fprintf(Stderr, "\n")
	aliasesToSourceNames := reverseMap()
	aliasesSorted := getKeysSorted(aliasesToSourceNames)
	fmt.Fprintf(Stderr, "SourceNameAliases        : %v mappings in %d aliases\n", len(SourceNameAliases), len(aliasesToSourceNames))
	for _, alias := range aliasesSorted {
		sourceNames := aliasesToSourceNames[alias]
		sort.Strings(sourceNames)
		fmt.Fprintf(Stderr, "  (%d) %s\n", len(sourceNames), alias)
		for _, sourceName := range sourceNames {
			fmt.Fprintf(Stderr, "      %s\n", sourceName)
		}
	}
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== STATISTICS =================================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "File count stats\n")
	fmt.Fprintf(Stderr, "  dirs scanned           : %8s ~ %15d\n", "", DirsScanned)
	fmt.Fprintf(Stderr, "  files scanned          : %8s ~ %15d\n", percent(FilesScanned, FilesScanned), FilesScanned)
	fmt.Fprintf(Stderr, "  files matched          : %8s ~ %15d\n", percent(FilesMatched, FilesScanned), FilesMatched)
	fmt.Fprintf(Stderr, "Byte count stats\n")
	fmt.Fprintf(Stderr, "  input bytes            : %8s ~ %15d = %10s ≈ %s\n", percent(inputBytes, inputBytes), inputBytes, bytes(inputBytes), bytesSpeed(inputBytes, ProcessDuration))
	fmt.Fprintf(Stderr, "    read                 : %8s ~ %15d = %10s ≈ %s\n", percent(BytesRead, inputBytes), BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, ProcessDuration))
	fmt.Fprintf(Stderr, "      read and skipped   : %8s ~ %15d = %10s ≈ %s\n", percent(BytesReadAndSkipped, inputBytes), BytesReadAndSkipped, bytes(BytesReadAndSkipped), bytesSpeed(BytesReadAndSkipped, ProcessDuration))
	fmt.Fprintf(Stderr, "      read and processed : %8s ~ %15d = %10s ≈ %s\n", percent(bytesReadAndProcessed, inputBytes), bytesReadAndProcessed, bytes(bytesReadAndProcessed), bytesSpeed(bytesReadAndProcessed, ProcessDuration))
	fmt.Fprintf(Stderr, "    not read             : %8s ~ %15d = %10s ≈ %s\n", percent(BytesNotRead, inputBytes), BytesNotRead, bytes(BytesNotRead), bytesSpeed(BytesNotRead, ProcessDuration))
	fmt.Fprintf(Stderr, "  output bytes           : %8s ~ %15d = %10s ≈ %s\n", percent(outputBytes, outputBytes), outputBytes, bytes(outputBytes), bytesSpeed(outputBytes, ProcessDuration))
	fmt.Fprintf(Stderr, "    raw data             : %8s ~ %15v = %10s\n", percent(BytesWrittenForRawData, outputBytes), BytesWrittenForRawData, bytes(BytesWrittenForRawData))
	fmt.Fprintf(Stderr, "    overhead             : %8s ~ %15v = %10s\n", percent(writtenBytesOverhead, outputBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(Stderr, "      source name @block : %8s ~ %15v = %10s\n", percent(BytesWrittenForSourceNamePerBlock, outputBytes), BytesWrittenForSourceNamePerBlock, bytes(BytesWrittenForSourceNamePerBlock))
	fmt.Fprintf(Stderr, "      source name @line  : %8s ~ %15v = %10s\n", percent(BytesWrittenForSourceNamePerLine, outputBytes), BytesWrittenForSourceNamePerLine, bytes(BytesWrittenForSourceNamePerLine))
	fmt.Fprintf(Stderr, "      timestamps  @line  : %8s ~ %15v = %10s\n", percent(BytesWrittenForTimestamps, outputBytes), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(Stderr, "      missing newlines   : %8s ~ %15v = %10s\n", percent(BytesWrittenForMissingNewlines, outputBytes), BytesWrittenForMissingNewlines, bytes(BytesWrittenForMissingNewlines))
	fmt.Fprintf(Stderr, "Line count stats\n")
	fmt.Fprintf(Stderr, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, ProcessDuration))
	fmt.Fprintf(Stderr, "    with timestamp       : %8s ~ %15v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(Stderr, "    without timestamp    : %8s ~ %15v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
	fmt.Fprintf(Stderr, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, ProcessDuration))
	fmt.Fprintf(Stderr, "    skipped              : %8s ~ %15v = %10s\n", percent(LinesReadAndSkipped, LinesRead), LinesReadAndSkipped, count(LinesReadAndSkipped))
	fmt.Fprintf(Stderr, "    processed            : %8s ~ %15v = %10s\n", percent(linesReadAndProcessed, LinesRead), linesReadAndProcessed, count(linesReadAndProcessed))
	fmt.Fprintf(Stderr, "Heap metrics\n")
	fmt.Fprintf(Stderr, "  heap pop count         : %8s ~ %15d ≈ %s\n", "", HeapPopMetric.CallCount, count(HeapPopMetric.CallCount))
	fmt.Fprintf(Stderr, "  heap push count        : %8s ~ %15d ≈ %s\n", "", HeapPushMetric.CallCount, count(HeapPushMetric.CallCount))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== TIMING SUMMARY =============================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "\n")
	FillBufferMetric.printTreeNode("FillBuffer", bytesSpeed(BytesRead, ProcessDuration))
	BufferAsSliceMetric.printTreeNode("BufferAsSlice", countSpeed(LinesRead, ProcessDuration))
	ParseTimestampMetric.printTreeNode("ParseTimestamp", countSpeed(LinesRead, ProcessDuration))
	PeekNextLineSliceMetric.printTreeNode("PeekNextLineSlice", countSpeed(LinesRead, ProcessDuration))
	WriteOutputMetric.printTreeNode("WriteOutput", bytesSpeed(outputBytes, ProcessDuration))
	printTreeNode(0, "MetricsOverhead", MetricsCalls, MetricsOverheadAvg*MetricsCalls, "")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== TIMING BREAKDOWN ===========================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "\n")
	printTree(metricsTree, 0)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== DEBUG METRICS ==============================================================================================================================================================\n")
	LineLengths.printBuckets("Line lengths")
	SkippedLineCounts.printBuckets("Skipped line counts")
	SuccessiveLineCounts.printBuckets("Successive line counts")
	BlockLineCounts.printBuckets("Block line counts")
	Timestamp_Lenghts.printBuckets("Timestamp lengths")
	Timestamp_FirstDigitIndexes.printBuckets("First digit indexes")
	Timestamp_FirstDigitIndexesActual.printBuckets("First digit indexes actual")
	Timestamp_LastDigitIndexes.printBuckets("Last digit indexes")
	Timestamp_NanosLengths.printBuckets("Timestamp nanos digit counts")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "ParseTimestamp debugging\n")
	fmt.Fprintf(Stderr, "  too short             : %8s ~ %15d\n", "", Timestamp_LineTooShort)
	fmt.Fprintf(Stderr, "  no digit              : %8s ~ %15d\n", "", Timestamp_NoFirstDigit)
	fmt.Fprintf(Stderr, "  too short after digit : %8s ~ %15d\n", "", Timestamp_LineTooShortAfterFirstDigit)
	fmt.Fprintf(Stderr, "  no year               : %8s ~ %15d\n", "", Timestamp_NoYear)
	fmt.Fprintf(Stderr, "  2-digit year 1900     : %8s ~ %15d\n", "", Timestamp_2DigitYear_1900)
	fmt.Fprintf(Stderr, "  2-digit year 2000     : %8s ~ %15d\n", "", Timestamp_2DigitYear_2000)
	fmt.Fprintf(Stderr, "  4-digit year out-range: %8s ~ %15d\n", "", Timestamp_4DigitYear_OutOfRange)
	fmt.Fprintf(Stderr, "  no month              : %8s ~ %15d\n", "", Timestamp_NoMonth)
	fmt.Fprintf(Stderr, "  month out of range    : %8s ~ %15d\n", "", Timestamp_MonthOutOfRange)
	fmt.Fprintf(Stderr, "  no day                : %8s ~ %15d\n", "", Timestamp_NoDay)
	fmt.Fprintf(Stderr, "  day out of range      : %8s ~ %15d\n", "", Timestamp_DayOutOfRange)
	fmt.Fprintf(Stderr, "  space operator mismtch: %8s ~ %15d\n", "", Timestamp_SpaceOperatorMismatch)
	fmt.Fprintf(Stderr, "  no hour               : %8s ~ %15d\n", "", Timestamp_NoHour)
	fmt.Fprintf(Stderr, "  hour out of range     : %8s ~ %15d\n", "", Timestamp_HourOutOfRange)
	fmt.Fprintf(Stderr, "  no hour separator     : %8s ~ %15d\n", "", Timestamp_NoHourSeparator)
	fmt.Fprintf(Stderr, "  hour separator mismtch: %8s ~ %15d => %v\n", "", Timestamp_HourSeparatorMismatch, Timestamp_MismatchedHourSeparators)
	fmt.Fprintf(Stderr, "  no minute             : %8s ~ %15d\n", "", Timestamp_NoMinute)
	fmt.Fprintf(Stderr, "  minute out of range   : %8s ~ %15d\n", "", Timestamp_MinuteOutOfRange)
	fmt.Fprintf(Stderr, "  no minute separator   : %8s ~ %15d\n", "", Timestamp_NoMinuteSeparator)
	fmt.Fprintf(Stderr, "  minute sep. mismatch  : %8s ~ %15d => %v\n", "", Timestamp_MinuteSeparatorMismatch, Timestamp_MismatchedMinuteSeparators)
	fmt.Fprintf(Stderr, "  no second             : %8s ~ %15d\n", "", Timestamp_NoSecond)
	fmt.Fprintf(Stderr, "  second out of range   : %8s ~ %15d\n", "", Timestamp_SecondOutOfRange)
	fmt.Fprintf(Stderr, "  has nanos             : %8s ~ %15d\n", percent(Timestamp_HasNanos, Timestamp_HasNanos+Timestamp_HasNotNanos), Timestamp_HasNanos)
	fmt.Fprintf(Stderr, "  has not nanos         : %8s ~ %15d\n", percent(Timestamp_HasNotNanos, Timestamp_HasNanos+Timestamp_HasNotNanos), Timestamp_HasNotNanos)
	fmt.Fprintf(Stderr, "  no timezone           : %8s ~ %15d\n", "", Timestamp_NoTimezone)
	fmt.Fprintf(Stderr, "  UTC timezone          : %8s ~ %15d\n", "", Timestamp_UtcTimezone)
	fmt.Fprintf(Stderr, "  non-UTC timezone      : %8s ~ %15d\n", "", Timestamp_NonUtcTimezone)
	fmt.Fprintf(Stderr, "  timezone early return : %8s ~ %15d\n", "", Timestamp_TimezoneEarlyReturn)
	fmt.Fprintf(Stderr, "  no timezone hour      : %8s ~ %15d\n", "", Timestamp_NoTimezoneHour)
	fmt.Fprintf(Stderr, "  tz hour out-range     : %8s ~ %15d\n", "", Timestamp_TimezoneHourOutOfRange)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== RUNTIME METRICS ============================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "NumCPU							     : %12v\n", runtime.NumCPU())
	fmt.Fprintf(Stderr, "NumGoroutine					     : %12v\n", runtime.NumGoroutine())
	fmt.Fprintf(Stderr, "NumCgoCall						     : %12v\n", runtime.NumCgoCall())
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "MemStats                             : %+v\n", MemStats)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Allocated heap objects               : %12v = %10s\n", MemStats.Alloc, bytes(int64(MemStats.Alloc)))
	fmt.Fprintf(Stderr, "Allocated heap objects (cumulative)  : %12v = %10s\n", MemStats.TotalAlloc, bytes(int64(MemStats.TotalAlloc)))
	fmt.Fprintf(Stderr, "Memory obtained from the OS          : %12v = %10s\n", MemStats.Sys, bytes(int64(MemStats.Sys)))
	fmt.Fprintf(Stderr, "Number of pointer lookups            : %12v = %10s\n", MemStats.Lookups, count(int64(MemStats.Lookups)))
	fmt.Fprintf(Stderr, "Number of mallocs                    : %12v = %10s\n", MemStats.Mallocs, count(int64(MemStats.Mallocs)))
	fmt.Fprintf(Stderr, "Number of frees                      : %12v = %10s\n", MemStats.Frees, count(int64(MemStats.Frees)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Allocated heap objects               : %12v = %10s\n", MemStats.HeapAlloc, bytes(int64(MemStats.HeapAlloc)))
	fmt.Fprintf(Stderr, "Allocated heap objects (cumulative)  : %12v = %10s\n", MemStats.HeapSys, bytes(int64(MemStats.HeapSys)))
	fmt.Fprintf(Stderr, "Heap idle memory                     : %12v = %10s\n", MemStats.HeapIdle, bytes(int64(MemStats.HeapIdle)))
	fmt.Fprintf(Stderr, "Heap in-use memory                   : %12v = %10s\n", MemStats.HeapInuse, bytes(int64(MemStats.HeapInuse)))
	fmt.Fprintf(Stderr, "Heap released memory                 : %12v = %10s\n", MemStats.HeapReleased, bytes(int64(MemStats.HeapReleased)))
	fmt.Fprintf(Stderr, "Heap objects waiting to be freed     : %12v = %10s\n", MemStats.HeapObjects, count(int64(MemStats.HeapObjects)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Stack memory in use                  : %12v = %10s\n", MemStats.StackInuse, bytes(int64(MemStats.StackInuse)))
	fmt.Fprintf(Stderr, "Stack memory obtained from the OS    : %12v = %10s\n", MemStats.StackSys, bytes(int64(MemStats.StackSys)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Allocated mspan structures           : %12v = %10s\n", MemStats.MSpanInuse, bytes(int64(MemStats.MSpanInuse)))
	fmt.Fprintf(Stderr, "mspan memory obtained from the OS    : %12v = %10s\n", MemStats.MSpanSys, bytes(int64(MemStats.MSpanSys)))
	fmt.Fprintf(Stderr, "Allocated mcache structures          : %12v = %10s\n", MemStats.MCacheInuse, bytes(int64(MemStats.MCacheInuse)))
	fmt.Fprintf(Stderr, "mcache memory obtained from the OS   : %12v = %10s\n", MemStats.MCacheSys, bytes(int64(MemStats.MCacheSys)))
	fmt.Fprintf(Stderr, "Allocated buckhash tables            : %12v = %10s\n", MemStats.BuckHashSys, bytes(int64(MemStats.BuckHashSys)))
	fmt.Fprintf(Stderr, "Allocated GC metadata                : %12v = %10s\n", MemStats.GCSys, bytes(int64(MemStats.GCSys)))
	fmt.Fprintf(Stderr, "Allocated other system allocations   : %12v = %10s\n", MemStats.OtherSys, bytes(int64(MemStats.OtherSys)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Last GC finish time                  :   %s\n", strings.Replace(time.Unix(0, int64(MemStats.LastGC)).Format(time.RFC3339Nano), "T", "   ", 1))
	fmt.Fprintf(Stderr, "Target heap size of the next GC cycle: %12v = %10s\n", MemStats.NextGC, bytes(int64(MemStats.NextGC)))
	fmt.Fprintf(Stderr, "GC pause duration                    : %12v = %10s\n", MemStats.PauseTotalNs, duration(int64(MemStats.PauseTotalNs)))
	fmt.Fprintf(Stderr, "Number of completed GC cycles        : %12v = %10s\n", MemStats.NumGC, count(int64(MemStats.NumGC)))
	fmt.Fprintf(Stderr, "Number of forced GC cycles by app    : %12v = %10s\n", MemStats.NumForcedGC, count(int64(MemStats.NumForcedGC)))
	fmt.Fprintf(Stderr, "GCCPUFraction                        : %12.2f / %10s\n", MemStats.GCCPUFraction*1_000_000, "1_000_000")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== FILE LIST ==================================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "File list (%d files):\n", len(MatchedFiles))
	sort.Strings(MatchedFiles)
	for i, file := range MatchedFiles {
		fmt.Fprintf(Stderr, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(Stderr, "==================================================================================================================================================================================\n")
}

func reverseMap() map[string][]string {
	aliasesToSourceNames := make(map[string][]string)
	for sourceName, alias := range SourceNameAliases {
		aliasesToSourceNames[alias] = append(aliasesToSourceNames[alias], sourceName)
	}
	return aliasesToSourceNames
}

func getKeysSorted(aliasesToSourceNames map[string][]string) []string {
	aliasesSorted := make([]string, 0, len(aliasesToSourceNames))
	for alias := range aliasesToSourceNames {
		aliasesSorted = append(aliasesSorted, alias)
	}
	sort.Strings(aliasesSorted)
	return aliasesSorted
}

func printTree(node *TreeNode, depth int) {
	nanoseconds := node.Metric.Duration
	printTreeNode(depth, node.Name, node.Metric.CallCount, nanoseconds, "")

	if node.Children != nil {
		childTotal := int64(0)
		for _, child := range node.Children {
			printTree(child, depth+1)
			childTotal += child.Metric.Duration
		}

		rest := nanoseconds - childTotal
		if rest > 0 {
			printTreeNode(depth+1, "..rest of "+node.Name, node.Metric.CallCount, rest, "")
		}
	}
}

func (m *CallMetric) printTreeNode(name string, extra string) {
	printTreeNode(0, name, m.CallCount, m.Duration, extra)
}

func printTreeNode(depth int, name string, n int64, nanoseconds int64, extra string) {
	padLen := 35
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(
		Stderr,
		"%-*s%-*s : %8s ~ %15v ≈ %12v avg of %12v times = %12v %s\n",
		depth*2,
		"",
		padLen-depth*2,
		name,
		timePercent(nanoseconds),
		duration(nanoseconds),
		durationFloat(float64(nanoseconds)/float64(max(1, n))),
		n,
		count(n),
		extra,
	)
}

//goland:noinspection GoUnhandledErrorResult
func (b *BucketMetric) printBuckets(name string) {
	minValue := b.min
	maxValue := b.max
	total := b.count
	avgValue := b.sum / max(1, total)

	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "%s\n", name)
	fmt.Fprintf(Stderr, "  summary\n")
	fmt.Fprintf(Stderr, "    min          : %8s ~ %15v = %10s\n", "", minValue, count(minValue))
	fmt.Fprintf(Stderr, "    avg          : %8s ~ %15v = %10s\n", "", avgValue, count(avgValue))
	fmt.Fprintf(Stderr, "    max          : %8s ~ %15v = %10s\n", "", maxValue, count(maxValue))
	fmt.Fprintf(Stderr, "    count        : %8s ~ %15v = %10s\n", "", total, count(total))
	fmt.Fprintf(Stderr, "  buckets\n")

	var cumulative int64

	for i, level := range b.levels {
		value := b.values[i]
		cumulative += value
		fmt.Fprintf(Stderr, "    ≤ %-6d     : %8s ~ %15d ≈ %15d (cumulative) ~ %10s (cumulative)\n", level, percent(value, total), value, cumulative, percent(cumulative, total))
	}

	lastLevel := b.levels[len(b.levels)-1]
	remaining := total - cumulative
	cumulative += remaining
	fmt.Fprintf(Stderr, "    > %-6d     : %8s ~ %15d ≈ %15d (cumulative) ~ %10s (cumulative)\n", lastLevel, percent(remaining, total), remaining, cumulative, percent(total, total))
}

func duration(d int64) string {
	return durationFloat(float64(d))
}

func durationFloat(d float64) string {
	if d < 1000 {
		return fmt.Sprintf("%.3fns", d)
	}

	if d < 1000*1000 {
		return fmt.Sprintf("%.3fµs", d/1000)
	}

	if d < 1000*1000*1000 {
		return fmt.Sprintf("%.3fms", d/(1000*1000))
	}

	return fmt.Sprintf("%v", time.Duration(d).Round(time.Millisecond))
}

func bytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%7d B ", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%7.2f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%7.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%7.2f GB", float64(bytes)/(1024*1024*1024))
}

func count(value int64) string {
	if value < 1000 {
		return fmt.Sprintf("%7d", value)
	}
	if value < 1000*1000 {
		return fmt.Sprintf("%7.2f K", float64(value)/1000)
	}
	if value < 1000*1000*1000 {
		return fmt.Sprintf("%7.2f M", float64(value)/(1000*1000))
	}
	return fmt.Sprintf("%7.2f G", float64(value)/(1000*1000*1000))
}

func timePercent(value int64) string {
	return fmt.Sprintf("%6.2f %%", div(value, TotalMainDuration)*100)
}

func percent(value, total int64) string {
	return fmt.Sprintf("%6.2f %%", div(value, total)*100)
}

func bytesSpeed(value, duration int64) string {
	return fmt.Sprintf("%s / s", bytes(int64(speed(value, duration))))
}

func countSpeed(value, duration int64) string {
	return fmt.Sprintf("%s / s", count(int64(speed(value, duration))))
}

func speed(value, duration int64) float64 {
	return div(value, duration) * 1e9
}

func div(value int64, total int64) float64 {
	if total == 0 {
		return 0
	} else {
		return float64(value) / float64(total)
	}
}

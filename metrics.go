package main

import (
	"fmt"
	"runtime"
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
	ListFilesDuration int64
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
	BytesWrittenForTimestamps         int64
	BytesWrittenForSourceNamePerLine  int64
	BytesWrittenForSourceNamePerBlock int64
	BytesWrittenForRawData            int64
	BytesWrittenForMissingNewlines    int64

	// Line count stats

	// TODO: Add SkippedLinesCount, SkippedBytesCount etc. to get better insights into filtered results
	LinesRead              int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64

	// Line length stats
	LineLengths = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)

	// Merge debugging
	HeapPopMetric        CallMetric
	HeapPushMetric       CallMetric
	SuccessiveLineCounts = NewBucketMetric(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100)

	// ParseTimestamp debugging

	ParseTimestamp_NoFirstDigit                int64
	ParseTimestamp_MinFirstDigitIndex          = 1<<31 - 1
	ParseTimestamp_MaxFirstDigitIndex          int
	ParseTimestamp_MinFirstDigitIndexActual    = 1<<31 - 1
	ParseTimestamp_MaxFirstDigitIndexActual    int
	ParseTimestamp_DigitIndexLevels            = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000}
	ParseTimestamp_FirstDigitIndexes           = NewBucketMetric(ParseTimestamp_DigitIndexLevels...)
	ParseTimestamp_FirstDigitIndexesActual     = NewBucketMetric(ParseTimestamp_DigitIndexLevels...)
	ParseTimestamp_LastDigitIndexes            = NewBucketMetric(ParseTimestamp_DigitIndexLevels...)
	ParseTimestamp_MinTimestampEndIndex        = 1<<31 - 1
	ParseTimestamp_MaxTimestampEndIndex        int
	ParseTimestamp_MinTimestampLength          = 1<<31 - 1
	ParseTimestamp_MaxTimestampLength          int
	ParseTimestamp_Lenghts                     = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 500, 1000, 10000, 50000)
	ParseTimestamp_LineTooShort                int64
	ParseTimestamp_LineTooShortAfterFirstDigit int64
	ParseTimestamp_NoYear                      int64
	ParseTimestamp_2DigitYear_1900             int64
	ParseTimestamp_2DigitYear_2000             int64
	ParseTimestamp_4DigitYear_OutOfRange       int64
	ParseTimestamp_NoMonth                     int64
	ParseTimestamp_MonthOutOfRange             int64
	ParseTimestamp_NoDay                       int64
	ParseTimestamp_DayOutOfRange               int64
	ParseTimestamp_SpaceOperatorMismatch       int64
	ParseTimestamp_NoHour                      int64
	ParseTimestamp_HourOutOfRange              int64
	ParseTimestamp_NoHourSeparator             int64
	ParseTimestamp_HourSeparatorMismatch       int64
	ParseTimestamp_MismatchedHourSeparators    []byte
	ParseTimestamp_NoMinute                    int64
	ParseTimestamp_MinuteOutOfRange            int64
	ParseTimestamp_NoMinuteSeparator           int64
	ParseTimestamp_MinuteSeparatorMismatch     int64
	ParseTimestamp_MismatchedMinuteSeparators  []byte
	ParseTimestamp_NoSecond                    int64
	ParseTimestamp_SecondOutOfRange            int64
	ParseTimestamp_HasNanos                    int64
	ParseTimestamp_HasNotNanos                 int64
	ParseTimestamp_NanosLengths                = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
	ParseTimestamp_NoTimezone                  int64
	ParseTimestamp_UtcTimezone                 int64
	ParseTimestamp_NonUtcTimezone              int64
	ParseTimestamp_TimezoneEarlyReturn         int64
	ParseTimestamp_NoTimezoneHour              int64
	ParseTimestamp_TimezoneHourOutOfRange      int64
)

func MeasureStart(name string) time.Time {
	if DisableMetricsCollection {
		return noTimestamp
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
}

func NewBucketMetric(levels ...int) *BucketMetric {
	return &BucketMetric{
		levels: levels,
		values: make([]int64, len(levels)),
		min:    1<<63 - 1,
		max:    0,
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
func PrintMetrics(startTime time.Time, elapsedTime time.Duration, inputPath string, err error) {
	writtenBytesOverhead := BytesWrittenForTimestamps + BytesWrittenForSourceNamePerLine + BytesWrittenForSourceNamePerBlock + BytesWrittenForMissingNewlines
	writtenBytes := BytesWrittenForRawData + writtenBytesOverhead

	TotalMainDuration = elapsedTime.Nanoseconds()
	metricsTree.Metric.Duration = TotalMainDuration

	fmt.Fprintf(Stderr, "===== SUMMARY ====================================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "Start time              : %s\n", startTime.Format(time.RFC3339Nano))
	fmt.Fprintf(Stderr, "Error                   : %v\n", err)
	fmt.Fprintf(Stderr, "Total main duration     : %v\n", elapsedTime)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== CONFIGURATION ==============================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "Input path              : %s\n", inputPath)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "Stdout path             : %s\n", Stdout.Name())
	fmt.Fprintf(Stderr, "Stderr path             : %s\n", Stderr.Name())
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "DisableMetricsCollection: %v\n", DisableMetricsCollection)
	fmt.Fprintf(Stderr, "EnableProfiling         : %v\n", EnableProfiling)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "WriteSourceNamesPerBlock: %v\n", WriteSourceNamesPerBlock)
	fmt.Fprintf(Stderr, "WriteSourceNamesPerLine : %v\n", WriteSourceNamesPerLine)
	fmt.Fprintf(Stderr, "WriteTimestampPerLine   : %v\n", WriteTimestampPerLine)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "MinTimestamp            : %12v\n", MinTimestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(Stderr, "MaxTimestamp            : %12v\n", MaxTimestamp.Format(time.RFC3339Nano))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "ShortestTimestampLen    : %12v = %10s\n", ShortestTimestampLen, bytes(int64(ShortestTimestampLen)))
	fmt.Fprintf(Stderr, "TimestampSearchEndIndex : %12v = %10s\n", TimestampSearchEndIndex, bytes(int64(TimestampSearchEndIndex)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "BufferSizeForRead       : %12v = %10s\n", BufferSizeForRead, bytes(int64(BufferSizeForRead)))
	fmt.Fprintf(Stderr, "BufferSizeForWrite      : %12v = %10s\n", BufferSizeForWrite, bytes(int64(BufferSizeForWrite)))
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "ExcludedStrictSuffixes  : %v\n", ExcludedStrictSuffixes)
	fmt.Fprintf(Stderr, "IncludedStrictSuffixes  : %v\n", IncludedStrictSuffixes)
	fmt.Fprintf(Stderr, "ExcludedLenientSuffixes : %v\n", ExcludedLenientSuffixes)
	fmt.Fprintf(Stderr, "IncludedLenientSuffixes : %v\n", IncludedLenientSuffixes)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== RUNTIME STATS ==============================================================================================================================================================\n")
	MemStats := runtime.MemStats{}
	runtime.ReadMemStats(&MemStats)
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
	fmt.Fprintf(Stderr, "Target heap size of the next GC cycle: %12v = %10s\n", MemStats.NextGC, bytes(int64(MemStats.NextGC)))
	fmt.Fprintf(Stderr, "Last GC finish time                  : %s\n", time.Unix(0, int64(MemStats.LastGC)).Format(time.RFC3339Nano))
	fmt.Fprintf(Stderr, "GC pause duration                    : %12v = %10s\n", MemStats.PauseTotalNs, duration(int64(MemStats.PauseTotalNs)))
	fmt.Fprintf(Stderr, "Number of completed GC cycles        : %12v = %10s\n", MemStats.NumGC, count(int64(MemStats.NumGC)))
	fmt.Fprintf(Stderr, "Number of forced GC cycles by app    : %12v = %10s\n", MemStats.NumForcedGC, count(int64(MemStats.NumForcedGC)))
	fmt.Fprintf(Stderr, "GCCPUFraction                        : %.2f / %s\n", MemStats.GCCPUFraction*1_000_000, "1_000_000")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== METRICS SUMMARY ============================================================================================================================================================\n")
	printTreeNode(0, "ListFiles", 1, ListFilesDuration, bytesSpeed(FilesScanned, ListFilesDuration))
	FillBufferMetric.printTreeNode("FillBuffer", bytesSpeed(BytesRead, ProcessDuration))
	BufferAsSliceMetric.printTreeNode("BufferAsSlice", countSpeed(LinesRead, ProcessDuration))
	ParseTimestampMetric.printTreeNode("ParseTimestamp", countSpeed(LinesRead, ProcessDuration))
	PeekNextLineSliceMetric.printTreeNode("PeekNextLineSlice", countSpeed(LinesRead, ProcessDuration))
	WriteOutputMetric.printTreeNode("WriteOutput", bytesSpeed(writtenBytes, ProcessDuration))
	printTreeNode(0, "MetricsOverhead", MetricsCalls, MetricsOverheadAvg*MetricsCalls, "")
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== METRICS TREE ===============================================================================================================================================================\n")
	printTree(metricsTree, 0)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== METRIC DETAILS =============================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "File count stats\n")
	fmt.Fprintf(Stderr, "  dirs scanned          : %8s ~ %12d\n", "", DirsScanned)
	fmt.Fprintf(Stderr, "  files scanned         : %8s ~ %12d ≈ %10s\n", percent(FilesScanned, FilesScanned), FilesScanned, countSpeed(FilesScanned, ListFilesDuration))
	fmt.Fprintf(Stderr, "  files matched         : %8s ~ %12d\n", percent(FilesMatched, FilesScanned), FilesMatched)
	fmt.Fprintf(Stderr, "Byte count stats\n")
	fmt.Fprintf(Stderr, "  bytes read            : %8s ~ %12d = %10s ≈ %s\n", "", BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, ProcessDuration))
	fmt.Fprintf(Stderr, "  bytes written         : %8s ~ %12d = %10s ≈ %s\n", percent(writtenBytes, writtenBytes), writtenBytes, bytes(writtenBytes), bytesSpeed(writtenBytes, ProcessDuration))
	fmt.Fprintf(Stderr, "    raw data            : %8s ~ %12v = %10s\n", percent(BytesWrittenForRawData, writtenBytes), BytesWrittenForRawData, bytes(BytesWrittenForRawData))
	fmt.Fprintf(Stderr, "    overhead            : %8s ~ %12v = %10s\n", percent(writtenBytesOverhead, writtenBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(Stderr, "      source name @block: %8s ~ %12v = %10s\n", percent(BytesWrittenForSourceNamePerBlock, writtenBytes), BytesWrittenForSourceNamePerBlock, bytes(BytesWrittenForSourceNamePerBlock))
	fmt.Fprintf(Stderr, "      source name @line : %8s ~ %12v = %10s\n", percent(BytesWrittenForSourceNamePerLine, writtenBytes), BytesWrittenForSourceNamePerLine, bytes(BytesWrittenForSourceNamePerLine))
	fmt.Fprintf(Stderr, "      timestamps  @line : %8s ~ %12v = %10s\n", percent(BytesWrittenForTimestamps, writtenBytes), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(Stderr, "      missing newlines  : %8s ~ %12v = %10s\n", percent(BytesWrittenForMissingNewlines, writtenBytes), BytesWrittenForMissingNewlines, bytes(BytesWrittenForMissingNewlines))
	fmt.Fprintf(Stderr, "Line count stats\n")
	fmt.Fprintf(Stderr, "  lines read            : %8s ~ %12d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, ProcessDuration))
	fmt.Fprintf(Stderr, "    with timestamp      : %8s ~ %12v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(Stderr, "    without timestamp   : %8s ~ %12v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
	fmt.Fprintf(Stderr, "Line length stats\n")
	fmt.Fprintf(Stderr, "  max line length       : %8s ~ %12d\n", "", LineLengths.max)
	fmt.Fprintf(Stderr, "  line length buckets\n")
	LineLengths.printBuckets(LinesRead)
	fmt.Fprintf(Stderr, "Merge debugging\n")
	fmt.Fprintf(Stderr, "  heap pop count        : %8s ~ %12d ≈ %s\n", "", HeapPopMetric.CallCount, count(HeapPopMetric.CallCount))
	fmt.Fprintf(Stderr, "  heap push count       : %8s ~ %12d ≈ %s\n", "", HeapPushMetric.CallCount, count(HeapPushMetric.CallCount))
	fmt.Fprintf(Stderr, "  max successive lines  : %8s ~ %12v = %10s\n", "", SuccessiveLineCounts.max, count(SuccessiveLineCounts.max))
	fmt.Fprintf(Stderr, "  successive line count buckets\n")
	SuccessiveLineCounts.printBuckets(LinesRead)
	fmt.Fprintf(Stderr, "ParseTimestamp debugging\n")
	fmt.Fprintf(Stderr, "  first digit index     : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinFirstDigitIndex, ParseTimestamp_MaxFirstDigitIndex)
	fmt.Fprintf(Stderr, "  start digit index     : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinFirstDigitIndexActual, ParseTimestamp_MaxFirstDigitIndexActual)
	fmt.Fprintf(Stderr, "  end digit index       : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinTimestampEndIndex, ParseTimestamp_MaxTimestampEndIndex)
	fmt.Fprintf(Stderr, "  digit index buckets\n")
	for i, level := range ParseTimestamp_DigitIndexLevels {
		fmt.Fprintf(Stderr, "    ≤ %-6d            : %8s ~ %7d frst ≈ %8d start ≈ %8d end\n", level, "", ParseTimestamp_FirstDigitIndexes.values[i], ParseTimestamp_FirstDigitIndexesActual.values[i], ParseTimestamp_LastDigitIndexes.values[i])
	}
	fmt.Fprintf(Stderr, "  timestamp length      : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinTimestampLength, ParseTimestamp_MaxTimestampLength)
	fmt.Fprintf(Stderr, "  timestamp length buckets\n")
	ParseTimestamp_Lenghts.printBuckets(LinesWithTimestamps)
	fmt.Fprintf(Stderr, "  too short             : %8s ~ %12d\n", "", ParseTimestamp_LineTooShort)
	fmt.Fprintf(Stderr, "  no digit              : %8s ~ %12d\n", "", ParseTimestamp_NoFirstDigit)
	fmt.Fprintf(Stderr, "  too short after digit : %8s ~ %12d\n", "", ParseTimestamp_LineTooShortAfterFirstDigit)
	fmt.Fprintf(Stderr, "  no year               : %8s ~ %12d\n", "", ParseTimestamp_NoYear)
	fmt.Fprintf(Stderr, "  2-digit year 1900     : %8s ~ %12d\n", "", ParseTimestamp_2DigitYear_1900)
	fmt.Fprintf(Stderr, "  2-digit year 2000     : %8s ~ %12d\n", "", ParseTimestamp_2DigitYear_2000)
	fmt.Fprintf(Stderr, "  4-digit year out-range: %8s ~ %12d\n", "", ParseTimestamp_4DigitYear_OutOfRange)
	fmt.Fprintf(Stderr, "  no month              : %8s ~ %12d\n", "", ParseTimestamp_NoMonth)
	fmt.Fprintf(Stderr, "  month out of range    : %8s ~ %12d\n", "", ParseTimestamp_MonthOutOfRange)
	fmt.Fprintf(Stderr, "  no day                : %8s ~ %12d\n", "", ParseTimestamp_NoDay)
	fmt.Fprintf(Stderr, "  day out of range      : %8s ~ %12d\n", "", ParseTimestamp_DayOutOfRange)
	fmt.Fprintf(Stderr, "  space operator mismtch: %8s ~ %12d\n", "", ParseTimestamp_SpaceOperatorMismatch)
	fmt.Fprintf(Stderr, "  no hour               : %8s ~ %12d\n", "", ParseTimestamp_NoHour)
	fmt.Fprintf(Stderr, "  hour out of range     : %8s ~ %12d\n", "", ParseTimestamp_HourOutOfRange)
	fmt.Fprintf(Stderr, "  no hour separator     : %8s ~ %12d\n", "", ParseTimestamp_NoHourSeparator)
	fmt.Fprintf(Stderr, "  hour separator mismtch: %8s ~ %12d => %v\n", "", ParseTimestamp_HourSeparatorMismatch, ParseTimestamp_MismatchedHourSeparators)
	fmt.Fprintf(Stderr, "  no minute             : %8s ~ %12d\n", "", ParseTimestamp_NoMinute)
	fmt.Fprintf(Stderr, "  minute out of range   : %8s ~ %12d\n", "", ParseTimestamp_MinuteOutOfRange)
	fmt.Fprintf(Stderr, "  no minute separator   : %8s ~ %12d\n", "", ParseTimestamp_NoMinuteSeparator)
	fmt.Fprintf(Stderr, "  minute sep. mismatch  : %8s ~ %12d => %v\n", "", ParseTimestamp_MinuteSeparatorMismatch, ParseTimestamp_MismatchedMinuteSeparators)
	fmt.Fprintf(Stderr, "  no second             : %8s ~ %12d\n", "", ParseTimestamp_NoSecond)
	fmt.Fprintf(Stderr, "  second out of range   : %8s ~ %12d\n", "", ParseTimestamp_SecondOutOfRange)
	fmt.Fprintf(Stderr, "  has nanos             : %8s ~ %12d\n", percent(ParseTimestamp_HasNanos, ParseTimestamp_HasNanos+ParseTimestamp_HasNotNanos), ParseTimestamp_HasNanos)
	fmt.Fprintf(Stderr, "  has not nanos         : %8s ~ %12d\n", percent(ParseTimestamp_HasNotNanos, ParseTimestamp_HasNanos+ParseTimestamp_HasNotNanos), ParseTimestamp_HasNotNanos)
	fmt.Fprintf(Stderr, "  nanos length buckets\n")
	ParseTimestamp_NanosLengths.printBuckets(ParseTimestamp_HasNanos)
	fmt.Fprintf(Stderr, "  no timezone           : %8s ~ %12d\n", "", ParseTimestamp_NoTimezone)
	fmt.Fprintf(Stderr, "  UTC timezone          : %8s ~ %12d\n", "", ParseTimestamp_UtcTimezone)
	fmt.Fprintf(Stderr, "  non-UTC timezone      : %8s ~ %12d\n", "", ParseTimestamp_NonUtcTimezone)
	fmt.Fprintf(Stderr, "  timezone early return : %8s ~ %12d\n", "", ParseTimestamp_TimezoneEarlyReturn)
	fmt.Fprintf(Stderr, "  no timezone hour      : %8s ~ %12d\n", "", ParseTimestamp_NoTimezoneHour)
	fmt.Fprintf(Stderr, "  tz hour out-range     : %8s ~ %12d\n", "", ParseTimestamp_TimezoneHourOutOfRange)
	fmt.Fprintf(Stderr, "\n")
	fmt.Fprintf(Stderr, "===== FILE LIST ==================================================================================================================================================================\n")
	fmt.Fprintf(Stderr, "File list (%d files):\n", len(MatchedFiles))
	for i, file := range MatchedFiles {
		fmt.Fprintf(Stderr, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(Stderr, "==================================================================================================================================================================================\n")
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
	padLen := 45
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(
		Stderr,
		"%-*s%-*s : %8s ~ %12v ≈ %12v avg of %9v times = %12v %s\n",
		depth*2,
		"",
		padLen-depth*2,
		name,
		timePercent(nanoseconds),
		duration(nanoseconds),
		duration(nanoseconds/max(1, n)),
		n,
		count(n),
		extra,
	)
}

func (b *BucketMetric) printBuckets(total int64) {
	var cumulative int64

	for i, level := range b.levels {
		value := b.values[i]
		cumulative += value
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(Stderr, "    ≤ %-6d            : %8s ~ %12d ≈ %8s (cumulative)\n", level, percent(value, total), value, percent(cumulative, total))
	}

	remaining := total - cumulative
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(Stderr, "    rest..              : %8s ~ %12d ≈ %8s (cumulative)\n", percent(remaining, total), remaining, percent(total, total))
}

func duration(d int64) time.Duration {
	return time.Duration(d)
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

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

	TotalMainDuration       int64
	ProcessDuration         int64
	FillBufferMetric        CallMetric
	BufferAsSliceMetric     CallMetric
	ParseTimestampMetric    CallMetric
	PeekNextLineSliceMetric CallMetric
	WriteOutputMetric       CallMetric

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

func MeasureStart(c *AppConfig, name string) time.Time {
	if !c.EnableMetricsCollection {
		return time.Time{}
	}
	enterContext(name)
	return time.Now()
}

func MeasureSince(c *AppConfig, startNanos time.Time) int64 {
	if !c.EnableMetricsCollection {
		return 0
	}
	elapsed := time.Since(startNanos).Nanoseconds()
	exitContext(elapsed)
	return elapsed
}

func (m *CallMetric) MeasureSince(c *AppConfig, startNanos time.Time) {
	m.Duration += MeasureSince(c, startNanos)
	m.CallCount++
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
func PrintMetrics(c *AppConfig, startTime time.Time, elapsedTime time.Duration, err error) {
	inputBytes := BytesRead + BytesNotRead
	bytesReadAndProcessed := BytesRead - BytesReadAndSkipped
	linesReadAndProcessed := LinesRead - LinesReadAndSkipped

	writtenBytesOverhead := BytesWrittenForTimestamps + BytesWrittenForAliasPerLine + BytesWrittenForAliasPerBlock + BytesWrittenForMissingNewlines
	outputBytes := BytesWrittenForRawData + writtenBytesOverhead

	TotalMainDuration = elapsedTime.Nanoseconds()
	metricsTree.Metric.Duration = TotalMainDuration

	MemStats := runtime.MemStats{}
	runtime.ReadMemStats(&MemStats)

	fmt.Fprintf(c.Stderr, "===== SUMMARY ====================================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Start time               : %s\n", startTime.Format(time.RFC3339Nano))
	fmt.Fprintf(c.Stderr, "Error                    : %v\n", err)
	fmt.Fprintf(c.Stderr, "Total main duration      : %v\n", elapsedTime)
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== CONFIGURATION ==============================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Input path               : %s\n", c.InputPath)
	fmt.Fprintf(c.Stderr, "Stdout path              : %s\n", c.Stdout.Name())
	fmt.Fprintf(c.Stderr, "Stderr path              : %s\n", c.Stderr.Name())
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "EnableMetricsCollection  : %v\n", c.EnableMetricsCollection)
	fmt.Fprintf(c.Stderr, "EnableProfiling          : %v\n", c.EnableProfiling)
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "WriteAliasPerBlock       : %v\n", c.WriteAliasPerBlock)
	fmt.Fprintf(c.Stderr, "WriteAliasPerLine        : %v\n", c.WriteAliasPerLine)
	fmt.Fprintf(c.Stderr, "WriteTimestampPerLine    : %v\n", c.WriteTimestampPerLine)
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "IgnoreTimezoneInfo       : %v\n", c.IgnoreTimezoneInfo)
	fmt.Fprintf(c.Stderr, "MinTimestamp             : %v\n", c.MinTimestamp.String())
	fmt.Fprintf(c.Stderr, "MaxTimestamp             : %v\n", c.MaxTimestamp.String())
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "ShortestTimestampLen     : %12v = %10s\n", c.ShortestTimestampLen, bytes(int64(c.ShortestTimestampLen)))
	fmt.Fprintf(c.Stderr, "TimestampSearchEndIndex  : %12v = %10s\n", c.TimestampSearchEndIndex, bytes(int64(c.TimestampSearchEndIndex)))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "BufferSizeForRead        : %12v = %10s\n", c.BufferSizeForRead, bytes(int64(c.BufferSizeForRead)))
	fmt.Fprintf(c.Stderr, "BufferSizeForWrite       : %12v = %10s\n", c.BufferSizeForWrite, bytes(int64(c.BufferSizeForWrite)))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "ExcludedStrictSuffixes   : %v\n", c.ExcludedStrictSuffixes)
	fmt.Fprintf(c.Stderr, "IncludedStrictSuffixes   : %v\n", c.IncludedStrictSuffixes)
	fmt.Fprintf(c.Stderr, "ExcludedLenientSuffixes  : %v\n", c.ExcludedLenientSuffixes)
	fmt.Fprintf(c.Stderr, "IncludedLenientSuffixes  : %v\n", c.IncludedLenientSuffixes)
	fmt.Fprintf(c.Stderr, "\n")
	aliasesToFiles := reverseMap(c.FileAliases)
	aliasesSorted := getKeysSorted(aliasesToFiles)
	fmt.Fprintf(c.Stderr, "FileAliases              : %v mappings in %d aliases\n", len(c.FileAliases), len(aliasesToFiles))
	for _, alias := range aliasesSorted {
		fileNames := aliasesToFiles[alias]
		sort.Strings(fileNames)
		fmt.Fprintf(c.Stderr, "  (%d) %s\n", len(fileNames), alias)
		for _, fileName := range fileNames {
			fmt.Fprintf(c.Stderr, "      %s\n", fileName)
		}
	}
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== STATISTICS =================================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "File count stats\n")
	fmt.Fprintf(c.Stderr, "  dirs scanned           : %8s ~ %15d\n", "", DirsScanned)
	fmt.Fprintf(c.Stderr, "  files scanned          : %8s ~ %15d\n", percent(FilesScanned, FilesScanned), FilesScanned)
	fmt.Fprintf(c.Stderr, "  files matched          : %8s ~ %15d\n", percent(FilesMatched, FilesScanned), FilesMatched)
	fmt.Fprintf(c.Stderr, "Byte count stats\n")
	fmt.Fprintf(c.Stderr, "  input bytes            : %8s ~ %15d = %10s ≈ %s\n", percent(inputBytes, inputBytes), inputBytes, bytes(inputBytes), bytesSpeed(inputBytes, ProcessDuration))
	fmt.Fprintf(c.Stderr, "    read                 : %8s ~ %15d = %10s ≈ %s\n", percent(BytesRead, inputBytes), BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, ProcessDuration))
	fmt.Fprintf(c.Stderr, "      read and skipped   : %8s ~ %15d = %10s ≈ %s\n", percent(BytesReadAndSkipped, inputBytes), BytesReadAndSkipped, bytes(BytesReadAndSkipped), bytesSpeed(BytesReadAndSkipped, ProcessDuration))
	fmt.Fprintf(c.Stderr, "      read and processed : %8s ~ %15d = %10s ≈ %s\n", percent(bytesReadAndProcessed, inputBytes), bytesReadAndProcessed, bytes(bytesReadAndProcessed), bytesSpeed(bytesReadAndProcessed, ProcessDuration))
	fmt.Fprintf(c.Stderr, "    not read             : %8s ~ %15d = %10s ≈ %s\n", percent(BytesNotRead, inputBytes), BytesNotRead, bytes(BytesNotRead), bytesSpeed(BytesNotRead, ProcessDuration))
	fmt.Fprintf(c.Stderr, "  output bytes           : %8s ~ %15d = %10s ≈ %s\n", percent(outputBytes, outputBytes), outputBytes, bytes(outputBytes), bytesSpeed(outputBytes, ProcessDuration))
	fmt.Fprintf(c.Stderr, "    raw data             : %8s ~ %15v = %10s\n", percent(BytesWrittenForRawData, outputBytes), BytesWrittenForRawData, bytes(BytesWrittenForRawData))
	fmt.Fprintf(c.Stderr, "    overhead             : %8s ~ %15v = %10s\n", percent(writtenBytesOverhead, outputBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(c.Stderr, "      source name @block : %8s ~ %15v = %10s\n", percent(BytesWrittenForAliasPerBlock, outputBytes), BytesWrittenForAliasPerBlock, bytes(BytesWrittenForAliasPerBlock))
	fmt.Fprintf(c.Stderr, "      source name @line  : %8s ~ %15v = %10s\n", percent(BytesWrittenForAliasPerLine, outputBytes), BytesWrittenForAliasPerLine, bytes(BytesWrittenForAliasPerLine))
	fmt.Fprintf(c.Stderr, "      timestamps  @line  : %8s ~ %15v = %10s\n", percent(BytesWrittenForTimestamps, outputBytes), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(c.Stderr, "      missing newlines   : %8s ~ %15v = %10s\n", percent(BytesWrittenForMissingNewlines, outputBytes), BytesWrittenForMissingNewlines, bytes(BytesWrittenForMissingNewlines))
	fmt.Fprintf(c.Stderr, "Line count stats\n")
	fmt.Fprintf(c.Stderr, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, ProcessDuration))
	fmt.Fprintf(c.Stderr, "    with timestamp       : %8s ~ %15v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(c.Stderr, "    without timestamp    : %8s ~ %15v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
	fmt.Fprintf(c.Stderr, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, ProcessDuration))
	fmt.Fprintf(c.Stderr, "    skipped              : %8s ~ %15v = %10s\n", percent(LinesReadAndSkipped, LinesRead), LinesReadAndSkipped, count(LinesReadAndSkipped))
	fmt.Fprintf(c.Stderr, "    processed            : %8s ~ %15v = %10s\n", percent(linesReadAndProcessed, LinesRead), linesReadAndProcessed, count(linesReadAndProcessed))
	fmt.Fprintf(c.Stderr, "Heap metrics\n")
	fmt.Fprintf(c.Stderr, "  heap pop count         : %8s ~ %15d ≈ %s\n", "", HeapPopMetric.CallCount, count(HeapPopMetric.CallCount))
	fmt.Fprintf(c.Stderr, "  heap push count        : %8s ~ %15d ≈ %s\n", "", HeapPushMetric.CallCount, count(HeapPushMetric.CallCount))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== TIMING SUMMARY =============================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "\n")
	FillBufferMetric.printTreeNode(c, "FillBuffer", bytesSpeed(BytesRead, ProcessDuration))
	BufferAsSliceMetric.printTreeNode(c, "BufferAsSlice", countSpeed(LinesRead, ProcessDuration))
	ParseTimestampMetric.printTreeNode(c, "ParseTimestamp", countSpeed(LinesRead, ProcessDuration))
	PeekNextLineSliceMetric.printTreeNode(c, "PeekNextLineSlice", countSpeed(LinesRead, ProcessDuration))
	WriteOutputMetric.printTreeNode(c, "WriteOutput", bytesSpeed(outputBytes, ProcessDuration))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== TIMING BREAKDOWN ===========================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "\n")
	printTree(c, metricsTree, 0)
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== RUNTIME METRICS ============================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "NumCPU                               : %12v\n", runtime.NumCPU())
	fmt.Fprintf(c.Stderr, "NumGoroutine                         : %12v\n", runtime.NumGoroutine())
	fmt.Fprintf(c.Stderr, "NumCgoCall                           : %12v\n", runtime.NumCgoCall())
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "MemStats                             : %+v\n", MemStats)
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Allocated heap objects               : %12v = %10s\n", MemStats.Alloc, bytes(int64(MemStats.Alloc)))
	fmt.Fprintf(c.Stderr, "Allocated heap objects (cumulative)  : %12v = %10s\n", MemStats.TotalAlloc, bytes(int64(MemStats.TotalAlloc)))
	fmt.Fprintf(c.Stderr, "Memory obtained from the OS          : %12v = %10s\n", MemStats.Sys, bytes(int64(MemStats.Sys)))
	fmt.Fprintf(c.Stderr, "Number of pointer lookups            : %12v = %10s\n", MemStats.Lookups, count(int64(MemStats.Lookups)))
	fmt.Fprintf(c.Stderr, "Number of mallocs                    : %12v = %10s\n", MemStats.Mallocs, count(int64(MemStats.Mallocs)))
	fmt.Fprintf(c.Stderr, "Number of frees                      : %12v = %10s\n", MemStats.Frees, count(int64(MemStats.Frees)))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Allocated heap objects               : %12v = %10s\n", MemStats.HeapAlloc, bytes(int64(MemStats.HeapAlloc)))
	fmt.Fprintf(c.Stderr, "Allocated heap objects (cumulative)  : %12v = %10s\n", MemStats.HeapSys, bytes(int64(MemStats.HeapSys)))
	fmt.Fprintf(c.Stderr, "Heap idle memory                     : %12v = %10s\n", MemStats.HeapIdle, bytes(int64(MemStats.HeapIdle)))
	fmt.Fprintf(c.Stderr, "Heap in-use memory                   : %12v = %10s\n", MemStats.HeapInuse, bytes(int64(MemStats.HeapInuse)))
	fmt.Fprintf(c.Stderr, "Heap released memory                 : %12v = %10s\n", MemStats.HeapReleased, bytes(int64(MemStats.HeapReleased)))
	fmt.Fprintf(c.Stderr, "Heap objects waiting to be freed     : %12v = %10s\n", MemStats.HeapObjects, count(int64(MemStats.HeapObjects)))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Stack memory in use                  : %12v = %10s\n", MemStats.StackInuse, bytes(int64(MemStats.StackInuse)))
	fmt.Fprintf(c.Stderr, "Stack memory obtained from the OS    : %12v = %10s\n", MemStats.StackSys, bytes(int64(MemStats.StackSys)))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Allocated mspan structures           : %12v = %10s\n", MemStats.MSpanInuse, bytes(int64(MemStats.MSpanInuse)))
	fmt.Fprintf(c.Stderr, "mspan memory obtained from the OS    : %12v = %10s\n", MemStats.MSpanSys, bytes(int64(MemStats.MSpanSys)))
	fmt.Fprintf(c.Stderr, "Allocated mcache structures          : %12v = %10s\n", MemStats.MCacheInuse, bytes(int64(MemStats.MCacheInuse)))
	fmt.Fprintf(c.Stderr, "mcache memory obtained from the OS   : %12v = %10s\n", MemStats.MCacheSys, bytes(int64(MemStats.MCacheSys)))
	fmt.Fprintf(c.Stderr, "Allocated buckhash tables            : %12v = %10s\n", MemStats.BuckHashSys, bytes(int64(MemStats.BuckHashSys)))
	fmt.Fprintf(c.Stderr, "Allocated GC metadata                : %12v = %10s\n", MemStats.GCSys, bytes(int64(MemStats.GCSys)))
	fmt.Fprintf(c.Stderr, "Allocated other system allocations   : %12v = %10s\n", MemStats.OtherSys, bytes(int64(MemStats.OtherSys)))
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "Last GC finish time                  :   %s\n", strings.Replace(time.Unix(0, int64(MemStats.LastGC)).Format(time.RFC3339Nano), "T", "   ", 1))
	fmt.Fprintf(c.Stderr, "Target heap size of the next GC cycle: %12v = %10s\n", MemStats.NextGC, bytes(int64(MemStats.NextGC)))
	fmt.Fprintf(c.Stderr, "GC pause duration                    : %12v = %10s\n", MemStats.PauseTotalNs, duration(int64(MemStats.PauseTotalNs)))
	fmt.Fprintf(c.Stderr, "Number of completed GC cycles        : %12v = %10s\n", MemStats.NumGC, count(int64(MemStats.NumGC)))
	fmt.Fprintf(c.Stderr, "Number of forced GC cycles by app    : %12v = %10s\n", MemStats.NumForcedGC, count(int64(MemStats.NumForcedGC)))
	fmt.Fprintf(c.Stderr, "GCCPUFraction                        : %12.2f / %10s\n", MemStats.GCCPUFraction*1_000_000, "1_000_000")
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== DEBUG METRICS ==============================================================================================================================================================\n")
	LineLengths.printBuckets(c, "Line lengths")
	SkippedLineCounts.printBuckets(c, "Skipped line counts")
	SuccessiveLineCounts.printBuckets(c, "Successive line counts")
	BlockLineCounts.printBuckets(c, "Block line counts")
	Timestamp_Lenghts.printBuckets(c, "Timestamp lengths")
	Timestamp_FirstDigitIndexes.printBuckets(c, "First digit indexes")
	Timestamp_FirstDigitIndexesActual.printBuckets(c, "First digit indexes actual")
	Timestamp_LastDigitIndexes.printBuckets(c, "Last digit indexes")
	Timestamp_NanosLengths.printBuckets(c, "Timestamp nanos digit counts")
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "ParseTimestamp debugging\n")
	fmt.Fprintf(c.Stderr, "  too short             : %8s ~ %15d\n", "", Timestamp_LineTooShort)
	fmt.Fprintf(c.Stderr, "  no digit              : %8s ~ %15d\n", "", Timestamp_NoFirstDigit)
	fmt.Fprintf(c.Stderr, "  too short after digit : %8s ~ %15d\n", "", Timestamp_LineTooShortAfterFirstDigit)
	fmt.Fprintf(c.Stderr, "  no year               : %8s ~ %15d\n", "", Timestamp_NoYear)
	fmt.Fprintf(c.Stderr, "  2-digit year 1900     : %8s ~ %15d\n", "", Timestamp_2DigitYear_1900)
	fmt.Fprintf(c.Stderr, "  2-digit year 2000     : %8s ~ %15d\n", "", Timestamp_2DigitYear_2000)
	fmt.Fprintf(c.Stderr, "  4-digit year out-range: %8s ~ %15d\n", "", Timestamp_4DigitYear_OutOfRange)
	fmt.Fprintf(c.Stderr, "  no month              : %8s ~ %15d\n", "", Timestamp_NoMonth)
	fmt.Fprintf(c.Stderr, "  month out of range    : %8s ~ %15d\n", "", Timestamp_MonthOutOfRange)
	fmt.Fprintf(c.Stderr, "  no day                : %8s ~ %15d\n", "", Timestamp_NoDay)
	fmt.Fprintf(c.Stderr, "  day out of range      : %8s ~ %15d\n", "", Timestamp_DayOutOfRange)
	fmt.Fprintf(c.Stderr, "  space operator mismtch: %8s ~ %15d\n", "", Timestamp_SpaceOperatorMismatch)
	fmt.Fprintf(c.Stderr, "  no hour               : %8s ~ %15d\n", "", Timestamp_NoHour)
	fmt.Fprintf(c.Stderr, "  hour out of range     : %8s ~ %15d\n", "", Timestamp_HourOutOfRange)
	fmt.Fprintf(c.Stderr, "  no hour separator     : %8s ~ %15d\n", "", Timestamp_NoHourSeparator)
	fmt.Fprintf(c.Stderr, "  hour separator mismtch: %8s ~ %15d => %v\n", "", Timestamp_HourSeparatorMismatch, Timestamp_MismatchedHourSeparators)
	fmt.Fprintf(c.Stderr, "  no minute             : %8s ~ %15d\n", "", Timestamp_NoMinute)
	fmt.Fprintf(c.Stderr, "  minute out of range   : %8s ~ %15d\n", "", Timestamp_MinuteOutOfRange)
	fmt.Fprintf(c.Stderr, "  no minute separator   : %8s ~ %15d\n", "", Timestamp_NoMinuteSeparator)
	fmt.Fprintf(c.Stderr, "  minute sep. mismatch  : %8s ~ %15d => %v\n", "", Timestamp_MinuteSeparatorMismatch, Timestamp_MismatchedMinuteSeparators)
	fmt.Fprintf(c.Stderr, "  no second             : %8s ~ %15d\n", "", Timestamp_NoSecond)
	fmt.Fprintf(c.Stderr, "  second out of range   : %8s ~ %15d\n", "", Timestamp_SecondOutOfRange)
	fmt.Fprintf(c.Stderr, "  has nanos             : %8s ~ %15d\n", percent(Timestamp_HasNanos, Timestamp_HasNanos+Timestamp_HasNotNanos), Timestamp_HasNanos)
	fmt.Fprintf(c.Stderr, "  has not nanos         : %8s ~ %15d\n", percent(Timestamp_HasNotNanos, Timestamp_HasNanos+Timestamp_HasNotNanos), Timestamp_HasNotNanos)
	fmt.Fprintf(c.Stderr, "  no timezone           : %8s ~ %15d\n", "", Timestamp_NoTimezone)
	fmt.Fprintf(c.Stderr, "  UTC timezone          : %8s ~ %15d\n", "", Timestamp_UtcTimezone)
	fmt.Fprintf(c.Stderr, "  non-UTC timezone      : %8s ~ %15d\n", "", Timestamp_NonUtcTimezone)
	fmt.Fprintf(c.Stderr, "  timezone early return : %8s ~ %15d\n", "", Timestamp_TimezoneEarlyReturn)
	fmt.Fprintf(c.Stderr, "  no timezone hour      : %8s ~ %15d\n", "", Timestamp_NoTimezoneHour)
	fmt.Fprintf(c.Stderr, "  tz hour out-range     : %8s ~ %15d\n", "", Timestamp_TimezoneHourOutOfRange)
	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "===== FILE LIST ==================================================================================================================================================================\n")
	fmt.Fprintf(c.Stderr, "File list (%d files):\n", len(MatchedFiles))
	sort.Strings(MatchedFiles)
	for i, file := range MatchedFiles {
		fmt.Fprintf(c.Stderr, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(c.Stderr, "==================================================================================================================================================================================\n")
}

func reverseMap(m map[string]string) map[string][]string {
	reversed := make(map[string][]string)
	for k, v := range m {
		reversed[v] = append(reversed[v], k)
	}
	return reversed
}

func getKeysSorted(m map[string][]string) []string {
	keysSorted := make([]string, 0, len(m))
	for k := range m {
		keysSorted = append(keysSorted, k)
	}
	sort.Strings(keysSorted)
	return keysSorted
}

func printTree(c *AppConfig, node *TreeNode, depth int) {
	nanoseconds := node.Metric.Duration
	printTreeNode(c, depth, node.Name, node.Metric.CallCount, nanoseconds, "")

	if node.Children != nil {
		childTotal := int64(0)
		for _, child := range node.Children {
			printTree(c, child, depth+1)
			childTotal += child.Metric.Duration
		}

		rest := nanoseconds - childTotal
		if rest > 0 {
			printTreeNode(c, depth+1, "..rest of "+node.Name, node.Metric.CallCount, rest, "")
		}
	}
}

func (m *CallMetric) printTreeNode(c *AppConfig, name string, extra string) {
	printTreeNode(c, 0, name, m.CallCount, m.Duration, extra)
}

func printTreeNode(c *AppConfig, depth int, name string, n int64, nanoseconds int64, extra string) {
	padLen := 35
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(
		c.Stderr,
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
func (b *BucketMetric) printBuckets(c *AppConfig, name string) {
	minValue := b.min
	maxValue := b.max
	total := b.count
	avgValue := b.sum / max(1, total)

	fmt.Fprintf(c.Stderr, "\n")
	fmt.Fprintf(c.Stderr, "%s\n", name)
	fmt.Fprintf(c.Stderr, "  summary\n")
	fmt.Fprintf(c.Stderr, "    min          : %8s ~ %15v = %10s\n", "", minValue, count(minValue))
	fmt.Fprintf(c.Stderr, "    avg          : %8s ~ %15v = %10s\n", "", avgValue, count(avgValue))
	fmt.Fprintf(c.Stderr, "    max          : %8s ~ %15v = %10s\n", "", maxValue, count(maxValue))
	fmt.Fprintf(c.Stderr, "    count        : %8s ~ %15v = %10s\n", "", total, count(total))
	fmt.Fprintf(c.Stderr, "  buckets\n")

	var cumulative int64

	for i, level := range b.levels {
		value := b.values[i]
		cumulative += value
		fmt.Fprintf(c.Stderr, "    ≤ %-6d     : %8s ~ %15d ≈ %15d (cumulative) ~ %10s (cumulative)\n", level, percent(value, total), value, cumulative, percent(cumulative, total))
	}

	lastLevel := b.levels[len(b.levels)-1]
	remaining := total - cumulative
	cumulative += remaining
	fmt.Fprintf(c.Stderr, "    > %-6d     : %8s ~ %15d ≈ %15d (cumulative) ~ %10s (cumulative)\n", lastLevel, percent(remaining, total), remaining, cumulative, percent(total, total))
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

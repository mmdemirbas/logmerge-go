package main

import (
	"fmt"
	"os"
	"time"
)

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
	MeasurementCalls       int64
	MeasurementOverheadAvg int64

	// Byte count stats

	BytesRead                      int64
	BytesWrittenForTimestamps      int64
	BytesWrittenForOutputNames     int64
	BytesWrittenForRawData         int64
	BytesWrittenForMissingNewlines int64

	// Line count stats

	LinesRead              int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64

	// Line length stats
	LineLengths = NewBucketMetric(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)

	// Merge debugging
	HeapPopCount         int64
	HeapPushCount        int64
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
	if disableMetricsCollection {
		return noTimestamp
	}
	enterContext(name)
	return time.Now()
}

func MeasureSince(startNanos time.Time) int64 {
	if disableMetricsCollection {
		return 0
	}

	elapsed := time.Since(startNanos).Nanoseconds()

	exitContext(elapsed)
	MeasurementCalls++

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
	if disableMetricsCollection {
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

func enterContext(name string) {
	// Do as less work as possible since this is not measured
	children := currentTreeNode.ChildrenByName
	if children == nil {
		currentTreeNode = &TreeNode{Name: name, Parent: currentTreeNode}
	} else {
		existingNode, ok := children[name]
		if !ok {
			currentTreeNode = &TreeNode{Name: name, Parent: currentTreeNode}
		} else {
			currentTreeNode = existingNode
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

	if parent == metricsTree {
		parent.Metric.Duration += duration
	}

	currentTreeNode = parent
}

//goland:noinspection GoUnhandledErrorResult
func PrintMetrics(
	stderr *os.File,
	startTime time.Time,
	elapsedTime time.Duration,
	inputPath string,
	stdoutName string,
	stderrName string,
	pprofEnabled bool,
	err error,
) {
	writtenBytesOverhead := BytesWrittenForTimestamps + BytesWrittenForOutputNames + BytesWrittenForMissingNewlines
	writtenBytes := BytesWrittenForRawData + writtenBytesOverhead

	if stdoutName == "" {
		stdoutName = "(stdout)"
	}
	if stderrName == "" {
		stderrName = "(stderr)"
	}

	fmt.Fprintf(stderr, "===== SUMMARY ====================================================================================================================================================================\n")
	fmt.Fprintf(stderr, "Start time              : %s\n", startTime.Format(time.RFC3339Nano))
	fmt.Fprintf(stderr, "Error                   : %v\n", err)
	fmt.Fprintf(stderr, "Total main duration     : %v\n", elapsedTime)
	fmt.Fprintf(stderr, "\n")
	fmt.Fprintf(stderr, "===== CONFIGURATION ==============================================================================================================================================================\n")
	fmt.Fprintf(stderr, "Input path              : %s\n", inputPath)
	fmt.Fprintf(stderr, "Output path             : %s\n", stdoutName)
	fmt.Fprintf(stderr, "Log path                : %s\n", stderrName)
	fmt.Fprintf(stderr, "pprof enabled           : %v\n", pprofEnabled)
	fmt.Fprintf(stderr, "enableDebugLogging      : %v\n", enableDebugLogging)
	fmt.Fprintf(stderr, "writeTimestamp          : %v\n", writeTimestamp)
	fmt.Fprintf(stderr, "writeSourceNames        : %v\n", writeSourceNames)
	fmt.Fprintf(stderr, "timestampSearchPrefixLen: %12v = %10s\n", timestampSearchPrefixLen, bytes(timestampSearchPrefixLen))
	fmt.Fprintf(stderr, "readerBufferSize        : %12v = %10s\n", readerBufferSize, bytes(readerBufferSize))
	fmt.Fprintf(stderr, "writerBufferSize        : %12v = %10s\n", writerBufferSize, bytes(writerBufferSize))
	fmt.Fprintf(stderr, "excludedStrictSuffixes  : %v\n", excludedStrictSuffixes)
	fmt.Fprintf(stderr, "includedStrictSuffixes  : %v\n", includedStrictSuffixes)
	fmt.Fprintf(stderr, "excludedLenientSuffixes : %v\n", excludedLenientSuffixes)
	fmt.Fprintf(stderr, "includedLenientSuffixes : %v\n", includedLenientSuffixes)
	fmt.Fprintf(stderr, "\n")
	fmt.Fprintf(stderr, "===== METRICS SUMMARY ============================================================================================================================================================\n")
	printTreeNode(stderr, 0, "ListFiles", 1, ListFilesDuration, bytesSpeed(FilesScanned, ListFilesDuration))
	printTreeNode(stderr, 0, "FillBuffer", FillBufferMetric.CallCount, FillBufferMetric.Duration, bytesSpeed(BytesRead, ProcessDuration))
	printTreeNode(stderr, 0, "BufferAsSlice", BufferAsSliceMetric.CallCount, BufferAsSliceMetric.Duration, countSpeed(LinesRead, ProcessDuration))
	printTreeNode(stderr, 0, "ParseTimestamp", ParseTimestampMetric.CallCount, ParseTimestampMetric.Duration, countSpeed(LinesRead, ProcessDuration))
	printTreeNode(stderr, 0, "PeekNextLineSlice", PeekNextLineSliceMetric.CallCount, PeekNextLineSliceMetric.Duration, countSpeed(LinesRead, ProcessDuration))
	printTreeNode(stderr, 0, "WriteOutput", WriteOutputMetric.CallCount, WriteOutputMetric.Duration, bytesSpeed(writtenBytes, ProcessDuration))
	printTreeNode(stderr, 0, "MeasurementOverhead", MeasurementCalls, MeasurementOverheadAvg*MeasurementCalls, "")
	fmt.Fprintf(stderr, "\n")
	fmt.Fprintf(stderr, "===== METRICS TREE ===============================================================================================================================================================\n")
	printTree(stderr, metricsTree, 0)
	fmt.Fprintf(stderr, "\n")
	fmt.Fprintf(stderr, "===== METRIC DETAILS =============================================================================================================================================================\n")
	fmt.Fprintf(stderr, "File count stats\n")
	fmt.Fprintf(stderr, "  dirs scanned          : %8s ~ %12d\n", "", DirsScanned)
	fmt.Fprintf(stderr, "  files scanned         : %8s ~ %12d ≈ %10s\n", percent(FilesScanned, FilesScanned), FilesScanned, countSpeed(FilesScanned, ListFilesDuration))
	fmt.Fprintf(stderr, "  files matched         : %8s ~ %12d\n", percent(FilesMatched, FilesScanned), FilesMatched)
	fmt.Fprintf(stderr, "Byte count stats\n")
	fmt.Fprintf(stderr, "  bytes read            : %8s ~ %12d = %10s ≈ %s\n", "", BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, ProcessDuration))
	fmt.Fprintf(stderr, "  bytes written         : %8s ~ %12d = %10s ≈ %s\n", percent(writtenBytes, writtenBytes), writtenBytes, bytes(writtenBytes), bytesSpeed(writtenBytes, ProcessDuration))
	fmt.Fprintf(stderr, "    raw data            : %8s ~ %12v = %10s\n", percent(BytesWrittenForRawData, writtenBytes), BytesWrittenForRawData, bytes(BytesWrittenForRawData))
	fmt.Fprintf(stderr, "    overhead            : %8s ~ %12v = %10s\n", percent(writtenBytesOverhead, writtenBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(stderr, "      timestamps        : %8s ~ %12v = %10s\n", percent(BytesWrittenForTimestamps, writtenBytes), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(stderr, "      source names      : %8s ~ %12v = %10s\n", percent(BytesWrittenForOutputNames, writtenBytes), BytesWrittenForOutputNames, bytes(BytesWrittenForOutputNames))
	fmt.Fprintf(stderr, "      missing newlines  : %8s ~ %12v = %10s\n", percent(BytesWrittenForMissingNewlines, writtenBytes), BytesWrittenForMissingNewlines, bytes(BytesWrittenForMissingNewlines))
	fmt.Fprintf(stderr, "Line count stats\n")
	fmt.Fprintf(stderr, "  lines read            : %8s ~ %12d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, ProcessDuration))
	fmt.Fprintf(stderr, "    with timestamp      : %8s ~ %12v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(stderr, "    without timestamp   : %8s ~ %12v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
	fmt.Fprintf(stderr, "Line length stats\n")
	fmt.Fprintf(stderr, "  max line length       : %8s ~ %12d\n", "", LineLengths.max)
	fmt.Fprintf(stderr, "  line length buckets\n")
	LineLengths.printBuckets(stderr, LinesRead)
	fmt.Fprintf(stderr, "Merge debugging\n")
	fmt.Fprintf(stderr, "  heap pop count        : %8s ~ %12d ≈ %s\n", "", HeapPopCount, count(HeapPopCount))
	fmt.Fprintf(stderr, "  heap push count       : %8s ~ %12d ≈ %s\n", "", HeapPushCount, count(HeapPushCount))
	fmt.Fprintf(stderr, "  max successive lines  : %8s ~ %12v = %10s\n", "", SuccessiveLineCounts.max, count(SuccessiveLineCounts.max))
	fmt.Fprintf(stderr, "  successive line count buckets\n")
	SuccessiveLineCounts.printBuckets(stderr, LinesRead)
	fmt.Fprintf(stderr, "ParseTimestamp debugging\n")
	fmt.Fprintf(stderr, "  first digit index     : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinFirstDigitIndex, ParseTimestamp_MaxFirstDigitIndex)
	fmt.Fprintf(stderr, "  start digit index     : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinFirstDigitIndexActual, ParseTimestamp_MaxFirstDigitIndexActual)
	fmt.Fprintf(stderr, "  end digit index       : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinTimestampEndIndex, ParseTimestamp_MaxTimestampEndIndex)
	fmt.Fprintf(stderr, "  digit index buckets\n")
	for i, level := range ParseTimestamp_DigitIndexLevels {
		fmt.Fprintf(stderr, "    ≤ %-6d            : %8s ~ %7d frst ≈ %8d start ≈ %8d end\n", level, "", ParseTimestamp_FirstDigitIndexes.values[i], ParseTimestamp_FirstDigitIndexesActual.values[i], ParseTimestamp_LastDigitIndexes.values[i])
	}
	fmt.Fprintf(stderr, "  timestamp length      : %8s ~ %8d min ≈ %8d max\n", "", ParseTimestamp_MinTimestampLength, ParseTimestamp_MaxTimestampLength)
	fmt.Fprintf(stderr, "  timestamp length buckets\n")
	ParseTimestamp_Lenghts.printBuckets(stderr, LinesWithTimestamps)
	fmt.Fprintf(stderr, "  too short             : %8s ~ %12d\n", "", ParseTimestamp_LineTooShort)
	fmt.Fprintf(stderr, "  no digit              : %8s ~ %12d\n", "", ParseTimestamp_NoFirstDigit)
	fmt.Fprintf(stderr, "  too short after digit : %8s ~ %12d\n", "", ParseTimestamp_LineTooShortAfterFirstDigit)
	fmt.Fprintf(stderr, "  no year               : %8s ~ %12d\n", "", ParseTimestamp_NoYear)
	fmt.Fprintf(stderr, "  2-digit year 1900     : %8s ~ %12d\n", "", ParseTimestamp_2DigitYear_1900)
	fmt.Fprintf(stderr, "  2-digit year 2000     : %8s ~ %12d\n", "", ParseTimestamp_2DigitYear_2000)
	fmt.Fprintf(stderr, "  4-digit year out-range: %8s ~ %12d\n", "", ParseTimestamp_4DigitYear_OutOfRange)
	fmt.Fprintf(stderr, "  no month              : %8s ~ %12d\n", "", ParseTimestamp_NoMonth)
	fmt.Fprintf(stderr, "  month out of range    : %8s ~ %12d\n", "", ParseTimestamp_MonthOutOfRange)
	fmt.Fprintf(stderr, "  no day                : %8s ~ %12d\n", "", ParseTimestamp_NoDay)
	fmt.Fprintf(stderr, "  day out of range      : %8s ~ %12d\n", "", ParseTimestamp_DayOutOfRange)
	fmt.Fprintf(stderr, "  space operator mismtch: %8s ~ %12d\n", "", ParseTimestamp_SpaceOperatorMismatch)
	fmt.Fprintf(stderr, "  no hour               : %8s ~ %12d\n", "", ParseTimestamp_NoHour)
	fmt.Fprintf(stderr, "  hour out of range     : %8s ~ %12d\n", "", ParseTimestamp_HourOutOfRange)
	fmt.Fprintf(stderr, "  no hour separator     : %8s ~ %12d\n", "", ParseTimestamp_NoHourSeparator)
	fmt.Fprintf(stderr, "  hour separator mismtch: %8s ~ %12d => %v\n", "", ParseTimestamp_HourSeparatorMismatch, ParseTimestamp_MismatchedHourSeparators)
	fmt.Fprintf(stderr, "  no minute             : %8s ~ %12d\n", "", ParseTimestamp_NoMinute)
	fmt.Fprintf(stderr, "  minute out of range   : %8s ~ %12d\n", "", ParseTimestamp_MinuteOutOfRange)
	fmt.Fprintf(stderr, "  no minute separator   : %8s ~ %12d\n", "", ParseTimestamp_NoMinuteSeparator)
	fmt.Fprintf(stderr, "  minute sep. mismatch  : %8s ~ %12d => %v\n", "", ParseTimestamp_MinuteSeparatorMismatch, ParseTimestamp_MismatchedMinuteSeparators)
	fmt.Fprintf(stderr, "  no second             : %8s ~ %12d\n", "", ParseTimestamp_NoSecond)
	fmt.Fprintf(stderr, "  second out of range   : %8s ~ %12d\n", "", ParseTimestamp_SecondOutOfRange)
	fmt.Fprintf(stderr, "  has nanos             : %8s ~ %12d\n", percent(ParseTimestamp_HasNanos, ParseTimestamp_HasNanos+ParseTimestamp_HasNotNanos), ParseTimestamp_HasNanos)
	fmt.Fprintf(stderr, "  has not nanos         : %8s ~ %12d\n", percent(ParseTimestamp_HasNotNanos, ParseTimestamp_HasNanos+ParseTimestamp_HasNotNanos), ParseTimestamp_HasNotNanos)
	fmt.Fprintf(stderr, "  nanos length buckets\n")
	ParseTimestamp_NanosLengths.printBuckets(stderr, ParseTimestamp_HasNanos)
	fmt.Fprintf(stderr, "  no timezone           : %8s ~ %12d\n", "", ParseTimestamp_NoTimezone)
	fmt.Fprintf(stderr, "  UTC timezone          : %8s ~ %12d\n", "", ParseTimestamp_UtcTimezone)
	fmt.Fprintf(stderr, "  non-UTC timezone      : %8s ~ %12d\n", "", ParseTimestamp_NonUtcTimezone)
	fmt.Fprintf(stderr, "  timezone early return : %8s ~ %12d\n", "", ParseTimestamp_TimezoneEarlyReturn)
	fmt.Fprintf(stderr, "  no timezone hour      : %8s ~ %12d\n", "", ParseTimestamp_NoTimezoneHour)
	fmt.Fprintf(stderr, "  tz hour out-range     : %8s ~ %12d\n", "", ParseTimestamp_TimezoneHourOutOfRange)
	fmt.Fprintf(stderr, "\n")
	fmt.Fprintf(stderr, "===== FILE LIST ==================================================================================================================================================================\n")
	fmt.Fprintf(stderr, "File list (%d files):\n", len(MatchedFiles))
	for i, file := range MatchedFiles {
		fmt.Fprintf(stderr, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(stderr, "==================================================================================================================================================================================\n")
}

func printTree(stderr *os.File, node *TreeNode, depth int) {
	nanoseconds := node.Metric.Duration
	printTreeNode(stderr, depth, node.Name, node.Metric.CallCount, nanoseconds, "")

	if node.Children != nil {
		childTotal := int64(0)
		for _, child := range node.Children {
			printTree(stderr, child, depth+1)
			childTotal += child.Metric.Duration
		}

		rest := nanoseconds - childTotal
		if rest > 0 {
			printTreeNode(stderr, depth+1, "..rest of "+node.Name, node.Metric.CallCount, rest, "")
		}
	}
}

func printTreeNode(stderr *os.File, depth int, name string, n int64, nanoseconds int64, extra string) {
	padLen := 45
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(
		stderr,
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

func (b *BucketMetric) printBuckets(stderr *os.File, total int64) {
	var cumulative int64

	for i, level := range b.levels {
		value := b.values[i]
		cumulative += value
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(stderr, "    ≤ %-6d            : %8s ~ %12d ≈ %8s (cumulative)\n", level, percent(value, total), value, percent(cumulative, total))
	}

	remaining := total - cumulative
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(stderr, "    rest..              : %8s ~ %12d ≈ %8s (cumulative)\n", percent(remaining, total), remaining, percent(total, total))
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

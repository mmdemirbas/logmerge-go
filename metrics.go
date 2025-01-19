// metrics.go
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

	// Timing stats by phase (nanoseconds)

	TotalMainDuration     int64
	ParseOptionsDuration  int64
	ListFilesDuration     int64
	OpenFilesDuration     int64
	MergeScannersDuration int64

	// Timing stats by operation (nanoseconds)

	NewWriterDuration            int64
	MaxOutputNameLenCalcDuration int64
	HeapInitDuration             int64
	HeapPopulateDuration         int64
	MergeLoopDuration            int64
	HeapPopDuration              int64
	InnerReadWriteDuration       int64
	InnerHeapPushDuration        int64
	ParseTimestampDuration       int64
	WriteLineDuration            int64
	AppendFormatDuration         int64

	// Byte count stats

	BytesRead int64

	// Written bytes breakdown

	BytesWrittenForRawLines    int64
	BytesWrittenForTimestamps  int64
	BytesWrittenForOutputNames int64

	// Line count stats

	LinesRead              int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64

	// Line length stats

	MaxLineLength              int
	LineLengthBucketThresholds = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000}
	LineLengthBucketSizes      = make([]int64, len(LineLengthBucketThresholds))

	// ParseTimestamp debugging

	ParseTimestamp_NoFirstDigit                int64
	ParseTimestamp_MinFirstDigitIndex          int
	ParseTimestamp_MaxFirstDigitIndex          int
	ParseTimestamp_MinFirstDigitIndexActual    int
	ParseTimestamp_MaxFirstDigitIndexActual    int
	ParseTimestamp_DigitIndexThresholds        = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000}
	ParseTimestamp_FirstDigitCounts            = make([]int64, len(ParseTimestamp_DigitIndexThresholds))
	ParseTimestamp_FirstDigitCountsActual      = make([]int64, len(ParseTimestamp_DigitIndexThresholds))
	ParseTimestamp_LastDigitCounts             = make([]int64, len(ParseTimestamp_DigitIndexThresholds))
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
	ParseTimestamp_MismatchedHourSeparators    []uint8
	ParseTimestamp_NoMinute                    int64
	ParseTimestamp_MinuteOutOfRange            int64
	ParseTimestamp_NoMinuteSeparator           int64
	ParseTimestamp_MinuteSeparatorMismatch     int64
	ParseTimestamp_NoSecond                    int64
	ParseTimestamp_SecondOutOfRange            int64
	ParseTimestamp_HasNanos                    int64
	ParseTimestamp_NanosLengthThresholds       = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	ParseTimestamp_NanosLengthCounts           = make([]int64, len(ParseTimestamp_NanosLengthThresholds))
	ParseTimestamp_NoTimezone                  int64
	ParseTimestamp_UtcTimezone                 int64
	ParseTimestamp_NonUtcTimezone              int64
	ParseTimestamp_TimezoneEarlyReturn         int64
	ParseTimestamp_NoTimezoneHour              int64
	ParseTimestamp_TimezoneHourOutOfRange      int64
	ParseTimestamp_MinTimestampEndIndex        int
	ParseTimestamp_MaxTimestampEndIndex        int
)

func MeasureSince(startNanos time.Time) int64 {
	return time.Since(startNanos).Nanoseconds()
}

func UpdateBucketCount(n int, thresholds []int, counts []int64) {
	for i, threshold := range thresholds {
		if n <= threshold {
			counts[i]++
			break
		}
	}
}
func PrintMetrics(basePath *string, err error) {
	restOfMainDuration := TotalMainDuration - (ParseOptionsDuration + ListFilesDuration + OpenFilesDuration + MergeScannersDuration)
	restOfMergeScannersDuration := MergeScannersDuration - (NewWriterDuration + MaxOutputNameLenCalcDuration + HeapInitDuration + +HeapPopulateDuration + MergeLoopDuration)
	restOfMergeLoopDuration := MergeLoopDuration - (HeapPopDuration + InnerReadWriteDuration + InnerHeapPushDuration)
	restOfMergeScannersBreakdownDuration := MergeScannersDuration - (ParseTimestampDuration + WriteLineDuration)
	writtenBytesOverhead := BytesWrittenForTimestamps + BytesWrittenForOutputNames
	writtenBytes := BytesWrittenForRawLines + writtenBytesOverhead

	fmt.Fprintf(os.Stderr, "===== METRICS =================================================================================\n")
	fmt.Fprintf(os.Stderr, "Base path               : %s\n", *basePath)
	fmt.Fprintf(os.Stderr, "Error                   : %v\n", err)
	fmt.Fprintf(os.Stderr, "Total main duration     : %v\n", duration(TotalMainDuration))
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File count stats\n")
	fmt.Fprintf(os.Stderr, "  dirs scanned          : %8s ~ %12d\n", "", DirsScanned)
	fmt.Fprintf(os.Stderr, "  files scanned         : %8s ~ %12d ≈ %10s\n", percent(FilesScanned, FilesScanned), FilesScanned, countSpeed(FilesScanned, ListFilesDuration))
	fmt.Fprintf(os.Stderr, "  files matched         : %8s ~ %12d\n", percent(FilesMatched, FilesScanned), FilesMatched)
	fmt.Fprintf(os.Stderr, "Timing stats (all)      : %8s ~ %12v\n", timePercent(TotalMainDuration), duration(TotalMainDuration))
	fmt.Fprintf(os.Stderr, "  parse options         : %8s ~ %12v\n", timePercent(ParseOptionsDuration), duration(ParseOptionsDuration))
	fmt.Fprintf(os.Stderr, "  list files            : %8s ~ %12v\n", timePercent(ListFilesDuration), duration(ListFilesDuration))
	fmt.Fprintf(os.Stderr, "  open files            : %8s ~ %12v\n", timePercent(OpenFilesDuration), duration(OpenFilesDuration))
	fmt.Fprintf(os.Stderr, "  merge scanners        : %8s ~ %12v\n", timePercent(MergeScannersDuration), duration(MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    new writer          : %8s ~ %12v\n", timePercent(NewWriterDuration), duration(NewWriterDuration))
	fmt.Fprintf(os.Stderr, "    out name calc       : %8s ~ %12v\n", timePercent(MaxOutputNameLenCalcDuration), duration(MaxOutputNameLenCalcDuration))
	fmt.Fprintf(os.Stderr, "    heap init           : %8s ~ %12v\n", timePercent(HeapInitDuration), duration(HeapInitDuration))
	fmt.Fprintf(os.Stderr, "    heap populate       : %8s ~ %12v\n", timePercent(HeapPopulateDuration), duration(HeapPopulateDuration))
	fmt.Fprintf(os.Stderr, "    merge loop          : %8s ~ %12v\n", timePercent(MergeLoopDuration), duration(MergeLoopDuration))
	fmt.Fprintf(os.Stderr, "      heap pop          : %8s ~ %12v\n", timePercent(HeapPopDuration), duration(HeapPopDuration))
	fmt.Fprintf(os.Stderr, "      read/write        : %8s ~ %12v\n", timePercent(InnerReadWriteDuration), duration(InnerReadWriteDuration))
	fmt.Fprintf(os.Stderr, "      inner heap push   : %8s ~ %12v\n", timePercent(InnerHeapPushDuration), duration(InnerHeapPushDuration))
	fmt.Fprintf(os.Stderr, "      rest..            : %8s ~ %12v\n", timePercent(restOfMergeLoopDuration), duration(restOfMergeLoopDuration))
	fmt.Fprintf(os.Stderr, "    rest..              : %8s ~ %12v\n", timePercent(restOfMergeScannersDuration), duration(restOfMergeScannersDuration))
	fmt.Fprintf(os.Stderr, "  merge scanners brkdwn\n")
	fmt.Fprintf(os.Stderr, "    parse timestamp     : %8s ~ %12v\n", timePercent(ParseTimestampDuration), duration(ParseTimestampDuration))
	fmt.Fprintf(os.Stderr, "    write line          : %8s ~ %12v\n", timePercent(WriteLineDuration), duration(WriteLineDuration))
	fmt.Fprintf(os.Stderr, "      append format     : %8s ~ %12v\n", timePercent(AppendFormatDuration), duration(AppendFormatDuration))
	fmt.Fprintf(os.Stderr, "    rest..              : %8s ~ %12v\n", timePercent(restOfMergeScannersBreakdownDuration), duration(restOfMergeScannersBreakdownDuration))
	fmt.Fprintf(os.Stderr, "  rest..                : %8s ~ %12v\n", timePercent(restOfMainDuration), duration(restOfMainDuration))
	fmt.Fprintf(os.Stderr, "Byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes read            : %8s ~ %12d = %10s ≈ %s\n", "", BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "  bytes written         : %8s ~ %12d = %10s ≈ %s\n", percent(writtenBytes, writtenBytes), writtenBytes, bytes(writtenBytes), bytesSpeed(writtenBytes, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    raw lines           : %8s ~ %12v = %10s\n", percent(BytesWrittenForRawLines, writtenBytes), BytesWrittenForRawLines, bytes(BytesWrittenForRawLines))
	fmt.Fprintf(os.Stderr, "    overhead            : %8s ~ %12v = %10s\n", percent(writtenBytesOverhead, writtenBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(os.Stderr, "      timestamps        : %8s ~ %12v = %10s\n", percent(BytesWrittenForTimestamps, writtenBytes), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(os.Stderr, "      source names      : %8s ~ %12v = %10s\n", percent(BytesWrittenForOutputNames, writtenBytes), BytesWrittenForOutputNames, bytes(BytesWrittenForOutputNames))
	fmt.Fprintf(os.Stderr, "Line count stats\n")
	fmt.Fprintf(os.Stderr, "  lines read            : %8s ~ %12d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    with timestamp      : %8s ~ %12v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(os.Stderr, "    without timestamp   : %8s ~ %12v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
	fmt.Fprintf(os.Stderr, "Line length stats\n")
	fmt.Fprintf(os.Stderr, "  max line length       : %8s ~ %12d\n", "", MaxLineLength)
	fmt.Fprintf(os.Stderr, "  line length buckets\n")
	var cumulative int64
	for i, threshold := range LineLengthBucketThresholds {
		lineCount := LineLengthBucketSizes[i]
		cumulative += lineCount
		fmt.Fprintf(os.Stderr, "    ≤ %-6d            : %8s ~ %12d ≈ %8s (cumulative)\n", threshold, percent(lineCount, LinesRead), lineCount, percent(cumulative, LinesRead))
	}
	remainingLineCount := LinesRead - cumulative
	cumulative += remainingLineCount
	fmt.Fprintf(os.Stderr, "    higher              : %8s ~ %12d ≈ %8s (cumulative)\n", percent(remainingLineCount, LinesRead), remainingLineCount, percent(cumulative, LinesRead))
	fmt.Fprintf(os.Stderr, "ParseTimestamp debugging\n")
	fmt.Fprintf(os.Stderr, "  min digit index       : %8s ~ %7d 1st# ≈ %8d start ≈ %8d end\n", "", ParseTimestamp_MinFirstDigitIndex, ParseTimestamp_MinFirstDigitIndexActual, ParseTimestamp_MinTimestampEndIndex)
	fmt.Fprintf(os.Stderr, "  max digit index       : %8s ~ %7d 1st# ≈ %8d start ≈ %8d end\n", "", ParseTimestamp_MaxFirstDigitIndex, ParseTimestamp_MaxFirstDigitIndexActual, ParseTimestamp_MaxTimestampEndIndex)
	fmt.Fprintf(os.Stderr, "  digit index buckets\n")
	for i, threshold := range ParseTimestamp_DigitIndexThresholds {
		fmt.Fprintf(os.Stderr, "    ≤ %-6d            : %8s ~ %7d 1st# ≈ %8d start ≈ %8d end\n", threshold, "", ParseTimestamp_FirstDigitCounts[i], ParseTimestamp_FirstDigitCountsActual[i], ParseTimestamp_LastDigitCounts[i])
	}
	fmt.Fprintf(os.Stderr, "  too short             : %8s ~ %12d\n", "", ParseTimestamp_LineTooShort)
	fmt.Fprintf(os.Stderr, "  no digit              : %8s ~ %12d\n", "", ParseTimestamp_NoFirstDigit)
	fmt.Fprintf(os.Stderr, "  too short after digit : %8s ~ %12d\n", "", ParseTimestamp_LineTooShortAfterFirstDigit)
	fmt.Fprintf(os.Stderr, "  no year               : %8s ~ %12d\n", "", ParseTimestamp_NoYear)
	fmt.Fprintf(os.Stderr, "  2-digit year 1900     : %8s ~ %12d\n", "", ParseTimestamp_2DigitYear_1900)
	fmt.Fprintf(os.Stderr, "  2-digit year 2000     : %8s ~ %12d\n", "", ParseTimestamp_2DigitYear_2000)
	fmt.Fprintf(os.Stderr, "  4-digit year our-range: %8s ~ %12d\n", "", ParseTimestamp_4DigitYear_OutOfRange)
	fmt.Fprintf(os.Stderr, "  no month             : %8s ~ %12d\n", "", ParseTimestamp_NoMonth)
	fmt.Fprintf(os.Stderr, "  month out of range   : %8s ~ %12d\n", "", ParseTimestamp_MonthOutOfRange)
	fmt.Fprintf(os.Stderr, "  no day                : %8s ~ %12d\n", "", ParseTimestamp_NoDay)
	fmt.Fprintf(os.Stderr, "  day out of range      : %8s ~ %12d\n", "", ParseTimestamp_DayOutOfRange)
	fmt.Fprintf(os.Stderr, "  space operator mismtch: %8s ~ %12d\n", "", ParseTimestamp_SpaceOperatorMismatch)
	fmt.Fprintf(os.Stderr, "  no hour               : %8s ~ %12d\n", "", ParseTimestamp_NoHour)
	fmt.Fprintf(os.Stderr, "  hour out of range     : %8s ~ %12d\n", "", ParseTimestamp_HourOutOfRange)
	fmt.Fprintf(os.Stderr, "  no hour separator     : %8s ~ %12d\n", "", ParseTimestamp_NoHourSeparator)
	fmt.Fprintf(os.Stderr, "  hour separator mismtch: %8s ~ %12d\n", "", ParseTimestamp_HourSeparatorMismatch)
	fmt.Fprintf(os.Stderr, "  mismatched hour seps  : %8s ~ %12d => %v\n", "", len(ParseTimestamp_MismatchedHourSeparators), ParseTimestamp_MismatchedHourSeparators)
	fmt.Fprintf(os.Stderr, "  no minute             : %8s ~ %12d\n", "", ParseTimestamp_NoMinute)
	fmt.Fprintf(os.Stderr, "  minute out of range   : %8s ~ %12d\n", "", ParseTimestamp_MinuteOutOfRange)
	fmt.Fprintf(os.Stderr, "  no minute separator   : %8s ~ %12d\n", "", ParseTimestamp_NoMinuteSeparator)
	fmt.Fprintf(os.Stderr, "  minute sep. mismatch  : %8s ~ %12d\n", "", ParseTimestamp_MinuteSeparatorMismatch)
	fmt.Fprintf(os.Stderr, "  no second             : %8s ~ %12d\n", "", ParseTimestamp_NoSecond)
	fmt.Fprintf(os.Stderr, "  second out of range   : %8s ~ %12d\n", "", ParseTimestamp_SecondOutOfRange)
	fmt.Fprintf(os.Stderr, "  has nanos             : %8s ~ %12d\n", "", ParseTimestamp_HasNanos)
	fmt.Fprintf(os.Stderr, "  nanos length buckets\n")
	for i, threshold := range ParseTimestamp_NanosLengthThresholds {
		fmt.Fprintf(os.Stderr, "    ≤ %-6d            : %8s ~ %12d\n", threshold, "", ParseTimestamp_NanosLengthCounts[i])
	}
	fmt.Fprintf(os.Stderr, "  no timezone           : %8s ~ %12d\n", "", ParseTimestamp_NoTimezone)
	fmt.Fprintf(os.Stderr, "  UTC timezone          : %8s ~ %12d\n", "", ParseTimestamp_UtcTimezone)
	fmt.Fprintf(os.Stderr, "  non-UTC timezone      : %8s ~ %12d\n", "", ParseTimestamp_NonUtcTimezone)
	fmt.Fprintf(os.Stderr, "  timezone early return : %8s ~ %12d\n", "", ParseTimestamp_TimezoneEarlyReturn)
	fmt.Fprintf(os.Stderr, "  no timezone hour      : %8s ~ %12d\n", "", ParseTimestamp_NoTimezoneHour)
	fmt.Fprintf(os.Stderr, "  tz hour out-range     : %8s ~ %12d\n", "", ParseTimestamp_TimezoneHourOutOfRange)
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File list (%d files):\n", len(MatchedFiles))
	for i, file := range MatchedFiles {
		fmt.Fprintf(os.Stderr, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
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

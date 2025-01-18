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
	PopulateParseLineDuration    int64
	PopulateHeapPushDuration     int64
	MergeLoopDuration            int64
	InnerMergeLoopDuration       int64
	WriteFirstLineDuration       int64
	WriteNextLinesDuration       int64
	InnerHeapPushDuration        int64

	HeapPopDuration        int64
	ParseLineFullDuration  int64
	ReadLineDuration       int64
	ReadLinePostDuration   int64
	ParseTimestampDuration int64
	WriteLineDuration      int64
	AppendFormatDuration   int64

	// Byte count stats

	BytesRead    int64
	BytesWritten int64

	// Written bytes breakdown

	BytesWrittenForRawLines    int64
	BytesWrittenForTimestamps  int64
	BytesWrittenForOutputNames int64

	// Line count stats

	LinesRead              int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64

	// Line length stats

	MaxLineLength        int
	LineLengthThresholds = []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000}
	LineLengths          = make([]int64, len(LineLengthThresholds))
)

func MeasureSince(startNanos time.Time) int64 {
	return time.Since(startNanos).Nanoseconds()
}

func PrintMetrics(basePath *string, err error) {
	restOfMainDuration := TotalMainDuration - (ParseOptionsDuration + ListFilesDuration + OpenFilesDuration + MergeScannersDuration)
	restOfMergeScannersDuration := MergeScannersDuration - (NewWriterDuration + MaxOutputNameLenCalcDuration + HeapInitDuration + +HeapPopulateDuration + MergeLoopDuration)
	restOfHeapPopulateDuration := HeapPopulateDuration - (PopulateParseLineDuration + PopulateHeapPushDuration)
	restOfInnerMergeLoopDuration := InnerMergeLoopDuration - (HeapPopDuration + WriteFirstLineDuration + WriteNextLinesDuration + InnerHeapPushDuration)
	restOfMergeLoopDuration := MergeLoopDuration - InnerMergeLoopDuration
	restOfMergeScannersBreakdownDuration := MergeScannersDuration - (ParseLineFullDuration + WriteLineDuration)
	writtenBytesOverhead := BytesWritten - BytesWrittenForRawLines

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
	fmt.Fprintf(os.Stderr, "      parse line        : %8s ~ %12v\n", timePercent(PopulateParseLineDuration), duration(PopulateParseLineDuration))
	fmt.Fprintf(os.Stderr, "      heap push         : %8s ~ %12v\n", timePercent(PopulateHeapPushDuration), duration(PopulateHeapPushDuration))
	fmt.Fprintf(os.Stderr, "      rest..            : %8s ~ %12v\n", timePercent(restOfHeapPopulateDuration), duration(restOfHeapPopulateDuration))
	fmt.Fprintf(os.Stderr, "    merge loop          : %8s ~ %12v\n", timePercent(MergeLoopDuration), duration(MergeLoopDuration))
	fmt.Fprintf(os.Stderr, "      inner merge loop  : %8s ~ %12v\n", timePercent(InnerMergeLoopDuration), duration(InnerMergeLoopDuration))
	fmt.Fprintf(os.Stderr, "        heap pop        : %8s ~ %12v\n", timePercent(HeapPopDuration), duration(HeapPopDuration))
	fmt.Fprintf(os.Stderr, "        write first line: %8s ~ %12v\n", timePercent(WriteFirstLineDuration), duration(WriteFirstLineDuration))
	fmt.Fprintf(os.Stderr, "        write next lines: %8s ~ %12v\n", timePercent(WriteNextLinesDuration), duration(WriteNextLinesDuration))
	fmt.Fprintf(os.Stderr, "        inner heap push : %8s ~ %12v\n", timePercent(InnerHeapPushDuration), duration(InnerHeapPushDuration))
	fmt.Fprintf(os.Stderr, "        rest..          : %8s ~ %12v\n", timePercent(restOfInnerMergeLoopDuration), duration(restOfInnerMergeLoopDuration))
	fmt.Fprintf(os.Stderr, "      rest..            : %8s ~ %12v\n", timePercent(restOfMergeLoopDuration), duration(restOfMergeLoopDuration))
	fmt.Fprintf(os.Stderr, "    rest..              : %8s ~ %12v\n", timePercent(restOfMergeScannersDuration), duration(restOfMergeScannersDuration))
	fmt.Fprintf(os.Stderr, "  merge scanners brkdwn\n")
	fmt.Fprintf(os.Stderr, "    parse line full     : %8s ~ %12v\n", timePercent(ParseLineFullDuration), duration(ParseLineFullDuration))
	fmt.Fprintf(os.Stderr, "      read line         : %8s ~ %12v\n", timePercent(ReadLineDuration), duration(ReadLineDuration))
	fmt.Fprintf(os.Stderr, "      read line post    : %8s ~ %12v\n", timePercent(ReadLinePostDuration), duration(ReadLinePostDuration))
	fmt.Fprintf(os.Stderr, "        parse timestamp : %8s ~ %12v\n", timePercent(ParseTimestampDuration), duration(ParseTimestampDuration))
	fmt.Fprintf(os.Stderr, "    write line          : %8s ~ %12v\n", timePercent(WriteLineDuration), duration(WriteLineDuration))
	fmt.Fprintf(os.Stderr, "      append format     : %8s ~ %12v\n", timePercent(AppendFormatDuration), duration(AppendFormatDuration))
	fmt.Fprintf(os.Stderr, "    rest..              : %8s ~ %12v\n", timePercent(restOfMergeScannersBreakdownDuration), duration(restOfMergeScannersBreakdownDuration))
	fmt.Fprintf(os.Stderr, "  rest..                : %8s ~ %12v\n", timePercent(restOfMainDuration), duration(restOfMainDuration))
	fmt.Fprintf(os.Stderr, "Byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes read            : %8s ~ %12d = %10s ≈ %s\n", "", BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "  bytes written         : %8s ~ %12d = %10s ≈ %s\n", percent(BytesWritten, BytesWritten), BytesWritten, bytes(BytesWritten), bytesSpeed(BytesWritten, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    raw lines           : %8s ~ %12v = %10s\n", percent(BytesWrittenForRawLines, BytesWritten), BytesWrittenForRawLines, bytes(BytesWrittenForRawLines))
	fmt.Fprintf(os.Stderr, "    overhead            : %8s ~ %12v = %10s\n", percent(writtenBytesOverhead, BytesWritten), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(os.Stderr, "      timestamps        : %8s ~ %12v = %10s\n", percent(BytesWrittenForTimestamps, BytesWritten), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(os.Stderr, "      source names      : %8s ~ %12v = %10s\n", percent(BytesWrittenForOutputNames, BytesWritten), BytesWrittenForOutputNames, bytes(BytesWrittenForOutputNames))
	fmt.Fprintf(os.Stderr, "Line count stats\n")
	fmt.Fprintf(os.Stderr, "  lines read            : %8s ~ %12d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    with timestamp      : %8s ~ %12v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(os.Stderr, "    without timestamp   : %8s ~ %12v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
	fmt.Fprintf(os.Stderr, "Line length stats\n")
	fmt.Fprintf(os.Stderr, "  max line length       : %8s ~ %12d\n", "", MaxLineLength)
	fmt.Fprintf(os.Stderr, "  line length counts\n")
	var cumulative int64
	for i, threshold := range LineLengthThresholds {
		lineCount := LineLengths[i]
		cumulative += lineCount
		fmt.Fprintf(os.Stderr, "    ≤ %-6d            : %8s ~ %12d ≈ %8s (cumulative)\n", threshold, percent(lineCount, LinesRead), lineCount, percent(cumulative, LinesRead))
	}
	remainingLineCount := LinesRead - cumulative
	cumulative += remainingLineCount
	fmt.Fprintf(os.Stderr, "    higher              : %8s ~ %12d ≈ %8s (cumulative)\n", percent(remainingLineCount, LinesRead), remainingLineCount, percent(cumulative, LinesRead))
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

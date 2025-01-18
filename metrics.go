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

	ParseLineFullDuration  int64
	ReadLineDuration       int64
	ReadLinePostDuration   int64
	ParseTimestampDuration int64
	WriteLineDuration      int64

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
)

func MeasureStart() time.Time {
	return time.Now()
}

func MeasureSince(startNanos time.Time) int64 {
	return time.Since(startNanos).Nanoseconds()
}

func PrintMetrics(basePath *string, err error) {
	restOfMainDuration := TotalMainDuration - (ParseOptionsDuration + ListFilesDuration + OpenFilesDuration + MergeScannersDuration)
	restOfMergeScannersDuration := MergeScannersDuration - (ParseLineFullDuration + WriteLineDuration)
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
	fmt.Fprintf(os.Stderr, "    parse line full     : %8s ~ %12v\n", timePercent(ParseLineFullDuration), duration(ParseLineFullDuration))
	fmt.Fprintf(os.Stderr, "      read line         : %8s ~ %12v\n", timePercent(ReadLineDuration), duration(ReadLineDuration))
	fmt.Fprintf(os.Stderr, "      read line post    : %8s ~ %12v\n", timePercent(ReadLinePostDuration), duration(ReadLinePostDuration))
	fmt.Fprintf(os.Stderr, "        parse timestamp : %8s ~ %12v\n", timePercent(ParseTimestampDuration), duration(ParseTimestampDuration))
	fmt.Fprintf(os.Stderr, "    write line          : %8s ~ %12v\n", timePercent(WriteLineDuration), duration(WriteLineDuration))
	fmt.Fprintf(os.Stderr, "    rest..              : %8s ~ %12v\n", timePercent(restOfMergeScannersDuration), duration(restOfMergeScannersDuration))
	fmt.Fprintf(os.Stderr, "  rest..                : %8s ~ %12v\n", timePercent(restOfMainDuration), duration(restOfMainDuration))
	fmt.Fprintf(os.Stderr, "Byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes read            : %8s ~ %12d = %10s ≈ %s\n", "", BytesRead, bytes(BytesRead), bytesSpeed(BytesRead, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "  bytes written         : %8s ~ %12d = %10s ≈ %s\n", percent(BytesWritten, BytesWritten), BytesWritten, bytes(BytesWritten), bytesSpeed(BytesWritten, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    raw lines           : %8s ~ %12v = %10s\n", percent(BytesWrittenForRawLines, BytesWritten), BytesWrittenForRawLines, bytes(BytesWrittenForRawLines))
	fmt.Fprintf(os.Stderr, "    overhead            : %8s ~ %12v = %10s\n", percent(writtenBytesOverhead, BytesWritten), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(os.Stderr, "      timestamps        : %8s ~ %12v = %10s\n", percent(BytesWrittenForTimestamps, BytesWritten), BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps))
	fmt.Fprintf(os.Stderr, "      source names      : %8s ~ %12v = %10s\n", percent(BytesWrittenForOutputNames, BytesWritten), BytesWrittenForOutputNames, bytes(BytesWrittenForOutputNames))
	fmt.Fprintf(os.Stderr, "Line count stats\n")
	fmt.Fprintf(os.Stderr, "  lines read           : %8s ~ %12d = %10s ≈ %s\n", percent(LinesRead, LinesRead), LinesRead, count(LinesRead), countSpeed(LinesRead, MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "    with timestamp     : %8s ~ %12v = %10s\n", percent(LinesWithTimestamps, LinesRead), LinesWithTimestamps, count(LinesWithTimestamps))
	fmt.Fprintf(os.Stderr, "    without timestamp  : %8s ~ %12v = %10s\n", percent(LinesWithoutTimestamps, LinesRead), LinesWithoutTimestamps, count(LinesWithoutTimestamps))
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

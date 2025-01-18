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

	ParseOptionsDuration  time.Duration
	ListFilesDuration     time.Duration
	OpenFilesDuration     time.Duration
	MergeScannersDuration time.Duration
	TotalMainDuration     time.Duration

	// Timing stats by operation (nanoseconds)

	ReadLineDuration       time.Duration
	ReadLinePostDuration   time.Duration
	ParseLineFullDuration  time.Duration
	ParseTimestampDuration time.Duration
	WriteLineDuration      time.Duration

	// Byte count stats

	BytesRead    int64
	BytesWritten int64

	// Written bytes breakdown

	BytesWrittenForTimestamps  int64
	BytesWrittenForOutputNames int64
	BytesWrittenForRawLines    int64

	// Line count stats

	LinesRead              int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64

	// Measurement overhead

	MeasureDurationCallCount int64
)

func MeasureDuration(f func()) (duration time.Duration) {
	startTime := time.Now()
	f()
	endTime := time.Now()
	duration = endTime.Sub(startTime)
	MeasureDurationCallCount++
	return
}

func PrintMetrics(basePath *string, err error) {
	// Show final stats
	fmt.Fprintf(os.Stderr, "===== METRICS =================================================================================\n")
	fmt.Fprintf(os.Stderr, "Base path           : %s\n", *basePath)
	fmt.Fprintf(os.Stderr, "Error               : %v\n", err)
	fmt.Fprintf(os.Stderr, "Total main duration : %v\n", TotalMainDuration)
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File count stats\n")
	fmt.Fprintf(os.Stderr, "  dirs scanned      : %12d\n", DirsScanned)
	fmt.Fprintf(os.Stderr, "  files scanned     : %12d ≈ %s\n", FilesScanned, countSpeed(FilesScanned))
	fmt.Fprintf(os.Stderr, "  files matched     : %12d ~ %s\n", FilesMatched, percent(FilesMatched, FilesScanned))
	fmt.Fprintf(os.Stderr, "Timing stats by phase\n")
	fmt.Fprintf(os.Stderr, "  parse options     : %12v ~ %s\n", ParseOptionsDuration, timePercent(ParseOptionsDuration))
	fmt.Fprintf(os.Stderr, "  list files        : %12v ~ %s\n", ListFilesDuration, timePercent(ListFilesDuration))
	fmt.Fprintf(os.Stderr, "  open files        : %12v ~ %s\n", OpenFilesDuration, timePercent(OpenFilesDuration))
	fmt.Fprintf(os.Stderr, "  merge scanners    : %12v ~ %s\n", MergeScannersDuration, timePercent(MergeScannersDuration))
	restPhaseTime := TotalMainDuration - ParseOptionsDuration - ListFilesDuration - OpenFilesDuration - MergeScannersDuration
	fmt.Fprintf(os.Stderr, "    ..rest..        : %12v ~ %s\n", restPhaseTime, timePercent(restPhaseTime))
	fmt.Fprintf(os.Stderr, "Timing stats by operation\n")
	fmt.Fprintf(os.Stderr, "  parse line full   : %12v ~ %s\n", ParseLineFullDuration, timePercent(ParseLineFullDuration))
	fmt.Fprintf(os.Stderr, "    read line       : %12v ~ %s\n", ReadLineDuration, timePercent(ReadLineDuration))
	fmt.Fprintf(os.Stderr, "    read line post  : %12v ~ %s\n", ReadLinePostDuration, timePercent(ReadLinePostDuration))
	fmt.Fprintf(os.Stderr, "      parse timestmp: %12v ~ %s\n", ParseTimestampDuration, timePercent(ParseTimestampDuration))
	fmt.Fprintf(os.Stderr, "  write line        : %12v ~ %s\n", WriteLineDuration, timePercent(WriteLineDuration))
	restOpTime := TotalMainDuration - ReadLineDuration - ParseTimestampDuration - WriteLineDuration
	fmt.Fprintf(os.Stderr, "    ..rest..        : %12v ~ %s\n", restOpTime, timePercent(restOpTime))
	fmt.Fprintf(os.Stderr, "Byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes read        : %12d = %s ≈ %s\n", BytesRead, bytes(BytesRead), bytesSpeed(BytesRead))
	fmt.Fprintf(os.Stderr, "  bytes written     : %12d = %s ≈ %s\n", BytesWritten, bytes(BytesWritten), bytesSpeed(BytesWritten))
	fmt.Fprintf(os.Stderr, "Written bytes breakdown\n")
	fmt.Fprintf(os.Stderr, "  bytes for ts      : %12d = %s ~ %s\n", BytesWrittenForTimestamps, bytes(BytesWrittenForTimestamps), percent(BytesWrittenForTimestamps, BytesWritten))
	fmt.Fprintf(os.Stderr, "  bytes for name    : %12d = %s ~ %s\n", BytesWrittenForOutputNames, bytes(BytesWrittenForOutputNames), percent(BytesWrittenForOutputNames, BytesWritten))
	fmt.Fprintf(os.Stderr, "  bytes for raw     : %12d = %s ~ %s\n", BytesWrittenForRawLines, bytes(BytesWrittenForRawLines), percent(BytesWrittenForRawLines, BytesWritten))
	fmt.Fprintf(os.Stderr, "Line count stats\n")
	fmt.Fprintf(os.Stderr, "  lines read        : %12d ≈ %s\n", LinesRead, countSpeed(LinesRead))
	fmt.Fprintf(os.Stderr, "  lines with ts     : %12d ~ %s\n", LinesWithTimestamps, percent(LinesWithTimestamps, LinesRead))
	fmt.Fprintf(os.Stderr, "  lines without ts  : %12d ~ %s\n", LinesWithoutTimestamps, percent(LinesWithoutTimestamps, LinesRead))
	fmt.Fprintf(os.Stderr, "Overhead:\n")
	// Call time.Now() 1_000_000 times
	callCount := 1_000_000
	totalTime := time.Duration(0)
	for i := 0; i < callCount; i++ {
		totalTime += MeasureDuration(func() {
			time.Now()
		})
	}
	averageTime := totalTime / time.Duration(callCount)
	fmt.Fprintf(os.Stderr, "  measure call cnt : %12d\n", MeasureDurationCallCount)
	fmt.Fprintf(os.Stderr, "  time.Now() avg.  : %12v\n", averageTime)
	fmt.Fprintf(os.Stderr, "  estimate overhead: %12v ~ %s\n", 2*averageTime*time.Duration(MeasureDurationCallCount), timePercent(2*averageTime*time.Duration(MeasureDurationCallCount)))
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File list (%d files):\n", len(MatchedFiles))
	for i, file := range MatchedFiles {
		fmt.Fprintf(os.Stderr, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
}

func bytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%7d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%7.2f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%7.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%7.2f GB", float64(bytes)/(1024*1024*1024))
}

func timePercent(value time.Duration) string {
	return fmt.Sprintf("%5.2f %%", div(value.Nanoseconds(), TotalMainDuration.Nanoseconds())*100)
}

func percent(value, total int64) string {
	return fmt.Sprintf("%5.2f %%", div(value, total)*100)
}

func bytesSpeed(value int64) string {
	return fmt.Sprintf("%s/s", bytes(int64(speed(value))))
}

func countSpeed(value int64) string {
	return fmt.Sprintf("%.2f/s", speed(value))
}

func speed(value int64) float64 {
	return div(value, TotalMainDuration.Nanoseconds()) * 1e9
}

func div(value int64, total int64) float64 {
	if total == 0 {
		return 0
	} else {
		return float64(value) / float64(total)
	}
}

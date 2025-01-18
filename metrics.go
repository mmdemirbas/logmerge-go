// metrics.go
package main

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// Metrics holds runtime statistics
type Metrics struct {
	// File list stats
	DirsScanned  int64
	FilesScanned int64
	FilesMatched int64
	MatchedFiles []string

	// Timing stats by phase (nanoseconds)
	ParseOptionsDuration  int64
	ListFilesDuration     int64
	OpenFilesDuration     int64
	MergeScannersDuration int64
	TotalMainDuration     int64

	// Timing stats by operation (nanoseconds)
	ReadLineDuration       int64
	ParseTimestampDuration int64
	WriteLineDuration      int64

	// Line count stats
	LinesRead              int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64

	// Byte count stats
	BytesRead    int64
	BytesWritten int64

	// Extra byte count stats
	BytesWrittenForTimestamps  int64
	BytesWrittenForOutputNames int64
	BytesWrittenForRawLines    int64
}

var GlobalMetrics = &Metrics{}

func (m *Metrics) IncDirsScanned() {
	atomic.AddInt64(&m.DirsScanned, 1)
}
func (m *Metrics) IncFilesScanned() {
	atomic.AddInt64(&m.FilesScanned, 1)
}
func (m *Metrics) IncFilesMatched() {
	atomic.AddInt64(&m.FilesMatched, 1)
}
func (m *Metrics) AddMatchedFiles(file ...string) {
	m.MatchedFiles = append(m.MatchedFiles, file...)
}
func (m *Metrics) AddParseOptionsDuration(duration int64) {
	atomic.AddInt64(&m.ParseOptionsDuration, duration)
}
func (m *Metrics) AddListFilesDuration(duration int64) {
	atomic.AddInt64(&m.ListFilesDuration, duration)
}
func (m *Metrics) AddOpenFilesDuration(duration int64) {
	atomic.AddInt64(&m.OpenFilesDuration, duration)
}
func (m *Metrics) AddMergeScannersDuration(duration int64) {
	atomic.AddInt64(&m.MergeScannersDuration, duration)
}
func (m *Metrics) AddTotalMainDuration(duration int64) {
	atomic.AddInt64(&m.TotalMainDuration, duration)
}
func (m *Metrics) AddReadLineDuration(duration int64) {
	atomic.AddInt64(&m.ReadLineDuration, duration)
}
func (m *Metrics) AddParseTimestampDuration(duration int64) {
	atomic.AddInt64(&m.ParseTimestampDuration, duration)
}
func (m *Metrics) AddWriteLineDuration(duration int64) {
	atomic.AddInt64(&m.WriteLineDuration, duration)
}
func (m *Metrics) IncLinesRead() {
	atomic.AddInt64(&m.LinesRead, 1)
}
func (m *Metrics) IncLinesWithTimestamps() {
	atomic.AddInt64(&m.LinesWithTimestamps, 1)
}
func (m *Metrics) IncLinesWithoutTimestamps() {
	atomic.AddInt64(&m.LinesWithoutTimestamps, 1)
}
func (m *Metrics) AddBytesRead(bytes int64) {
	atomic.AddInt64(&m.BytesRead, bytes)
}
func (m *Metrics) AddBytesWritten(bytes int64) {
	atomic.AddInt64(&m.BytesWritten, bytes)
}
func (m *Metrics) AddBytesWrittenForTimestamps(bytes int64) {
	atomic.AddInt64(&m.BytesWrittenForTimestamps, bytes)
}
func (m *Metrics) AddBytesWrittenForOutputNames(bytes int64) {
	atomic.AddInt64(&m.BytesWrittenForOutputNames, bytes)
}
func (m *Metrics) AddBytesWrittenForRawLines(bytes int64) {
	atomic.AddInt64(&m.BytesWrittenForRawLines, bytes)
}

func measureDuration(f func() error) (duration time.Duration, err error) {
	startTime := time.Now()
	err = f()
	endTime := time.Now()
	duration = endTime.Sub(startTime)
	return
}

func (m *Metrics) Print(basePath *string, err error) {
	// Show final stats
	fmt.Fprintf(os.Stderr, "===== METRICS =================================================================================\n")
	fmt.Fprintf(os.Stderr, "Base path           : %s\n", *basePath)
	fmt.Fprintf(os.Stderr, "Error               : %v\n", err)
	fmt.Fprintf(os.Stderr, "Total main duration : %v\n", duration(m.TotalMainDuration))
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File count stats\n")
	fmt.Fprintf(os.Stderr, "  dirs scanned      : %12d\n", m.DirsScanned)
	fmt.Fprintf(os.Stderr, "  files scanned     : %12d ≈ %s\n", m.FilesScanned, countSpeed(m.FilesScanned))
	fmt.Fprintf(os.Stderr, "  files matched     : %12d ~ %s\n", m.FilesMatched, percent(m.FilesMatched, m.FilesScanned))
	fmt.Fprintf(os.Stderr, "Timing stats by phase\n")
	fmt.Fprintf(os.Stderr, "  parse options     : %12v ~ %s\n", duration(m.ParseOptionsDuration), percent(m.ParseOptionsDuration, m.TotalMainDuration))
	fmt.Fprintf(os.Stderr, "  list files        : %12v ~ %s\n", duration(m.ListFilesDuration), percent(m.ListFilesDuration, m.TotalMainDuration))
	fmt.Fprintf(os.Stderr, "  open files        : %12v ~ %s\n", duration(m.OpenFilesDuration), percent(m.OpenFilesDuration, m.TotalMainDuration))
	fmt.Fprintf(os.Stderr, "  merge scanners    : %12v ~ %s\n", duration(m.MergeScannersDuration), percent(m.MergeScannersDuration, m.TotalMainDuration))
	restPhaseTime := m.TotalMainDuration - m.ParseOptionsDuration - m.ListFilesDuration - m.OpenFilesDuration - m.MergeScannersDuration
	fmt.Fprintf(os.Stderr, "    ..rest..        : %12v ~ %s\n", duration(restPhaseTime), percent(restPhaseTime, m.TotalMainDuration))
	fmt.Fprintf(os.Stderr, "Timing stats by operation\n")
	fmt.Fprintf(os.Stderr, "  read line         : %12v ~ %s ≈ %s\n", duration(m.ReadLineDuration), percent(m.ReadLineDuration, m.TotalMainDuration), countSpeed(m.LinesRead))
	fmt.Fprintf(os.Stderr, "  parse timestamp   : %12v ~ %s ≈ %s\n", duration(m.ParseTimestampDuration), percent(m.ParseTimestampDuration, m.TotalMainDuration), countSpeed(m.ParseTimestampDuration))
	fmt.Fprintf(os.Stderr, "  write line        : %12v ~ %s ≈ %s\n", duration(m.WriteLineDuration), percent(m.WriteLineDuration, m.TotalMainDuration), countSpeed(m.WriteLineDuration))
	restOpTime := m.TotalMainDuration - m.ReadLineDuration - m.ParseTimestampDuration - m.WriteLineDuration
	fmt.Fprintf(os.Stderr, "    ..rest..        : %12v ~ %s ≈ %s\n", duration(restOpTime), percent(restOpTime, m.TotalMainDuration), countSpeed(restOpTime))
	fmt.Fprintf(os.Stderr, "Byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes read        : %12d = %s ≈ %s\n", m.BytesRead, bytes(m.BytesRead), bytesSpeed(m.BytesRead))
	fmt.Fprintf(os.Stderr, "  bytes written     : %12d = %s ≈ %s\n", m.BytesWritten, bytes(m.BytesWritten), bytesSpeed(m.BytesWritten))
	fmt.Fprintf(os.Stderr, "Written byte breakdown\n")
	fmt.Fprintf(os.Stderr, "  bytes for ts      : %12d = %s ~ %s\n", m.BytesWrittenForTimestamps, bytes(m.BytesWrittenForTimestamps), percent(m.BytesWrittenForTimestamps, m.BytesWritten))
	fmt.Fprintf(os.Stderr, "  bytes for name    : %12d = %s ~ %s\n", m.BytesWrittenForOutputNames, bytes(m.BytesWrittenForOutputNames), percent(m.BytesWrittenForOutputNames, m.BytesWritten))
	fmt.Fprintf(os.Stderr, "  bytes for raw     : %12d = %s ~ %s\n", m.BytesWrittenForRawLines, bytes(m.BytesWrittenForRawLines), percent(m.BytesWrittenForRawLines, m.BytesWritten))
	fmt.Fprintf(os.Stderr, "Line count stats\n")
	fmt.Fprintf(os.Stderr, "  lines read        : %12d ≈ %s\n", m.LinesRead, countSpeed(m.LinesRead))
	fmt.Fprintf(os.Stderr, "  lines with ts     : %12d ~ %s\n", m.LinesWithTimestamps, percent(m.LinesWithTimestamps, m.LinesRead))
	fmt.Fprintf(os.Stderr, "  lines without ts  : %12d ~ %s\n", m.LinesWithoutTimestamps, percent(m.LinesWithoutTimestamps, m.LinesRead))
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File list (%d files):\n", len(m.MatchedFiles))
	for i, file := range m.MatchedFiles {
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

func duration(duration int64) time.Duration {
	return time.Duration(duration)
}

func percent(value, total int64) string {
	return fmt.Sprintf("%5.2f %%", div(value, total)*100)
}

func bytesSpeed(value int64) string {
	return fmt.Sprintf("%s/s", bytes(int64(div(value, GlobalMetrics.TotalMainDuration)*1e9)))
}

func countSpeed(value int64) string {
	return fmt.Sprintf("%.2f/s", div(value, GlobalMetrics.TotalMainDuration)*1e9)
}

func div(value int64, total int64) float64 {
	var speed float64
	if total == 0 {
		speed = 0
	} else {
		speed = float64(value) / float64(total)
	}
	return speed
}

// metrics.go
package main

import (
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

package logtime_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/logtime"
)

var (
	sinkTimestamp Timestamp
	sinkInt       int
	sinkInt2      int
)

// --- ParseTimestampWithEnd benchmarks ---

func BenchmarkParseTimestampWithEnd_NumericAtStart(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("2026-03-13 01:21:22.000000000 [INFO] Simulation log line 1741825282000000100\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

func BenchmarkParseTimestampWithEnd_NumericWithPrefix(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("<165> 2024-08-04T12:00:01Z server1 appname 12345\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

func BenchmarkParseTimestampWithEnd_SpaceMillis(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("2026-03-07 23:59:41 134 INFO 172-16-148-241 Spark [1092810]\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

func BenchmarkParseTimestampWithEnd_Ctime(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("Sat Mar 07 23:59:43 CST 2026 Error to run option:[-test, -d, /]\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

func BenchmarkParseTimestampWithEnd_NoTimestamp(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("  at com.example.Main(Main.java:42) caused by NullPointerException\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

func BenchmarkParseTimestampWithEnd_TimezoneOffset(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("2024-12-23T15:55:26.569+0800: 1.138: [GC (Allocation Failure)]\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

// --- ParseTimestampForStrip benchmarks ---

func BenchmarkParseTimestampForStrip_PipeDelimited(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("2025-01-15 10:00:00,179 | INFO | sidecar-instance-check.sh:53\n")
	var ts Timestamp
	var start, end int
	for i := 0; i < b.N; i++ {
		ts, start, end = ParseTimestampForStrip(c, buf)
	}
	sinkTimestamp, sinkInt, sinkInt2 = ts, start, end
}

func BenchmarkParseTimestampForStrip_BracketDelimited(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("[2025-01-09 20:27:27,236] [sidecar-bg-task 3850964] [INFO]\n")
	var ts Timestamp
	var start, end int
	for i := 0; i < b.N; i++ {
		ts, start, end = ParseTimestampForStrip(c, buf)
	}
	sinkTimestamp, sinkInt, sinkInt2 = ts, start, end
}

// --- ParseTimestamp (value only, no position) ---

func BenchmarkParseTimestamp_Simple(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("2026-03-13 01:21:22.000000000 [INFO] message\n")
	var ts Timestamp
	for i := 0; i < b.N; i++ {
		ts = ParseTimestamp(c, buf)
	}
	sinkTimestamp = ts
}

// --- Multi-line buffer (simulates PeekSlice with subsequent lines) ---

func BenchmarkParseTimestampWithEnd_MultilineBuffer(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	buf := []byte("2026-03-07 23:59:41 134 INFO Spark msg\n2026-03-07 23:59:43 779 INFO next line\n2026-03-08 00:00:00 data\n")
	var ts Timestamp
	var end int
	for i := 0; i < b.N; i++ {
		ts, end = ParseTimestampWithEnd(c, buf)
	}
	sinkTimestamp, sinkInt = ts, end
}

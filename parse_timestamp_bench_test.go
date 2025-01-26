package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func BenchmarkParseTimestamp(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen: 15,
		IgnoreTimezoneInfo:   false,
	}
	m := NewParseTimestampMetrics()
	tsBytes := []byte(("2025-01-15 19:24:08.123Z"))
	for i := 0; i < b.N; i++ {
		ParseTimestamp(c, m, tsBytes)
	}
}

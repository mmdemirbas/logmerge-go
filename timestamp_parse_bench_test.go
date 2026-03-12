package main_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge"
)

func BenchmarkParseTimestamp(b *testing.B) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen: 15,
		IgnoreTimezoneInfo:   false,
	}
	tsBytes := []byte(("2025-01-15 19:24:08.123Z"))
	for n := 0; n < b.N; n++ {
		ParseTimestamp(c, tsBytes)
	}
}

package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"os"
	"testing"
)

func BenchmarkParseTimestamp(b *testing.B) {
	c := &AppConfig{
		ShortestTimestampLen: 15,
		Stderr:               os.Stderr,
		IgnoreTimezoneInfo:   false,
	}
	tsBytes := []byte(("2025-01-15 19:24:08.123Z"))
	for i := 0; i < b.N; i++ {
		ParseTimestamp(c, tsBytes)
	}
}

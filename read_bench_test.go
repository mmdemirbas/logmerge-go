package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"strings"
	"testing"
)

func BenchmarkParseTimestamp(b *testing.B) {
	r := NewRingBuffer(128)
	for i := 0; i < b.N; i++ {
		r.Fill(strings.NewReader("2025-01-15 19:24:08.123Z"))
		ParseTimestamp(r)
	}
}

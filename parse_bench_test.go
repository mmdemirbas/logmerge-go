package main

import (
	"testing"
)

func BenchmarkParseTimestamp(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ParseTimestamp("2025-01-15 19:24:08.123Z")
	}
}

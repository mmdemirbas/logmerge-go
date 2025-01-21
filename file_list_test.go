package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestShouldIncludeFile(t *testing.T) {
	tests := []struct {
		input    string
		expected bool
	}{
		{"some/path/usual.log", true},
		{"some/path/usual.log.zip", false},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			actual := ShouldIncludeFile(tt.input)
			assertEquals(t, tt.expected, actual)
		})
	}
}

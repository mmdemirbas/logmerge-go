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
		// TODO: Add more tests to cover all cases
	}

	c := &ListFilesConfig{
		ExcludedSuffixes:   []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"},
		IncludedSuffixes:   []string{},
		ExcludedSubstrings: []string{},
		IncludedSubstrings: []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".out", ".debug", ".trace"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			actual := ShouldIncludeFile(c, tt.input)
			if actual != tt.expected {
				t.Errorf(expectedFormat, tt.expected, tt.expected, actual, actual)
			}
		})
	}
}

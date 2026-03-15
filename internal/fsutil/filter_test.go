package fsutil_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/testutil"
)

func TestMatcherShouldInclude(t *testing.T) {
	tests := []struct {
		name     string
		patterns []string
		filePath string
		expected bool
	}{
		// No rules match → include
		{"no rules", []string{}, "app.log", true},
		{"no matching rules", []string{"*.tar"}, "app.log", true},

		// Basic exclude
		{"exclude gz", []string{"*.gz"}, "archive.gz", false},
		{"exclude gz no match", []string{"*.gz"}, "app.log", true},

		// Negation override
		{"negation override", []string{"*.gz", "!important.gz"}, "important.gz", true},
		{"negation override other still excluded", []string{"*.gz", "!important.gz"}, "other.gz", false},

		// Order dependence: last matching rule wins
		{"last rule wins exclude", []string{"!*.gz", "*.gz"}, "archive.gz", false},
		{"last rule wins include", []string{"*.gz", "!*.gz"}, "archive.gz", true},

		// Multiple patterns
		{"multiple excludes", []string{"*.gz", "*.zip"}, "archive.zip", false},
		{"multiple excludes no match", []string{"*.gz", "*.zip"}, "app.log", true},

		// Comments and empty lines are skipped
		{"comment ignored", []string{"# this is a comment", "*.gz"}, "archive.gz", false},
		{"empty line ignored", []string{"", "  ", "*.gz"}, "archive.gz", false},

		// Patterns with '/' match at any depth in the path
		{"dir pattern shallow", []string{"*/memartscc/*"}, "a/memartscc/b.log", false},
		{"dir pattern deep", []string{"*/memartscc/*"}, "x/y/memartscc/z.log", false},
		{"dir pattern no match", []string{"*/memartscc/*"}, "x/y/other/z.log", true},
		{"dir pattern negation deep", []string{"*/memartscc/*", "!*/memartscc/keep.log"}, "x/y/memartscc/keep.log", true},
		{"dir pattern negation other excluded", []string{"*/memartscc/*", "!*/memartscc/keep.log"}, "x/y/memartscc/other.log", false},

		// Filename-only patterns still work with deep paths
		{"filename pattern deep path", []string{"*.gz"}, "a/b/c/archive.gz", false},
		{"filename pattern deep path no match", []string{"*.gz"}, "a/b/c/app.log", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMatcher(tt.patterns)
			actual := m.ShouldInclude(tt.filePath)
			if actual != tt.expected {
				t.Errorf(testutil.ExpectedFormat, tt.expected, tt.expected, actual, actual)
			}
		})
	}
}

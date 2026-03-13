package logmerge_test

import (
	. "github.com/mmdemirbas/logmerge/internal/logmerge"
	"testing"
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewMatcher(tt.patterns)
			actual := m.ShouldInclude(tt.filePath)
			if actual != tt.expected {
				t.Errorf(expectedFormat, tt.expected, tt.expected, actual, actual)
			}
		})
	}
}

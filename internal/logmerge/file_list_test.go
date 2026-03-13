package logmerge_test

import (
	. "github.com/mmdemirbas/logmerge/internal/logmerge"
	"testing"
)

func TestGetAliasPatternMatching(t *testing.T) {
	tests := []struct {
		name     string
		aliases  map[string]string
		filePath string
		expected string
	}{
		{
			name:     "exact match",
			aliases:  map[string]string{"console.log": "driver"},
			filePath: "console.log",
			expected: "driver",
		},
		{
			name:     "glob pattern match",
			aliases:  map[string]string{"*.log": "logs"},
			filePath: "app.log",
			expected: "logs",
		},
		{
			name:     "no match falls back to filename",
			aliases:  map[string]string{"other.log": "other"},
			filePath: "app.log",
			expected: "app.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ListFilesConfig{
				InputPaths:  []string{"."},
				FileAliases: tt.aliases,
			}
			actual := GetAlias(c, tt.filePath)
			if actual != tt.expected {
				t.Errorf(expectedFormat, tt.expected, tt.expected, actual, actual)
			}
		})
	}
}

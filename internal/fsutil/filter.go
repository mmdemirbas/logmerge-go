package fsutil

import (
	"path/filepath"
	"strings"
)

type patternRule struct {
	pattern  string
	negation bool // true means "include if matched"
}

type Matcher struct {
	rules []patternRule
}

// NewMatcher creates a Matcher from gitignore-style glob patterns. Empty lines
// and comments (starting with #) are skipped. Patterns prefixed with ! are negations.
func NewMatcher(patterns []string) *Matcher {
	rules := make([]patternRule, 0, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" || strings.HasPrefix(p, "#") {
			continue
		}
		negation := false
		if strings.HasPrefix(p, "!") {
			negation = true
			p = p[1:]
		}
		rules = append(rules, patternRule{pattern: p, negation: negation})
	}
	return &Matcher{rules: rules}
}

// ShouldInclude returns whether filePath passes the filter rules. Rules are
// evaluated in order; the last matching rule wins. Default is include.
func (m *Matcher) ShouldInclude(filePath string) (included bool) {
	_, name := filepath.Split(filePath)
	included = true // default: include if no rules match
	for _, rule := range m.rules {
		if matchGitignorePattern(rule.pattern, filePath, name) {
			included = rule.negation
		}
	}
	return included
}

// matchGitignorePattern matches a gitignore-style pattern against a file path.
// Patterns without '/' are matched against the filename only.
// Patterns with '/' are matched against the full path, with each sub-path tried
// to emulate gitignore's "match anywhere in path" behavior.
func matchGitignorePattern(pattern, filePath, name string) bool {
	if !strings.Contains(pattern, "/") {
		matched, _ := filepath.Match(pattern, name)
		return matched
	}

	// Pattern contains '/' — try matching against the full path and every
	// suffix starting at a path separator to emulate "**/"-style matching.
	if matched, _ := filepath.Match(pattern, filePath); matched {
		return true
	}
	for i := 0; i < len(filePath); i++ {
		if filePath[i] == filepath.Separator || filePath[i] == '/' {
			sub := filePath[i+1:]
			if matched, _ := filepath.Match(pattern, sub); matched {
				return true
			}
		}
	}
	return false
}

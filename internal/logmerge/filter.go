package logmerge

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
		nameMatched, _ := filepath.Match(rule.pattern, name)
		pathMatched, _ := filepath.Match(rule.pattern, filePath)
		if nameMatched || pathMatched {
			included = rule.negation
		}
	}
	return included
}

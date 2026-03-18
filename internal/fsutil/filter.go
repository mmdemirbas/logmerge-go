package fsutil

import (
	"path"
	"path/filepath"
	"strings"
)

type patternRule struct {
	pattern  string
	negation bool // true means "include if matched"
	rootOnly bool // true when pattern had a leading / (match from root only)
}

type Matcher struct {
	rules []patternRule
}

// NewMatcher creates a Matcher from gitignore-style glob patterns.
// Blank lines and lines starting with # are skipped. Patterns prefixed with !
// are negations. A backslash before # or ! escapes the special meaning.
// Trailing spaces are ignored unless escaped with backslash (\ ).
func NewMatcher(patterns []string) *Matcher {
	rules := make([]patternRule, 0, len(patterns))
	for _, p := range patterns {
		// Trim unescaped trailing spaces: remove trailing spaces, but if the
		// last non-space char is \, keep one trailing space.
		p = trimTrailingUnescapedSpaces(p)
		if p == "" {
			continue
		}
		// # is a comment unless escaped with backslash
		if strings.HasPrefix(p, "#") {
			continue
		}
		if strings.HasPrefix(p, "\\#") {
			p = p[1:] // strip backslash, keep #
		}
		// ! is negation unless escaped with backslash
		negation := false
		if strings.HasPrefix(p, "!") {
			negation = true
			p = p[1:]
		} else if strings.HasPrefix(p, "\\!") {
			p = p[1:] // strip backslash, keep !
		}
		if p == "" {
			continue
		}
		// Leading / means root-relative: strip and flag
		rootOnly := false
		if strings.HasPrefix(p, "/") {
			rootOnly = true
			p = p[1:]
		}
		if p == "" {
			continue
		}
		rules = append(rules, patternRule{pattern: p, negation: negation, rootOnly: rootOnly})
	}
	return &Matcher{rules: rules}
}

// trimTrailingUnescapedSpaces removes trailing whitespace, but preserves a
// single trailing space if it is escaped with a backslash (e.g. "foo\ ").
func trimTrailingUnescapedSpaces(s string) string {
	s = strings.TrimRight(s, " \t")
	// nothing left or no trailing backslash → done
	if s == "" || s[len(s)-1] != '\\' {
		return s
	}
	// The backslash was escaping a space; restore it
	return s + " "
}

// ShouldInclude returns whether filePath passes the filter rules. Rules are
// evaluated in order; the last matching rule wins. Default is include.
func (m *Matcher) ShouldInclude(filePath string) (included bool) {
	// Normalize to forward slashes for consistent gitignore-style matching.
	// Gitignore patterns always use '/', even on Windows.
	filePath = filepath.ToSlash(filePath)
	_, name := splitLast(filePath)
	included = true // default: include if no rules match
	for _, rule := range m.rules {
		if matchGitignorePattern(rule.pattern, filePath, name, rule.rootOnly) {
			included = rule.negation
		}
	}
	return included
}

// splitLast returns the directory and file name parts of a slash-separated path.
func splitLast(path string) (string, string) {
	i := strings.LastIndex(path, "/")
	if i < 0 {
		return "", path
	}
	return path[:i], path[i+1:]
}

// matchGitignorePattern matches a gitignore-style pattern against a file path.
//   - Patterns without '/' are matched against the filename only (unless rootOnly).
//   - Patterns containing '**' use gitignore-style doublestar matching where
//     '**' matches zero or more directory segments.
//   - rootOnly patterns (originally prefixed with /) match only against the full
//     path from the input root — they don't try sub-path suffixes.
//   - Other patterns with '/' are matched against the full path, with each
//     sub-path suffix tried to emulate "match anywhere in path" behavior.
func matchGitignorePattern(pattern, filePath, name string, rootOnly bool) bool {
	if rootOnly {
		// Root-relative: match only against the full path from root
		if strings.Contains(pattern, "**") {
			return matchDoublestar(pattern, filePath)
		}
		matched, _ := path.Match(pattern, filePath)
		return matched
	}

	if !strings.Contains(pattern, "/") {
		matched, _ := path.Match(pattern, name)
		return matched
	}

	// Patterns with ** use segment-based matching for gitignore compatibility
	if strings.Contains(pattern, "**") {
		return matchDoublestar(pattern, filePath)
	}

	// Pattern contains '/' but no ** — try matching against the full path
	// and every suffix starting at a path separator.
	if matched, _ := path.Match(pattern, filePath); matched {
		return true
	}
	for i := 0; i < len(filePath); i++ {
		if filePath[i] == '/' {
			sub := filePath[i+1:]
			if matched, _ := path.Match(pattern, sub); matched {
				return true
			}
		}
	}
	return false
}

// matchDoublestar handles patterns containing ** by splitting into segments
// and matching with ** consuming zero or more path segments.
//
// Examples:
//
//	"**"            matches everything
//	"**/yarn/**"    matches any file under a "yarn" directory at any depth
//	"**/*.log"      matches any .log file at any depth
//	"foo/**/bar"    matches foo/bar, foo/x/bar, foo/x/y/bar, etc.
func matchDoublestar(pattern, filePath string) bool {
	patParts := splitPath(pattern)
	pathParts := splitPath(filePath)
	return matchSegments(patParts, pathParts)
}

// matchSegments recursively matches pattern segments against path segments,
// where a "**" pattern segment matches zero or more path segments.
func matchSegments(pat, segs []string) bool {
	for len(pat) > 0 && len(segs) > 0 {
		if pat[0] == "**" {
			pat = pat[1:]
			// Skip consecutive ** segments
			for len(pat) > 0 && pat[0] == "**" {
				pat = pat[1:]
			}
			if len(pat) == 0 {
				return true // trailing ** matches everything
			}
			// Try matching remaining pattern at every position in path
			for i := 0; i <= len(segs); i++ {
				if matchSegments(pat, segs[i:]) {
					return true
				}
			}
			return false
		}

		matched, _ := path.Match(pat[0], segs[0])
		if !matched {
			return false
		}
		pat = pat[1:]
		segs = segs[1:]
	}

	// If path is exhausted but pattern has only trailing ** left:
	// trailing /** means "everything inside", so an empty remaining path
	// (the directory itself) should NOT match.
	hasTrailingStar := false
	for len(pat) > 0 && pat[0] == "**" {
		hasTrailingStar = true
		pat = pat[1:]
	}
	if hasTrailingStar && len(segs) == 0 {
		return false
	}

	return len(pat) == 0 && len(segs) == 0
}

// splitPath splits a path by '/' separator, filtering out empty segments.
func splitPath(p string) []string {
	raw := strings.Split(p, "/")
	parts := make([]string, 0, len(raw))
	for _, s := range raw {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return parts
}

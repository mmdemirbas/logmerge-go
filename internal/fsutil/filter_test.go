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
		// ── Blank lines and comments ──────────────────────────────────
		// "A blank line matches no files, so it can serve as a separator"
		{"blank line is separator", []string{"", "*.gz"}, "archive.gz", false},
		{"whitespace-only line is separator", []string{"  ", "*.gz"}, "archive.gz", false},
		// "A line starting with # serves as a comment"
		{"comment ignored", []string{"# this is a comment", "*.gz"}, "archive.gz", false},
		{"comment with leading space", []string{"  # also a comment"}, "file.log", true},
		// "Put a backslash in front of the first hash for patterns that begin with a hash"
		{"escaped hash is literal pattern", []string{"\\#file"}, "#file", false},
		{"escaped hash doesn't match without hash", []string{"\\#file"}, "file", true},

		// ── Negation ──────────────────────────────────────────────────
		// 'An optional prefix "!" which negates the pattern'
		{"negation re-includes", []string{"*.gz", "!important.gz"}, "important.gz", true},
		{"negation other still excluded", []string{"*.gz", "!important.gz"}, "other.gz", false},
		// "Put a backslash in front of the first ! for patterns that begin with a literal !"
		{"escaped bang is literal pattern", []string{"\\!important!.txt"}, "!important!.txt", false},
		{"escaped bang doesn't negate", []string{"\\!important!.txt"}, "important!.txt", true},

		// ── Order dependence (last matching rule wins) ────────────────
		{"last rule wins exclude", []string{"!*.gz", "*.gz"}, "archive.gz", false},
		{"last rule wins include", []string{"*.gz", "!*.gz"}, "archive.gz", true},
		{"multiple excludes", []string{"*.gz", "*.zip"}, "archive.zip", false},
		{"no rules match → include", []string{}, "app.log", true},
		{"no matching rules → include", []string{"*.tar"}, "app.log", true},

		// ── Trailing spaces ───────────────────────────────────────────
		// "Trailing spaces are ignored unless they are quoted with backslash"
		{"trailing space ignored", []string{"*.gz  "}, "archive.gz", false},
		{"escaped trailing space", []string{"foo\\ "}, "foo ", false},
		{"escaped trailing space no match without space", []string{"foo\\ "}, "foo", true},

		// ── Asterisk * ────────────────────────────────────────────────
		// 'An asterisk "*" matches anything except a slash'
		{"star matches filename", []string{"*.log"}, "app.log", false},
		{"star matches any filename chars", []string{"*.log"}, "a-b_c.log", false},
		{"star at depth via filename match", []string{"*.gz"}, "a/b/c/archive.gz", false},
		{"star doesn't match slash", []string{"*.gz"}, "a/b/c/app.log", true},

		// ── Question mark ? ───────────────────────────────────────────
		// '"?" matches any one character except "/"'
		{"question mark matches one char", []string{"?.log"}, "a.log", false},
		{"question mark no match two chars", []string{"?.log"}, "ab.log", true},
		{"question mark no match slash", []string{"a?b"}, "a/b", true},

		// ── Character range [a-z] ─────────────────────────────────────
		// 'The range notation, e.g. [a-zA-Z], can be used'
		{"range matches", []string{"[abc].log"}, "b.log", false},
		{"range no match", []string{"[abc].log"}, "d.log", true},
		{"range alpha", []string{"[a-z].log"}, "f.log", false},
		{"range alpha no match digit", []string{"[a-z].log"}, "1.log", true},

		// ── Slash / as separator ──────────────────────────────────────
		// "If there is a separator at the beginning or middle of the pattern,
		//  then the pattern is relative to the directory level"
		// (In our tool there's no .gitignore root, so patterns with / are
		//  tried against every sub-path suffix of the file path.)

		// Pattern with / in middle: matches at sub-path level
		{"dir/file pattern", []string{"doc/frotz"}, "doc/frotz", false},
		{"dir/file pattern at depth", []string{"doc/frotz"}, "a/doc/frotz", false},

		// ── Patterns without / match at any level ─────────────────────
		{"no-slash pattern matches anywhere", []string{"frotz"}, "frotz", false},
		{"no-slash pattern matches deep", []string{"frotz"}, "a/b/frotz", false},

		// ── Leading ** followed by / ──────────────────────────────────
		// '"**/foo" matches file or directory "foo" anywhere, same as pattern "foo"'
		{"**/foo matches at root", []string{"**/foo"}, "foo", false},
		{"**/foo matches nested", []string{"**/foo"}, "a/b/foo", false},
		{"**/foo no match prefix", []string{"**/foo"}, "foobar", true},
		// '"**/foo/bar" matches "bar" anywhere directly under directory "foo"'
		{"**/foo/bar at root", []string{"**/foo/bar"}, "foo/bar", false},
		{"**/foo/bar nested", []string{"**/foo/bar"}, "a/b/foo/bar", false},
		{"**/foo/bar no match if not direct child", []string{"**/foo/bar"}, "foo/x/bar", true},

		// ── Trailing /** ──────────────────────────────────────────────
		// '"abc/**" matches all files inside directory "abc", with infinite depth'
		{"abc/** direct child", []string{"abc/**"}, "abc/file.log", false},
		{"abc/** deep", []string{"abc/**"}, "abc/d/e/f.log", false},
		{"abc/** doesn't match abc itself", []string{"abc/**"}, "abc", true},
		{"abc/** doesn't match sibling", []string{"abc/**"}, "abd/file.log", true},

		// ── /**/ in middle ────────────────────────────────────────────
		// '"a/**/b" matches "a/b", "a/x/b", "a/x/y/b" and so on'
		{"a/**/b zero dirs", []string{"a/**/b"}, "a/b", false},
		{"a/**/b one dir", []string{"a/**/b"}, "a/x/b", false},
		{"a/**/b two dirs", []string{"a/**/b"}, "a/x/y/b", false},
		{"a/**/b no match wrong start", []string{"a/**/b"}, "c/x/b", true},
		{"a/**/b no match wrong end", []string{"a/**/b"}, "a/x/c", true},

		// ── Other consecutive asterisks ───────────────────────────────
		// "Other consecutive asterisks are considered regular asterisks"
		{"triple star = regular star", []string{"***.log"}, "app.log", false},
		{"triple star doesn't cross slash", []string{"***.log"}, "a/app.log", false},

		// ── ** alone ──────────────────────────────────────────────────
		{"** matches everything", []string{"**"}, "a/b/c/file.log", false},
		{"** matches single file", []string{"**"}, "file.log", false},

		// ── Doublestar combined with negation (real-world) ────────────
		{"**/dir/** recursive include", []string{"*", "!**/yarn/**"}, "a/b/yarn/rm/file.log", true},
		{"**/dir/** direct child", []string{"*", "!**/yarn/**"}, "a/yarn/file.log", true},
		{"**/dir/** deeply nested", []string{"*", "!**/yarn/**"}, "a/b/c/yarn/d/e/f.log", true},
		{"**/dir/** no match other dir", []string{"*", "!**/yarn/**"}, "a/b/other/file.log", false},
		{"**/dir/** name in filename not dir", []string{"*", "!**/yarn/**"}, "a/yarn-status.log", false},
		{"**/*.log matches at any depth", []string{"**/*.log"}, "a/b/c/app.log", false},
		{"**/*.log matches root", []string{"**/*.log"}, "app.log", false},
		{"**/*.log no match wrong ext", []string{"**/*.log"}, "a/b/c/app.txt", true},

		// ── Real-world: yarn/spark include, nm exclude ────────────────
		{"yarn+spark real case", []string{"*", "!**/yarn/**", "!**/spark/**", "**/nm/**"},
			"app/172.16.240.143/yarn/rm/hadoop.log", true},
		{"yarn+spark nm excluded", []string{"*", "!**/yarn/**", "!**/spark/**", "**/nm/**"},
			"app/172.16.254.61/yarn/nm/nodemanager.log", false},
		{"yarn+spark spark included", []string{"*", "!**/yarn/**", "!**/spark/**", "**/nm/**"},
			"app/172.16.254.61/spark/executor.log", true},
		{"yarn+spark unrelated excluded", []string{"*", "!**/yarn/**", "!**/spark/**", "**/nm/**"},
			"app/172.16.240.143/oms/pms/pms.log", false},
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

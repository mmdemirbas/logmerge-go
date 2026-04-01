// Package loglevel detects and normalizes log severity levels from raw log lines.
package loglevel

// Level represents a normalized log severity level.
type Level byte

const (
	Unknown Level = 0
	Trace   Level = 1
	Debug   Level = 2
	Info    Level = 3
	Notice  Level = 4
	Warn    Level = 5
	Error   Level = 6
	Fatal   Level = 7
)

// label is the fixed-width (5-char + space) output string for each level.
var label = [8][]byte{
	[]byte("      "), // Unknown — blank, preserves alignment
	[]byte("TRACE "),
	[]byte("DEBUG "),
	[]byte("INFO  "),
	[]byte("NOTE  "),
	[]byte("WARN  "),
	[]byte("ERROR "),
	[]byte("FATAL "),
}

// Label returns the 6-byte fixed-width label (5 chars + trailing space).
func (l Level) Label() []byte {
	if l > Fatal {
		return label[0]
	}
	return label[l]
}

// String returns the level name without padding.
func (l Level) String() string {
	names := [8]string{"UNKNOWN", "TRACE", "DEBUG", "INFO", "NOTICE", "WARN", "ERROR", "FATAL"}
	if l > Fatal {
		return names[0]
	}
	return names[l]
}

// ParseResult holds the detected level and its byte positions in the line.
type ParseResult struct {
	Level Level
	Start int // byte offset where the level token starts (including delimiters like '[')
	End   int // byte offset where the level token ends (including delimiters like ']')
}

// ParseLevel detects a log level from a raw line buffer.
//
// It searches in two places:
//  1. The byte immediately before tsStart (glog single-letter prefix: I, W, E, F, D)
//  2. A window of up to 30 bytes after tsEnd (bracketed, pipe-delimited, or bare words)
//
// The first confident match wins. Lines without a detectable level return Unknown.
func ParseLevel(buffer []byte, tsStart int, tsEnd int) ParseResult {
	n := len(buffer)

	// --- Strategy 1: glog prefix before the timestamp ---
	// Format: I20250115 19:29:15... or E20250115 19:29:15...
	// The letter is at tsStart-1, and tsStart is the first digit of the timestamp.
	if tsStart > 0 {
		b := buffer[tsStart-1]
		// Only match if the letter is at position 0 or preceded by a newline/space
		if tsStart == 1 || buffer[tsStart-2] == '\n' || buffer[tsStart-2] == ' ' {
			if lvl := glogLevel(b); lvl != Unknown {
				return ParseResult{Level: lvl, Start: tsStart - 1, End: tsStart}
			}
		}
	}

	// --- Strategy 2: scan after the timestamp ---
	// Search a small window for level tokens.
	searchEnd := tsEnd + 40
	if searchEnd > n {
		searchEnd = n
	}
	// Stop at newline
	for i := tsEnd; i < searchEnd; i++ {
		if buffer[i] == '\n' || buffer[i] == '\r' {
			searchEnd = i
			break
		}
	}

	i := tsEnd
	for i < searchEnd {
		b := buffer[i]

		// Skip whitespace, pipes, colons, brackets, parens — common delimiters
		if b == ' ' || b == '\t' || b == '|' || b == ':' ||
			b == '[' || b == ']' || b == '(' || b == ')' {
			i++
			continue
		}

		// Try to match a level word starting at position i
		lvl, end := matchLevelWord(buffer, i, searchEnd)
		if lvl != Unknown {
			// Expand start backward to include a leading '[' or '(' if present
			start := i
			if start > tsEnd && (buffer[start-1] == '[' || buffer[start-1] == '(') {
				start--
			}
			// Expand end forward to include a trailing ']', ')', or delimiter
			if end < searchEnd {
				c := buffer[end]
				if c == ']' || c == ')' || c == ' ' || c == '|' || c == ':' {
					end++
				}
			}
			return ParseResult{Level: lvl, Start: start, End: end}
		}

		// Not a level — the first non-delimiter, non-level word means we've
		// entered message content. Stop searching to avoid false positives.
		return ParseResult{Level: Unknown}
	}

	return ParseResult{Level: Unknown}
}

// levelKeywords maps lowercase level keyword abbreviations to their Level values.
// SEVERE and CRITICAL are normalised to ERROR and FATAL respectively.
// Go optimises m[string(byteSlice)] map lookups to avoid heap allocation.
var levelKeywords = map[string]Level{
	"info": Info, "warn": Warn, "warning": Warn,
	"error": Error, "debug": Debug, "trace": Trace,
	"fatal": Fatal, "notice": Notice,
	"severe": Error, "critical": Fatal,
}

// matchLevelWord checks if buffer[start:limit] begins with a known level keyword.
// Returns the normalized level and the end position of the matched word.
// Only matches if the word is followed by a non-alpha boundary (space, |, ], etc.).
func matchLevelWord(buf []byte, start, limit int) (Level, int) {
	if limit-start < 2 {
		return Unknown, start
	}
	// Collect lowercase alpha bytes (up to 9) into a fixed array.
	// b|0x20 maps ASCII uppercase to lowercase; values outside 'a'–'z' stop the loop.
	var lower [9]byte
	wordLen := 0
	for wordLen < 9 && start+wordLen < limit {
		b := buf[start+wordLen] | 0x20
		if b < 'a' || b > 'z' {
			break
		}
		lower[wordLen] = b
		wordLen++
	}
	// Reject if too short or if more alpha chars follow (not a word boundary).
	if wordLen < 2 || isAlpha(buf, start+wordLen, limit) {
		return Unknown, start
	}
	lvl, ok := levelKeywords[string(lower[:wordLen])]
	if !ok {
		return Unknown, start
	}
	return lvl, start + wordLen
}

// isAlphaNum returns true if buf[pos] is a letter or digit (meaning the match
// didn't end at a word boundary).
func isAlpha(buf []byte, pos, limit int) bool {
	if pos >= limit {
		return false
	}
	b := buf[pos]
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

func glogLevel(b byte) Level {
	switch b {
	case 'I':
		return Info
	case 'W':
		return Warn
	case 'E':
		return Error
	case 'F':
		return Fatal
	case 'D':
		return Debug
	}
	return Unknown
}


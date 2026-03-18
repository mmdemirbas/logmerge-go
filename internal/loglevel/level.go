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
			for end < searchEnd {
				c := buffer[end]
				if c == ']' || c == ')' || c == ' ' || c == '|' || c == ':' {
					end++
					break
				}
				break
			}
			return ParseResult{Level: lvl, Start: start, End: end}
		}

		// Not a level — skip to next delimiter to try again
		for i < searchEnd && buffer[i] != ' ' && buffer[i] != '\t' &&
			buffer[i] != '|' && buffer[i] != '[' && buffer[i] != ']' &&
			buffer[i] != ':' {
			i++
		}
	}

	return ParseResult{Level: Unknown}
}

// matchLevelWord checks if buffer[start:limit] begins with a known level keyword.
// Returns the normalized level and the end position of the matched word.
// Only matches if the word is followed by a non-alpha boundary (space, |, ], etc.).
func matchLevelWord(buf []byte, start, limit int) (Level, int) {
	remaining := limit - start
	if remaining < 2 {
		return Unknown, start
	}

	// Check first byte to quickly dispatch
	switch buf[start] {
	case 'I', 'i':
		if remaining >= 4 && eqFold4(buf[start:], "INFO") && !isAlpha(buf, start+4, limit) {
			return Info, start + 4
		}
	case 'W', 'w':
		if remaining >= 7 && eqFold7(buf[start:], "WARNING") && !isAlpha(buf, start+7, limit) {
			return Warn, start + 7
		}
		if remaining >= 4 && eqFold4(buf[start:], "WARN") && !isAlpha(buf, start+4, limit) {
			return Warn, start + 4
		}
	case 'E', 'e':
		if remaining >= 5 && eqFold5(buf[start:], "ERROR") && !isAlpha(buf, start+5, limit) {
			return Error, start + 5
		}
	case 'D', 'd':
		if remaining >= 5 && eqFold5(buf[start:], "DEBUG") && !isAlpha(buf, start+5, limit) {
			return Debug, start + 5
		}
	case 'T', 't':
		if remaining >= 5 && eqFold5(buf[start:], "TRACE") && !isAlpha(buf, start+5, limit) {
			return Trace, start + 5
		}
	case 'F', 'f':
		if remaining >= 5 && eqFold5(buf[start:], "FATAL") && !isAlpha(buf, start+5, limit) {
			return Fatal, start + 5
		}
	case 'N', 'n':
		if remaining >= 6 && eqFold6(buf[start:], "NOTICE") && !isAlpha(buf, start+6, limit) {
			return Notice, start + 6
		}
	case 'S', 's':
		if remaining >= 6 && eqFold6(buf[start:], "SEVERE") && !isAlpha(buf, start+6, limit) {
			return Error, start + 6 // normalize SEVERE → ERROR
		}
	case 'C', 'c':
		if remaining >= 8 && eqFold8(buf[start:], "CRITICAL") && !isAlpha(buf, start+8, limit) {
			return Fatal, start + 8 // normalize CRITICAL → FATAL
		}
	}
	return Unknown, start
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

// Case-insensitive comparison helpers (fixed-length, no allocation).
func eqFold4(b []byte, s string) bool {
	return (b[0]|0x20) == (s[0]|0x20) && (b[1]|0x20) == (s[1]|0x20) &&
		(b[2]|0x20) == (s[2]|0x20) && (b[3]|0x20) == (s[3]|0x20)
}
func eqFold5(b []byte, s string) bool {
	return eqFold4(b, s) && (b[4]|0x20) == (s[4]|0x20)
}
func eqFold6(b []byte, s string) bool {
	return eqFold5(b, s) && (b[5]|0x20) == (s[5]|0x20)
}
func eqFold7(b []byte, s string) bool {
	return eqFold6(b, s) && (b[6]|0x20) == (s[6]|0x20)
}
func eqFold8(b []byte, s string) bool {
	return eqFold7(b, s) && (b[7]|0x20) == (s[7]|0x20)
}

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
	if r := parseGlogPrefix(buffer, tsStart); r.Level != Unknown {
		return r
	}
	n := len(buffer)
	searchEnd := levelSearchEnd(buffer, n, tsEnd)
	for i := tsEnd; i < searchEnd; {
		b := buffer[i]
		if isLevelDelimiter(b) {
			i++
			continue
		}
		lvl, end := matchLevelWord(buffer, i, searchEnd)
		if lvl == Unknown {
			return ParseResult{Level: Unknown}
		}
		start := i
		if start > tsEnd && (buffer[start-1] == '[' || buffer[start-1] == '(') {
			start--
		}
		if end < searchEnd && isTrailingDelimiter(buffer[end]) {
			end++
		}
		return ParseResult{Level: lvl, Start: start, End: end}
	}
	return ParseResult{Level: Unknown}
}

// parseGlogPrefix detects glog-style single-letter level prefixes (I, W, E, F, D)
// that immediately precede the timestamp (e.g. "I20250115 19:29:15...").
func parseGlogPrefix(buffer []byte, tsStart int) ParseResult {
	if tsStart == 0 {
		return ParseResult{Level: Unknown}
	}
	b := buffer[tsStart-1]
	if tsStart == 1 || buffer[tsStart-2] == '\n' || buffer[tsStart-2] == ' ' {
		if lvl := glogLevel(b); lvl != Unknown {
			return ParseResult{Level: lvl, Start: tsStart - 1, End: tsStart}
		}
	}
	return ParseResult{Level: Unknown}
}

// levelSearchEnd returns the index at which the level scan should stop:
// the first newline after tsEnd, or tsEnd+40, whichever comes first.
func levelSearchEnd(buffer []byte, n, tsEnd int) int {
	end := min(tsEnd+40, n)
	for i := tsEnd; i < end; i++ {
		if buffer[i] == '\n' || buffer[i] == '\r' {
			return i
		}
	}
	return end
}

// isLevelDelimiter reports whether b is a character that may appear between
// the timestamp and the level token (whitespace, pipe, colon, brackets).
func isLevelDelimiter(b byte) bool {
	return b == ' ' || b == '\t' || b == '|' || b == ':' ||
		b == '[' || b == ']' || b == '(' || b == ')'
}

// isTrailingDelimiter reports whether b is a character that should be consumed
// after a matched level token (closing bracket, space, pipe, colon).
func isTrailingDelimiter(b byte) bool {
	return b == ']' || b == ')' || b == ' ' || b == '|' || b == ':'
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


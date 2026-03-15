package logtime

// TODO: Consider supporting other time formats like 1 Jan 2006; Jan 1, 2006; 01/02/2006 etc.

type ParseTimestampConfig struct {
	IgnoreTimezoneInfo      bool `yaml:"IgnoreTimezoneInfo"`
	ShortestTimestampLen    int  `yaml:"ShortestTimestampLen"`
	TimestampSearchEndIndex int  `yaml:"TimestampSearchEndIndex"`
}

// ParseTimestamp scans the first TimestampSearchEndIndex bytes of buffer for a
// recognizable timestamp pattern and returns it, or ZeroTimestamp if none found.
func ParseTimestamp(c *ParseTimestampConfig, buffer []byte) Timestamp {
	ts, _, _ := ParseTimestampWithEnd(c, buffer)
	return ts
}

// ParseTimestampWithEnd scans the buffer for a timestamp and returns the parsed
// timestamp, the byte offset where the timestamp section starts (including any
// leading delimiters that should be stripped), and the byte offset where the
// timestamp section ends (including any trailing delimiters).
// If no timestamp is found, or the timestamp is on a subsequent line (past a
// newline), start and end are both 0 — indicating nothing should be stripped.
func ParseTimestampWithEnd(c *ParseTimestampConfig, buffer []byte) (Timestamp, int, int) {
	n := min(len(buffer), c.TimestampSearchEndIndex)

	// Find the first newline — the end offset is only meaningful within the
	// first line. If the parser finds a timestamp past a newline, that
	// timestamp belongs to a later line and should not cause stripping of
	// the current line.
	firstNewline := n
	for j := 0; j < n; j++ {
		if buffer[j] == '\n' || buffer[j] == '\r' {
			firstNewline = j
			break
		}
	}

	var timestamp Timestamp
	var tsStart int
	var end int
	for i := 0; timestamp == ZeroTimestamp && i < n; {
		timestamp, tsStart, i = tryParseTimestamp(c, buffer, i, n)
		end = i
	}
	if timestamp == ZeroTimestamp || end > firstNewline {
		return timestamp, 0, 0
	}

	// Strip trailing delimiters after the timestamp (up to 3 chars).
	// Only closers and separators — not openers like '[' or '(' which start new sections.
	tsEnd := end
	for scanned := 0; tsEnd < n && tsEnd <= firstNewline && scanned < 3; scanned++ {
		b := buffer[tsEnd]
		switch b {
		case ' ', '\t', '|', ']', ')', '}', ':', ',':
			tsEnd++
		default:
			goto doneTrailing
		}
	}
doneTrailing:

	// Look backward from tsStart for opening brackets (up to 3 chars)
	prefixStart := tsStart
	for back := 0; prefixStart > 0 && back < 3; back++ {
		b := buffer[prefixStart-1]
		switch b {
		case '[', '(':
			prefixStart--
		default:
			goto donePrefix
		}
	}
donePrefix:
	// Only strip prefix brackets if we reached start-of-line or a space/tab boundary
	if prefixStart > 0 {
		b := buffer[prefixStart-1]
		if b != ' ' && b != '\t' {
			prefixStart = tsStart // revert: no valid boundary found
		}
	}

	return timestamp, prefixStart, tsEnd
}

func tryParseTimestamp(c *ParseTimestampConfig, buffer []byte, i int, n int) (Timestamp, int, int) {
	if n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, i, n
	}

	var hitNewline bool
	i, hitNewline = skipToFirstDigit(buffer, n, i)
	if hitNewline {
		return ZeroTimestamp, i, i
	}

	tsStart := i // position of the first digit

	if i >= n || n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, tsStart, n
	}

	for j := i + c.ShortestTimestampLen - 1; j >= i; j-- {
		b := buffer[j]
		if b == '\n' || b == '\r' {
			return ZeroTimestamp, tsStart, j + 1
		}
	}

	year, count := parseDigits(buffer, n, i, 4)
	if count == 0 {
		return ZeroTimestamp, tsStart, i + 1
	} else if count == 2 {
		if year < 69 {
			year += 2000
		} else {
			year += 1900
		}
	} else if year > 2050 || year < 1969 {
		return ZeroTimestamp, tsStart, i + count
	}

	i += count
	if i >= n {
		return ZeroTimestamp, tsStart, n
	}
	b := buffer[i]

	// if b == '-' || b == '/' { i++ }
	if b == '-' || b == '/' {
		i++
	}

	month, mcount := parseMax2Digits(buffer, n, i)
	if mcount == 0 {
		return ZeroTimestamp, tsStart, i + 1
	}
	if month > 12 || month < 1 {
		return ZeroTimestamp, tsStart, i + mcount
	}

	i += mcount
	if i >= n {
		return ZeroTimestamp, tsStart, n
	}
	b = buffer[i]

	// if b == '-' || b == '/' { i++ }
	if b == '-' || b == '/' {
		i++
	}

	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 {
		return ZeroTimestamp, tsStart, i + 1
	}
	if day > 31 || day < 1 {
		return ZeroTimestamp, tsStart, i + dcount
	}

	i += dcount
	if i >= n {
		return ZeroTimestamp, tsStart, n
	}
	b = buffer[i]
	i++
	if i >= n || (b != ' ' && b != 'T' && b != '_') {
		return ZeroTimestamp, tsStart, i
	}

	hour, hcount := parseMax2Digits(buffer, n, i)
	if hcount == 0 {
		return ZeroTimestamp, tsStart, i + 1
	}
	if hour > 23 {
		return ZeroTimestamp, tsStart, i + hcount
	}

	i += hcount
	if i >= n {
		return ZeroTimestamp, tsStart, n
	}

	b = buffer[i]
	i++
	if b != ':' && b != '.' && b != '-' {
		return ZeroTimestamp, tsStart, i
	}

	minute, mincount := parseMax2Digits(buffer, n, i)
	if mincount == 0 {
		return ZeroTimestamp, tsStart, i + 1
	}
	if minute > 59 {
		return ZeroTimestamp, tsStart, i + mincount
	}

	i += mincount
	if i >= n {
		return ZeroTimestamp, tsStart, n
	}

	b = buffer[i]
	i++
	if b != ':' && b != '.' && b != '-' {
		return ZeroTimestamp, tsStart, i
	}

	second, scount := parseMax2Digits(buffer, n, i)
	if scount == 0 {
		return ZeroTimestamp, tsStart, i + 1
	}
	if second > 59 {
		return ZeroTimestamp, tsStart, i + scount
	}

	i += scount
	var nsec int
	if i < n && (buffer[i] == '.' || buffer[i] == ',') {
		i++
		var ncount int
		nsec, ncount = parseDigits(buffer, n, i, 9)
		i += ncount
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	}

	tzSign, tzHour, tzMin, i := parseTimezone(c, buffer, n, i)

	return NewTimestamp(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin), tsStart, i
}

func skipToFirstDigit(buffer []byte, n, i int) (int, bool) {
	for i < n {
		b := buffer[i]
		if b >= '0' && b <= '9' {
			break
		}
		i++
		if b == '\r' || b == '\n' {
			return i, true
		}
	}
	return i, false
}

func parseTimezone(c *ParseTimestampConfig, buffer []byte, n int, i int) (tzSign, tzHour, tzMin, nextI int) {
	nextI = i
	if c.IgnoreTimezoneInfo || nextI >= n {
		return 0, 0, 0, nextI
	}
	b := buffer[nextI]
	nextI++

	switch b {
	case 'Z':
		// Already using UTC
	case '+', '-':
		tzSign = int(',') - int(b)
		if nextI+2 > n {
			break
		}
		var hcount int
		tzHour, hcount = parseMax2Digits(buffer, n, nextI)
		if hcount == 0 || tzHour > 23 {
			break
		}
		nextI += hcount
		if nextI < n && buffer[nextI] == ':' {
			nextI++
			var mcount int
			tzMin, mcount = parseMax2Digits(buffer, n, nextI)
			nextI += mcount
		} else {
			// +0800 format (no colon)
			var mcount int
			tzMin, mcount = parseMax2Digits(buffer, n, nextI)
			nextI += mcount
		}
	}
	return tzSign, tzHour, tzMin, nextI
}

func parseDigits(buffer []byte, n int, i int, maxCount int) (val int, count int) {
	if i >= n {
		return 0, 0
	}

	for count < maxCount && i < n {
		d := int(buffer[i] - '0')
		if d < 0 || d > 9 {
			break
		}

		val = val*10 + d
		count++
		i++
	}

	return val, count
}

func parseMax2Digits(buffer []byte, n int, i int) (int, int) {
	if i >= n {
		return 0, 0
	}
	b1 := buffer[i]
	if b1 < '0' || b1 > '9' {
		return 0, 0
	}
	v := int(b1 - '0')
	if i+1 < n {
		b2 := buffer[i+1]
		if b2 >= '0' && b2 <= '9' {
			return v*10 + int(b2-'0'), 2
		}
	}
	return v, 1
}

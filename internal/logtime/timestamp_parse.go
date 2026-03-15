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
	ts, _ := ParseTimestampWithEnd(c, buffer)
	return ts
}

// ParseTimestampWithEnd scans the buffer for a timestamp and returns both the
// parsed timestamp and the byte offset where the timestamp ends. The end offset
// can be used to strip the original timestamp from output.
// If no timestamp is found, or the timestamp is on a subsequent line (past a
// newline), the end offset is 0 — indicating nothing should be stripped from
// the current line.
func ParseTimestampWithEnd(c *ParseTimestampConfig, buffer []byte) (Timestamp, int) {
	n := min(len(buffer), c.TimestampSearchEndIndex)

	// Fast path: try parsing from position 0. If tryParseTimestamp succeeds,
	// the timestamp is guaranteed to be on the first line because
	// skipToFirstDigit (called internally) bails on newlines.
	timestamp, end := tryParseTimestamp(c, buffer, 0, n)
	if timestamp != ZeroTimestamp {
		return timestamp, end
	}

	// Slow path: first attempt failed. Continue scanning, but track the
	// first newline to avoid returning timestamps from subsequent lines.
	firstNewline := n
	for j := 0; j < n; j++ {
		if buffer[j] == '\n' || buffer[j] == '\r' {
			firstNewline = j
			break
		}
	}

	for i := end; timestamp == ZeroTimestamp && i < n; {
		timestamp, i = tryParseTimestamp(c, buffer, i, n)
		end = i
	}
	if end > firstNewline {
		if timestamp != ZeroTimestamp {
			timestamp = ZeroTimestamp
		}
		timestamp, _, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	} else if timestamp == ZeroTimestamp {
		timestamp, _, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	}
	if timestamp == ZeroTimestamp {
		return timestamp, 0
	}
	return timestamp, end
}

// ParseTimestampForStrip scans the buffer for a timestamp and returns the parsed
// timestamp plus the start and end byte offsets of the timestamp section including
// surrounding delimiters. Used when stripping timestamps from output.
func ParseTimestampForStrip(c *ParseTimestampConfig, buffer []byte) (Timestamp, int, int) {
	n := min(len(buffer), c.TimestampSearchEndIndex)

	// Fast path: try parsing from position 0.
	timestamp, end := tryParseTimestamp(c, buffer, 0, n)
	if timestamp != ZeroTimestamp {
		tsStart, _ := skipToFirstDigit(buffer, n, 0)
		prefixStart, tsEnd := computeStripBounds(buffer, n, n, tsStart, end)
		return timestamp, prefixStart, tsEnd
	}

	// Slow path: continue scanning with newline tracking.
	firstNewline := n
	for j := 0; j < n; j++ {
		if buffer[j] == '\n' || buffer[j] == '\r' {
			firstNewline = j
			break
		}
	}

	var lastI int
	for i := end; timestamp == ZeroTimestamp && i < n; {
		lastI = i
		timestamp, i = tryParseTimestamp(c, buffer, i, n)
		end = i
	}

	var tsStart int
	if end > firstNewline {
		if timestamp != ZeroTimestamp {
			timestamp = ZeroTimestamp
		}
		timestamp, tsStart, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	} else if timestamp != ZeroTimestamp {
		tsStart, _ = skipToFirstDigit(buffer, n, lastI)
	} else {
		timestamp, tsStart, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	}

	if timestamp == ZeroTimestamp {
		return timestamp, 0, 0
	}
	prefixStart, tsEnd := computeStripBounds(buffer, n, firstNewline, tsStart, end)
	return timestamp, prefixStart, tsEnd
}

func computeStripBounds(buffer []byte, n int, firstNewline int, tsStart int, end int) (int, int) {
	// Strip trailing delimiters after the timestamp (up to 3 chars).
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

	// Look backward from tsStart for opening brackets (up to 3 chars).
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
	if prefixStart > 0 {
		b := buffer[prefixStart-1]
		if b != ' ' && b != '\t' {
			prefixStart = tsStart
		}
	}

	return prefixStart, tsEnd
}

func tryParseTimestamp(c *ParseTimestampConfig, buffer []byte, i int, n int) (Timestamp, int) {
	if n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, n
	}

	var hitNewline bool
	i, hitNewline = skipToFirstDigit(buffer, n, i)
	if hitNewline {
		return ZeroTimestamp, i
	}

	if i >= n || n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, n
	}

	for j := i + c.ShortestTimestampLen - 1; j >= i; j-- {
		b := buffer[j]
		if b == '\n' || b == '\r' {
			return ZeroTimestamp, j + 1
		}
	}

	year, count := parseDigits(buffer, n, i, 4)
	if count == 0 {
		return ZeroTimestamp, i + 1
	} else if count == 2 {
		if year < 69 {
			year += 2000
		} else {
			year += 1900
		}
	} else if year > 2050 || year < 1969 {
		return ZeroTimestamp, i + count
	}

	i += count
	if i >= n {
		return ZeroTimestamp, n
	}
	b := buffer[i]

	// if b == '-' || b == '/' { i++ }
	if b == '-' || b == '/' {
		i++
	}

	month, mcount := parseMax2Digits(buffer, n, i)
	if mcount == 0 {
		return ZeroTimestamp, i + 1
	}
	if month > 12 || month < 1 {
		return ZeroTimestamp, i + mcount
	}

	i += mcount
	if i >= n {
		return ZeroTimestamp, n
	}
	b = buffer[i]

	// if b == '-' || b == '/' { i++ }
	if b == '-' || b == '/' {
		i++
	}

	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 {
		return ZeroTimestamp, i + 1
	}
	if day > 31 || day < 1 {
		return ZeroTimestamp, i + dcount
	}

	i += dcount
	if i >= n {
		return ZeroTimestamp, n
	}
	b = buffer[i]
	i++
	if i >= n || (b != ' ' && b != 'T' && b != '_') {
		return ZeroTimestamp, i
	}

	hour, hcount := parseMax2Digits(buffer, n, i)
	if hcount == 0 {
		return ZeroTimestamp, i + 1
	}
	if hour > 23 {
		return ZeroTimestamp, i + hcount
	}

	i += hcount
	if i >= n {
		return ZeroTimestamp, n
	}

	b = buffer[i]
	i++
	if b != ':' && b != '.' && b != '-' {
		return ZeroTimestamp, i
	}

	minute, mincount := parseMax2Digits(buffer, n, i)
	if mincount == 0 {
		return ZeroTimestamp, i + 1
	}
	if minute > 59 {
		return ZeroTimestamp, i + mincount
	}

	i += mincount
	if i >= n {
		return ZeroTimestamp, n
	}

	b = buffer[i]
	i++
	if b != ':' && b != '.' && b != '-' {
		return ZeroTimestamp, i
	}

	second, scount := parseMax2Digits(buffer, n, i)
	if scount == 0 {
		return ZeroTimestamp, i + 1
	}
	if second > 59 {
		return ZeroTimestamp, i + scount
	}

	i += scount
	var nsec int
	if i < n && (buffer[i] == '.' || buffer[i] == ',') {
		i++
		var ncount int
		nsec, ncount = parseDigits(buffer, n, i, 9)
		i += ncount
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	} else {
		nsec, i = parseSpaceSeparatedMillis(buffer, n, i)
	}

	tzSign, tzHour, tzMin, i := parseTimezone(c, buffer, n, i)

	return NewTimestamp(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin), i
}

// tryParseCtimeTimestamp scans for ctime-style timestamps:
//
//	[DayOfWeek ]Mon DD HH:MM:SS[ TZ] YYYY
//
// Examples: "Sat Mar 07 23:59:43 CST 2026", "Mar  7 23:59:43 2026"
func tryParseCtimeTimestamp(c *ParseTimestampConfig, buffer []byte, n int) (Timestamp, int, int) {
	for scanPos := 0; scanPos+15 <= n; scanPos++ {
		// Look for a month name
		month := parseMonthName(buffer, scanPos)
		if month == 0 {
			continue
		}
		// Validate boundary: month name must be at start or preceded by space
		if scanPos > 0 && buffer[scanPos-1] != ' ' && buffer[scanPos-1] != '\t' {
			continue
		}
		ts, start, end := parseCtimeFrom(c, buffer, n, scanPos, month)
		if ts != ZeroTimestamp {
			return ts, start, end
		}
	}
	return ZeroTimestamp, 0, 0
}

func parseCtimeFrom(c *ParseTimestampConfig, buffer []byte, n int, monthPos int, month int) (Timestamp, int, int) {
	i := monthPos + 3

	// Expect space after month name
	if i >= n || buffer[i] != ' ' {
		return ZeroTimestamp, 0, 0
	}
	i++

	// Parse day (1-2 digits, may be space-padded like "Mar  7")
	if i < n && buffer[i] == ' ' {
		i++
	}
	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 || day < 1 || day > 31 {
		return ZeroTimestamp, 0, 0
	}
	i += dcount

	// Expect space
	if i >= n || buffer[i] != ' ' {
		return ZeroTimestamp, 0, 0
	}
	i++

	// Parse HH:MM:SS
	hour, hcount := parseMax2Digits(buffer, n, i)
	if hcount == 0 || hour > 23 {
		return ZeroTimestamp, 0, 0
	}
	i += hcount
	if i >= n || buffer[i] != ':' {
		return ZeroTimestamp, 0, 0
	}
	i++
	minute, mincount := parseMax2Digits(buffer, n, i)
	if mincount == 0 || minute > 59 {
		return ZeroTimestamp, 0, 0
	}
	i += mincount
	if i >= n || buffer[i] != ':' {
		return ZeroTimestamp, 0, 0
	}
	i++
	second, scount := parseMax2Digits(buffer, n, i)
	if scount == 0 || second > 59 {
		return ZeroTimestamp, 0, 0
	}
	i += scount

	// Expect space
	if i >= n || buffer[i] != ' ' {
		return ZeroTimestamp, 0, 0
	}
	i++

	// Optional timezone name (2-5 uppercase letters)
	tzStart := i
	for i < n && buffer[i] >= 'A' && buffer[i] <= 'Z' {
		i++
	}
	tzLen := i - tzStart
	if tzLen >= 2 && tzLen <= 5 {
		// Valid timezone abbreviation — expect space before year
		if i >= n || buffer[i] != ' ' {
			return ZeroTimestamp, 0, 0
		}
		i++
	} else if tzLen > 0 {
		// Not a valid timezone — revert
		return ZeroTimestamp, 0, 0
	}
	// If tzLen == 0, we're already at the year position

	// Parse year (4 digits)
	year, ycount := parseDigits(buffer, n, i, 4)
	if ycount != 4 || year < 1969 || year > 2050 {
		return ZeroTimestamp, 0, 0
	}
	i += ycount

	// Determine the start of the timestamp section for stripping.
	// If preceded by a day-of-week like "Sat ", include it.
	tsStart := monthPos
	if monthPos >= 4 && buffer[monthPos-1] == ' ' {
		dow := monthPos - 4
		if dow >= 0 && isAlpha(buffer[dow]) && isAlpha(buffer[dow+1]) && isAlpha(buffer[dow+2]) {
			// Looks like a day-of-week abbreviation
			if dow == 0 || buffer[dow-1] == ' ' || buffer[dow-1] == '\t' {
				tsStart = dow
			}
		}
	}

	return NewTimestamp(year, month, day, hour, minute, second, 0, 0, 0, 0), tsStart, i
}

func parseMonthName(buffer []byte, i int) int {
	if i+3 > len(buffer) {
		return 0
	}
	switch buffer[i] {
	case 'J':
		if buffer[i+1] == 'a' && buffer[i+2] == 'n' {
			return 1
		}
		if buffer[i+1] == 'u' && buffer[i+2] == 'n' {
			return 6
		}
		if buffer[i+1] == 'u' && buffer[i+2] == 'l' {
			return 7
		}
	case 'F':
		if buffer[i+1] == 'e' && buffer[i+2] == 'b' {
			return 2
		}
	case 'M':
		if buffer[i+1] == 'a' && buffer[i+2] == 'r' {
			return 3
		}
		if buffer[i+1] == 'a' && buffer[i+2] == 'y' {
			return 5
		}
	case 'A':
		if buffer[i+1] == 'p' && buffer[i+2] == 'r' {
			return 4
		}
		if buffer[i+1] == 'u' && buffer[i+2] == 'g' {
			return 8
		}
	case 'S':
		if buffer[i+1] == 'e' && buffer[i+2] == 'p' {
			return 9
		}
	case 'O':
		if buffer[i+1] == 'c' && buffer[i+2] == 't' {
			return 10
		}
	case 'N':
		if buffer[i+1] == 'o' && buffer[i+2] == 'v' {
			return 11
		}
	case 'D':
		if buffer[i+1] == 'e' && buffer[i+2] == 'c' {
			return 12
		}
	}
	return 0
}

func isAlpha(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func parseSpaceSeparatedMillis(buffer []byte, n int, i int) (nsec int, nextI int) {
	if i+1 < n && buffer[i] == ' ' {
		frac, fcount := parseDigits(buffer, n, i+1, 3)
		if fcount > 0 && (i+1+fcount >= n || buffer[i+1+fcount] < '0' || buffer[i+1+fcount] > '9') {
			nsec = frac
			nextI = i + 1 + fcount
			for fcount < 9 {
				nsec *= 10
				fcount++
			}
			return nsec, nextI
		}
	}
	return 0, i
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
		}
		var mcount int
		tzMin, mcount = parseMax2Digits(buffer, n, nextI)
		nextI += mcount
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

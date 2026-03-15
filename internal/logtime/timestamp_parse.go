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
	var end int
	for i := 0; timestamp == ZeroTimestamp && i < n; {
		timestamp, i = tryParseTimestamp(c, buffer, i, n)
		end = i
	}
	if timestamp == ZeroTimestamp || end > firstNewline {
		return timestamp, 0
	}
	return timestamp, end
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
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	}

	tzSign, tzHour, tzMin, i := parseTimezone(c, buffer, n, i)

	return NewTimestamp(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin), i
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
		nextI += hcount // Fix: correctly advance over the timezone hours
		if nextI < n && buffer[nextI] == ':' {
			nextI++
			if nextI+2 <= n {
				tzMin, _ = parseMax2Digits(buffer, n, nextI)
			}
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

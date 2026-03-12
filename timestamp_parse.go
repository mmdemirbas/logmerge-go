package main

import "fmt"

// TODO: Consider supporting other time formats like 1 Jan 2006; Jan 1, 2006; 01/02/2006 etc.

type ParseTimestampConfig struct {
	IgnoreTimezoneInfo      bool `yaml:"IgnoreTimezoneInfo"`
	ShortestTimestampLen    int  `yaml:"ShortestTimestampLen"`
	TimestampSearchEndIndex int  `yaml:"TimestampSearchEndIndex"`
}

var parseTimestampBuffer []byte

func UpdateTimestamp(c *ParseTimestampConfig, file *FileHandle) error {
	bufLen := file.Buffer.Len()
	if bufLen < c.TimestampSearchEndIndex {
		startTime := GlobalMetricsTree.Start("FillBuffer")
		err := file.FillBuffer()
		if err != nil {
			file.LineTimestampParsed = false
			file.LineTimestamp = ZeroTimestamp
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
		GlobalMetricsTree.Stop(startTime)

		if bufLen == 0 && file.Buffer.IsEmpty() {
			file.LineTimestampParsed = false
			file.LineTimestamp = ZeroTimestamp
			return nil
		}
	}

	if parseTimestampBuffer == nil {
		parseTimestampBuffer = make([]byte, c.TimestampSearchEndIndex)
	}

	startTime := GlobalMetricsTree.Start("BufferAsSlice")
	var latestCharWasCR bool
	buf, _ := file.Buffer.PeekNextLineSlice(&latestCharWasCR)
	GlobalMetricsTree.Stop(startTime)

	startTime = GlobalMetricsTree.Start("ParseTimestamp")
	timestamp := ParseTimestamp(c, buf)
	GlobalMetricsTree.Stop(startTime)

	file.LineTimestampParsed = true
	file.LineTimestamp = timestamp
	return nil
}

func ParseTimestamp(c *ParseTimestampConfig, buffer []byte) Timestamp {
	n := min(len(buffer), c.TimestampSearchEndIndex)

	var timestamp Timestamp
	for i := 0; timestamp == ZeroTimestamp && i < n; {
		timestamp, i = tryParseTimestamp(c, buffer, i, n)
	}
	return timestamp
}

func tryParseTimestamp(c *ParseTimestampConfig, buffer []byte, i int, n int) (Timestamp, int) {
	if n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, n
	}

	// Skip until the first digit
	for i < n {
		b := buffer[i]
		c := int(b - '0')
		if 0 <= c && c <= 9 {
			break
		}

		i++
		if b == '\r' || b == '\n' {
			return ZeroTimestamp, i
		}
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
	b := buffer[i]

	// if b == '-' || b == '/' { i++ }
	i -= ((int(b)^int('-'))*(int(b)^int('/')) - 1) >> 31

	month, mcount := parseMax2Digits(buffer, n, i)
	if mcount == 0 {
		return ZeroTimestamp, i + 1
	}
	if month > 12 || month < 1 {
		return ZeroTimestamp, i + mcount
	}

	i += mcount
	b = buffer[i]

	// if b == '-' || b == '/' { i++ }
	i -= ((int(b)^int('-'))&(int(b)^int('/')) - 1) >> 31

	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 {
		return ZeroTimestamp, i + 1
	}
	if day > 31 || day < 1 {
		return ZeroTimestamp, i + dcount
	}

	i += dcount
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

	tzSign := 0
	tzHour := 0
	tzMin := 0

	if !c.IgnoreTimezoneInfo && i < n {
		b = buffer[i]
		i++

		switch b {
		case 'Z':
			// Already using UTC

		case '+', '-':
			tzSign = int(',') - int(b)

			if i+2 > n {
				break
			}

			tzHour, hcount = parseMax2Digits(buffer, n, i)
			if hcount == 0 {
				break
			}
			if tzHour > 23 {
				break
			}

			tzMin = 0
			if i < n && buffer[i] == ':' {
				i++
				if i+2 <= n {
					tzMin, _ = parseMax2Digits(buffer, n, i)
				}
			}
		}
	}

	return NewTimestamp(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin), i
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
	if i+1 < n {
		b1 := int(buffer[i])
		b2 := int(buffer[i+1])

		isDigit1 := ((47 - b1) >> 31) & ((b1 - 58) >> 31)
		isDigit2 := ((47 - b2) >> 31) & ((b2 - 58) >> 31)

		oneDigit := isDigit1 & ^isDigit2
		twoDigits := isDigit1 & isDigit2

		return twoDigits&(10*b1+b2-528) | oneDigit&(b1-48), twoDigits&2 | oneDigit&1
	}
	if i < n {
		b1 := int(buffer[i])
		isDigit := ((47 - b1) >> 31) & ((b1 - 58) >> 31)
		return isDigit&(b1-48) | ^isDigit, isDigit
	}
	return 0, 0
}

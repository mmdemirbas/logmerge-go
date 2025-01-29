package main

import "fmt"

// TODO: Consider supporting other time formats like 1 Jan 2006; Jan 1, 2006; 01/02/2006 etc.

type ParseTimestampConfig struct {
	IgnoreTimezoneInfo      bool `yaml:"IgnoreTimezoneInfo"`
	ShortestTimestampLen    int  `yaml:"ShortestTimestampLen"`
	TimestampSearchEndIndex int  `yaml:"TimestampSearchEndIndex"`
}

type ParseTimestampMetrics struct {
	Timestamp_Lenghts                     *BucketMetric
	Timestamp_FirstDigitIndexes           *BucketMetric
	Timestamp_FirstDigitIndexesActual     *BucketMetric
	Timestamp_LastDigitIndexes            *BucketMetric
	Timestamp_NanosLengths                *BucketMetric
	Timestamp_NoFirstDigit                int64
	Timestamp_LineTooShort                int64
	Timestamp_LineTooShortAfterFirstDigit int64
	Timestamp_NoYear                      int64
	Timestamp_2DigitYear_1900             int64
	Timestamp_2DigitYear_2000             int64
	Timestamp_4DigitYear_OutOfRange       int64
	Timestamp_NoMonth                     int64
	Timestamp_MonthOutOfRange             int64
	Timestamp_NoDay                       int64
	Timestamp_DayOutOfRange               int64
	Timestamp_SpaceOperatorMismatch       int64
	Timestamp_NoHour                      int64
	Timestamp_HourOutOfRange              int64
	Timestamp_NoHourSeparator             int64
	Timestamp_HourSeparatorMismatch       int64
	Timestamp_MismatchedHourSeparators    map[byte]int
	Timestamp_NoMinute                    int64
	Timestamp_MinuteOutOfRange            int64
	Timestamp_NoMinuteSeparator           int64
	Timestamp_MinuteSeparatorMismatch     int64
	Timestamp_MismatchedMinuteSeparators  map[byte]int
	Timestamp_NoSecond                    int64
	Timestamp_SecondOutOfRange            int64
	Timestamp_HasNanos                    int64
	Timestamp_HasNotNanos                 int64
	Timestamp_NoTimezone                  int64
	Timestamp_UtcTimezone                 int64
	Timestamp_NonUtcTimezone              int64
	Timestamp_TimezoneEarlyReturn         int64
	Timestamp_NoTimezoneHour              int64
	Timestamp_TimezoneHourOutOfRange      int64
}

func NewParseTimestampMetrics() *ParseTimestampMetrics {
	return &ParseTimestampMetrics{
		Timestamp_Lenghts:                    NewBucketMetric("Timestamp_Lenghts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 500, 1000, 10000, 50000),
		Timestamp_FirstDigitIndexes:          NewBucketMetric("Timestamp_FirstDigitIndexes", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000),
		Timestamp_FirstDigitIndexesActual:    NewBucketMetric("Timestamp_FirstDigitIndexesActual", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000),
		Timestamp_LastDigitIndexes:           NewBucketMetric("Timestamp_LastDigitIndexes", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 225, 250, 300, 350, 400, 450, 500, 1000, 5000, 10000, 20000, 30000, 40000, 50000),
		Timestamp_NanosLengths:               NewBucketMetric("Timestamp_NanosLengths", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9),
		Timestamp_MismatchedHourSeparators:   make(map[byte]int),
		Timestamp_MismatchedMinuteSeparators: make(map[byte]int),
	}
}

var parseTimestampBuffer []byte

func UpdateTimestamp(c *ParseTimestampConfig, m *ParseTimestampMetrics, file *FileHandle) error {
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
	buf := file.Buffer.AsSlice(parseTimestampBuffer)
	GlobalMetricsTree.Stop(startTime)

	startTime = GlobalMetricsTree.Start("ParseTimestamp")
	timestamp := ParseTimestamp(c, m, buf)
	GlobalMetricsTree.Stop(startTime)

	file.LineTimestampParsed = true
	file.LineTimestamp = timestamp
	return nil
}

func ParseTimestamp(c *ParseTimestampConfig, m *ParseTimestampMetrics, buffer []byte) Timestamp {
	n := FastMin(len(buffer), c.TimestampSearchEndIndex)

	var timestamp Timestamp
	for i := 0; timestamp == ZeroTimestamp && i < n; {
		timestamp, i = tryParseTimestamp(c, m, buffer, i, n)
	}
	return timestamp
}

func tryParseTimestamp(c *ParseTimestampConfig, m *ParseTimestampMetrics, buffer []byte, i int, n int) (Timestamp, int) {
	// TODO: Measure and optimize this method - Maybe I should simplify the code to be able to touch for performance optimizations

	// TODO: Remove statistics in this method. Not needed anymore.

	if n < i+c.ShortestTimestampLen {
		m.Timestamp_LineTooShort++
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
			m.Timestamp_NoFirstDigit++
			return ZeroTimestamp, i
		}
	}

	firstDigitIndex := i
	if i < n {
		m.Timestamp_FirstDigitIndexes.UpdateBucketCount(firstDigitIndex)
	} else {
		m.Timestamp_NoFirstDigit++
		return ZeroTimestamp, n
	}
	if n < i+c.ShortestTimestampLen {
		m.Timestamp_LineTooShortAfterFirstDigit++
		return ZeroTimestamp, n
	}

	for j := i + c.ShortestTimestampLen - 1; j >= i; j-- {
		b := buffer[j]
		if b == '\n' || b == '\r' {
			m.Timestamp_LineTooShortAfterFirstDigit++
			return ZeroTimestamp, j + 1
		}
	}

	year, count := parseDigits(buffer, n, i, 4)
	if count == 0 {
		m.Timestamp_NoYear++
		return ZeroTimestamp, i + 1
	} else if count == 2 {
		if year < 69 {
			m.Timestamp_2DigitYear_2000++
			year += 2000
		} else {
			m.Timestamp_2DigitYear_1900++
			year += 1900
		}
	} else if year > 2050 || year < 1969 {
		m.Timestamp_4DigitYear_OutOfRange++
		return ZeroTimestamp, i + count
	}

	i += count
	b := buffer[i]

	// if b == '-' || b == '/' { i++ }
	i -= ((int(b)^int('-'))*(int(b)^int('/')) - 1) >> 31

	month, mcount := parseMax2Digits(buffer, n, i)
	if mcount == 0 {
		m.Timestamp_NoMonth++
		return ZeroTimestamp, i + 1
	}

	if month > 12 || month < 1 {
		m.Timestamp_MonthOutOfRange++
		return ZeroTimestamp, i + mcount
	}

	i += mcount
	b = buffer[i]

	// if b == '-' || b == '/' { i++ }
	i -= ((int(b)^int('-'))&(int(b)^int('/')) - 1) >> 31

	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 {
		m.Timestamp_NoDay++
		return ZeroTimestamp, i + 1
	}

	if day > 31 || day < 1 {
		m.Timestamp_DayOutOfRange++
		return ZeroTimestamp, i + dcount
	}

	i += dcount
	b = buffer[i]
	i++

	if i >= n || (b != ' ' && b != 'T' && b != '_') {
		m.Timestamp_SpaceOperatorMismatch++
		return ZeroTimestamp, i
	}

	hour, hcount := parseMax2Digits(buffer, n, i)
	if hcount == 0 {
		m.Timestamp_NoHour++
		return ZeroTimestamp, i + 1
	}

	if hour > 23 {
		m.Timestamp_HourOutOfRange++
		return ZeroTimestamp, i + hcount
	}

	i += hcount
	if i >= n {
		m.Timestamp_NoHourSeparator++
		return ZeroTimestamp, n
	}

	b = buffer[i]
	i++

	if b != ':' && b != '.' && b != '-' {
		m.Timestamp_HourSeparatorMismatch++
		v, ok := m.Timestamp_MismatchedHourSeparators[b]
		if ok {
			m.Timestamp_MismatchedHourSeparators[b] = v + 1
		} else {
			m.Timestamp_MismatchedHourSeparators[b] = 1
		}
		return ZeroTimestamp, i
	}

	minute, mincount := parseMax2Digits(buffer, n, i)
	if mincount == 0 {
		m.Timestamp_NoMinute++
		return ZeroTimestamp, i + 1
	}

	if minute > 59 {
		m.Timestamp_MinuteOutOfRange++
		return ZeroTimestamp, i + mincount
	}

	i += mincount
	if i >= n {
		m.Timestamp_NoMinuteSeparator++
		return ZeroTimestamp, n
	}

	b = buffer[i]
	i++

	if b != ':' && b != '.' && b != '-' {
		m.Timestamp_MinuteSeparatorMismatch++
		v, ok := m.Timestamp_MismatchedMinuteSeparators[b]
		if ok {
			m.Timestamp_MismatchedMinuteSeparators[b] = v + 1
		} else {
			m.Timestamp_MismatchedMinuteSeparators[b] = 1
		}
		return ZeroTimestamp, i
	}

	second, scount := parseMax2Digits(buffer, n, i)
	if scount == 0 {
		m.Timestamp_NoSecond++
		return ZeroTimestamp, i + 1
	}

	if second > 59 {
		m.Timestamp_SecondOutOfRange++
		return ZeroTimestamp, i + scount
	}

	i += scount
	var nsec int
	if i < n && (buffer[i] == '.' || buffer[i] == ',') {
		i++
		m.Timestamp_HasNanos++
		var ncount int
		nsec, ncount = parseDigits(buffer, n, i, 9)
		i += ncount

		m.Timestamp_NanosLengths.UpdateBucketCount(ncount)
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	} else {
		m.Timestamp_HasNotNanos++
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
			m.Timestamp_UtcTimezone++
			break

		case '+', '-':
			m.Timestamp_NonUtcTimezone++
			tzSign = int(',') - int(b)

			if i+2 > n {
				m.Timestamp_TimezoneEarlyReturn++
				break
			}

			tzHour, hcount = parseMax2Digits(buffer, n, i)
			if hcount == 0 {
				m.Timestamp_NoTimezoneHour++
				break
			}
			if tzHour > 23 {
				m.Timestamp_TimezoneHourOutOfRange++
				break
			}

			tzMin = 0
			if i < n && buffer[i] == ':' {
				i++
				if i+2 <= n {
					tzMin, _ = parseMax2Digits(buffer, n, i)
				}
			}

		default:
			m.Timestamp_NoTimezone++
		}
	}

	m.Timestamp_LastDigitIndexes.UpdateBucketCount(i)
	m.Timestamp_FirstDigitIndexesActual.UpdateBucketCount(firstDigitIndex)
	m.Timestamp_Lenghts.UpdateBucketCount(i - firstDigitIndex)

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

package main

import (
	"fmt"
	"time"
)

var (
	// Return the minimum time for the lines with no timestamp, so that those lines are listed first.
	// Otherwise, we could miss the correct order for the upcoming lines with timestamps.
	noTimestamp = time.Time{}
)

type LinePrefix struct {
	Source    *FileReader
	Timestamp time.Time
}

func ReadLinePrefix(reader *FileReader) (*LinePrefix, error) {
	if reader.Buffer.Len() < timestampSearchPrefixLen {
		startOfFillBuffer := MeasureStart("FillBuffer")
		err := reader.FillBuffer()
		if err != nil {
			return nil, fmt.Errorf("failed to fill buffer: %v", err)
		}
		TotalFillBufferDuration += MeasureSince(startOfFillBuffer)
	}

	if reader.Buffer.IsEmpty() {
		return nil, nil
	}

	startOfParseTimestamp := MeasureStart("ParseTimestamp")
	timestamp := ParseTimestamp(reader.Buffer)
	TotalParseTimestampDuration += MeasureSince(startOfParseTimestamp)

	if timestamp.Equal(noTimestamp) {
		LinesWithoutTimestamps++
	} else {
		LinesWithTimestamps++
	}
	return &LinePrefix{Source: reader, Timestamp: timestamp}, nil
}

func ParseTimestamp(buffer *RingBuffer) time.Time {
	// TODO: What if we have digits before the actual timestamp?
	//   In this case, we should skip non-digits after the first digit and try parsing from there.

	n := buffer.Len()

	// TODO: Optimize buffer.Peek(i) usages. Timestamp will be at most ~40 chars. Copy that portion maybe?
	// TODO: Consider getting a Slice from the buffer and using it instead of Peek(i) calls.

	i := 0
	for i < n && (buffer.Peek(i) < '0' || buffer.Peek(i) > '9') {
		if buffer.Peek(i) == '\r' || buffer.Peek(i) == '\n' {
			ParseTimestamp_NoFirstDigit++
			return noTimestamp
		}
		i++
	}
	firstDigitIndex := i
	if i < n {
		ParseTimestamp_MinFirstDigitIndex = min(ParseTimestamp_MinFirstDigitIndex, firstDigitIndex)
		ParseTimestamp_MaxFirstDigitIndex = max(ParseTimestamp_MaxFirstDigitIndex, firstDigitIndex)
		UpdateBucketCount(firstDigitIndex, ParseTimestamp_DigitIndexLevels, ParseTimestamp_FirstDigitIndexValues)
	} else {
		ParseTimestamp_NoFirstDigit++
		return noTimestamp
	}
	if i+15 > n {
		ParseTimestamp_LineTooShortAfterFirstDigit++
		return noTimestamp
	}
	for j := i + 15; j >= i; j-- {
		if buffer.Peek(j) == '\r' || buffer.Peek(j) == '\n' {
			ParseTimestamp_LineTooShortAfterFirstDigit++
			return noTimestamp
		}
	}

	year, count := parseDigits(buffer, &i, 4)
	if count == 0 {
		ParseTimestamp_NoYear++
		return noTimestamp
	} else if count == 2 {
		if year >= 69 {
			ParseTimestamp_2DigitYear_1900++
			year += 1900
		} else {
			ParseTimestamp_2DigitYear_2000++
			year += 2000
		}
	} else if year < 1969 || year > 2050 {
		ParseTimestamp_4DigitYear_OutOfRange++
		return noTimestamp
	}

	if buffer.Peek(i) == '-' {
		i++
	}

	month, mcount := parseDigits(buffer, &i, 2)
	if mcount == 0 {
		ParseTimestamp_NoMonth++
		return noTimestamp
	}
	if month < 1 || month > 12 {
		ParseTimestamp_MonthOutOfRange++
		return noTimestamp
	}

	if buffer.Peek(i) == '-' {
		i++
	}

	day, dcount := parseDigits(buffer, &i, 2)
	if dcount == 0 {
		ParseTimestamp_NoDay++
		return noTimestamp
	}
	if day < 1 || day > 31 {
		ParseTimestamp_DayOutOfRange++
		return noTimestamp
	}

	if i >= n || (buffer.Peek(i) != ' ' && buffer.Peek(i) != 'T' && buffer.Peek(i) != '_') {
		ParseTimestamp_SpaceOperatorMismatch++
		return noTimestamp
	}
	i++

	hour, hcount := parseDigits(buffer, &i, 2)
	if hcount == 0 {
		ParseTimestamp_NoHour++
		return noTimestamp
	}
	if hour > 23 {
		ParseTimestamp_HourOutOfRange++
		return noTimestamp
	}

	if i >= n {
		ParseTimestamp_NoHourSeparator++
		return noTimestamp
	}
	if buffer.Peek(i) != ':' && buffer.Peek(i) != '.' && buffer.Peek(i) != '-' {
		ParseTimestamp_HourSeparatorMismatch++
		ParseTimestamp_MismatchedHourSeparators = append(ParseTimestamp_MismatchedHourSeparators, buffer.Peek(i))
		return noTimestamp
	}
	i++

	minute, mincount := parseDigits(buffer, &i, 2)
	if mincount == 0 {
		ParseTimestamp_NoMinute++
		return noTimestamp
	}
	if minute > 59 {
		ParseTimestamp_MinuteOutOfRange++
		return noTimestamp
	}

	if i >= n {
		ParseTimestamp_NoMinuteSeparator++
		return noTimestamp
	}
	if buffer.Peek(i) != ':' && buffer.Peek(i) != '.' && buffer.Peek(i) != '-' {
		ParseTimestamp_MinuteSeparatorMismatch++
		ParseTimestamp_MismatchedMinuteSeparators = append(ParseTimestamp_MismatchedMinuteSeparators, buffer.Peek(i))
		return noTimestamp
	}
	i++

	second, scount := parseDigits(buffer, &i, 2)
	if scount == 0 {
		ParseTimestamp_NoSecond++
		return noTimestamp
	}
	if second > 59 {
		ParseTimestamp_SecondOutOfRange++
		return noTimestamp
	}

	var nsec int
	if i < n && (buffer.Peek(i) == '.' || buffer.Peek(i) == ',') {
		ParseTimestamp_HasNanos++
		i++
		var ncount int
		nsec, ncount = parseDigits(buffer, &i, 9)
		UpdateBucketCount(ncount, ParseTimestamp_NanosLengthBucketLevels, ParseTimestamp_NanosLengthBucketValues)
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	} else {
		ParseTimestamp_HasNotNanos++
	}

	utc := time.UTC
	if i < n {
		switch buffer.Peek(i) {
		case 'Z':
			// Already using UTC
			ParseTimestamp_UtcTimezone++
			i++
			break
		case '+', '-':
			ParseTimestamp_NonUtcTimezone++
			sign := int(',') - int(buffer.Peek(i))
			i++

			if i+2 > n {
				ParseTimestamp_TimezoneEarlyReturn++
				break
			}

			tzHour, hcount := parseDigits(buffer, &i, 2)
			if hcount == 0 {
				ParseTimestamp_NoTimezoneHour++
				break
			}
			if tzHour > 23 {
				ParseTimestamp_TimezoneHourOutOfRange++
				break
			}

			tzMin := 0
			if i < n && buffer.Peek(i) == ':' {
				i++
				if i+2 <= n {
					tzMin, _ = parseDigits(buffer, &i, 2)
				}
			}

			if tzHour != 0 || tzMin != 0 {
				utc = time.FixedZone("", sign*(tzHour*3600+tzMin*60))
			}
		default:
			ParseTimestamp_NoTimezone++
		}
	}

	ParseTimestamp_MinTimestampEndIndex = min(ParseTimestamp_MinTimestampEndIndex, i)
	ParseTimestamp_MaxTimestampEndIndex = max(ParseTimestamp_MaxTimestampEndIndex, i)
	UpdateBucketCount(i, ParseTimestamp_DigitIndexLevels, ParseTimestamp_LastDigitIndexValues)

	ParseTimestamp_MinFirstDigitIndexActual = min(ParseTimestamp_MinFirstDigitIndexActual, firstDigitIndex)
	ParseTimestamp_MaxFirstDigitIndexActual = max(ParseTimestamp_MaxFirstDigitIndexActual, firstDigitIndex)
	UpdateBucketCount(firstDigitIndex, ParseTimestamp_DigitIndexLevels, ParseTimestamp_FirstDigitIndexValuesActual)

	timestampLen := i - firstDigitIndex
	ParseTimestamp_MinTimestampLength = min(ParseTimestamp_MinTimestampLength, timestampLen)
	ParseTimestamp_MaxTimestampLength = max(ParseTimestamp_MaxTimestampLength, timestampLen)
	UpdateBucketCount(timestampLen, ParseTimestamp_LenghtBucketLevels, ParseTimestamp_LengthBucketValues)

	return time.Date(year, time.Month(month), day, hour, minute, second, nsec, utc)
}

func parseDigits(buffer *RingBuffer, i *int, maxCount int) (val, count int) {
	end := *i + maxCount
	if end > buffer.Len() {
		end = buffer.Len()
	}
	for ; *i < end; *i++ {
		c := buffer.Peek(*i)
		if c < '0' || c > '9' {
			break
		}
		val = val*10 + int(c-'0')
		count++
	}
	return
}

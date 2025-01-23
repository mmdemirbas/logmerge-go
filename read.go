package main

import (
	"fmt"
)

var (
	// Return the minimum time for the lines with no timestamp, so that those lines are listed first.
	// Otherwise, we could miss the correct order for the upcoming lines with timestamps.
	noTimestamp = MyTime(0)

	timestampBuffer = make([]byte, 0, TimestampSearchEndIndex)
)

func UpdateTimestamp(reader *InputFile) error {
	bufLen := reader.Buffer.Len()
	if bufLen < TimestampSearchEndIndex {
		startTime := MeasureStart("FillBuffer")
		err := reader.FillBuffer()
		if err != nil {
			reader.TimestampParsed = false
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
		FillBufferMetric.MeasureSince(startTime)

		if bufLen == 0 && reader.Buffer.IsEmpty() {
			reader.TimestampParsed = false
			return nil
		}
	}

	startTime := MeasureStart("BufferAsSlice")
	buf := reader.Buffer.AsSlice(timestampBuffer)
	BufferAsSliceMetric.MeasureSince(startTime)

	startTime = MeasureStart("ParseTimestamp")
	timestamp := ParseTimestamp(buf)
	ParseTimestampMetric.MeasureSince(startTime)

	if timestamp == noTimestamp {
		LinesWithoutTimestamps++
	} else {
		LinesWithTimestamps++
	}

	reader.TimestampParsed = true
	reader.Timestamp = timestamp
	return nil
}

func ParseTimestamp(buffer []byte) MyTime {
	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "ParseTimestamp: Recovered from panic: %v. Buffer: %s\n", r, buffer)
		}
	}()
	// TODO: What if we have digits before the actual timestamp?
	//   In this case, we should skip non-digits after the first digit and try parsing from there.

	n := len(buffer)
	if n < ShortestTimestampLen {
		ParseTimestamp_LineTooShort++
		return noTimestamp
	}

	i := 0
	for i < n {
		b := buffer[i]
		if b < '9' && b > '0' {
			break
		}
		if b == '\r' || b == '\n' {
			ParseTimestamp_NoFirstDigit++
			return noTimestamp
		}
		i++
	}

	firstDigitIndex := i
	if i < n {
		ParseTimestamp_MinFirstDigitIndex = min(ParseTimestamp_MinFirstDigitIndex, firstDigitIndex)
		ParseTimestamp_MaxFirstDigitIndex = max(ParseTimestamp_MaxFirstDigitIndex, firstDigitIndex)
		ParseTimestamp_FirstDigitIndexes.UpdateBucketCount(firstDigitIndex)
	} else {
		ParseTimestamp_NoFirstDigit++
		return noTimestamp
	}
	if n < i+ShortestTimestampLen {
		ParseTimestamp_LineTooShortAfterFirstDigit++
		return noTimestamp
	}

	for j := i + ShortestTimestampLen - 1; j >= i; j-- {
		b := buffer[j]
		if b == '\n' || b == '\r' {
			ParseTimestamp_LineTooShortAfterFirstDigit++
			return noTimestamp
		}
	}

	year, count := parseDigits(buffer, n, &i, 4)
	if count == 0 {
		ParseTimestamp_NoYear++
		return noTimestamp
	} else if count == 2 {
		if year < 69 {
			ParseTimestamp_2DigitYear_2000++
			year += 2000
		} else {
			ParseTimestamp_2DigitYear_1900++
			year += 1900
		}
	} else if year > 2050 || year < 1969 {
		ParseTimestamp_4DigitYear_OutOfRange++
		return noTimestamp
	}

	if buffer[i] == '-' {
		i++
	}

	month, mcount := parseDigits(buffer, n, &i, 2)
	if mcount == 0 {
		ParseTimestamp_NoMonth++
		return noTimestamp
	}

	if month > 12 || month < 1 {
		ParseTimestamp_MonthOutOfRange++
		return noTimestamp
	}

	if buffer[i] == '-' {
		i++
	}

	day, dcount := parseDigits(buffer, n, &i, 2)
	if dcount == 0 {
		ParseTimestamp_NoDay++
		return noTimestamp
	}

	if day > 31 || day < 1 {
		ParseTimestamp_DayOutOfRange++
		return noTimestamp
	}

	b := buffer[i]
	if i >= n || (b != ' ' && b != 'T' && b != '_') {
		ParseTimestamp_SpaceOperatorMismatch++
		return noTimestamp
	}
	i++

	hour, hcount := parseDigits(buffer, n, &i, 2)
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

	b = buffer[i]
	if b != ':' && b != '.' && b != '-' {
		ParseTimestamp_HourSeparatorMismatch++
		ParseTimestamp_MismatchedHourSeparators = append(ParseTimestamp_MismatchedHourSeparators, b)
		return noTimestamp
	}
	i++

	minute, mincount := parseDigits(buffer, n, &i, 2)
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

	b = buffer[i]
	if b != ':' && b != '.' && b != '-' {
		ParseTimestamp_MinuteSeparatorMismatch++
		ParseTimestamp_MismatchedMinuteSeparators = append(ParseTimestamp_MismatchedMinuteSeparators, b)
		return noTimestamp
	}
	i++

	second, scount := parseDigits(buffer, n, &i, 2)
	if scount == 0 {
		ParseTimestamp_NoSecond++
		return noTimestamp
	}

	if second > 59 {
		ParseTimestamp_SecondOutOfRange++
		return noTimestamp
	}

	var nsec int
	if i < n && (buffer[i] == '.' || buffer[i] == ',') {
		ParseTimestamp_HasNanos++
		i++
		var ncount int
		nsec, ncount = parseDigits(buffer, n, &i, 9)
		ParseTimestamp_NanosLengths.UpdateBucketCount(ncount)
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	} else {
		ParseTimestamp_HasNotNanos++
	}

	//utc := time.UTC
	tzSign := 0
	tzHour := 0
	tzMin := 0
	if i < n {
		b = buffer[i]
		switch b {
		case 'Z':
			// Already using UTC
			ParseTimestamp_UtcTimezone++
			i++
			break
		case '+', '-':
			ParseTimestamp_NonUtcTimezone++
			tzSign = int(',') - int(b)
			i++

			if i+2 > n {
				ParseTimestamp_TimezoneEarlyReturn++
				break
			}

			tzHour, hcount = parseDigits(buffer, n, &i, 2)
			if hcount == 0 {
				ParseTimestamp_NoTimezoneHour++
				break
			}
			if tzHour > 23 {
				ParseTimestamp_TimezoneHourOutOfRange++
				break
			}

			tzMin = 0
			if i < n && buffer[i] == ':' {
				i++
				if i+2 <= n {
					tzMin, _ = parseDigits(buffer, n, &i, 2)
				}
			}

		default:
			ParseTimestamp_NoTimezone++
		}
	}

	ParseTimestamp_MinTimestampEndIndex = min(ParseTimestamp_MinTimestampEndIndex, i)
	ParseTimestamp_MaxTimestampEndIndex = max(ParseTimestamp_MaxTimestampEndIndex, i)
	ParseTimestamp_LastDigitIndexes.UpdateBucketCount(i)

	ParseTimestamp_MinFirstDigitIndexActual = min(ParseTimestamp_MinFirstDigitIndexActual, firstDigitIndex)
	ParseTimestamp_MaxFirstDigitIndexActual = max(ParseTimestamp_MaxFirstDigitIndexActual, firstDigitIndex)
	ParseTimestamp_FirstDigitIndexesActual.UpdateBucketCount(firstDigitIndex)

	timestampLen := i - firstDigitIndex
	ParseTimestamp_MinTimestampLength = min(ParseTimestamp_MinTimestampLength, timestampLen)
	ParseTimestamp_MaxTimestampLength = max(ParseTimestamp_MaxTimestampLength, timestampLen)
	ParseTimestamp_Lenghts.UpdateBucketCount(timestampLen)

	return NewMyTime(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin)
}

// TODO consider inlining or improving parseDigits performance
func parseDigits(buffer []byte, n int, i *int, maxCount int) (val, count int) {
	end := *i + maxCount
	if end > n {
		end = n
	}
	for ; *i < end; *i++ {
		c := buffer[*i]
		if c > '9' || c < '0' {
			break
		}
		val = val*10 + int(c-'0')
		count++
	}
	return
}

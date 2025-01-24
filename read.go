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

func UpdateTimestamp(reader *FileReader) error {
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
		Timestamp_LineTooShort++
		return noTimestamp
	}

	i := 0
	for i < n {
		b := buffer[i]
		if b < '9' && b > '0' {
			break
		}
		if b == '\r' || b == '\n' {
			Timestamp_NoFirstDigit++
			return noTimestamp
		}
		i++
	}

	firstDigitIndex := i
	if i < n {
		Timestamp_MinFirstDigitIndex = min(Timestamp_MinFirstDigitIndex, firstDigitIndex)
		Timestamp_MaxFirstDigitIndex = max(Timestamp_MaxFirstDigitIndex, firstDigitIndex)
		Timestamp_FirstDigitIndexes.UpdateBucketCount(firstDigitIndex)
	} else {
		Timestamp_NoFirstDigit++
		return noTimestamp
	}
	if n < i+ShortestTimestampLen {
		Timestamp_LineTooShortAfterFirstDigit++
		return noTimestamp
	}

	for j := i + ShortestTimestampLen - 1; j >= i; j-- {
		b := buffer[j]
		if b == '\n' || b == '\r' {
			Timestamp_LineTooShortAfterFirstDigit++
			return noTimestamp
		}
	}

	year, count := parseDigits(buffer, n, &i, 4)
	if count == 0 {
		Timestamp_NoYear++
		return noTimestamp
	} else if count == 2 {
		if year < 69 {
			Timestamp_2DigitYear_2000++
			year += 2000
		} else {
			Timestamp_2DigitYear_1900++
			year += 1900
		}
	} else if year > 2050 || year < 1969 {
		Timestamp_4DigitYear_OutOfRange++
		return noTimestamp
	}

	if buffer[i] == '-' {
		i++
	}

	month, mcount := parseDigits(buffer, n, &i, 2)
	if mcount == 0 {
		Timestamp_NoMonth++
		return noTimestamp
	}

	if month > 12 || month < 1 {
		Timestamp_MonthOutOfRange++
		return noTimestamp
	}

	if buffer[i] == '-' {
		i++
	}

	day, dcount := parseDigits(buffer, n, &i, 2)
	if dcount == 0 {
		Timestamp_NoDay++
		return noTimestamp
	}

	if day > 31 || day < 1 {
		Timestamp_DayOutOfRange++
		return noTimestamp
	}

	b := buffer[i]
	if i >= n || (b != ' ' && b != 'T' && b != '_') {
		Timestamp_SpaceOperatorMismatch++
		return noTimestamp
	}
	i++

	hour, hcount := parseDigits(buffer, n, &i, 2)
	if hcount == 0 {
		Timestamp_NoHour++
		return noTimestamp
	}

	if hour > 23 {
		Timestamp_HourOutOfRange++
		return noTimestamp
	}

	if i >= n {
		Timestamp_NoHourSeparator++
		return noTimestamp
	}

	b = buffer[i]
	if b != ':' && b != '.' && b != '-' {
		Timestamp_HourSeparatorMismatch++
		v, ok := Timestamp_MismatchedHourSeparators[b]
		if ok {
			Timestamp_MismatchedHourSeparators[b] = v + 1
		} else {
			Timestamp_MismatchedHourSeparators[b] = 1
		}
		return noTimestamp
	}
	i++

	minute, mincount := parseDigits(buffer, n, &i, 2)
	if mincount == 0 {
		Timestamp_NoMinute++
		return noTimestamp
	}

	if minute > 59 {
		Timestamp_MinuteOutOfRange++
		return noTimestamp
	}

	if i >= n {
		Timestamp_NoMinuteSeparator++
		return noTimestamp
	}

	b = buffer[i]
	if b != ':' && b != '.' && b != '-' {
		Timestamp_MinuteSeparatorMismatch++
		v, ok := Timestamp_MismatchedMinuteSeparators[b]
		if ok {
			Timestamp_MismatchedMinuteSeparators[b] = v + 1
		} else {
			Timestamp_MismatchedMinuteSeparators[b] = 1
		}
		return noTimestamp
	}
	i++

	second, scount := parseDigits(buffer, n, &i, 2)
	if scount == 0 {
		Timestamp_NoSecond++
		return noTimestamp
	}

	if second > 59 {
		Timestamp_SecondOutOfRange++
		return noTimestamp
	}

	var nsec int
	if i < n && (buffer[i] == '.' || buffer[i] == ',') {
		Timestamp_HasNanos++
		i++
		var ncount int
		nsec, ncount = parseDigits(buffer, n, &i, 9)
		Timestamp_NanosLengths.UpdateBucketCount(ncount)
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	} else {
		Timestamp_HasNotNanos++
	}

	tzSign := 0
	tzHour := 0
	tzMin := 0

	if !IgnoreTimezoneInfo && i < n {
		b = buffer[i]
		switch b {
		case 'Z':
			// Already using UTC
			Timestamp_UtcTimezone++
			i++
			break
		case '+', '-':
			Timestamp_NonUtcTimezone++
			tzSign = int(',') - int(b)
			i++

			if i+2 > n {
				Timestamp_TimezoneEarlyReturn++
				break
			}

			tzHour, hcount = parseDigits(buffer, n, &i, 2)
			if hcount == 0 {
				Timestamp_NoTimezoneHour++
				break
			}
			if tzHour > 23 {
				Timestamp_TimezoneHourOutOfRange++
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
			Timestamp_NoTimezone++
		}
	}

	Timestamp_MinTimestampEndIndex = min(Timestamp_MinTimestampEndIndex, i)
	Timestamp_MaxTimestampEndIndex = max(Timestamp_MaxTimestampEndIndex, i)
	Timestamp_LastDigitIndexes.UpdateBucketCount(i)

	Timestamp_MinFirstDigitIndexActual = min(Timestamp_MinFirstDigitIndexActual, firstDigitIndex)
	Timestamp_MaxFirstDigitIndexActual = max(Timestamp_MaxFirstDigitIndexActual, firstDigitIndex)
	Timestamp_FirstDigitIndexesActual.UpdateBucketCount(firstDigitIndex)

	timestampLen := i - firstDigitIndex
	Timestamp_MinTimestampLength = min(Timestamp_MinTimestampLength, timestampLen)
	Timestamp_MaxTimestampLength = max(Timestamp_MaxTimestampLength, timestampLen)
	Timestamp_Lenghts.UpdateBucketCount(timestampLen)

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

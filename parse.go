package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

var (
	// Return the minimum time for the lines with no timestamp, so that those lines are listed first.
	// Otherwise, we could miss the correct order for the upcoming lines with timestamps.
	noTimestamp = time.Time{}
)

func ParseLine(sourceName string, scanner *bufio.Scanner) *LogLine {
	// TODO: Read limited number of chars to a fixed buffer to avoid more allocations
	//    - preallocate every piece of necessary memory at the beginning of the program

	// TODO: Read only enough chars to parse the timestamp, will remove need to read buffers maybe.

	// TODO: What if we have digits before the actual timestamp?
	//   In this case, we should skip non-digits after the first digit and try parsing from there.

	var (
		line      string
		scan      bool
		timestamp time.Time
		result    *LogLine = nil
	)
	if scanner.Scan() {
		scan = true
		line = scanner.Text()
	} else {
		scan = false
		line = ""
	}

	if scan {
		LinesRead++
		BytesRead += int64(len(line))

		startOfParseTimestamp := time.Now()
		timestamp = ParseTimestamp(line)
		ParseTimestampDuration += MeasureSince(startOfParseTimestamp)

		if timestamp.Equal(noTimestamp) {
			LinesWithoutTimestamps++
		} else {
			LinesWithTimestamps++
		}
		result = &LogLine{
			Timestamp:  timestamp,
			SourceName: sourceName,
			RawLine:    line,
		}
	}

	return result
}

func ParseTimestamp(line string) time.Time {
	// Recover
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "Recovered from panic: %v. Line was: %q\n", r, line)
		}
	}()

	n := len(line)
	MaxLineLength = max(MaxLineLength, n)
	UpdateBucketCount(n, LineLengthBucketLevels, LineLengthBucketValues)

	i := 0
	for i < n && (line[i] < '0' || line[i] > '9') {
		i++
	}
	firstDigitIndex := i
	if i < n {
		ParseTimestamp_MinFirstDigitIndex = min(ParseTimestamp_MinFirstDigitIndex, firstDigitIndex)
		ParseTimestamp_MaxFirstDigitIndex = max(ParseTimestamp_MaxFirstDigitIndex, firstDigitIndex)
		UpdateBucketCount(firstDigitIndex, ParseTimestamp_DigitIndexLevels, ParseTimestamp_FirstDigitIndexValues)
	} else {
		ParseTimestamp_NoFirstDigit++
	}
	if i+15 > n {
		ParseTimestamp_LineTooShortAfterFirstDigit++
		return noTimestamp
	}

	year, count := parseDigits(line, &i, 4)
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

	if c := line[i]; c == '-' {
		i++
	}

	month, mcount := parseDigits(line, &i, 2)
	if mcount == 0 {
		ParseTimestamp_NoMonth++
		return noTimestamp
	}
	if month < 1 || month > 12 {
		ParseTimestamp_MonthOutOfRange++
		return noTimestamp
	}

	if line[i] == '-' {
		i++
	}

	day, dcount := parseDigits(line, &i, 2)
	if dcount == 0 {
		ParseTimestamp_NoDay++
		return noTimestamp
	}
	if day < 1 || day > 31 {
		ParseTimestamp_DayOutOfRange++
		return noTimestamp
	}

	if i >= n || (line[i] != ' ' && line[i] != 'T' && line[i] != '_') {
		ParseTimestamp_SpaceOperatorMismatch++
		return noTimestamp
	}
	i++

	hour, hcount := parseDigits(line, &i, 2)
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
	if line[i] != ':' && line[i] != '.' && line[i] != '-' {
		ParseTimestamp_HourSeparatorMismatch++
		ParseTimestamp_MismatchedHourSeparators = append(ParseTimestamp_MismatchedHourSeparators, line[i])
		return noTimestamp
	}
	i++

	minute, mincount := parseDigits(line, &i, 2)
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
	if line[i] != ':' && line[i] != '.' && line[i] != '-' {
		ParseTimestamp_MinuteSeparatorMismatch++
		ParseTimestamp_MismatchedMinuteSeparators = append(ParseTimestamp_MismatchedMinuteSeparators, line[i])
		return noTimestamp
	}
	i++

	second, scount := parseDigits(line, &i, 2)
	if scount == 0 {
		ParseTimestamp_NoSecond++
		return noTimestamp
	}
	if second > 59 {
		ParseTimestamp_SecondOutOfRange++
		return noTimestamp
	}

	var nsec int
	if i < n && (line[i] == '.' || line[i] == ',') {
		ParseTimestamp_HasNanos++
		i++
		var ncount int
		nsec, ncount = parseDigits(line, &i, 9)
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
		switch line[i] {
		case 'Z':
			// Already using UTC
			ParseTimestamp_UtcTimezone++
			i++
			break
		case '+', '-':
			ParseTimestamp_NonUtcTimezone++
			sign := int(',') - int(line[i])
			i++

			if i+2 > n {
				ParseTimestamp_TimezoneEarlyReturn++
				break
			}

			tzHour, hcount := parseDigits(line, &i, 2)
			if hcount == 0 {
				ParseTimestamp_NoTimezoneHour++
				break
			}
			if tzHour > 23 {
				ParseTimestamp_TimezoneHourOutOfRange++
				break
			}

			tzMin := 0
			if i < n && line[i] == ':' {
				i++
				if i+2 <= n {
					tzMin, _ = parseDigits(line, &i, 2)
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

func parseDigits(line string, i *int, maxCount int) (val, count int) {
	// Pre-calculate end boundary to avoid repeated len() calls
	end := *i + maxCount
	if end > len(line) {
		end = len(line)
	}

	// Use direct slice indexing instead of repeated bounds checking
	s := line[*i:end]
	for j := 0; j < len(s); j++ {
		c := s[j]
		if c < '0' || c > '9' {
			break
		}
		val = val*10 + int(c-'0')
		count++
	}
	*i += count
	return
}

package main

import (
	"bufio"
)

var (
	// Return the minimum time for the lines with no timestamp, so that those lines are listed first.
	// Otherwise, we could miss the correct order for the upcoming lines with timestamps.
	noTimestamp = [2]uint64{0, 0}
)

func parseLine(sourceName string, scanner *bufio.Scanner) *LogLine {
	if scanner.Scan() {
		line := scanner.Text()
		return &LogLine{
			Ordinal:    ParseTimestamp(line),
			SourceName: sourceName,
			RawLine:    line,
		}
	}
	return nil
}

func ParseTimestamp(line string) [2]uint64 {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			printErr("Error parsing timestamp: %v at line: %s\n", err, line)
		}
	}()

	// Early length check
	if len(line) < 12 {
		return noTimestamp
	}

	// Find first digit more efficiently using index of common prefixes
	i := 0
	for i < len(line) && (line[i] < '0' || line[i] > '9') {
		i++
	}
	if i+12 > len(line) {
		return noTimestamp
	}

	// Pre-allocate timezone for common case
	timeOffsetMinutes := 0

	// Parse year with fewer branches
	year, count := parseDigits(line, &i, 4)
	if count == 0 {
		return noTimestamp
	} else if count == 2 {
		if year >= 69 {
			year += 1900
		} else {
			year += 2000
		}
	} else if year < 1969 || year > 2050 {
		return noTimestamp
	}

	// Combine separator checks
	if c := line[i]; c == '-' {
		i++
	}

	// Parse remaining fields with fewer conditional branches
	month, mcount := parseDigits(line, &i, 2)
	if mcount == 0 || month < 1 || month > 12 {
		return noTimestamp
	}

	if line[i] == '-' {
		i++
	}

	day, dcount := parseDigits(line, &i, 2)
	if dcount == 0 || day < 1 || day > 31 {
		return noTimestamp
	}

	// Optimize common case of space separator
	if i >= len(line) || (line[i] != ' ' && line[i] != 'T' && line[i] != '_') {
		return noTimestamp
	}
	i++

	// Parse time components more efficiently
	hour, hcount := parseDigits(line, &i, 2)
	if hcount == 0 || hour > 23 {
		return noTimestamp
	}

	if i >= len(line) || (line[i] != ':' && line[i] != '.') {
		return noTimestamp
	}
	i++

	minute, mincount := parseDigits(line, &i, 2)
	if mincount == 0 || minute > 59 {
		return noTimestamp
	}

	if i >= len(line) || (line[i] != ':' && line[i] != '.') {
		return noTimestamp
	}
	i++

	second, scount := parseDigits(line, &i, 2)
	if scount == 0 || second > 59 {
		return noTimestamp
	}

	// Handle subseconds more efficiently
	var nsec int
	if i < len(line) && (line[i] == '.' || line[i] == ',') {
		i++
		var ncount int
		nsec, ncount = parseDigits(line, &i, 9)
		// Normalize nanoseconds in one step
		for ncount < 9 {
			nsec *= 10
			ncount++
		}
	}

	// Optimize timezone parsing
	if i < len(line) {
		switch line[i] {
		case 'Z':
			// Already using UTC
			break
		case '+', '-':
			sign := int(',') - int(line[i]) // +: -4, -: +4 for the offset
			i++

			if i+2 > len(line) {
				break
			}

			tzHour, hcount := parseDigits(line, &i, 2)
			if hcount == 0 || tzHour > 23 {
				break
			}

			tzMin := 0
			if i < len(line) && line[i] == ':' {
				i++
				if i+2 <= len(line) {
					tzMin, _ = parseDigits(line, &i, 2)
				}
			}

			if tzHour != 0 || tzMin != 0 {
				timeOffsetMinutes = sign * (tzHour*60 + tzMin)
			}
		}
	}

	// epoch nanos-like timestamp
	return ToOrdinal(year, month, day, hour, minute, second, nsec, timeOffsetMinutes)
}

func ToOrdinal(year int, month int, day int, hour int, minute int, second int, nsec int, timeOffsetMinutes int) [2]uint64 {
	offsetSign := 1
	if timeOffsetMinutes < 0 {
		offsetSign = 0
		timeOffsetMinutes = -timeOffsetMinutes
	}
	v1 := uint64(((((((((((year-1969)*12+(month-1))*31)+(day-1))*24)+hour)*60)+minute-timeOffsetMinutes)*60)+second)*1_000_000_000 + nsec)
	v2 := uint64(timeOffsetMinutes*2 + offsetSign)
	//printErr("ordinal[0] = ((((((((((%d-1969)*12+(%d-1))*31)+(%d-1))*24)+%d)*60)+%d-%d)*60)+%d) = %d\n", year, month, day, hour, minute, timeOffsetMinutes, second, v1)
	//printErr("ordinal[1] = (((%d)*1440)+%d)*2+%d = %d\n", nsec, timeOffsetMinutes, offsetSign, v2)
	return [2]uint64{v1, v2}
}

func FromOrdinal(ordinal [2]uint64) (year int, month int, day int, hour int, minute int, second int, nsec int, timeOffsetMinutes int) {
	v1 := ordinal[0]
	v2 := ordinal[1]

	offsetSign := int(v2 % 2)
	if offsetSign == 0 {
		offsetSign = -1
	}
	v2 /= 2
	timeOffsetMinutes = int(v2%1440) * offsetSign
	v2 /= 1440

	nsec = int(v1 % 1_000_000_000)
	v1 /= 1_000_000_000
	second = int(v1 % 60)
	v1 /= 60
	shiftedMinutes := int(v1 % 60)
	minute = shiftedMinutes + timeOffsetMinutes
	v1 /= 60
	hour = int(v1 % 24)
	v1 /= 24
	day = int(v1%31) + 1
	v1 /= 31
	month = int(v1%12) + 1
	v1 /= 12
	year = int(v1) + 1969
	return
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

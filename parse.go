package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"time"
)

var (
	// TimestampParseMethod is the method used to parse timestamps
	TimestampParseMethod string

	// Return the minimum time for the lines with no timestamp, so that those lines are listed first.
	// Otherwise, we could miss the correct order for the upcoming lines with timestamps.
	noTimestamp = time.Time{}

	timeFormats = []string{
		time.RFC3339,
		"2006-01-02 15:04:05,000",
		"2006-01-02 15:04:05-07:00",
		"20060102 15:04:05.000000",      // Custom format (e.g., I20250115 19:29:15.463310)
		"2006-01-02T15:04:05.000-07:00", // ISO 8601 with milliseconds and timezone
		"2006-01-02 15:04:05,000 -07:00",
		"2006-01-02T15:04:05,000-0700",
		"2006-01-02 15:04:05", // Basic date-time without milliseconds
		"06-1-2 15:04:05",     // Custom format (e.g., 25-1-15 19:11:07)
	}

	// Regular expressions for extracting timestamps in unique formats
	regexTimestampPatterns = []string{
		`((?:19|20)\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,.]\d+)`,                 // e.g., 2025-01-15 05:26:33,179
		`((?:19|20)\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,.]\d+[-+]\d{2}:?\d{2})`, // e.g., 2024-12-23T15:55:26.569+0800
		`((?:19|20)\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[-+]\d{2}:\d{2})?)`,    // e.g., 2025-01-15 19:24:08-08:00
		`((?:19|20)\d{2}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})`,                        // e.g., 2025-01-07 22:46:00
		`((?:19|20)\d{6} \d{2}:\d{2}:\d{2}\.\d{6})`,                                // e.g., 20250115 19:29:15.463310
		`(\d{2}-\d{1,2}-\d{2} \d{2}:\d{2}:\d{2})`,                                  // e.g., 25-1-15 19:11:07
	}

	timestampRegex = regexp.MustCompile(`^\D*(?:((?:19|20)?\d{2})-(\d{1,2})-(\d{1,2})|((?:19|20)\d{2})(\d{2})(\d{2}))[ _T](\d{1,2}):(\d{1,2}):(\d{1,2})(?:[,.](\d{1,9}))?(Z|([+-])(\d{2}):?(\d{2}))?`)
)

func init() {
	if TimestampParseMethod == "" {
		TimestampParseMethod = os.Getenv("TIMESTAMP_PARSE_METHOD")
		if TimestampParseMethod == "" {
			TimestampParseMethod = "manual"
		}
	}
}

func parseLine(sourceName string, scanner *bufio.Scanner) *LogLine {
	if scanner.Scan() {
		line := scanner.Text()
		return &LogLine{
			Timestamp:  ParseTimestamp(line),
			SourceName: sourceName,
			RawLine:    line,
		}
	}
	return nil
}

func ParseTimestamp(line string) time.Time {
	defer func() {
		if r := recover(); r != nil {
			err := r.(error)
			printErr("Error parsing timestamp: %v at line: %s\n", err, line)
		}
	}()
	switch TimestampParseMethod {
	case "manual":
		return manual(line)
	case "builtin":
		return builtin(line)
	case "regex":
		return regex(line)
	case "mixed":
		return mixed(line)
	default:
		panic("Unknown timestamp parsing method: " + TimestampParseMethod)
	}
}

func manual(line string) time.Time {
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
	utc := time.UTC

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
				utc = time.FixedZone("", sign*(tzHour*3600+tzMin*60))
			}
		}
	}

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

func builtin(line string) time.Time {
	// skip non digits to find the first digit
	i := 0
	n := len(line)
	for i < n && (line[i] < '0' || line[i] > '9') {
		i++
	}

	// skip allowed chars (0123456789TZ:.,+- and space) to find the end of the timestamp
	j := i
	// skip year
	for j < n && ((line[j] >= '0' && line[j] <= '9') || line[j] == '-') {
		j++
	}
	// skip date-time separator
	if j < n && (line[j] == ' ' || line[j] == 'T') {
		j++
	}
	// skip time without subsecond
	for j < n && ((line[j] >= '0' && line[j] <= '9') || line[j] == '-' || line[j] == ':') {
		j++
	}
	// skip subsecond
	if j < n && (line[j] == '.' || line[j] == ',') {
		j++
		for j < n && (line[j] >= '0' && line[j] <= '9') {
			j++
		}
	}
	// skip offset sign
	if j < n && (line[j] == 'Z') {
		j++
	} else if j < n && (line[j] == '+' || line[j] == '-') {
		j++
		// skip offset hour assuming two-digits
		j += 2
		// skip offset hour-minute separator
		if j < n && line[j] == ':' {
			j++
		}
		// skip offset minute assuming two-digits
		j += 2
	}

	//parse the timestamp
	if j > i {
		for _, format := range timeFormats {
			if ts, err := time.Parse(format, line[i:j]); err == nil {
				return ts
			}
		}
	}
	return noTimestamp
}

func regex(line string) time.Time {
	submatch := timestampRegex.FindStringSubmatch(line)
	if len(submatch) > 0 {
		year := submatch[1]
		month := submatch[2]
		day := submatch[3]
		year2 := submatch[4]
		month2 := submatch[5]
		day2 := submatch[6]
		hour := submatch[7]
		minute := submatch[8]
		second := submatch[9]
		subsecond := submatch[10]
		offset := submatch[11]
		offsetSign := submatch[12]
		offsetHour := submatch[13]
		offsetMinute := submatch[14]

		// Use alternative year, month, and day if the first set is empty
		if year == "" {
			year = year2
			month = month2
			day = day2
		}

		// If the year is 2-digits, convert it to 4-digits
		if len(year) == 2 {
			// To be consistent with Go: NN >= 69 is 19NN, otherwise 20NN
			if year >= "69" {
				year = "19" + year
			} else {
				year = "20" + year
			}
		}

		// Build a timestamp string in RFC3339Nano format
		s := fmt.Sprintf("%04s-%02s-%02sT%02s:%02s:%02s", year, month, day, hour, minute, second)

		// Add subsecond if it exists
		if subsecond != "" {
			// Pad to nanoseconds
			for len(subsecond) < 9 {
				subsecond += "0"
			}
			s += "." + subsecond
		}

		// Add timezone offset if it exists
		if offset == "" || offset == "Z" {
			s += "Z"
		} else {
			s += offsetSign + offsetHour + ":" + offsetMinute
		}

		// Parse the timestamp
		ts, err := time.Parse(time.RFC3339Nano, s)
		if err == nil {
			return ts
		}
		printErr("Error parsing timestamp: %v\n", err)
	}

	// Default to noTimestamp if no valid timestamp is found
	return noTimestamp
}

func mixed(line string) time.Time {
	// Try extracting a timestamp using regular expressions
	for _, pattern := range regexTimestampPatterns {
		if matches := regexp.MustCompile(pattern).FindStringSubmatch(line); matches != nil {
			for _, format := range timeFormats {
				if ts, err := time.Parse(format, matches[1]); err == nil {
					return ts
				}
			}
		}
	}

	// Default to noTimestamp if no valid timestamp is found
	return noTimestamp
}

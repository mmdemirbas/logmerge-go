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
	// regex:
	// `^\D*(?:(\d{2,4})-(\d{1,2})-(\d{1,2})|(\d{4})(\d{2})(\d{2}))[ _T](\d{1,2}):(\d{1,2}):(\d{1,2})(?:[,.](\d{1,9}))?(Z|([+-])(\d{2}):?(\d{2}))?`
	n := len(line)
	if n < 12 {
		return noTimestamp
	}

	// Skip non-digits
	i := 0
	for {
		c := line[i]
		if c >= '0' && c <= '9' {
			break
		}
		i++
		if i == n {
			return noTimestamp
		}
	}

	if i+12 > n {
		return noTimestamp
	}

	// Year: 20250123 or 2025-01-23 or 25-1-23
	year, count := parseDigits(line, &i, 4)
	switch {
	case count == 4:
		// Return early if not a reasonable log year
		if year < 1969 || year > 2050 {
			return noTimestamp
		}
	case count == 2:
		// To match Go's behavior
		if year < 69 {
			year += 2000
		} else {
			year += 1900
		}
	default:
		return noTimestamp
	}

	// Month: 0112 or 01-12 or 1-12
	if line[i] == '-' {
		i++
	}
	month, count := parseDigits(line, &i, 2)
	if count == 0 {
		return noTimestamp
	}

	// Day
	if line[i] == '-' {
		i++
	}
	day, count := parseDigits(line, &i, 2)
	if count == 0 {
		return noTimestamp
	}

	// Hour
	c := line[i]
	if c == ' ' || c == 'T' || c == '_' {
		i++
	}
	hour, count := parseDigits(line, &i, 2)
	if count == 0 || i+4 > n {
		return noTimestamp
	}

	// Minute
	c = line[i]
	if c == ':' || c == '.' {
		i++
	}
	minute, count := parseDigits(line, &i, 2)
	if count == 0 {
		return noTimestamp
	}

	// Second
	c = line[i]
	if c == ':' || c == '.' {
		i++
	}
	second, count := parseDigits(line, &i, 2)
	if count == 0 {
		return noTimestamp
	}

	// Subsecond
	if i == n {
		return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	}
	c = line[i]
	if c == ',' || c == '.' {
		i++
		if i == n {
			return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
		}
	}
	subsecond, count := parseDigits(line, &i, 9)
	for count < 9 {
		subsecond *= 10
		count++
	}

	if i == n {
		return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, time.UTC)
	}

	// Timezone offset
	c = line[i]
	if c == 'Z' {
		return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, time.UTC)
	}
	if c != '+' && c != '-' {
		return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, time.UTC)
	}

	var sign int
	if line[i] == '-' {
		sign = -1
	} else {
		sign = 1
	}

	i++
	if i == n {
		return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, time.UTC)
	}

	tzHour, count := parseDigits(line, &i, 2)
	if count == 0 {
		return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, time.UTC)
	}
	if i == n {
		loc := time.UTC
		if tzHour != 0 {
			loc = time.FixedZone("", sign*tzHour*3600)
		}
		return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, loc)
	}

	tzMinute := 0
	c = line[i]
	if c == ':' {
		i++
		if i == n {
			loc := time.UTC
			if tzHour != 0 {
				loc = time.FixedZone("", sign*tzHour*3600)
			}
			return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, loc)
		}
	}
	tzMinute, count = parseDigits(line, &i, 2)
	loc := time.UTC
	if tzHour != 0 || tzMinute != 0 {
		loc = time.FixedZone("", sign*tzHour*3600+sign*tzMinute*60)
	}
	return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, loc)
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

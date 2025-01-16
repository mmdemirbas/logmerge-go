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

	cachedOffsets = make(map[string]*time.Location)
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
	i := 0
	n := len(line)

	// Skip non-digits
	for i < n && (line[i] < '0' || line[i] > '9') {
		i++
	}

	// Year: 20250123 or 2025-01-23 or 25-1-23
	year, count := parseDigits(line, &i, 4)
	switch count {
	case 4:
		// Return early if not a reasonable log year
		if year <= 1900 || year >= 2100 {
			return noTimestamp
		}
	case 2:
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
	if i < n && line[i] == '-' {
		i++
	}
	month, count := parseDigits(line, &i, 2)
	if count == 0 {
		return noTimestamp
	}

	// Day
	if i < n && line[i] == '-' {
		i++
	}
	day, count := parseDigits(line, &i, 2)
	if count == 0 {
		return noTimestamp
	}

	// Hour
	if i < n && (line[i] == ' ' || line[i] == 'T' || line[i] == '_') {
		i++
	}
	hour, count := parseDigits(line, &i, 2)
	if count == 0 {
		return time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
	}

	// Minute
	if i < n && (line[i] == ':' || line[i] == '.') {
		i++
	}
	minute, count := parseDigits(line, &i, 2)
	if count == 0 {
		return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.UTC)
	}

	// Second
	if i < n && (line[i] == ':' || line[i] == '.') {
		i++
	}
	second, count := parseDigits(line, &i, 2)
	if count == 0 {
		return time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	}

	// Subsecond
	if i < n && (line[i] == ',' || line[i] == '.') {
		i++
	}
	subsecond, count := parseDigits(line, &i, 9)
	for count < 9 {
		subsecond *= 10
		count++
	}

	// Timezone offset
	loc := time.UTC
	if i < n {
		if line[i] == 'Z' {
			i++
		} else if line[i] == '+' || line[i] == '-' {
			offsetStart := i
			sign := 1
			if line[i] == '-' {
				sign = -1
			}
			i++
			tzHour, _ := parseDigits(line, &i, 2)
			tzMinute := 0
			if i < n && line[i] == ':' {
				i++
				tzMinute, _ = parseDigits(line, &i, 2)
			}
			offset := sign * (tzHour*3600 + tzMinute*60)
			if offset != 0 {
				offsetString := line[offsetStart:i]
				if loc = cachedOffsets[offsetString]; loc == nil {
					loc = time.FixedZone("", offset)
					cachedOffsets[offsetString] = loc
				}
			}
		}
	}

	return time.Date(year, time.Month(month), day, hour, minute, second, subsecond, loc)
}

func parseDigits(line string, i *int, maxCount int) (int, int) {
	val := 0
	count := 0
	n := len(line)
	for count < maxCount && *i < n && '0' <= line[*i] && line[*i] <= '9' {
		d := int(line[*i] - '0')
		val = val*10 + d
		*i++
		count++
	}
	return val, count
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
		printErr("Error parsing timestamp: %v", err)
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

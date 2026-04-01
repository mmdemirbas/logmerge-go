package logtime

import "bytes"

// TODO: Consider supporting other time formats like 1 Jan 2006; Jan 1, 2006; 01/02/2006 etc.

type ParseTimestampConfig struct {
	IgnoreTimezoneInfo      bool `yaml:"IgnoreTimezoneInfo"`
	ShortestTimestampLen    int  `yaml:"ShortestTimestampLen"`
	TimestampSearchEndIndex int  `yaml:"TimestampSearchEndIndex"`
}

// ParseTimestamp scans the first TimestampSearchEndIndex bytes of buffer for a
// recognizable timestamp pattern and returns it, or ZeroTimestamp if none found.
func ParseTimestamp(c *ParseTimestampConfig, buffer []byte) Timestamp {
	ts, _ := ParseTimestampWithEnd(c, buffer)
	return ts
}

// ParseTimestampWithEnd scans the buffer for a timestamp and returns both the
// parsed timestamp and the byte offset where the timestamp ends. The end offset
// can be used to strip the original timestamp from output.
// If no timestamp is found, or the timestamp is on a subsequent line (past a
// newline), the end offset is 0 — indicating nothing should be stripped from
// the current line.
func ParseTimestampWithEnd(c *ParseTimestampConfig, buffer []byte) (Timestamp, int) {
	n := min(len(buffer), c.TimestampSearchEndIndex)

	// Fast path: try parsing from position 0. If tryParseTimestamp succeeds,
	// the timestamp is guaranteed to be on the first line because
	// skipToFirstDigit (called internally) bails on newlines.
	timestamp, end := tryParseTimestamp(c, buffer, 0, n)
	if timestamp != ZeroTimestamp {
		return timestamp, end
	}

	// Slow path: first attempt failed. Continue scanning, but track the
	// first newline to avoid returning timestamps from subsequent lines.
	firstNewline := n
	if idx := bytes.IndexAny(buffer[:n], "\n\r"); idx >= 0 {
		firstNewline = idx
	}

	for i := end; timestamp == ZeroTimestamp && i < n; {
		timestamp, i = tryParseTimestamp(c, buffer, i, n)
		end = i
	}
	if end > firstNewline {
		if timestamp != ZeroTimestamp {
			timestamp = ZeroTimestamp
		}
		timestamp, _, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	} else if timestamp == ZeroTimestamp {
		timestamp, _, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	}
	if timestamp == ZeroTimestamp {
		return timestamp, 0
	}
	return timestamp, end
}

// ParseTimestampForStrip scans the buffer for a timestamp and returns the parsed
// timestamp plus the start and end byte offsets of the timestamp section including
// surrounding delimiters. Used when stripping timestamps from output.
func ParseTimestampForStrip(c *ParseTimestampConfig, buffer []byte) (Timestamp, int, int) {
	n := min(len(buffer), c.TimestampSearchEndIndex)

	// Fast path: try parsing from position 0.
	timestamp, end := tryParseTimestamp(c, buffer, 0, n)
	if timestamp != ZeroTimestamp {
		tsStart, _ := skipToFirstDigit(buffer, n, 0)
		prefixStart, tsEnd := computeStripBounds(buffer, n, n, tsStart, end)
		return timestamp, prefixStart, tsEnd
	}

	// Slow path: continue scanning with newline tracking.
	firstNewline := n
	if idx := bytes.IndexAny(buffer[:n], "\n\r"); idx >= 0 {
		firstNewline = idx
	}

	var lastI int
	for i := end; timestamp == ZeroTimestamp && i < n; {
		lastI = i
		timestamp, i = tryParseTimestamp(c, buffer, i, n)
		end = i
	}

	var tsStart int
	switch {
	case end > firstNewline:
		timestamp = ZeroTimestamp
		timestamp, tsStart, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	case timestamp != ZeroTimestamp:
		tsStart, _ = skipToFirstDigit(buffer, n, lastI)
	default:
		timestamp, tsStart, end = tryParseCtimeTimestamp(c, buffer, firstNewline)
	}

	if timestamp == ZeroTimestamp {
		return timestamp, 0, 0
	}
	prefixStart, tsEnd := computeStripBounds(buffer, n, firstNewline, tsStart, end)
	return timestamp, prefixStart, tsEnd
}

func computeStripBounds(buffer []byte, n int, firstNewline int, tsStart int, end int) (int, int) {
	// Strip trailing delimiters after the timestamp (up to 3 chars).
	// scanned=3 in the default case forces loop exit on next iteration.
	limit := min(n, firstNewline+1)
	tsEnd := end
	for scanned := 0; tsEnd < limit && scanned < 3; scanned++ {
		switch buffer[tsEnd] {
		case ' ', '\t', '|', ']', ')', '}', ':', ',':
			tsEnd++
		default:
			scanned = 3
		}
	}

	return scanBackwardBracket(buffer, tsStart), tsEnd
}

// scanBackwardBracket scans backward from tsStart for up to 3 opening brackets ('[' or '('),
// then verifies the bracket is preceded by whitespace. Returns the adjusted prefix start.
func scanBackwardBracket(buffer []byte, tsStart int) int {
	prefixStart := tsStart
	for back := 0; prefixStart > 0 && back < 3; back++ {
		switch buffer[prefixStart-1] {
		case '[', '(':
			prefixStart--
		default:
			back = 3
		}
	}
	if prefixStart > 0 {
		b := buffer[prefixStart-1]
		if b != ' ' && b != '\t' {
			return tsStart
		}
	}
	return prefixStart
}

func tryParseTimestamp(c *ParseTimestampConfig, buffer []byte, i int, n int) (Timestamp, int) {
	if n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, n
	}
	if ts, j := tryParseISO8601Fast(c, buffer, i, n); ts != ZeroTimestamp {
		return ts, j
	}
	return tryParseTimestampSlow(c, buffer, i, n)
}

// tryParseISO8601Fast is the hot path for "YYYY-MM-DD HH:MM:SS[.nnn][TZ]".
// Separator pattern is checked first (cheap), then digits are decoded inline.
// Returns ZeroTimestamp (not an error) when the pattern does not match at i.
func tryParseISO8601Fast(c *ParseTimestampConfig, buffer []byte, i, n int) (Timestamp, int) {
	if n < i+19 ||
		buffer[i+4] != '-' || buffer[i+7] != '-' ||
		(buffer[i+10] != ' ' && buffer[i+10] != 'T' && buffer[i+10] != '_') ||
		buffer[i+13] != ':' || buffer[i+16] != ':' {
		return ZeroTimestamp, 0
	}
	year := int(buffer[i]-'0')*1000 + int(buffer[i+1]-'0')*100 + int(buffer[i+2]-'0')*10 + int(buffer[i+3]-'0')
	month := int(buffer[i+5]-'0')*10 + int(buffer[i+6]-'0')
	day := int(buffer[i+8]-'0')*10 + int(buffer[i+9]-'0')
	hour := int(buffer[i+11]-'0')*10 + int(buffer[i+12]-'0')
	minute := int(buffer[i+14]-'0')*10 + int(buffer[i+15]-'0')
	second := int(buffer[i+17]-'0')*10 + int(buffer[i+18]-'0')
	if !isValidDateTime(year, month, day, hour, minute, second) {
		return ZeroTimestamp, 0
	}
	j := i + 19
	nsec, j := parseNsec(buffer, n, j)
	tzSign, tzHour, tzMin, j := parseTimezone(c, buffer, n, j)
	return NewTimestamp(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin), j
}

// isValidDateTime reports whether the given date/time components are in range.
func isValidDateTime(year, month, day, hour, minute, second int) bool {
	return year >= 1969 && year <= 2050 &&
		month >= 1 && month <= 12 && day >= 1 && day <= 31 &&
		hour <= 23 && minute <= 59 && second <= 59
}

// parseNsec parses optional fractional seconds (prefixed by '.' or ',') or
// space-separated milliseconds. Returns normalised nanoseconds and the next index.
func parseNsec(buffer []byte, n, j int) (nsec, nextJ int) {
	if j >= n {
		return 0, j
	}
	if buffer[j] != '.' && buffer[j] != ',' {
		return parseSpaceSeparatedMillis(buffer, n, j)
	}
	j++
	ncount := 0
	for ncount < 9 && j < n {
		d := buffer[j] - '0'
		if d > 9 {
			break
		}
		nsec = nsec*10 + int(d)
		ncount++
		j++
	}
	for ncount < 9 {
		nsec *= 10
		ncount++
	}
	return nsec, j
}

// tryParseTimestampSlow parses flexible date formats (YYYY-MM-DD, YY/MM/DD, etc.)
// with a single separator character and time in HH:MM:SS form.
func tryParseTimestampSlow(c *ParseTimestampConfig, buffer []byte, i, n int) (Timestamp, int) {
	var hitNewline bool
	i, hitNewline = skipToFirstDigit(buffer, n, i)
	if hitNewline {
		return ZeroTimestamp, i
	}
	if i >= n || n < i+c.ShortestTimestampLen {
		return ZeroTimestamp, n
	}
	// Check for newlines within the timestamp window. When i == 0 the buffer
	// starts at the current line boundary, so no newline can precede the timestamp.
	if i > 0 {
		for j := i + c.ShortestTimestampLen - 1; j >= i; j-- {
			b := buffer[j]
			if b == '\n' || b == '\r' {
				return ZeroTimestamp, j + 1
			}
		}
	}
	year, month, day, nextI, ok := parseFlexDate(buffer, n, i)
	if !ok {
		return ZeroTimestamp, nextI
	}
	hour, minute, second, nextI, ok := parseFlexTime(buffer, n, nextI)
	if !ok {
		return ZeroTimestamp, nextI
	}
	nsec, nextI := parseNsec(buffer, n, nextI)
	tzSign, tzHour, tzMin, nextI := parseTimezone(c, buffer, n, nextI)
	return NewTimestamp(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin), nextI
}

// parseFlexDate parses a date in YYYY-MM-DD, YY/MM/DD, or similar flexible format.
// On failure, failPos indicates where to resume scanning.
func parseFlexDate(buffer []byte, n, i int) (int, int, int, int, bool) {
	rawYear, count := parseDigits(buffer, n, i, 4)
	year, ok := normalizeYear(rawYear, count)
	if !ok {
		return 0, 0, 0, i + max(count, 1), false
	}
	month, nextI, ok := parseFlexMonth(buffer, n, i+count)
	if !ok {
		return 0, 0, 0, nextI, false
	}
	day, nextI, ok := parseFlexDay(buffer, n, nextI)
	if !ok {
		return 0, 0, 0, nextI, false
	}
	return year, month, day, nextI, true
}

// parseFlexMonth parses an optional separator ('-' or '/') followed by a
// 1–2 digit month number in [1, 12].
func parseFlexMonth(buffer []byte, n, i int) (month, nextI int, ok bool) {
	if i >= n {
		return 0, n, false
	}
	b := buffer[i]
	if b == '-' || b == '/' {
		i++
	}
	month, mcount := parseMax2Digits(buffer, n, i)
	if mcount == 0 {
		return 0, i + 1, false
	}
	if month > 12 || month < 1 {
		return 0, i + mcount, false
	}
	return month, i + mcount, true
}

// parseFlexDay parses an optional separator ('-' or '/') followed by a
// 1–2 digit day number in [1, 31].
func parseFlexDay(buffer []byte, n, i int) (day, nextI int, ok bool) {
	if i >= n {
		return 0, n, false
	}
	b := buffer[i]
	if b == '-' || b == '/' {
		i++
	}
	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 {
		return 0, i + 1, false
	}
	if day > 31 || day < 1 {
		return 0, i + dcount, false
	}
	return day, i + dcount, true
}

// normalizeYear converts a raw year value and digit count to a canonical 4-digit
// year. Two-digit years < 69 map to 2000–2068; ≥ 69 map to 1969–1999.
// Returns (0, false) for 0-digit or out-of-range 4-digit years.
func normalizeYear(year, count int) (int, bool) {
	switch count {
	case 0:
		return 0, false
	case 2:
		if year < 69 {
			return year + 2000, true
		}
		return year + 1900, true
	default:
		if year > 2050 || year < 1969 {
			return 0, false
		}
		return year, true
	}
}

// parseFlexTime parses "SHH:MM:SS" where S is a date-time separator
// (' ', 'T', or '_') and ':' may be replaced by '.' or '-'.
func parseFlexTime(buffer []byte, n, i int) (hour, minute, second, nextI int, ok bool) {
	i, ok = readDTSep(buffer, n, i)
	if !ok {
		return 0, 0, 0, i, false
	}
	hour, i, ok = parseTimeField(buffer, n, i, 23)
	if !ok {
		return 0, 0, 0, i + 1, false
	}
	i, ok = readTimeSep(buffer, n, i)
	if !ok {
		return 0, 0, 0, i, false
	}
	minute, i, ok = parseTimeField(buffer, n, i, 59)
	if !ok {
		return 0, 0, 0, i + 1, false
	}
	i, ok = readTimeSep(buffer, n, i)
	if !ok {
		return 0, 0, 0, i, false
	}
	second, i, ok = parseTimeField(buffer, n, i, 59)
	if !ok {
		return 0, 0, 0, i + 1, false
	}
	return hour, minute, second, i, true
}

// readDTSep reads a date-time separator (' ', 'T', or '_') at buffer[i],
// returning the index after it. Returns (i+1, false) if the byte is not a
// valid separator, or (n, false) if i is out of bounds.
func readDTSep(buffer []byte, n, i int) (int, bool) {
	if i >= n {
		return n, false
	}
	b := buffer[i]
	if b != ' ' && b != 'T' && b != '_' {
		return i + 1, false
	}
	return i + 1, true
}

// readTimeSep reads a time separator (':', '.', or '-') at buffer[i],
// returning the index after it. Returns (i+1, false) if the byte is not a
// valid separator, or (n, false) if i is out of bounds.
func readTimeSep(buffer []byte, n, i int) (int, bool) {
	if i >= n {
		return n, false
	}
	b := buffer[i]
	if b != ':' && b != '.' && b != '-' {
		return i + 1, false
	}
	return i + 1, true
}

// tryParseCtimeTimestamp scans for ctime-style timestamps:
//
//	[DayOfWeek ]Mon DD HH:MM:SS[ TZ] YYYY
//
// Examples: "Sat Mar 07 23:59:43 CST 2026", "Mar  7 23:59:43 2026"
func tryParseCtimeTimestamp(c *ParseTimestampConfig, buffer []byte, n int) (Timestamp, int, int) {
	for scanPos := 0; scanPos+15 <= n; scanPos++ {
		// Look for a month name
		month := parseMonthName(buffer, scanPos)
		if month == 0 {
			continue
		}
		// Validate boundary: month name must be at start or preceded by space
		if scanPos > 0 && buffer[scanPos-1] != ' ' && buffer[scanPos-1] != '\t' {
			continue
		}
		ts, start, end := parseCtimeFrom(c, buffer, n, scanPos, month)
		if ts != ZeroTimestamp {
			return ts, start, end
		}
	}
	return ZeroTimestamp, 0, 0
}

func parseCtimeFrom(c *ParseTimestampConfig, buffer []byte, n int, monthPos int, month int) (Timestamp, int, int) {
	i := monthPos + 3
	if i >= n || buffer[i] != ' ' {
		return ZeroTimestamp, 0, 0
	}
	day, hour, minute, second, nextI, ok := parseCtimeDayAndTime(buffer, n, i+1)
	if !ok {
		return ZeroTimestamp, 0, 0
	}
	i, ok = parseCtimeTZ(buffer, n, nextI)
	if !ok {
		return ZeroTimestamp, 0, 0
	}
	year, nextI, ok := parseCtimeYear(buffer, n, i)
	if !ok {
		return ZeroTimestamp, 0, 0
	}
	return NewTimestamp(year, month, day, hour, minute, second, 0, 0, 0, 0), ctimeTimestampStart(buffer, monthPos), nextI
}

// parseCtimeDayAndTime parses "[space]DD HH:MM:SS" — the portion after the mandatory
// space that follows the month name. The leading space is optional (handles "Mar  7").
func parseCtimeDayAndTime(buffer []byte, n, i int) (day, hour, minute, second, nextI int, ok bool) {
	if i < n && buffer[i] == ' ' { // optional space-pad for single-digit day ("Mar  7")
		i++
	}
	day, dcount := parseMax2Digits(buffer, n, i)
	if dcount == 0 || day < 1 || day > 31 {
		return 0, 0, 0, 0, i, false
	}
	i += dcount
	if i >= n || buffer[i] != ' ' {
		return 0, 0, 0, 0, i, false
	}
	hour, minute, second, nextI, ok = parseHHMMSS(buffer, n, i+1)
	return day, hour, minute, second, nextI, ok
}

// parseHHMMSS parses a HH:MM:SS time string starting at buffer[i].
func parseHHMMSS(buffer []byte, n, i int) (hour, minute, second, nextI int, ok bool) {
	hour, i, ok = parseTimeField(buffer, n, i, 23)
	if !ok {
		return 0, 0, 0, i, false
	}
	if i >= n || buffer[i] != ':' {
		return 0, 0, 0, i, false
	}
	i++
	minute, i, ok = parseTimeField(buffer, n, i, 59)
	if !ok {
		return 0, 0, 0, i, false
	}
	if i >= n || buffer[i] != ':' {
		return 0, 0, 0, i, false
	}
	i++
	second, i, ok = parseTimeField(buffer, n, i, 59)
	if !ok {
		return 0, 0, 0, i, false
	}
	return hour, minute, second, i, true
}

// parseTimeField parses up to 2 decimal digits from buffer[i:n], returning the
// value and the index after the last digit. Fails if no digits are found or the
// value exceeds maxVal.
func parseTimeField(buffer []byte, n, i, maxVal int) (val, nextI int, ok bool) {
	val, count := parseMax2Digits(buffer, n, i)
	if count == 0 || val > maxVal {
		return 0, i, false
	}
	return val, i + count, true
}

// parseCtimeTZ parses the optional timezone field in a ctime-style timestamp.
// On entry, i points to the space after the seconds field.
// On success, returns the index of the first byte of the year field.
func parseCtimeTZ(buffer []byte, n, i int) (int, bool) {
	if i >= n || buffer[i] != ' ' {
		return i, false
	}
	i++
	tzEnd := scanUppercase(buffer, n, i)
	tzLen := tzEnd - i
	if tzLen == 0 {
		return tzEnd, true // no TZ abbreviation — already at year
	}
	if tzLen < 2 || tzLen > 5 {
		return tzEnd, false // not a valid TZ abbreviation
	}
	if tzEnd >= n || buffer[tzEnd] != ' ' {
		return tzEnd, false
	}
	return tzEnd + 1, true
}

// scanUppercase returns the index of the first non-uppercase-letter byte at or
// after i, bounded by n.
func scanUppercase(buffer []byte, n, i int) int {
	for i < n && buffer[i] >= 'A' && buffer[i] <= 'Z' {
		i++
	}
	return i
}

// parseCtimeYear parses a 4-digit year in the range [1969, 2050] from buffer[i:n].
func parseCtimeYear(buffer []byte, n, i int) (year, nextI int, ok bool) {
	year, ycount := parseDigits(buffer, n, i, 4)
	if ycount != 4 || year < 1969 || year > 2050 {
		return 0, i, false
	}
	return year, i + ycount, true
}

// ctimeTimestampStart returns the start index of the ctime timestamp section,
// expanding backward to include a leading day-of-week abbreviation if present.
func ctimeTimestampStart(buffer []byte, monthPos int) int {
	if monthPos < 4 || buffer[monthPos-1] != ' ' {
		return monthPos
	}
	dow := monthPos - 4
	if !isAlpha(buffer[dow]) || !isAlpha(buffer[dow+1]) || !isAlpha(buffer[dow+2]) {
		return monthPos
	}
	if dow > 0 && buffer[dow-1] != ' ' && buffer[dow-1] != '\t' {
		return monthPos
	}
	return dow
}

// monthNameIndex maps 3-letter English month abbreviations to 1-based month numbers.
// Go optimises m[string(byteSlice)] map lookups to avoid heap allocation.
var monthNameIndex = map[string]int{
	"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4,
	"May": 5, "Jun": 6, "Jul": 7, "Aug": 8,
	"Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

func parseMonthName(buffer []byte, i int) int {
	if i+3 > len(buffer) {
		return 0
	}
	return monthNameIndex[string(buffer[i:i+3])]
}

func isAlpha(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')
}

func parseSpaceSeparatedMillis(buffer []byte, n int, i int) (nsec int, nextI int) {
	if i+1 < n && buffer[i] == ' ' {
		frac, fcount := parseDigits(buffer, n, i+1, 3)
		if fcount > 0 && (i+1+fcount >= n || buffer[i+1+fcount] < '0' || buffer[i+1+fcount] > '9') {
			nsec = frac
			nextI = i + 1 + fcount
			for fcount < 9 {
				nsec *= 10
				fcount++
			}
			return nsec, nextI
		}
	}
	return 0, i
}

func skipToFirstDigit(buffer []byte, n, i int) (int, bool) {
	for i < n {
		b := buffer[i]
		if b >= '0' && b <= '9' {
			break
		}
		i++
		if b == '\r' || b == '\n' {
			return i, true
		}
	}
	return i, false
}

func parseTimezone(c *ParseTimestampConfig, buffer []byte, n int, i int) (tzSign, tzHour, tzMin, nextI int) {
	nextI = i
	if nextI >= n {
		return 0, 0, 0, nextI
	}
	b := buffer[nextI]
	nextI++

	switch b {
	case 'Z':
		// Already using UTC
	case '+', '-':
		tzSign, tzHour, tzMin, nextI = parseOffsetTZ(c, buffer, n, nextI, b)
	}
	return tzSign, tzHour, tzMin, nextI
}

func parseOffsetTZ(c *ParseTimestampConfig, buffer []byte, n int, i int, sign byte) (tzSign, tzHour, tzMin, nextI int) {
	nextI = i
	if !c.IgnoreTimezoneInfo {
		tzSign = int(',') - int(sign)
	}
	if nextI+2 > n {
		return tzSign, 0, 0, nextI
	}
	h, hcount := parseMax2Digits(buffer, n, nextI)
	if hcount == 0 || h > 23 {
		return tzSign, 0, 0, nextI
	}
	if !c.IgnoreTimezoneInfo {
		tzHour = h
	}
	nextI += hcount
	if nextI < n && buffer[nextI] == ':' {
		nextI++
	}
	m, mcount := parseMax2Digits(buffer, n, nextI)
	if !c.IgnoreTimezoneInfo {
		tzMin = m
	}
	nextI += mcount
	return tzSign, tzHour, tzMin, nextI
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
	if i >= n {
		return 0, 0
	}
	b1 := buffer[i]
	if b1 < '0' || b1 > '9' {
		return 0, 0
	}
	v := int(b1 - '0')
	if i+1 < n {
		b2 := buffer[i+1]
		if b2 >= '0' && b2 <= '9' {
			return v*10 + int(b2-'0'), 2
		}
	}
	return v, 1
}

package logtime_test

import (
	"fmt"
	"strings"
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/testutil"
)

func TestParseTimestamp(t *testing.T) {
	tests := []struct {
		expected string
		input    string
	}{
		{"2025-01-09 20:27:27.236000000 ", "[2025-01-09 20:27:27,236] [sidecar-bg-task 3850964] [140300208650704] [metrics_ns:408] [INFO   ] Producer update metric time: 2025-01-09 20:27:27.236157"},
		{"2025-01-15 17:21:51.292000000 ", "2025-01-15 17:21:51,292:3239354(0x7fa656b3d640):ZOO_INFO@log_env@1250: Client environment:zookeeper.version=zookeeper C client 3.7.0"},
		{"2025-01-15 19:29:15.463310000 ", "I20250115 19:29:15.463310 3239941 glogger.cpp:61] conf_negotiate_server.cpp:196 [CACHE_CORE][INFO] The request node(172.16.0.33) not in cluster view"},
		{"2025-01-15 19:29:15.686245000 ", "E20250115 19:29:15.686245 3239482 glogger.cpp:71] delegation_token_mgr.cpp:116 [CACHE_CORE][ERROR] Verify token failed, token has expired, expired time is 1736940381, current time is 1736940555."},
		{"2025-01-15 05:26:33.179000000 ", "2025-01-15 05:26:33,179 | INFO | sidecar-instance-check.sh:53 | Running sidecar-instance-check.sh."},
		{"2025-01-15 19:11:07.000000000 ", "25-1-15 19:11:07[INFO][3239354 KeCallbackDestroyThreadLock:303]CallBackDestroyThreadLock completed"},
		{"2025-01-15 05:24:59.930000000 ", "2025-01-15 05:24:59,930 | INFO | sidecar-instance-check.sh:63 | SideCar Health Status normal."},
		{"2024-12-23 15:47:50.000000000 ", "2024-12-23 15:47:50 [INFO] ./install/install_vm_mrs.sh: 307  delete cache directory: /srv/BigData/data1/memarts data successfullly!"},
		{"2024-12-23 15:55:08.000000000 ", "========== 2024-12-23 15:55:08 start nodemanager by NORMAL =========="},
		{"2025-01-07 22:46:00.000000000 ", "2025-01-07 22:46:00"},
		{"2024-12-23 07:55:26.569000000 ", "2024-12-23T15:55:26.569+0800: 1.138: [GC (Allocation Failure) 2024-12-23T15:55:26.569+0800: 1.138: [ParNew: 104960K->8530K(118016K), 0.0108196 secs] 104960K->8530K(511232K), 0.0109303 secs] [Times: user=0.02 sys=0.01, real=0.01 secs]"},
		{"2025-01-02 01:16:55.000000000 ", "2025-01-02 01:16:55 GC log file created /var/log/Bigdata/yarn/nm/nodemanager-omm-20241223155524-pid154200-gc.log.4"},
		{"2025-01-15 19:23:42.042000000 ", "2025-01-15 19:23:42,042 | WARN  | ContainerLocalizer #0 | Exception encountered while connecting to the server  | Client.java:756"},
		{"2025-01-15 11:23:49.752000000 ", "2025-01-15T19:23:49.752+0800: 1.412: [GC (Allocation Failure) [PSYoungGen: 128512K->12717K(149504K)] 128512K->12725K(491008K), 0.0111485 secs] [Times: user=0.03 sys=0.01, real=0.01 secs] "},
		{"2025-01-16 03:24:08.000000000 ", "2025-01-15 19:24:08-08:00 | INFO  | [139837877704256] shard_view_mgt.cpp:109 [SHARD_VIEW][INFO] Update view success, version is 117."},
		{"2024-08-04 12:00:01.000000000 ", "<165> 2024-08-04T12:00:01Z server1 appname 12345 ID47 [exampleSDID@32473 event=\"LoginSuccess\" user=\"admin\" src_ip=\"192.168.1.10\" dst_ip=\"192.168.1.20\"] User login successful\n\n"},
		{"2024-08-04 12:00:01.000000000 ", "12345 2024-08-04T12:00:01Z server1 appname 12345 ID47 [exampleSDID@32473 event=\"LoginSuccess\" user=\"admin\" src_ip=\"192.168.1.10\" dst_ip=\"192.168.1.20\"] User login successful\n\n"},
	}

	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
		IgnoreTimezoneInfo:      false,
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			actual := ts.String()
			if strings.Compare(tt.expected, actual) != 0 {
				t.Errorf(testutil.ExpectedFormat, tt.expected, tt.expected, actual, actual)
			}
		})
	}
}

func TestParseTimestamp_BufferBoundary(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name  string
		input string
	}{
		{"year only", "2024"},
		{"year-month", "2024-01"},
		{"date only", "2024-01-15"},
		{"mid-hour", "2024-01-15 1"},
		{"hour:minute no seconds", "2024-01-15 10:30"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			testutil.AssertEquals(t, ZeroTimestamp, ts)
		})
	}
}

func TestParseTimestamp_TwoDigitYearBoundary(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		year     string
		expected string
	}{
		{"68", "2068-01-15 10:30:00.000000000 "},
		{"00", "2000-01-15 10:30:00.000000000 "},
		{"99", "1999-01-15 10:30:00.000000000 "},
	}

	for _, tt := range tests {
		t.Run("year="+tt.year, func(t *testing.T) {
			input := fmt.Sprintf("%s-01-15 10:30:00", tt.year)
			ts := ParseTimestamp(c, []byte(input))
			testutil.AssertEquals(t, tt.expected, ts.String())
		})
	}

	// Year 69 maps to 1969, which is before Unix epoch. The parser accepts it
	// but the uint64 nanosecond representation wraps around, producing garbled
	// output from String(). Verify the parser does not panic.
	t.Run("year=69 does not panic", func(t *testing.T) {
		input := "69-01-15 10:30:00"
		ts := ParseTimestamp(c, []byte(input))
		testutil.AssertNotEquals(t, ZeroTimestamp, ts)
	})
}

func TestParseTimestamp_TimezoneEdgeCases(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name     string
		suffix   string
		expected string
	}{
		{"+08:00 with colon", "+08:00", "2024-01-15 02:30:00.000000000 "},
		{"+0800 without colon", "+0800", "2024-01-15 02:30:00.000000000 "},
		{"-05:30 negative with colon", "-05:30", "2024-01-15 16:00:00.000000000 "},
		{"Z UTC", "Z", "2024-01-15 10:30:00.000000000 "},
		{"+00:00 explicit UTC", "+00:00", "2024-01-15 10:30:00.000000000 "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := "2024-01-15T10:30:00" + tt.suffix
			ts := ParseTimestamp(c, []byte(input))
			testutil.AssertEquals(t, tt.expected, ts.String())
		})
	}

	t.Run("IgnoreTimezoneInfo=true", func(t *testing.T) {
		cIgnore := &ParseTimestampConfig{
			ShortestTimestampLen:    15,
			TimestampSearchEndIndex: 250,
			IgnoreTimezoneInfo:      true,
		}
		input := "2024-01-15T10:30:00+08:00"
		ts := ParseTimestamp(cIgnore, []byte(input))
		testutil.AssertEquals(t, "2024-01-15 10:30:00.000000000 ", ts.String())
	})
}

func TestParseTimestamp_FractionalSeconds(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name     string
		frac     string
		expected string
	}{
		{".1", ".1", "2024-01-15 10:30:00.100000000 "},
		{".12", ".12", "2024-01-15 10:30:00.120000000 "},
		{".123456789", ".123456789", "2024-01-15 10:30:00.123456789 "},
		{",123 comma", ",123", "2024-01-15 10:30:00.123000000 "},
		{".0", ".0", "2024-01-15 10:30:00.000000000 "},
		{".000000001", ".000000001", "2024-01-15 10:30:00.000000001 "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := "2024-01-15 10:30:00" + tt.frac
			ts := ParseTimestamp(c, []byte(input))
			testutil.AssertEquals(t, tt.expected, ts.String())
		})
	}
}

func TestParseTimestamp_NoTimestamp(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name  string
		input string
	}{
		{"empty buffer", ""},
		{"only text", "hello world"},
		{"only newlines", "\n\n\n"},
		{"invalid number sequence", "99999"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			testutil.AssertEquals(t, ZeroTimestamp, ts)
		})
	}
}

func TestParseTimestamp_ShortTimestampLen(t *testing.T) {
	tests := []struct {
		name        string
		shortestLen int
		input       string
		expectZero  bool
	}{
		{"len=10 valid timestamp", 10, "2024-01-15 10:30:00", false},
		{"len=10 short input", 10, "2024-01-15", true},
		{"len=19 valid timestamp", 19, "2024-01-15 10:30:00", false},
		{"len=20 with 19-char input", 20, "2024-01-15 10:30:00", true},
		{"len=1 valid timestamp", 1, "2024-01-15 10:30:00", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &ParseTimestampConfig{
				ShortestTimestampLen:    tt.shortestLen,
				TimestampSearchEndIndex: 250,
			}
			ts := ParseTimestamp(c, []byte(tt.input))
			if tt.expectZero {
				testutil.AssertEquals(t, ZeroTimestamp, ts)
			} else {
				testutil.AssertNotEquals(t, ZeroTimestamp, ts)
			}
		})
	}
}

func TestParseTimestamp_InvalidDateValues(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name  string
		input string
	}{
		{"month 13", "2024-13-01 10:00:00"},
		{"month 0", "2024-00-01 10:00:00"},
		{"day 32", "2024-01-32 10:00:00"},
		{"day 0", "2024-01-00 10:00:00"},
		{"hour 24", "2024-01-15 24:00:00"},
		{"minute 60", "2024-01-15 10:60:00"},
		{"second 60", "2024-01-15 10:00:60"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			testutil.AssertEquals(t, ZeroTimestamp, ts)
		})
	}
}

func TestParseTimestamp_MultipleTimestampsInLine(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			"text before timestamp",
			"ERROR at 2024-01-15 10:30:00 something",
			"2024-01-15 10:30:00.000000000 ",
		},
		{
			"digits prefix before timestamp",
			"[12345] 2024-01-15 10:30:00",
			"2024-01-15 10:30:00.000000000 ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			testutil.AssertNotEquals(t, ZeroTimestamp, ts)
			testutil.AssertEquals(t, tt.expected, ts.String())
		})
	}
}

func TestParseTimestamp_RepeatedParsingInLine(t *testing.T) {
	// "port 8080" has "8080" which looks like a year candidate but is > 2050.
	// The parser should skip past it and find the real timestamp.
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	input := "port 8080 at 2024-01-15 10:30:00 done"
	ts := ParseTimestamp(c, []byte(input))
	testutil.AssertNotEquals(t, ZeroTimestamp, ts)
	testutil.AssertEquals(t, "2024-01-15 10:30:00.000000000 ", ts.String())
}

func TestParseTimestamp_TimestampWithMicroseconds(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name     string
		input    string
		expected string
		nsec     int
	}{
		{
			name:     "microsecond precision",
			input:    "2024-01-15 10:30:00.123456",
			expected: "2024-01-15 10:30:00.123456000 ",
			nsec:     123456000,
		},
		{
			name:     "single fractional digit",
			input:    "2024-01-15 10:30:00.1",
			expected: "2024-01-15 10:30:00.100000000 ",
			nsec:     100000000,
		},
		{
			name:     "six fractional digits with leading zeros",
			input:    "2024-01-15 10:30:00.000001",
			expected: "2024-01-15 10:30:00.000001000 ",
			nsec:     1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			testutil.AssertNotEquals(t, ZeroTimestamp, ts)
			testutil.AssertEquals(t, tt.expected, ts.String())
		})
	}
}

func TestParseTimestamp_AdjacentTimestamps(t *testing.T) {
	// Two timestamps back-to-back with no separator.
	// The parser should find the first timestamp (10:30:00).
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	input := "2024-01-15 10:30:002024-01-15 11:00:00"
	ts := ParseTimestamp(c, []byte(input))
	testutil.AssertNotEquals(t, ZeroTimestamp, ts)

	// The first timestamp should be parsed. The "2024" after "00" would be
	// consumed as fractional seconds or ignored.
	got := ts.String()
	// The parser sees "00" then "2024" — after parsing seconds "00", it checks
	// for '.' or ',' for fractional seconds. The next char is '2' which is neither,
	// so no fractional seconds. Then it checks for timezone. '2' is not Z/+/-.
	// So the timestamp should be 10:30:00 with no fractional part.
	if !strings.HasPrefix(got, "2024-01-15 10:30:00") {
		t.Errorf("expected first timestamp 10:30:00, got: %s", got)
	}
}

func TestParseTimestamp_YearOutOfRange(t *testing.T) {
	c := &ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	tests := []struct {
		name       string
		input      string
		expectZero bool
	}{
		{"year 2051 above max", "2051-01-15 10:30:00", true},
		{"year 1968 below min", "1968-01-15 10:30:00", true},
		{"year 2050 at max", "2050-01-15 10:30:00", false},
		{"year 1969 at min", "1969-01-15 10:30:00", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ts := ParseTimestamp(c, []byte(tt.input))
			if tt.expectZero {
				testutil.AssertEquals(t, ZeroTimestamp, ts)
			} else {
				testutil.AssertNotEquals(t, ZeroTimestamp, ts)
			}
		})
	}
}

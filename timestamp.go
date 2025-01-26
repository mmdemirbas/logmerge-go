package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"time"
)

// TODO: Consider caching epoch days for each year and if the year is leap year

// TODO: Consider bitwise operation to multiply with 1e9 = 2^9 * 5^9 => *5 = x << 2 + x

const (
	ZeroTimestamp = Timestamp(0)

	secondsPerMinute = 60
	secondsPerHour   = 60 * 60
	secondsPerDay    = 60 * 60 * 24
	daysFrom1970     = 1969*365 + 1969/4 - 1969/100 + 1969/400
)

var daysAfter = [13]int{
	0,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31 + 28 + 31,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31 + 28,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31 + 31,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30 + 31,
	daysFrom1970 + 1 + 31 + 30 + 31 + 30,
	daysFrom1970 + 1 + 31 + 30 + 31,
	daysFrom1970 + 1 + 31 + 30,
	daysFrom1970 + 1 + 31,
}

type Timestamp uint64

func NewTimestamp(y, M, d, H, m, s, S, tzSgn, tzH, tzM int) Timestamp {
	y4 := y >> 2
	ed := y*365 + y4 - y4/25 + (y4>>2)/25 - daysAfter[M] + d
	if y&0x03 == 0 && M <= 2 && (y4&0x03 == 0 || y4%25 != 0) {
		ed--
	}

	return Timestamp(uint64(ed*secondsPerDay+(H-tzSgn*tzH)*secondsPerHour+(m-tzSgn*tzM)*secondsPerMinute+s)*1e9 + uint64(S))
}

func (t Timestamp) MarshalYAML() (interface{}, error) {
	return time.Unix(0, int64(t)).UTC().Format(time.RFC3339Nano), nil
}

func (t *Timestamp) UnmarshalYAML(value *yaml.Node) error {
	var timeString string
	err := value.Decode(&timeString)
	if err != nil {
		return fmt.Errorf("failed to decode Timestamp: %w", err)
	}

	ts, err := time.Parse(time.RFC3339Nano, timeString)
	if err != nil {
		return fmt.Errorf("failed to parse time string <%s>: %w", timeString, err)
	}

	*t = Timestamp(ts.UnixNano())
	return nil
}

// TODO: Remove need to these arrays and simplify the String method
var nonLeapMonthDays = []int{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365, 396}
var leapMonthDays = []int{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366, 397}

var zeroString = []byte("1970-01-01 00:00:00.000000000 ")
var timestampStringBuffer = make([]byte, 30)

func (t Timestamp) FormatAsString() string {
	return string(t.FormatAsBytes())
}

func (t Timestamp) FormatAsBytes() []byte {
	if t == 0 {
		return zeroString
	}

	v := uint64(t)

	// Performance-optimized way of formatting time as yyyy-MM-dd HH:mm:ss.SSSSSSSSS
	timestampStringBuffer[29] = ' '
	timestampStringBuffer[28] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[27] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[26] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[25] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[24] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[23] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[22] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[21] = byte('0' + v%10)
	v /= 10
	timestampStringBuffer[20] = byte('0' + v%10)
	v /= 10
	sec := v % 60
	v /= 60
	timestampStringBuffer[19] = '.'
	timestampStringBuffer[18] = byte('0' + sec%10)
	timestampStringBuffer[17] = byte('0' + sec/10)
	m := v % 60
	v /= 60
	timestampStringBuffer[16] = ':'
	timestampStringBuffer[15] = byte('0' + m%10)
	timestampStringBuffer[14] = byte('0' + m/10)
	hour := v % 24
	v /= 24
	timestampStringBuffer[13] = ':'
	timestampStringBuffer[12] = byte('0' + hour%10)
	timestampStringBuffer[11] = byte('0' + hour/10)
	timestampStringBuffer[10] = ' '

	year := (v / 366) + 1970
	pastYear := year - 1
	epochDays := (pastYear*365 + pastYear/4 - pastYear/100 + pastYear/400) - daysFrom1970
	leapYear := year%4 == 0 && (year%100 != 0 || year%400 == 0)

	var daysInYear int
	if leapYear {
		daysInYear = 366
	} else {
		daysInYear = 365
	}

	dayOfYear := int(v - epochDays)
	if dayOfYear >= daysInYear {
		dayOfYear -= daysInYear
		year++
		leapYear = year%4 == 0 && (year%100 != 0 || year%400 == 0)
	}

	var monthDays []int
	if leapYear {
		monthDays = leapMonthDays
	} else {
		monthDays = nonLeapMonthDays
	}

	month := dayOfYear / 31
	if monthDays[month+1] <= dayOfYear {
		month++
	}

	day := dayOfYear - monthDays[month] + 1
	month++
	timestampStringBuffer[9] = byte('0' + day%10)
	timestampStringBuffer[8] = byte('0' + day/10)
	timestampStringBuffer[7] = '-'
	timestampStringBuffer[6] = byte('0' + month%10)
	timestampStringBuffer[5] = byte('0' + month/10)
	timestampStringBuffer[4] = '-'
	timestampStringBuffer[3] = byte('0' + year%10)
	year /= 10
	timestampStringBuffer[2] = byte('0' + year%10)
	year /= 10
	timestampStringBuffer[1] = byte('0' + year%10)
	timestampStringBuffer[0] = byte('0' + year/10)

	return timestampStringBuffer
}

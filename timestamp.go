package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"time"
)

// TODO: Consider bitwise operation to multiply with 1e9 = 2^9 * 5^9 => *5 = x << 2 + x

type Timestamp uint64

const (
	ZeroTimestamp = Timestamp(0)
	daysFrom1970  = 1969*365 + 1969/4 - 1969/100 + 1969/400
)

// region: NewTimestamp

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

func NewTimestamp(y, M, d, H, m, s, ns, tzSgn, tzH, tzM int) Timestamp {
	y4 := y >> 2
	ed := y*365 + y4 - y4/25 + (y4>>2)/25 - daysAfter[M] + d
	if y&0x03 == 0 && M <= 2 && (y4&0x03 == 0 || y4%25 != 0) {
		ed--
	}

	return Timestamp(uint64(ed*86400+(H-tzSgn*tzH)*3600+(m-tzSgn*tzM)*60+s)*1e9 + uint64(ns))
}

// endregion: NewTimestamp

// region: String

var timestampStringBuffer = []byte("1970-01-01 00:00:00.000000000 ")
var extraDaysToMonthDay = make([]int, 1+365+366)

const extraDaysToMonthDayShift = 4
const extraDaysToMonthMonthMask = 1<<extraDaysToMonthDayShift - 1

func init() {
	monthDayCounts := []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	i := 1
	j := 366
	for m := 12; m > 0; m-- {
		monthDayCount := monthDayCounts[m-1]
		if m == 2 {
			extraDaysToMonthDay[j] = 29<<extraDaysToMonthDayShift | 2
			j++
		}
		for d := monthDayCount; d > 0; d-- {
			extraDaysToMonthDay[i] = d<<extraDaysToMonthDayShift | m
			extraDaysToMonthDay[j] = d<<extraDaysToMonthDayShift | m
			i++
			j++
		}
	}
}

func (t Timestamp) String() string {
	return string(t.FormatAsBytes())
}

func (t Timestamp) FormatAsBytes() []byte {
	sec := t / 1_000_000_000
	s := sec % 60
	m := (sec / 60) % 60
	h := (sec / 3600) % 24
	days := sec / 86400

	year := (days / 365) + 1970
	y4 := year >> 2

	extraDaysIndex := (year*365 + y4 - y4/25 + (y4>>2)/25) - daysFrom1970 - days
	leapYear := year&0x03 == 0 && (y4&0x03 == 0 || y4%25 != 0)
	if leapYear && extraDaysIndex > 366 {
		extraDaysIndex-- // match correct indexes (-366+365)
		year--
	} else if leapYear {
		extraDaysIndex += 365 // match correct indexes
	} else if extraDaysIndex > 365 {
		year--
		y4 = year >> 2
		if year&0x03 != 0 || (y4&0x03 != 0 && y4%25 == 0) {
			extraDaysIndex -= 365 // match correct indexes
		}
	}

	monthDay := extraDaysToMonthDay[extraDaysIndex]
	day := monthDay >> extraDaysToMonthDayShift
	month := monthDay & extraDaysToMonthMonthMask

	b := timestampStringBuffer

	b[0] = byte('0' + (year/1000)%10)
	b[1] = byte('0' + (year/100)%10)
	b[2] = byte('0' + (year/10)%10)
	b[3] = byte('0' + (year)%10)

	b[5] = byte('0' + (month/10)%10)
	b[6] = byte('0' + (month)%10)

	b[8] = byte('0' + (day/10)%10)
	b[9] = byte('0' + (day)%10)

	b[11] = byte('0' + (h/10)%10)
	b[12] = byte('0' + (h)%10)

	b[14] = byte('0' + (m/10)%10)
	b[15] = byte('0' + (m)%10)

	b[17] = byte('0' + (s/10)%10)
	b[18] = byte('0' + (s)%10)

	b[20] = byte('0' + ((t / 100_000_000) % 10))
	b[21] = byte('0' + ((t / 10_000_000) % 10))
	b[22] = byte('0' + ((t / 1_000_000) % 10))
	b[23] = byte('0' + ((t / 100_000) % 10))
	b[24] = byte('0' + ((t / 10_000) % 10))
	b[25] = byte('0' + ((t / 1_000) % 10))
	b[26] = byte('0' + ((t / 100) % 10))
	b[27] = byte('0' + ((t / 10) % 10))
	b[28] = byte('0' + ((t) % 10))

	return b
}

// endregion: String

// region: YAML

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

// endregion: YAML

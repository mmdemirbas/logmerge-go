package main

import (
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
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
	31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31 + 28 + 31,
	31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31 + 28,
	31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
	31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
	31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
	31 + 30 + 31 + 30 + 31 + 31 + 30,
	31 + 30 + 31 + 30 + 31 + 31,
	31 + 30 + 31 + 30 + 31,
	31 + 30 + 31 + 30,
	31 + 30 + 31,
	31 + 30,
	31,
}

func NewTimestamp(y, M, d, H, m, s, ns, tzSgn, tzH, tzM int) Timestamp {
	leapDay := -isLeapYear(y) & ((M - 3) >> 31)
	ed := epochDaysIncludingYear(y) - daysFrom1970 - 1 - daysAfter[M] + d - leapDay
	return Timestamp(uint64(ed*86400+(H-tzSgn*tzH)*3600+(m-tzSgn*tzM)*60+s)*1e9 + uint64(ns))
}

// endregion: NewTimestamp

// region: String

const timestampStringTemplate = "1970-01-01 00:00:00.000000000 "

var extraDaysToMonthDay = make([]int, 1+365+366)

const extraDaysToMonthDayShift = 4
const extraDaysToMonthMonthMask = 1<<extraDaysToMonthDayShift - 1
const extraDaysToMonthLeapYearIndexShift = 365

func init() {
	monthDayCounts := []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	i := 1
	j := i + extraDaysToMonthLeapYearIndexShift
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
	var buf [30]byte
	t.FormatTo(buf[:])
	return string(buf[:])
}

func (t Timestamp) FormatTo(b []byte) {
	nano := int64(t)

	sec := int(nano / 1_000_000_000)
	s := sec % 60
	m := (sec / 60) % 60
	h := (sec / 3600) % 24
	days := sec / 86400

	// Guess the year
	year := (days / 365) + 1970

	// Calculate the epoch days based on the guessed year
	extraDays := epochDaysIncludingYear(year) - daysFrom1970 - days

	// daysInYear = isLeapYear(year) ? 366 : 365
	daysInYear := 365 - isLeapYear(year)

	// daysInYear < extraDays => tooMuchDays, we need to decrement the year
	tooMuchDays := (daysInYear - extraDays) >> 31

	// if tooMuchDays, extraDays -= daysInYear
	extraDays -= tooMuchDays & daysInYear

	// if tooMuchDays, year--
	year -= tooMuchDays & 1

	// if isLeapYear(year), extraDays += extraDaysToMonthLeapYearIndexShift
	// Note that we need to re-calculate isLeapYear here since year might have changed
	extraDays += isLeapYear(year) & extraDaysToMonthLeapYearIndexShift

	monthDay := extraDaysToMonthDay[extraDays]
	day := monthDay >> extraDaysToMonthDayShift
	month := monthDay & extraDaysToMonthMonthMask

	copy(b, timestampStringTemplate)

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

	b[20] = byte('0' + ((nano / 100_000_000) % 10))
	b[21] = byte('0' + ((nano / 10_000_000) % 10))
	b[22] = byte('0' + ((nano / 1_000_000) % 10))
	b[23] = byte('0' + ((nano / 100_000) % 10))
	b[24] = byte('0' + ((nano / 10_000) % 10))
	b[25] = byte('0' + ((nano / 1_000) % 10))
	b[26] = byte('0' + ((nano / 100) % 10))
	b[27] = byte('0' + ((nano / 10) % 10))
	b[28] = byte('0' + ((nano) % 10))
}

func epochDaysIncludingYear(y int) int {
	y4 := y >> 2
	y16 := y >> 4
	return y*365 + y4 - y4/25 + y16/25
}

func isLeapYear(y int) int {
	if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
		return -1
	}
	return 0
}

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

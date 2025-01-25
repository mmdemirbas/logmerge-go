package main

import (
	"fmt"
	"os"
)

// TODO: Try inlining the constants and arrays

// TODO: Consider caching

// TODO: Use uint64

// TODO: Consider calculating backwards instead of forwards (calculate end of year then subtract days)

const (
	secondsPerMinute = 60
	secondsPerHour   = 60 * 60
	secondsPerDay    = 60 * 60 * 24
	daysFrom1970     = 1969*365 + 1969/4 - 1969/100 + 1969/400
)

var daysBefore = [...]int{
	-daysFrom1970 - 1 + 0,
	-daysFrom1970 - 1 + 31,
	-daysFrom1970 - 1 + 31 + 28,
	-daysFrom1970 - 1 + 31 + 28 + 31,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30 + 31,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30 + 31 + 31,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30,
	-daysFrom1970 - 1 + 31 + 28 + 31 + 30 + 31 + 30 + 31 + 31 + 30 + 31 + 30 + 31,
}

var nonLeapMonthDays = []int{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365, 396}
var leapMonthDays = []int{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366, 397}

type MyTime uint64

func NewMyTime(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin int) MyTime {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "NewMyTime: Recovered from panic: %v. year=%d, month=%d, day=%d, hour=%d, minute=%d, second=%d, nsec=%d, tzSign=%d, tzHour=%d, tzMin=%d\n", r, year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin)
		}
	}()

	startTime := MeasureStart("NewMyTime")

	epochNSec := method1(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin)

	NewMyTimeMetric.MeasureSince(startTime)
	return epochNSec
}

func method1(year, month, day, hour, minute, second, nsec, tzSign, tzHour, tzMin int) MyTime {
	y := year - 1
	y4 := y >> 2

	m := month - 1
	d := y*365 + y4 - y4/25 + (y4>>2)/25 + daysBefore[m] + day

	if year&0x03 == 0 && m > 1 {
		yy4 := year >> 2
		if yy4%25 != 0 || yy4&0x03 == 0 {
			d++
		}
	}

	return MyTime(uint64(d*secondsPerDay+(hour-tzSign*tzHour)*secondsPerHour+(minute-tzSign*tzMin)*secondsPerMinute+second)*1e9 + uint64(nsec))
}

func (t MyTime) String() string {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "MyTime.String: Recovered from panic: %v. t=%d\n", r, uint64(t))
		}
	}()

	startTime := MeasureStart("MyTime.String")
	v := uint64(t)
	if v == 0 {
		result := "1970-01-01 00:00:00.000000000 "
		MyTimeStringMetric.MeasureSince(startTime)
		return result
	}

	nsec := v % 1_000_000_000
	v /= 1_000_000_000

	sec := v % 60
	v /= 60

	m := v % 60
	v /= 60

	hour := v % 24
	v /= 24

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

	day := dayOfYear - monthDays[month]
	result := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%09d ", year, month+1, day+1, hour, m, sec, nsec)

	MyTimeStringMetric.MeasureSince(startTime)
	return result
}

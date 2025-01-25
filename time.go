package main

import (
	"fmt"
	"os"
)

// TODO: Consider caching epoch days for each year and if the year is leap year

// TODO: Consider bitwise operation to multiply with 1e9 = 2^9 * 5^9 => *5 = x << 2 + x

const (
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

type MyTime uint64

func NewMyTime(y, M, d, H, m, s, S, tzSgn, tzH, tzM int) MyTime {
	startTime := MeasureStart("NewMyTime")

	y4 := y >> 2
	ed := y*365 + y4 - y4/25 + (y4>>2)/25 - daysAfter[M] + d
	if y&0x03 == 0 && M <= 2 && (y4&0x03 == 0 || y4%25 != 0) {
		ed--
	}

	tm := MyTime(uint64(ed*secondsPerDay+(H-tzSgn*tzH)*secondsPerHour+(m-tzSgn*tzM)*secondsPerMinute+s)*1e9 + uint64(S))

	NewMyTimeMetric.MeasureSince(startTime)
	return tm
}

// TODO: Remove need to these arrays and simplify the String method
var nonLeapMonthDays = []int{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365, 396}
var leapMonthDays = []int{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366, 397}

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

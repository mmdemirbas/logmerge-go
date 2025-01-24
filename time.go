package main

import "fmt"

const epochDaysUntil1970 = 719527

// TODO: Consider optimizing monthDays by using a single array maybe. Research better methods.
var nonLeapMonthDays = []int{0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365, 396}
var leapMonthDays = []int{0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366, 397}

type MyTime int64

func NewMyTime(year int, month int, day int, hour int, minute int, second int, nsec int, tzSign int, tzHour int, tzMin int) MyTime {
	startTime := MeasureStart("NewMyTime")
	pastYear := year - 1
	epochDay := year*365 + nonLeapMonthDays[month-1] + (day - 1) + pastYear/4 - pastYear/100 + pastYear/400 - epochDaysUntil1970
	if month > 2 && (year%4 == 0 && (year%100 != 0 || year%400 == 0)) {
		epochDay++
	}

	timeOffsetMinutes := tzSign * (tzHour*60 + tzMin)
	// TODO: Consider optimizing this by distributing the multiplication
	result := MyTime((((epochDay*24+hour)*60+minute-timeOffsetMinutes)*60+second)*1_000_000_000 + nsec)
	NewMyTimeMetric.MeasureSince(startTime)
	return result
}

func (t MyTime) String() string {
	startTime := MeasureStart("MyTime.String")
	v := int64(t)
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
	epochDays := (year*365 + pastYear/4 - pastYear/100 + pastYear/400) - epochDaysUntil1970
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

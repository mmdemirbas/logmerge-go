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

var extraDaysToMonthDay = make([]int, 1+365+366)

func init() {
	monthDayCounts := []int{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	i := 1
	j := 366
	for m := 12; m > 0; m-- {
		monthDayCount := monthDayCounts[m-1]
		if m == 2 {
			extraDaysToMonthDay[j] = 29<<8 | 2
			j++
		}
		for d := monthDayCount; d > 0; d-- {
			extraDaysToMonthDay[i] = d<<8 | m
			extraDaysToMonthDay[j] = d<<8 | m
			i++
			j++
		}
	}
}

type Timestamp uint64

func NewTimestamp(y, M, d, H, m, s, ns, tzSgn, tzH, tzM int) Timestamp {
	y4 := y >> 2
	ed := y*365 + y4 - y4/25 + (y4>>2)/25 - daysAfter[M] + d
	if y&0x03 == 0 && M <= 2 && (y4&0x03 == 0 || y4%25 != 0) {
		ed--
	}

	return Timestamp(uint64(ed*secondsPerDay+(H-tzSgn*tzH)*secondsPerHour+(m-tzSgn*tzM)*secondsPerMinute+s)*1e9 + uint64(ns))
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

var timestampStringBuffer = []byte("1970-01-01 00:00:00.000000000 ")

func (t Timestamp) String() string {
	return string(t.FormatAsBytes())
}

func (t Timestamp) FormatAsBytes() []byte {
	startTime := GlobalMetricsTree.MeasureStart("Timestamp.String/init")
	GlobalMetricsTree.MeasureSince(startTime)

	nano := uint64(t)
	ns := nano % 1_000_000_000
	sec := nano / 1_000_000_000

	startTime = GlobalMetricsTree.MeasureStart("Timestamp.String/dhms")
	s := sec % 60
	m := (sec / 60) % 60
	h := (sec / 3600) % 24
	days := sec / 86400
	GlobalMetricsTree.MeasureSince(startTime)

	startTime = GlobalMetricsTree.MeasureStart("Timestamp.String/step1")
	year := (days / 365) + 1970
	y4 := year >> 2
	epochDays := (year*365 + y4 - y4/25 + (y4>>2)/25) - daysFrom1970
	leapYear := year&0x03 == 0 && (y4&0x03 == 0 || y4%25 != 0)
	GlobalMetricsTree.MeasureSince(startTime)

	startTime = GlobalMetricsTree.MeasureStart("Timestamp.String/step2")
	extraDays := int(epochDays - days)
	if extraDays > 366 || (!leapYear && extraDays == 366) {
		extraDays -= 365
		if leapYear {
			extraDays--
		}
		year--
		y4 = year >> 2
		leapYear = !leapYear && year&0x03 == 0 && (y4&0x03 == 0 || y4%25 != 0)
	}
	if leapYear {
		extraDays += 365
	}
	GlobalMetricsTree.MeasureSince(startTime)

	startTime = GlobalMetricsTree.MeasureStart("Timestamp.String/step3")
	monthDay := extraDaysToMonthDay[extraDays]
	month := monthDay & 0xF
	day := monthDay >> 8
	GlobalMetricsTree.MeasureSince(startTime)

	startTime = GlobalMetricsTree.MeasureStart("Timestamp.String/byte[]")
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
	GlobalMetricsTree.MeasureSince(startTime)

	startTime = GlobalMetricsTree.MeasureStart("Timestamp.String/byte[]-loop")
	tmp := ns
	for i := 28; i >= 20; i-- {
		b[i] = byte('0' + (tmp % 10))
		tmp /= 10
	}
	GlobalMetricsTree.MeasureSince(startTime)

	return b
}

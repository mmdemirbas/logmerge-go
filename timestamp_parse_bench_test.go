package main_test

import (
	"testing"
)

var (
	GlobalVal   int
	GlobalCount int
)

// Your current bitwise implementation
func parseMax2DigitsBitwise(buffer []byte, n int, i int) (int, int) {
	if i+1 < n {
		b1 := int(buffer[i])
		b2 := int(buffer[i+1])

		isDigit1 := ((47 - b1) >> 31) & ((b1 - 58) >> 31)
		isDigit2 := ((47 - b2) >> 31) & ((b2 - 58) >> 31)

		oneDigit := isDigit1 & ^isDigit2
		twoDigits := isDigit1 & isDigit2

		return twoDigits&(10*b1+b2-528) | oneDigit&(b1-48), twoDigits&2 | oneDigit&1
	}
	if i < n {
		b1 := int(buffer[i])
		isDigit := ((47 - b1) >> 31) & ((b1 - 58) >> 31)
		return isDigit&(b1-48) | ^isDigit, isDigit
	}
	return 0, 0
}

// Standard branching implementation
func parseMax2DigitsBranch(buffer []byte, n int, i int) (int, int) {
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

func BenchmarkParseMax2Digits_Bitwise(b *testing.B) {
	buf := []byte("12:34:56")
	n := len(buf)
	var val, count int
	for i := 0; i < b.N; i++ {
		val, count = parseMax2DigitsBitwise(buf, n, 0)
	}
	GlobalVal, GlobalCount = val, count
}

func BenchmarkParseMax2Digits_Branch(b *testing.B) {
	buf := []byte("12:34:56")
	n := len(buf)
	var val, count int
	for i := 0; i < b.N; i++ {
		val, count = parseMax2DigitsBranch(buf, n, 0)
	}
	GlobalVal, GlobalCount = val, count
}

package logmerge_test

import (
	"testing"
)

var resultLeap int

func isLeapYearBitwise(y int) int {
	y4 := y >> 2
	return ((y&0x03 - 1) & ((y4&0x03 - 1) | (^(y4%25 - 1)))) >> 31
}

func isLeapYearBranch(y int) int {
	if y%4 == 0 && (y%100 != 0 || y%400 == 0) {
		return 1
	}
	return 0
}

func BenchmarkIsLeapYear_Bitwise(b *testing.B) {
	var r int
	for i := 0; i < b.N; i++ {
		// Test across a repeating 100-year span to simulate real-world variance
		r = isLeapYearBitwise(1970 + (i % 100))
	}
	resultLeap = r
}

func BenchmarkIsLeapYear_Branch(b *testing.B) {
	var r int
	for i := 0; i < b.N; i++ {
		r = isLeapYearBranch(1970 + (i % 100))
	}
	resultLeap = r
}

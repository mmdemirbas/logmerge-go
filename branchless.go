package main

func FastMin(a, b int) int {
	return a + ((b - a) & ((b - a) >> 31))
}

func FastMax(a, b int) int {
	return a - ((a - b) & ((a - b) >> 31))
}

func FastMin64(a, b int64) int64 {
	return a + ((b - a) & ((b - a) >> 63))
}

func FastMax64(a, b int64) int64 {
	return a - ((a - b) & ((a - b) >> 63))
}

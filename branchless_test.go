package main_test

import (
	"fmt"
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestFastMin(t *testing.T) {
	testFastMin(t, 1, 2, 1)
	testFastMin(t, 2, 1, 1)
	testFastMin(t, 1, 1, 1)
	testFastMin(t, 0, 0, 0)
	testFastMin(t, 0, 1, 0)
	testFastMin(t, 1, 0, 0)
	testFastMin(t, 0, -1, -1)
	testFastMin(t, -1, 0, -1)
	testFastMin(t, -1, -2, -2)
	testFastMin(t, -2, -1, -2)
	testFastMin(t, -1, -1, -1)
	testFastMin(t, -1, 1, -1)
	testFastMin(t, 1, -1, -1)
}

func testFastMin(t *testing.T, a, b, expected int) {
	t.Run(fmt.Sprintf("FastMin(%d, %d)", a, b), func(t *testing.T) {
		if FastMin(a, b) != expected {
			t.Errorf("FastMin(%d, %d) should be %d", a, b, expected)
		}
	})
}

func TestFastMax(t *testing.T) {
	testFastMax(t, 1, 2, 2)
	testFastMax(t, 2, 1, 2)
	testFastMax(t, 1, 1, 1)
	testFastMax(t, 0, 0, 0)
	testFastMax(t, 0, 1, 1)
	testFastMax(t, 1, 0, 1)
	testFastMax(t, 0, -1, 0)
	testFastMax(t, -1, 0, 0)
	testFastMax(t, -1, -2, -1)
	testFastMax(t, -2, -1, -1)
	testFastMax(t, -1, -1, -1)
	testFastMax(t, -1, 1, 1)
	testFastMax(t, 1, -1, 1)
}

func testFastMax(t *testing.T, a, b, expected int) {
	t.Run(fmt.Sprintf("FastMax(%d, %d)", a, b), func(t *testing.T) {
		if FastMax(a, b) != expected {
			t.Errorf("FastMax(%d, %d) should be %d", a, b, expected)
		}
	})
}

func TestFastMin64(t *testing.T) {
	testFastMin64(t, 1, 2, 1)
	testFastMin64(t, 2, 1, 1)
	testFastMin64(t, 1, 1, 1)
	testFastMin64(t, 0, 0, 0)
	testFastMin64(t, 0, 1, 0)
	testFastMin64(t, 1, 0, 0)
	testFastMin64(t, 0, -1, -1)
	testFastMin64(t, -1, 0, -1)
	testFastMin64(t, -1, -2, -2)
	testFastMin64(t, -2, -1, -2)
	testFastMin64(t, -1, -1, -1)
	testFastMin64(t, -1, 1, -1)
	testFastMin64(t, 1, -1, -1)
}

func testFastMin64(t *testing.T, a, b, expected int64) {
	t.Run(fmt.Sprintf("FastMin64(%d, %d)", a, b), func(t *testing.T) {
		if FastMin64(a, b) != expected {
			t.Errorf("FastMin64(%d, %d) should be %d", a, b, expected)
		}
	})
}

func TestFastMax64(t *testing.T) {
	testFastMax64(t, 1, 2, 2)
	testFastMax64(t, 2, 1, 2)
	testFastMax64(t, 1, 1, 1)
	testFastMax64(t, 0, 0, 0)
	testFastMax64(t, 0, 1, 1)
	testFastMax64(t, 1, 0, 1)
	testFastMax64(t, 0, -1, 0)
	testFastMax64(t, -1, 0, 0)
	testFastMax64(t, -1, -2, -1)
	testFastMax64(t, -2, -1, -1)
	testFastMax64(t, -1, -1, -1)
	testFastMax64(t, -1, 1, 1)
	testFastMax64(t, 1, -1, 1)
}

func testFastMax64(t *testing.T, a, b, expected int64) {
	t.Run(fmt.Sprintf("FastMax64(%d, %d)", a, b), func(t *testing.T) {
		if FastMax64(a, b) != expected {
			t.Errorf("FastMax64(%d, %d) should be %d", a, b, expected)
		}
	})
}

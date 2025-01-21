package main_test

import (
	"testing"
)

import "reflect"

func assertEquals(t *testing.T, expected, actual any) {
	if !isDeepEqual(expected, actual) {
		t.Errorf("\nExpected: <%v> (%T)\nActual  : <%v> (%T)", expected, expected, actual, actual)
	}
}

func assertNotEquals(t *testing.T, expected, actual any) {
	if isDeepEqual(expected, actual) {
		t.Errorf("\nNot expected: <%v> (%T)\nActual      : <%v> (%T)", expected, expected, actual, actual)
	}
}

func isDeepEqual(expected, actual any) bool {
	return reflect.DeepEqual(expected, actual) || (isNil(expected) && isNil(actual))
}

func isNil(v any) bool {
	if v == nil {
		return true
	}
	value := reflect.ValueOf(v)
	kind := value.Kind()
	if kind >= reflect.Chan && kind <= reflect.Slice {
		return value.IsNil()
	}
	return false
}

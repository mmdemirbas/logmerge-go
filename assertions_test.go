package main_test

import (
	"testing"
)

import "reflect"

const (
	expectedFormat    = "\nExpected: <%v> (%T)\nActual  : <%v> (%T)"
	notExpectedFormat = "\nNot expected: <%v> (%T)\nActual      : <%v> (%T)"
)

func assertEquals(t *testing.T, expected, actual any) {
	if !isDeepEqual(expected, actual) {
		t.Errorf(expectedFormat, expected, expected, actual, actual)
	}
}

func assertNotEquals(t *testing.T, expected, actual any) {
	if isDeepEqual(expected, actual) {
		t.Errorf(notExpectedFormat, expected, expected, actual, actual)
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

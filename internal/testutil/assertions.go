package testutil

import (
	"reflect"
	"testing"
)

const (
	ExpectedFormat    = "\nExpected: <%v> (%T)\nActual  : <%v> (%T)"
	NotExpectedFormat = "\nNot expected: <%v> (%T)\nActual      : <%v> (%T)"
)

func AssertEquals(t *testing.T, expected, actual any) {
	t.Helper()
	if !IsDeepEqual(expected, actual) {
		t.Errorf(ExpectedFormat, expected, expected, actual, actual)
	}
}

func AssertNotEquals(t *testing.T, expected, actual any) {
	t.Helper()
	if IsDeepEqual(expected, actual) {
		t.Errorf(NotExpectedFormat, expected, expected, actual, actual)
	}
}

func IsDeepEqual(expected, actual any) bool {
	return reflect.DeepEqual(expected, actual) || (IsNil(expected) && IsNil(actual))
}

func IsNil(v any) bool {
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

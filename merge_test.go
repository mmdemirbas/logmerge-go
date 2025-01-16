package main_test

import (
	"bufio"
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestMergeScanners(t *testing.T) {
	tests := []struct {
		testName    string
		sourceNames []string
		outputNames map[string]string
		scanners    map[string]*bufio.Scanner
		expected    bool
	}{}
	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			MergeScanners(tt.sourceNames, tt.outputNames, tt.scanners)
			// TODO: Maybe we can pass printOut method as parameter, or use a channel to capture the output in the implementation
		})
	}
}

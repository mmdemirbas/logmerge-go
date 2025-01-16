package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var (
	printDurationCtx        []string
	printDurationCtxPadding = 30
)

func printFileList(files []string) {
	printDuration("printFileList", func() {
		for _, file := range files {
			printErr("  %s\n", file)
		}
	})
}

func printFileStats(files []string) {
	printDuration("printFileStats", func() {
		extensions := make(map[string]int)
		for _, file := range files {
			ext := filepath.Ext(file)
			extensions[ext]++
		}
		for ext, count := range extensions {
			printErr("  %3d: %s\n", count, ext)
		}
	})
}

func printDuration(name string, f func()) {
	printDurationCtx = append(printDurationCtx, name)
	ctx := fmt.Sprintf("%-*s", printDurationCtxPadding, strings.Join(printDurationCtx, " > "))

	startTime := time.Now()
	printErr("%s: %-*s\n", startTime.Format(time.RFC3339), printDurationCtxPadding, ctx)

	f()

	endTime := time.Now()
	duration := endTime.Sub(startTime)
	printErr("%s: %-*s took: %v\n", endTime.Format(time.RFC3339), printDurationCtxPadding, ctx, duration)

	printDurationCtx = printDurationCtx[:len(printDurationCtx)-1]
}

func printErr(format string, args ...any) {
	_, err := fmt.Fprintf(os.Stderr, format, args...)
	if err != nil {
		panic(err)
	}
}

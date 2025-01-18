package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

type LogLine struct {
	SourceName string
	RawLine    string
	// Parsed values
	Timestamp time.Time
}

func main() {
	var (
		basePath *string
		err      error
	)

	TotalMainDuration = MeasureDuration(func() {
		// Enable profiling only if configured
		if os.Getenv("ENABLE_PPROF") == "true" {
			fmt.Fprintf(os.Stderr, "Profiling enabled\n")

			// Start CPU profiling
			cpuFile, err := os.Create("out/cpu.prof")
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
			} else {
				defer cpuFile.Close()
				if err := pprof.StartCPUProfile(cpuFile); err != nil {
					fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
				} else {
					defer pprof.StopCPUProfile()
				}
			}
		}

		ParseOptionsDuration = MeasureDuration(func() {
			// If there is no argument provided, print usage and exit.
			if len(os.Args) < 2 {
				fmt.Fprintf(os.Stderr, "logmerge\n")
				fmt.Fprintf(os.Stderr, "  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
				fmt.Fprintf(os.Stderr, "  All well-known timestamp formats are supported.\n")
				fmt.Fprintf(os.Stderr, "  Output is written to stdout. Use redirection to save it to a file.\n")
				fmt.Fprintf(os.Stderr, "  Program messages are written to stderr to avoid mixing with log lines.\n")
				fmt.Fprintf(os.Stderr, "\n")
				fmt.Fprintf(os.Stderr, "  Usage: %s <path>...\n\n", os.Args[0])
				fmt.Fprintf(os.Stderr, "\n")
				fmt.Fprintf(os.Stderr, "  <path>...  Path to the log files or directories containing log files\n")
				fmt.Fprintf(os.Stderr, "\n")
				os.Exit(1)
			}
			// Parse the first argument as basePath
			// TODO: Get all arguments as basePaths
			basePath = &os.Args[1]
		})

		err = MergeLogs(*basePath)

		if os.Getenv("ENABLE_PPROF") == "true" {
			// Capture memory profile
			memFile, err := os.Create("out/mem.prof")
			if err != nil {
				fmt.Fprintf(os.Stderr, "could not create memory profile: %v\n", err)
			} else {
				defer memFile.Close()
				if err := pprof.WriteHeapProfile(memFile); err != nil {
					fmt.Fprintf(os.Stderr, "could not write memory profile: %v\n", err)
				}
			}
		}
	})

	PrintMetrics(basePath, err)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

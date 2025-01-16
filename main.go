package main

import (
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
	// Enable profiling only if configured
	if os.Getenv("ENABLE_PPROF") == "true" {
		printErr("Profiling enabled\n")

		// Start CPU profiling
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			panic("could not create CPU profile: " + err.Error())
		}
		defer cpuFile.Close()
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			panic("could not start CPU profile: " + err.Error())
		}
		defer pprof.StopCPUProfile()
	}

	printDuration("main", func() {
		basePath := parseOptions()
		printErr("basePath: %s\n", *basePath)

		if err := mergeLogs(*basePath); err != nil {
			printErr("Error: %v\n", err)
			os.Exit(1)
		}
	})

	if os.Getenv("ENABLE_PPROF") == "true" {
		// Capture memory profile
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			panic("could not create memory profile: " + err.Error())
		}
		defer memFile.Close()
		if err := pprof.WriteHeapProfile(memFile); err != nil {
			panic("could not write memory profile: " + err.Error())
		}
	}
}

func parseOptions() *string {
	var (
		basePath *string
	)
	printDuration("parseOptions", func() {
		// If there is no argument provided, print usage and exit.
		if len(os.Args) < 2 {
			printUsage()
			os.Exit(1)
		}
		// Parse the first argument as basePath
		basePath = &os.Args[1]
	})
	return basePath
}

func printUsage() {
	printErr("logmerge\n")
	printErr("  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
	printErr("  All well-known timestamp formats are supported.\n")
	printErr("  Output is written to stdout. Use redirection to save it to a file.\n")
	printErr("  Program messages are written to stderr to avoid mixing with log lines.\n")
	printErr("\n")
	printErr("  Usage: %s <path>...\n\n", os.Args[0])
	printErr("\n")
	printErr("  <path>...  Path to the log files or directories containing log files\n")
	printErr("\n")
}

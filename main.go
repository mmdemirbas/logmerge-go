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
	)

	mainDuration, err := measureDuration(func() error {
		// Enable profiling only if configured
		if os.Getenv("ENABLE_PPROF") == "true" {
			fmt.Fprintf(os.Stderr, "Profiling enabled\n")

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

		parseOptionsDuration, _ := measureDuration(func() error {
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
			return nil
		})
		GlobalMetrics.AddParseOptionsDuration(int64(parseOptionsDuration))

		err := mergeLogs(*basePath)

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
		return err
	})
	GlobalMetrics.AddTotalMainDuration(int64(mainDuration))

	// Show final stats
	fmt.Fprintf(os.Stderr, "===== METRICS =================================================================================\n")
	fmt.Fprintf(os.Stderr, "Base path           : %s\n", *basePath)
	fmt.Fprintf(os.Stderr, "Error               : %v\n", err)
	fmt.Fprintf(os.Stderr, "Total main duration : %v\n", time.Duration(GlobalMetrics.TotalMainDuration))
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File list stats\n")
	fmt.Fprintf(os.Stderr, "  dirs scanned      : %d\n", GlobalMetrics.DirsScanned)
	fmt.Fprintf(os.Stderr, "  files scanned     : %d\n", GlobalMetrics.FilesScanned)
	fmt.Fprintf(os.Stderr, "  files matched     : %d\n", GlobalMetrics.FilesMatched)
	fmt.Fprintf(os.Stderr, "  file match ratio  : %d%%\n", int(float64(GlobalMetrics.FilesMatched)/float64(max(1, GlobalMetrics.FilesScanned))*100))
	fmt.Fprintf(os.Stderr, "Timing stats by phase\n")
	fmt.Fprintf(os.Stderr, "  parse options     : %v\n", time.Duration(GlobalMetrics.ParseOptionsDuration))
	fmt.Fprintf(os.Stderr, "  list files        : %v\n", time.Duration(GlobalMetrics.ListFilesDuration))
	fmt.Fprintf(os.Stderr, "  open files        : %v\n", time.Duration(GlobalMetrics.OpenFilesDuration))
	fmt.Fprintf(os.Stderr, "  merge scanners    : %v\n", time.Duration(GlobalMetrics.MergeScannersDuration))
	fmt.Fprintf(os.Stderr, "Timing stats by operation\n")
	fmt.Fprintf(os.Stderr, "  read line         : %v\n", time.Duration(GlobalMetrics.ReadLineDuration))
	fmt.Fprintf(os.Stderr, "  parse timestamp   : %v\n", time.Duration(GlobalMetrics.ParseTimestampDuration))
	fmt.Fprintf(os.Stderr, "  write line        : %v\n", time.Duration(GlobalMetrics.WriteLineDuration))
	fmt.Fprintf(os.Stderr, "Line count stats\n")
	fmt.Fprintf(os.Stderr, "  lines read        : %d\n", GlobalMetrics.LinesRead)
	fmt.Fprintf(os.Stderr, "  lines with ts     : %d\n", GlobalMetrics.LinesWithTimestamps)
	fmt.Fprintf(os.Stderr, "  lines without ts  : %d\n", GlobalMetrics.LinesWithoutTimestamps)
	fmt.Fprintf(os.Stderr, "Byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes read        : %d = %s\n", GlobalMetrics.BytesRead, humanizeBytes(GlobalMetrics.BytesRead))
	fmt.Fprintf(os.Stderr, "  bytes written     : %d = %s\n", GlobalMetrics.BytesWritten, humanizeBytes(GlobalMetrics.BytesWritten))
	fmt.Fprintf(os.Stderr, "Extra byte count stats\n")
	fmt.Fprintf(os.Stderr, "  bytes for ts      : %d = %s\n", GlobalMetrics.BytesWrittenForTimestamps, humanizeBytes(GlobalMetrics.BytesWrittenForTimestamps))
	fmt.Fprintf(os.Stderr, "  bytes for output  : %d = %s\n", GlobalMetrics.BytesWrittenForOutputNames, humanizeBytes(GlobalMetrics.BytesWrittenForOutputNames))
	fmt.Fprintf(os.Stderr, "  bytes for raw     : %d = %s\n", GlobalMetrics.BytesWrittenForRawLines, humanizeBytes(GlobalMetrics.BytesWrittenForRawLines))
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")
	fmt.Fprintf(os.Stderr, "File list (%d files):\n", len(GlobalMetrics.MatchedFiles))
	for i, file := range GlobalMetrics.MatchedFiles {
		fmt.Fprintf(os.Stderr, "%5d %s\n", i, file)
	}
	fmt.Fprintf(os.Stderr, "===============================================================================================\n")

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func humanizeBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%.2f GB", float64(bytes)/(1024*1024*1024))
}

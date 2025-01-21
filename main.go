package main

import (
	"fmt"
	"os"
	"runtime/pprof"
)

const (
	enableDebugLogging       = false
	writeTimestamp           = true
	writeSourceNames         = true
	timestampSearchPrefixLen = 1024 * 1024 // per file
	outputBufferSize         = 1024 * 1024 * 100
)

var (
	excludedStrictSuffixes  = []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"}
	includedStrictSuffixes  = []string{}
	excludedLenientSuffixes = []string{}
	includedLenientSuffixes = []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".txt", ".out", ".debug", ".trace"}
)

func main() {
	startOfMain := MeasureStart("Main")

	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "Recovered from panic: %v\n", r)
		}
	}()

	// Enable profiling only if configured
	pprofEnabled := os.Getenv("ENABLE_PPROF") == "true"
	if pprofEnabled {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "Profiling enabled\n")

		// Start CPU profiling
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "could not create CPU profile: %v\n", err)
		} else {
			defer cpuFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(os.Stderr, "could not start CPU profile: %v\n", err)
			} else {
				defer pprof.StopCPUProfile()
			}
		}
	}

	startOfParseOptions := MeasureStart("ParseOptions")

	//goland:noinspection GoUnhandledErrorResult
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "logmerge\n")
		fmt.Fprintf(os.Stderr, "  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s <inputPath> [outputPath]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  <inputPath>   Path to the log file or a directory containing log files\n")
		fmt.Fprintf(os.Stderr, "  [outputPath]  Optional output path. If not provided, output is written to stdout\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "- Well-known timestamp formats are supported.\n")
		fmt.Fprintf(os.Stderr, "- Program messages are written to stderr to avoid mixing with log lines.\n")
		os.Exit(1)
	}
	// TODO: Support multiple base paths
	inputPath := os.Args[1]
	outputPath := ""
	if len(os.Args) > 2 {
		outputPath = os.Args[2]
	}
	ParseOptionsDuration = MeasureSince(startOfParseOptions)

	err := MergeFiles(inputPath, outputPath)

	if pprofEnabled {
		// Capture memory profile
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "could not create memory profile: %v\n", err)
		} else {
			defer memFile.Close()
			if err := pprof.WriteHeapProfile(memFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(os.Stderr, "could not write memory profile: %v\n", err)
			}
		}
	}
	TotalMainDuration = MeasureSince(startOfMain)

	PrintMetrics(startOfMain, inputPath, outputPath, pprofEnabled, err)

	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

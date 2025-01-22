package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

// TODO: These settings can be made configurable via command-line flags
const (
	disableMetricsCollection = true
	enableDebugLogging       = false
	writeTimestamp           = true
	writeSourceNames         = true
	timestampSearchPrefixLen = 250
	readerBufferSize         = max(timestampSearchPrefixLen, 1024*128) // per file
	writerBufferSize         = 1024 * 1024 * 100
)

var (
	excludedStrictSuffixes  = []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"}
	includedStrictSuffixes  = []string{}
	excludedLenientSuffixes = []string{}
	includedLenientSuffixes = []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".txt", ".out", ".debug", ".trace"}
)

func main() {
	startTime := time.Now()
	stdout := os.Stdout
	stderr := os.Stderr
	var err error

	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(stderr, "main: Recovered from panic: %v\n", r)
		}
	}()

	startOfMain := MeasureStart("Main")

	// Enable profiling only if configured
	startOfCpuProfile := MeasureStart("CpuProfile")
	pprofEnabled := os.Getenv("ENABLE_PPROF") == "true"
	if pprofEnabled {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(stderr, "Profiling enabled\n")

		// Start CPU profiling
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(stderr, "could not create CPU profile: %v\n", err)
		} else {
			defer cpuFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(stderr, "could not start CPU profile: %v\n", err)
			} else {
				defer pprof.StopCPUProfile()
			}
		}
	}
	MeasureSince(startOfCpuProfile)

	startOfParseOptions := MeasureStart("ParseOptions")
	//goland:noinspection GoUnhandledErrorResult
	if len(os.Args) < 2 {
		fmt.Fprintf(stderr, "logmerge\n")
		fmt.Fprintf(stderr, "  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
		fmt.Fprintf(stderr, "\n")
		fmt.Fprintf(stderr, "Usage:\n")
		fmt.Fprintf(stderr, "  %s <inputPath> [outputPath] [logPath]\n", os.Args[0])
		fmt.Fprintf(stderr, "\n")
		fmt.Fprintf(stderr, "  <inputPath>   Path to the log file or a directory containing log files\n")
		fmt.Fprintf(stderr, "  [outputPath]  Optional output path. If not provided, output is written to stdout\n")
		fmt.Fprintf(stderr, "  [logPath]     Optional log path. If not provided, logs are written to stderr\n")
		fmt.Fprintf(stderr, "\n")
		os.Exit(1)
	}
	// TODO: Support multiple base paths
	inputPath := os.Args[1]
	if len(os.Args) > 2 {
		stdout, err = os.Create(os.Args[2])
		if err != nil {
			err = fmt.Errorf("failed to create stdout file: %v", err)
		} else {
			defer stdout.Close()
		}
	}
	if len(os.Args) > 3 {
		stderr, err = os.Create(os.Args[3])
		if err != nil {
			err = fmt.Errorf("failed to create stderr file: %v", err)
		} else {
			defer stderr.Close()
		}
	}
	MeasureSince(startOfParseOptions)

	startOfMergeFiles := MeasureStart("MergeFiles")
	err = MergeFiles(inputPath, stdout)
	ProcessDuration = MeasureSince(startOfMergeFiles)

	startOfMemProfile := MeasureStart("MemProfile")
	if pprofEnabled {
		// Capture memory profile
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(stderr, "could not create memory profile: %v\n", err)
		} else {
			defer memFile.Close()
			if err := pprof.WriteHeapProfile(memFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(stderr, "could not write memory profile: %v\n", err)
			}
		}
	}
	MeasureSince(startOfMemProfile)

	TotalMainDuration = MeasureSince(startOfMain)

	elapsedTime := time.Since(startTime)
	PrintMetrics(stderr, startTime, elapsedTime, inputPath, stdout.Name(), stderr.Name(), pprofEnabled, err)

	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

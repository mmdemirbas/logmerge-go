package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

// TODO: These settings can be made configurable via command-line flags or env vars
var (
	Stdout = os.Stdout
	Stderr = os.Stderr

	DisableMetricsCollection = false
	EnableProfiling          = os.Getenv("ENABLE_PPROF") == "true"

	WriteSourceNamesPerBlock = false
	WriteSourceNamesPerLine  = true
	WriteTimestampPerLine    = true

	MinTimestamp = noTimestamp
	MaxTimestamp = MyTime(1<<63 - 1)

	ShortestTimestampLen    = 15
	TimestampSearchEndIndex = 250

	BufferSizeForRead  = 1024 * 1024 * 100
	BufferSizeForWrite = 1024 * 1024 * 100

	ExcludedStrictSuffixes  = []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"}
	IncludedStrictSuffixes  = []string{}
	ExcludedLenientSuffixes = []string{}
	IncludedLenientSuffixes = []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".txt", ".out", ".debug", ".trace"}
)

func main() {
	programStartTime := time.Now() // measure program duration even if metrics disabled
	var err error

	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "main: Recovered from panic: %v\n", r)
		}
	}()

	// Enable profiling only if configured
	if EnableProfiling {
		// Start CPU profiling
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "could not create CPU profile: %v\n", err)
		} else {
			defer cpuFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "could not start CPU profile: %v\n", err)
			} else {
				defer pprof.StopCPUProfile()
			}
		}
	}

	//goland:noinspection GoUnhandledErrorResult
	if len(os.Args) < 2 {
		fmt.Fprintf(Stderr, "logmerge\n")
		fmt.Fprintf(Stderr, "  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
		fmt.Fprintf(Stderr, "\n")
		fmt.Fprintf(Stderr, "Usage:\n")
		fmt.Fprintf(Stderr, "  %s <inputPath> [outputPath] [logPath]\n", os.Args[0])
		fmt.Fprintf(Stderr, "\n")
		fmt.Fprintf(Stderr, "  <inputPath>   Path to the log file or a directory containing log files\n")
		fmt.Fprintf(Stderr, "  [outputPath]  Optional output path. If not provided, output is written to stdout\n")
		fmt.Fprintf(Stderr, "  [logPath]     Optional log path. If not provided, logs are written to stderr\n")
		fmt.Fprintf(Stderr, "\n")
		os.Exit(1)
	}
	// TODO: Support multiple base paths
	inputPath := os.Args[1]
	if len(os.Args) > 2 {
		Stdout, err = os.Create(os.Args[2])
		if err != nil {
			err = fmt.Errorf("failed to create stdout file: %v", err)
		} else {
			defer Stdout.Close()
		}
	}
	if len(os.Args) > 3 {
		Stderr, err = os.Create(os.Args[3])
		if err != nil {
			err = fmt.Errorf("failed to create stderr file: %v", err)
		} else {
			defer Stderr.Close()
		}
	}

	err = MergeFiles(inputPath)

	if EnableProfiling {
		// Capture memory profile
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "could not create memory profile: %v\n", err)
		} else {
			defer memFile.Close()
			if err := pprof.WriteHeapProfile(memFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "could not write memory profile: %v\n", err)
			}
		}
	}

	if !DisableMetricsCollection {
		startTime := MeasureStart("CalcMetricsOverhead")
		testCount := 1_000_000
		for i := 0; i < testCount; i++ {
			startTime := MeasureStart("CalcMetricsOverhead")
			MeasureSince(startTime)
		}
		MetricsOverheadAvg = MeasureSince(startTime) / int64(testCount-1)
	}

	elapsedTime := time.Since(programStartTime)
	PrintMetrics(programStartTime, elapsedTime, inputPath, err)

	if err != nil {
		os.Exit(1)
	}
}

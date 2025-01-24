package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

// TODO: These settings can be made configurable via command-line flags or env vars
var (
	InputPath = "/Users/md/code/spark-kit/memartscc-token-renewal/remote-test/log/application_1737096599066_0003-WORKING-WITH-PROTOBUF/console.log"
	Stdout    = createFile("/Users/md/dev/mmdemirbas/logmerge/out/stdout.log") // os.Stdout
	Stderr    = createFile("/Users/md/dev/mmdemirbas/logmerge/out/stderr.log") // os.Stderr

	//InputPath = ""
	//Stdout    = os.Stdout
	//Stderr    = os.Stderr

	DisableMetricsCollection = false
	EnableProfiling          = os.Getenv("ENABLE_PPROF") == "true"

	WriteSourceNamesPerBlock = true
	WriteSourceNamesPerLine  = false
	WriteTimestampPerLine    = false

	// Default values: noTimestamp and MyTime(1<<63 - 1)
	// 20250117 13:12:41.434816 to 2025-01-17 13:14:12,863
	MinTimestamp       = NewMyTime(2025, 1, 17, 13, 12, 41, 434816000, 0, 0, 0)
	MaxTimestamp       = NewMyTime(2025, 1, 17, 13, 14, 12, 863000000, 0, 0, 0)
	IgnoreTimezoneInfo = true

	ShortestTimestampLen    = 15
	TimestampSearchEndIndex = 250

	BufferSizeForRead  = 1024 * 1024 * 100
	BufferSizeForWrite = 1024 * 1024 * 100

	ExcludedStrictSuffixes  = []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"}
	IncludedStrictSuffixes  = []string{}
	ExcludedLenientSuffixes = []string{}
	IncludedLenientSuffixes = []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".out", ".debug", ".trace"}

	SourceNameAliases = map[string]string{
		"172.16.0.240/memartscc/cc-worker/ccworker.INFO.20250115-172150.3239354":         "cc-worker",
		"172.16.0.240/memartscc/cc-worker/ccworker.INFO":                                 "cc-worker",
		"172.16.0.240/memartscc/cc-worker/ccworker.WARNING.20250115-172623.3239354":      "cc-worker",
		"172.16.0.240/memartscc/cc-worker/ccworker.WARNING":                              "cc-worker",
		"172.16.0.240/memartscc/cc-worker/ccworker.ERROR.20250115-172623.3239354":        "cc-worker",
		"172.16.0.240/memartscc/cc-worker/ccworker.ERROR":                                "cc-worker",
		"172.16.0.240/memartscc/check-worker-instance.log":                               "cc-worker",
		"172.16.0.240/memartscc/cc-sidecar/cc-sidecar.log":                               "cc-sidecar",
		"172.16.0.240/memartscc/check-sidecar-instance.log":                              "cc-sidecar",
		"172.16.0.240/memartscc/cc-sidecar/cc-sidecar-bg-task.log":                       "cc-sidecar",
		"172.16.0.240/memartscc/checkServiceHealthCheck.log":                             "cc-health",
		"172.16.0.240/yarn/nm/yarn-nodemanager-period-check.log":                         "yarn-nodemanager-period-check",
		"172.16.0.240/yarn/nm/nodemanager-omm-20250107224846-pid340890-gc.log.0.current": "yarn-gc",
	}
)

func main() {
	programStartTime := time.Now() // measure program duration even if metrics disabled
	var err error

	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "main: Recovered from panic: %v\n", r)
		}
		Stdout.Close()
		Stderr.Close()
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

	if len(os.Args) > 1 {
		// TODO: Support multiple base paths
		InputPath = os.Args[1]
		if len(os.Args) > 2 {
			Stdout = createFile(os.Args[2])
		}
		if len(os.Args) > 3 {
			Stderr = createFile(os.Args[3])
		}
	} else if len(InputPath) == 0 {
		//goland:noinspection GoUnhandledErrorResult
		{
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
		}
		os.Exit(1)
	}

	err = MergeFiles(InputPath)
	exitOnError(err)

	// TODO: Catch interrupt signal during merge process and do the post-work anyway

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
	PrintMetrics(programStartTime, elapsedTime, err)
}

func createFile(path string) *os.File {
	f, err := os.Create(path)
	exitOnError(err)
	return f
}

func exitOnError(err error) {
	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "failed to create file: %v", err)
		os.Exit(1)
	}
}

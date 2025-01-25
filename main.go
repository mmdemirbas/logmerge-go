package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

// EffectiveConfig defines the configuration of the application.
// It is initialized with default values and then updated with the values from the configuration file.
var EffectiveConfig = &AppConfig{
	InputPath:  "",
	OutputPath: "", // empty means stdout
	LogPath:    "", // empty means stderr

	EnableMetricsCollection: true,
	EnableProfiling:         false,

	WriteAliasPerBlock:    true,
	WriteAliasPerLine:     false,
	WriteTimestampPerLine: false,

	IgnoreTimezoneInfo: false,
	MinTimestamp:       noTimestamp,
	MaxTimestamp:       Timestamp(1<<63 - 1),

	ShortestTimestampLen:    15,
	TimestampSearchEndIndex: 250,

	BufferSizeForRead:  1024 * 1024 * 100,
	BufferSizeForWrite: 1024 * 1024 * 100,

	ExcludedStrictSuffixes:  []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"},
	IncludedStrictSuffixes:  []string{},
	ExcludedLenientSuffixes: []string{},
	IncludedLenientSuffixes: []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".out", ".debug", ".trace"},

	FileAliases: map[string]string{},
}

// TODO: Catch interrupt signal during merge process and do the post-work anyway

func main() {
	// Print usage if used incorrectly
	//goland:noinspection GoUnhandledErrorResult,
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "logmerge\n")
		fmt.Fprintf(os.Stderr, "  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "Usage:\n")
		fmt.Fprintf(os.Stderr, "  %s <confFile>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "  <confFile>   Path to the configuration file in YAML format.\n")
		fmt.Fprintf(os.Stderr, "\n")
		os.Exit(1)
	}

	// Measure program duration even if metrics disabled
	programStartTime := time.Now()

	yml := &YamlConfig{}
	err := yml.LoadYamlConfig(os.Args[1])
	if err == nil {
		err = EffectiveConfig.LoadAppConfig(yml)
		if err == nil {
			// Enable profiling only if configured
			if EffectiveConfig.EnableProfiling {
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

			files, err := ListFiles(EffectiveConfig)
			if err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(EffectiveConfig.Stderr, "failed to list files: %v", err)
				os.Exit(1)
			} else {

				// Print progress
				go func() {
					// TODO: Make printProgress params configurable (initial delay, interval, etc)
					// Print progress only if it takes some time
					time.Sleep(1 * time.Second)

					//goland:noinspection GoUnhandledErrorResult
					fmt.Fprintf(EffectiveConfig.Stderr, "\n")

					ticker := time.NewTicker(1000 * time.Millisecond)
					for range ticker.C {
						printProgress(files, programStartTime)
					}
				}()

				// Process files
				err = ProcessFiles(EffectiveConfig, files)
				if err != nil {
					// Print progress one last time (for 100% mostly)
					printProgress(files, programStartTime)
				}
			}

			if EffectiveConfig.EnableProfiling {
				// Capture memory profile
				memFile, err := os.Create("out/mem.prof")
				if err != nil {
					//goland:noinspection GoUnhandledErrorResult
					fmt.Fprintf(EffectiveConfig.Stderr, "could not create memory profile: %v\n", err)
				} else {
					defer memFile.Close()
					if err := pprof.WriteHeapProfile(memFile); err != nil {
						//goland:noinspection GoUnhandledErrorResult
						fmt.Fprintf(EffectiveConfig.Stderr, "could not write memory profile: %v\n", err)
					}
				}
			}
		}
	}

	elapsedTime := time.Since(programStartTime)
	PrintMetrics(EffectiveConfig, programStartTime, elapsedTime, err)

	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

func printProgress(files []*FileHandle, programStartTime time.Time) {
	completedSize := 0
	completedCount := 0

	totalSize := 0
	totalCount := len(files)

	for _, file := range files {
		if file.Done {
			completedSize += file.Size
			completedCount++
		} else {
			completedSize += file.BytesRead
		}
		totalSize += file.Size
	}

	totalSize = max(totalSize, 1)
	totalCount = max(totalCount, 1)

	elapsedTime := time.Since(programStartTime)

	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(os.Stderr, "Progress: %6.2f %% of data (%12s / %12s) - %6.2f %% of files (%5d / %5d) - Elapsed: %s\r",
		float64(completedSize)/(float64(totalSize)/100), bytes(int64(completedSize)), bytes(int64(totalSize)),
		float64(completedCount)/(float64(totalCount)/100), int64(completedCount), int64(totalCount),
		elapsedTime.Round(time.Millisecond).String(),
	)
}

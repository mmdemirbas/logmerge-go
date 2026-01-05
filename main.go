package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

// config defines the default configuration which will be overwritten by the configuration file
var config = &MainConfig{
	OutputFile: &WritableFile{os.Stdout},
	LogFile:    &WritableFile{os.Stderr},

	ProfilingEnabled: false,

	ListFilesConfig: &ListFilesConfig{
		InputPath:          "", // required
		ExcludedSuffixes:   []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"},
		IncludedSuffixes:   []string{},
		ExcludedSubstrings: []string{},
		IncludedSubstrings: []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".out", ".debug", ".trace"},
		FileAliases:        map[string]string{},
	},

	ParseTimestampConfig: &ParseTimestampConfig{
		IgnoreTimezoneInfo:      false,
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	},

	MergeConfig: &MergeConfig{
		MetricsTreeEnabled:    false,
		WriteAliasPerBlock:    false,
		WriteAliasPerLine:     false,
		WriteTimestampPerLine: false,
		MinTimestamp:          ZeroTimestamp,
		MaxTimestamp:          Timestamp(1<<63 - 1),
		BufferSizeForRead:     1024 * 1024 * 100,
		BufferSizeForWrite:    1024 * 1024 * 100,
	},

	PrintProgressConfig: &PrintProgressConfig{
		PrintProgressEnabled: true,
		InitialDelayMillis:   1000,
		PeriodMillis:         1000,
	},
}

var metrics *MainMetrics

var GlobalMetricsTree = NewMetricsTree()

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

	ymlFile := os.Args[1]
	err := config.LoadYAML(ymlFile)
	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "failed to load configuration from file %s: %v\n", ymlFile, err)
		os.Exit(1)
	}

	// Enable profiling only if configured
	if config.ProfilingEnabled {
		// Start CPU profiling
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(config.LogFile, "could not create CPU profile: %v\n", err)
		} else {
			defer cpuFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(config.LogFile, "could not start CPU profile: %v\n", err)
			} else {
				defer pprof.StopCPUProfile()
			}
		}
	}

	outputFile := config.OutputFile
	logFile := config.LogFile

	metrics = NewMetrics()
	GlobalMetricsTree.Enabled = config.MergeConfig.MetricsTreeEnabled
	files, err := ListFiles(
		config.ListFilesConfig,
		metrics.ListFilesMetrics,
		config.MergeConfig.BufferSizeForRead,
		config.ParseTimestampConfig.ShortestTimestampLen,
		logFile,
	)
	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(config.LogFile, "failed to list files: %v", err)
		os.Exit(1)
	}

	// Print progress periodically
	go PrintProgressPeriodically(config.PrintProgressConfig, files, programStartTime)
	defer func() {
		if err == nil {
			PrintProgress(config.PrintProgressConfig, files, programStartTime)
		}
	}()

	writer := NewBufferedWriter(outputFile, config.MergeConfig.BufferSizeForWrite)
	defer writer.Close()

	// Process files
	err = ProcessFiles(
		config.MergeConfig,
		metrics.MergeMetrics,
		files,
		writer,
		logFile,
		func(file *FileHandle) error {
			return UpdateTimestamp(config.ParseTimestampConfig, metrics.ParseTimestampMetrics, file)
		},
	)

	if config.ProfilingEnabled {
		// Capture memory profile
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(config.LogFile, "could not create memory profile: %v\n", err)
		} else {
			defer memFile.Close()
			if err := pprof.WriteHeapProfile(memFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(config.LogFile, "could not write memory profile: %v\n", err)
			}
		}
	}

	elapsedTime := time.Since(programStartTime)
	metrics.PrintMetrics(config, programStartTime, elapsedTime, err)

	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(logFile, "%v\n", err)
		os.Exit(1)
	}
}

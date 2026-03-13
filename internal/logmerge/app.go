package logmerge

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"time"
)

// stringSliceFlag implements flag.Value for repeated string flags.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// config defines the default configuration
var config = &MainConfig{
	OutputFile: &WritableFile{File: os.Stdout},
	LogFile:    &WritableFile{File: os.Stderr},

	ProfilingEnabled: false,

	ListFilesConfig: &ListFilesConfig{
		InputPaths:     []string{},
		IgnorePatterns: []string{},
		FileAliases:    map[string]string{},
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

// TODO: Catch interrupt signal during merge process and do the post-work anyway

func Run() error {
	// Define CLI flags
	outputFlag := flag.String("output", "", "Output file path (default: stdout)")
	flag.StringVar(outputFlag, "o", "", "Output file path (default: stdout)")

	logFlag := flag.String("log", "", "Log file path (default: stderr)")
	flag.StringVar(logFlag, "l", "", "Log file path (default: stderr)")

	configFlag := flag.String("config", "", "Path to base YAML configuration file")

	var filterFlags stringSliceFlag
	flag.Var(&filterFlags, "filter", "Filter pattern (repeatable)")
	flag.Var(&filterFlags, "f", "Filter pattern (repeatable)")

	filterFileFlag := flag.String("filter-file", "", "Path to file containing filter patterns")

	var aliasFlags stringSliceFlag
	flag.Var(&aliasFlags, "alias", "File alias in pattern=alias format (repeatable)")

	writeTimestamp := flag.Bool("write-timestamp", false, "Write timestamp per line")
	flag.BoolVar(writeTimestamp, "t", false, "Write timestamp per line")

	writeBlockAlias := flag.Bool("write-block-alias", false, "Write alias per block")
	flag.BoolVar(writeBlockAlias, "b", false, "Write alias per block")

	writeLineAlias := flag.Bool("write-line-alias", false, "Write alias per line")
	flag.BoolVar(writeLineAlias, "a", false, "Write alias per line")

	sinceFlag := flag.String("since", "", "Minimum timestamp (RFC3339)")
	untilFlag := flag.String("until", "", "Maximum timestamp (RFC3339)")

	ignoreTimezone := flag.Bool("ignore-timezone", false, "Ignore timezone info in timestamps")
	minTsLen := flag.Int("min-ts-len", 0, "Shortest timestamp length")
	tsSearchLimit := flag.Int("ts-search-limit", 0, "Timestamp search end index")

	bufRead := flag.Int("buf-read", 0, "Read buffer size in bytes")
	bufWrite := flag.Int("buf-write", 0, "Write buffer size in bytes")
	metricsFlag := flag.Bool("metrics", false, "Enable metrics tree")
	profileFlag := flag.Bool("profile", false, "Enable CPU/memory profiling")

	progressFlag := flag.Bool("progress", false, "Enable progress display")
	flag.BoolVar(progressFlag, "p", false, "Enable progress display")

	flag.Parse()

	// Track which flags were explicitly set
	explicitlySet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		explicitlySet[f.Name] = true
	})

	// Measure program duration even if metrics disabled
	programStartTime := time.Now()

	// Load YAML config if provided
	if *configFlag != "" {
		err := config.LoadYAML(*configFlag)
		if err != nil {
			return fmt.Errorf("failed to load configuration from file %s: %w", *configFlag, err)
		}
	}

	// Override config with explicitly set flags
	if explicitlySet["output"] || explicitlySet["o"] {
		if *outputFlag != "" {
			config.OutputFile = &WritableFile{Path: *outputFlag}
			if err := config.OutputFile.Initialize(); err != nil {
				return err
			}
		} else {
			config.OutputFile = &WritableFile{File: os.Stdout}
		}
	}
	if explicitlySet["log"] || explicitlySet["l"] {
		if *logFlag != "" {
			config.LogFile = &WritableFile{Path: *logFlag}
			if err := config.LogFile.Initialize(); err != nil {
				return err
			}
		} else {
			config.LogFile = &WritableFile{File: os.Stderr}
		}
	}
	if explicitlySet["write-timestamp"] || explicitlySet["t"] {
		config.MergeConfig.WriteTimestampPerLine = *writeTimestamp
	}
	if explicitlySet["write-block-alias"] || explicitlySet["b"] {
		config.MergeConfig.WriteAliasPerBlock = *writeBlockAlias
	}
	if explicitlySet["write-line-alias"] || explicitlySet["a"] {
		config.MergeConfig.WriteAliasPerLine = *writeLineAlias
	}
	if explicitlySet["since"] {
		ts, err := time.Parse(time.RFC3339Nano, *sinceFlag)
		if err != nil {
			return fmt.Errorf("failed to parse --since value %q: %w", *sinceFlag, err)
		}
		config.MergeConfig.MinTimestamp = Timestamp(ts.UnixNano())
	}
	if explicitlySet["until"] {
		ts, err := time.Parse(time.RFC3339Nano, *untilFlag)
		if err != nil {
			return fmt.Errorf("failed to parse --until value %q: %w", *untilFlag, err)
		}
		config.MergeConfig.MaxTimestamp = Timestamp(ts.UnixNano())
	}
	if explicitlySet["ignore-timezone"] {
		config.ParseTimestampConfig.IgnoreTimezoneInfo = *ignoreTimezone
	}
	if explicitlySet["min-ts-len"] {
		config.ParseTimestampConfig.ShortestTimestampLen = *minTsLen
	}
	if explicitlySet["ts-search-limit"] {
		config.ParseTimestampConfig.TimestampSearchEndIndex = *tsSearchLimit
	}
	if explicitlySet["buf-read"] {
		config.MergeConfig.BufferSizeForRead = *bufRead
	}
	if explicitlySet["buf-write"] {
		config.MergeConfig.BufferSizeForWrite = *bufWrite
	}
	if explicitlySet["metrics"] {
		config.MergeConfig.MetricsTreeEnabled = *metricsFlag
	}
	if explicitlySet["profile"] {
		config.ProfilingEnabled = *profileFlag
	}
	if explicitlySet["progress"] || explicitlySet["p"] {
		config.PrintProgressConfig.PrintProgressEnabled = *progressFlag
	}

	// Append --filter flags to IgnorePatterns
	config.ListFilesConfig.IgnorePatterns = append(config.ListFilesConfig.IgnorePatterns, filterFlags...)

	// Parse --alias flags (pattern=alias)
	for _, a := range aliasFlags {
		parts := strings.SplitN(a, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --alias format %q, expected pattern=alias", a)
		}
		config.ListFilesConfig.FileAliases[parts[0]] = parts[1]
	}

	// Positional args become input paths
	if args := flag.Args(); len(args) > 0 {
		config.ListFilesConfig.InputPaths = args
	}

	// Read filter file if provided
	if *filterFileFlag != "" {
		lines, err := readFilterFile(*filterFileFlag)
		if err != nil {
			return err
		}
		// Prepend filter-file patterns before CLI filter patterns
		config.ListFilesConfig.IgnorePatterns = append(lines, config.ListFilesConfig.IgnorePatterns...)
	}

	if len(config.ListFilesConfig.InputPaths) == 0 {
		flag.Usage()
		return fmt.Errorf("no input paths specified")
	}

	// Enable profiling only if configured
	if config.ProfilingEnabled {
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			fmt.Fprintf(config.LogFile, "could not create CPU profile: %v\n", err)
		} else {
			defer cpuFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				fmt.Fprintf(config.LogFile, "could not start CPU profile: %v\n", err)
			} else {
				defer pprof.StopCPUProfile()
			}
		}
	}

	outputFile := config.OutputFile
	logFile := config.LogFile

	metrics = NewMetrics()
	metrics.Tree = NewMetricsTree()
	metrics.Tree.Enabled = config.MergeConfig.MetricsTreeEnabled

	files, err := ListFiles(
		config.ListFilesConfig,
		metrics.ListFilesMetrics,
		config.MergeConfig.BufferSizeForRead,
		config.ParseTimestampConfig.ShortestTimestampLen,
		logFile,
	)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// Enable local metrics for each file handle
	for _, f := range files {
		f.Metrics.Enabled = config.MergeConfig.MetricsTreeEnabled
	}

	// Print progress periodically
	go PrintProgressPeriodically(config.PrintProgressConfig, files, programStartTime)
	defer func() {
		if err == nil {
			PrintProgress(config.PrintProgressConfig, files, programStartTime)
		}
	}()

	writer := bufio.NewWriterSize(outputFile, config.MergeConfig.BufferSizeForWrite)
	defer func() {
		writer.Flush()
		outputFile.Close()
	}()

	// Process files
	err = ProcessFiles(
		config.MergeConfig,
		metrics.MergeMetrics,
		files,
		writer,
		logFile,
		func(file *FileHandle) error {
			return UpdateTimestamp(config.ParseTimestampConfig, file)
		},
	)

	// Final aggregation of localized metrics
	for _, f := range files {
		metrics.MergeMetrics.Merge(f.MergeMetrics)
		metrics.Tree.Merge(f.Metrics)
	}
	metrics.Tree.Root.Metric.Duration = time.Since(programStartTime).Nanoseconds()

	if config.ProfilingEnabled {
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			fmt.Fprintf(config.LogFile, "could not create memory profile: %v\n", err)
		} else {
			defer memFile.Close()
			if err := pprof.WriteHeapProfile(memFile); err != nil {
				fmt.Fprintf(config.LogFile, "could not write memory profile: %v\n", err)
			}
		}
	}

	elapsedTime := time.Since(programStartTime)
	metrics.PrintMetrics(config, programStartTime, elapsedTime, err)

	return err
}

func readFilterFile(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read filter file %s: %w", path, err)
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		lines = append(lines, line)
	}
	return lines, nil
}

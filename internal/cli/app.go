package cli

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/mmdemirbas/logmerge/internal/core"
	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
)

// Version is set at build time via -ldflags="-X ...cli.Version=v1.0.0".
// When empty, "dev" is shown.
var Version = "dev"

// stringSliceFlag implements flag.Value for repeated string flags.
type stringSliceFlag []string

func (s *stringSliceFlag) String() string { return strings.Join(*s, ",") }
func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

// config defines the default configuration
var config = &MainConfig{
	OutputFile: &fsutil.WritableFile{File: os.Stdout},
	LogFile:    &fsutil.WritableFile{File: os.Stderr},

	ProfilingEnabled: false,

	ListFilesConfig: &fsutil.ListFilesConfig{
		IgnorePatterns: []string{},
		FileAliases:    map[string]string{},
	},

	ParseTimestampConfig: &logtime.ParseTimestampConfig{
		IgnoreTimezoneInfo:      false,
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	},

	MergeConfig: &core.MergeConfig{
		MetricsTreeEnabled:    false,
		WriteAliasPerBlock:    false,
		WriteAliasPerLine:     false,
		WriteTimestampPerLine: false,
		MinTimestamp:          logtime.ZeroTimestamp,
		MaxTimestamp:          logtime.Timestamp(1<<63 - 1),
		BufferSizeForRead:     1024 * 1024 * 100,
		BufferSizeForWrite:    1024 * 1024 * 100,
	},

	PrintProgressConfig: &metrics.PrintProgressConfig{
		PrintProgressEnabled: true,
		InitialDelayMillis:   1000,
		PeriodMillis:         1000,
	},
}

var appMetrics *metrics.MainMetrics

// TODO: Catch interrupt signal during merge process and do the post-work anyway

// Run parses CLI flags, loads configuration, and executes the log merge pipeline.
func Run() error {
	// Define CLI flags
	outputFlag := flag.String("out", "", "")
	flag.StringVar(outputFlag, "o", "", "")

	logFlag := flag.String("log", "", "")
	flag.StringVar(logFlag, "l", "", "")

	configFlag := flag.String("config", "", "")

	var inputFlags stringSliceFlag
	flag.Var(&inputFlags, "in", "")
	flag.Var(&inputFlags, "i", "")

	var excludeFlags stringSliceFlag
	flag.Var(&excludeFlags, "exclude", "")
	flag.Var(&excludeFlags, "e", "")

	ignoreFileFlag := flag.String("ignore-file", "", "")
	ignoreArchivesFlag := flag.Bool("ignore-archives", false, "")

	var aliasFlags stringSliceFlag
	flag.Var(&aliasFlags, "alias", "")

	writeTimestamp := flag.Bool("write-timestamp", false, "")
	flag.BoolVar(writeTimestamp, "t", false, "")

	stripTimestamp := flag.Bool("strip-timestamp", false, "")
	flag.BoolVar(stripTimestamp, "s", false, "")

	writeLevel := flag.Bool("write-level", false, "")
	stripLevel := flag.Bool("strip-level", false, "")

	writeBlockAlias := flag.Bool("write-block-alias", false, "")
	flag.BoolVar(writeBlockAlias, "b", false, "")

	writeLineAlias := flag.Bool("write-line-alias", false, "")
	flag.BoolVar(writeLineAlias, "a", false, "")

	sinceFlag := flag.String("since", "", "")
	untilFlag := flag.String("until", "", "")

	ignoreTimezone := flag.Bool("ignore-timezone", false, "")
	minTsLen := flag.Int("min-ts-len", 0, "")
	tsSearchLimit := flag.Int("ts-search-limit", 0, "")

	bufRead := flag.Int("buf-read", 0, "")
	bufWrite := flag.Int("buf-write", 0, "")
	metricsFlag := flag.Bool("metrics", false, "")
	profileFlag := flag.Bool("profile", false, "")

	progressFlag := flag.Bool("progress", false, "")
	flag.BoolVar(progressFlag, "p", false, "")

	dryRunFlag := flag.Bool("dry-run", false, "")
	followSymlinksFlag := flag.Bool("follow-symlinks", false, "")

	completionsFlag := flag.String("completions", "", "")
	versionFlag := flag.Bool("version", false, "")
	flag.BoolVar(versionFlag, "v", false, "")

	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage: %s [flags] <path>...\n", os.Args[0])
		fmt.Fprintf(w, "\nMerge multiple log files into a single chronologically-ordered stream.\n")
		fmt.Fprintf(w, "\nI/O:\n")
		fmt.Fprintf(w, "  -i, --in <path>           Input file or directory (repeatable, added to positional args)\n")
		fmt.Fprintf(w, "  -o, --out <path>          Output file path (default: stdout)\n")
		fmt.Fprintf(w, "  -l, --log <path>          Log/stats file path (default: stderr)\n")
		fmt.Fprintf(w, "\nFiltering:\n")
		fmt.Fprintf(w, "  -e, --exclude <pattern>   Gitignore-style exclude pattern (repeatable)\n")
		fmt.Fprintf(w, "      --ignore-file <path>  File containing exclude patterns (one per line)\n")
		fmt.Fprintf(w, "      --ignore-archives     Auto-ignore archive files (.zip, .gz, .tar, etc.)\n")
		fmt.Fprintf(w, "      --alias <pat>=<name>  File alias mapping (repeatable)\n")
		fmt.Fprintf(w, "\nFormatting:\n")
		fmt.Fprintf(w, "  -t, --write-timestamp     Prepend normalized timestamp to each line\n")
		fmt.Fprintf(w, "  -s, --strip-timestamp     Remove original timestamp from each line\n")
		fmt.Fprintf(w, "      --write-level         Prepend normalized log level (TRACE/DEBUG/INFO/WARN/ERROR/FATAL)\n")
		fmt.Fprintf(w, "      --strip-level         Remove original log level from each line\n")
		fmt.Fprintf(w, "  -b, --write-block-alias   Insert separator when file source changes\n")
		fmt.Fprintf(w, "  -a, --write-line-alias    Prepend file alias to each line\n")
		fmt.Fprintf(w, "\nTime range:\n")
		fmt.Fprintf(w, "      --since <timestamp>   Minimum timestamp, RFC3339 (e.g. 2025-01-17T13:12:00Z)\n")
		fmt.Fprintf(w, "      --until <timestamp>   Maximum timestamp, RFC3339\n")
		fmt.Fprintf(w, "\nParsing:\n")
		fmt.Fprintf(w, "      --ignore-timezone     Ignore timezone info in log timestamps\n")
		fmt.Fprintf(w, "      --min-ts-len <n>      Shortest timestamp length (default: 15)\n")
		fmt.Fprintf(w, "      --ts-search-limit <n> How far into each line to search for timestamps (default: 250)\n")
		fmt.Fprintf(w, "\nPerformance:\n")
		fmt.Fprintf(w, "      --buf-read <bytes>    Read buffer size (default: 100 MB)\n")
		fmt.Fprintf(w, "      --buf-write <bytes>   Write buffer size (default: 100 MB)\n")
		fmt.Fprintf(w, "      --metrics             Enable detailed metrics tree\n")
		fmt.Fprintf(w, "      --profile             Enable CPU/memory profiling\n")
		fmt.Fprintf(w, "\nFile discovery:\n")
		fmt.Fprintf(w, "      --dry-run             List matched files without merging\n")
		fmt.Fprintf(w, "      --follow-symlinks     Follow symbolic links during directory traversal\n")
		fmt.Fprintf(w, "\nDisplay:\n")
		fmt.Fprintf(w, "  -p, --progress            Show progress bar\n")
		fmt.Fprintf(w, "\nSystem:\n")
		fmt.Fprintf(w, "  -v, --version             Print version and exit\n")
		fmt.Fprintf(w, "      --config <path>       Base YAML configuration file (flags override YAML values)\n")
		fmt.Fprintf(w, "      --completions <shell> Generate shell completion script (bash, zsh, fish, powershell)\n")
	}

	flag.Parse()

	// Handle --version early exit
	if *versionFlag {
		fmt.Printf("logmerge %s\n", Version)
		return nil
	}

	// Handle --completions early exit
	if *completionsFlag != "" {
		switch *completionsFlag {
		case "bash":
			generateBashCompletion(os.Stdout)
		case "zsh":
			generateZshCompletion(os.Stdout)
		case "fish":
			generateFishCompletion(os.Stdout)
		case "powershell":
			generatePowershellCompletion(os.Stdout)
		default:
			return fmt.Errorf("unknown shell %q for --completions (supported: bash, zsh, fish, powershell)", *completionsFlag)
		}
		return nil
	}

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
	if explicitlySet["out"] || explicitlySet["o"] {
		if *outputFlag != "" {
			config.OutputFile = &fsutil.WritableFile{Path: *outputFlag}
			if err := config.OutputFile.Initialize(); err != nil {
				return err
			}
		} else {
			config.OutputFile = &fsutil.WritableFile{File: os.Stdout}
		}
	}
	if explicitlySet["log"] || explicitlySet["l"] {
		if *logFlag != "" {
			config.LogFile = &fsutil.WritableFile{Path: *logFlag}
			if err := config.LogFile.Initialize(); err != nil {
				return err
			}
		} else {
			config.LogFile = &fsutil.WritableFile{File: os.Stderr}
		}
	}
	if explicitlySet["write-timestamp"] || explicitlySet["t"] {
		config.MergeConfig.WriteTimestampPerLine = *writeTimestamp
	}
	if explicitlySet["strip-timestamp"] || explicitlySet["s"] {
		config.MergeConfig.StripOriginalTimestamp = *stripTimestamp
	}
	if explicitlySet["write-level"] {
		config.MergeConfig.WriteLevelPerLine = *writeLevel
	}
	if explicitlySet["strip-level"] {
		config.MergeConfig.StripOriginalLevel = *stripLevel
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
		config.MergeConfig.MinTimestamp = logtime.Timestamp(ts.UnixNano())
	}
	if explicitlySet["until"] {
		ts, err := time.Parse(time.RFC3339Nano, *untilFlag)
		if err != nil {
			return fmt.Errorf("failed to parse --until value %q: %w", *untilFlag, err)
		}
		config.MergeConfig.MaxTimestamp = logtime.Timestamp(ts.UnixNano())
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

	// Override ignore-file and ignore-archives from CLI
	if explicitlySet["ignore-file"] {
		config.ListFilesConfig.IgnoreFile = *ignoreFileFlag
	}
	if explicitlySet["ignore-archives"] {
		config.ListFilesConfig.IgnoreArchives = *ignoreArchivesFlag
	}
	if explicitlySet["follow-symlinks"] {
		config.ListFilesConfig.FollowSymlinks = *followSymlinksFlag
	}

	// Append --exclude flags to IgnorePatterns
	config.ListFilesConfig.IgnorePatterns = append(config.ListFilesConfig.IgnorePatterns, excludeFlags...)

	// Parse --alias flags (pattern=alias)
	for _, a := range aliasFlags {
		parts := strings.SplitN(a, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --alias format %q, expected pattern=alias", a)
		}
		config.ListFilesConfig.FileAliases[parts[0]] = parts[1]
	}

	// Positional args + --in flags become input paths
	if args := flag.Args(); len(args) > 0 {
		config.InputPaths = args
	}
	config.InputPaths = append(config.InputPaths, inputFlags...)

	// Read ignore file if provided (from CLI or YAML)
	ignoreFile := config.ListFilesConfig.IgnoreFile
	if ignoreFile != "" {
		lines, err := readFilterFile(ignoreFile)
		if err != nil {
			return err
		}
		// Prepend ignore-file patterns before CLI ignore patterns
		config.ListFilesConfig.IgnorePatterns = append(lines, config.ListFilesConfig.IgnorePatterns...)
	}

	// Expand --ignore-archives into standard archive globs
	if config.ListFilesConfig.IgnoreArchives {
		archiveGlobs := []string{"*.zip", "*.tar", "*.gz", "*.rar", "*.7z", "*.tgz", "*.bz2", "*.tbz2", "*.xz", "*.txz"}
		config.ListFilesConfig.IgnorePatterns = append(config.ListFilesConfig.IgnorePatterns, archiveGlobs...)
	}

	if len(config.InputPaths) == 0 {
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

	appMetrics = metrics.NewMetrics()
	appMetrics.Tree = metrics.NewMetricsTree()
	appMetrics.Tree.Enabled = config.MergeConfig.MetricsTreeEnabled

	files, err := fsutil.ListFiles(
		config.InputPaths,
		config.ListFilesConfig,
		appMetrics.ListFilesMetrics,
		config.MergeConfig.BufferSizeForRead,
		config.ParseTimestampConfig.ShortestTimestampLen,
		logFile,
	)
	if err != nil {
		return fmt.Errorf("failed to list files: %w", err)
	}

	// Handle --dry-run: list matched files and exit
	if *dryRunFlag {
		for _, f := range files {
			fmt.Fprintln(outputFile, f.File.Name())
			f.Close()
		}
		return nil
	}

	// Configure per-file metrics based on MetricsTreeEnabled
	for _, f := range files {
		if config.MergeConfig.MetricsTreeEnabled {
			f.Metrics.Enabled = true
		} else {
			// Nil out to eliminate function-call overhead on disabled metrics
			f.Metrics = nil
			f.MergeMetrics = metrics.NewMergeMetricsLite()
		}
	}

	// Print progress periodically
	progressFiles := make([]metrics.ProgressFile, len(files))
	for i, f := range files {
		progressFiles[i] = f
	}
	go metrics.PrintProgressPeriodically(config.PrintProgressConfig, progressFiles, programStartTime)
	defer func() {
		if err == nil {
			metrics.PrintProgress(config.PrintProgressConfig, progressFiles, programStartTime)
		}
	}()

	writer := bufio.NewWriterSize(outputFile, config.MergeConfig.BufferSizeForWrite)
	defer func() {
		writer.Flush()
		outputFile.Close()
	}()

	// Process files
	err = core.ProcessFiles(
		config.MergeConfig,
		appMetrics.MergeMetrics,
		files,
		writer,
		logFile,
		func(file *fsutil.FileHandle) error {
			return core.UpdateTimestamp(config.ParseTimestampConfig, file, config.MergeConfig.StripOriginalTimestamp)
		},
	)

	// Final aggregation of localized metrics
	for _, f := range files {
		appMetrics.MergeMetrics.Merge(f.MergeMetrics)
		appMetrics.Tree.Merge(f.Metrics)
	}
	appMetrics.Tree.Root.Metric.Duration = time.Since(programStartTime).Nanoseconds()

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
	configYAML, yamlErr := config.ToYAML()
	if yamlErr != nil {
		configYAML = fmt.Sprintf("Failed to convert configuration to YAML: %v", yamlErr)
	}
	appMetrics.PrintMetrics(config.LogFile, configYAML, programStartTime, elapsedTime, err)

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

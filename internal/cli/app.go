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
// parsedFlags holds pointers to all registered CLI flag values.
type parsedFlags struct {
	output         *string
	log            *string
	configFile     *string
	completions    *string
	inputFlags     *stringSliceFlag
	excludeFlags   *stringSliceFlag
	aliasFlags     *stringSliceFlag
	ignoreFile     *string
	since          *string
	until          *string
	writeTimestamp *bool
	stripTimestamp *bool
	writeLevel     *bool
	stripLevel     *bool
	writeBlockAlias *bool
	writeLineAlias  *bool
	ignoreArchives  *bool
	ignoreTimezone  *bool
	followSymlinks  *bool
	version         *bool
	dryRun          *bool
	minTsLen        *int
	tsSearchLimit   *int
	bufRead         *int
	bufWrite        *int
	metrics         *bool
	profile         *bool
	progress        *bool
}

func Run() error {
	pf := defineFlags()
	flag.Parse()

	if *pf.version {
		fmt.Printf("logmerge %s\n", Version)
		return nil
	}
	if *pf.completions != "" {
		return handleCompletions(*pf.completions)
	}

	explicitlySet := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) { explicitlySet[f.Name] = true })
	programStartTime := time.Now()

	if *pf.configFile != "" {
		if err := config.LoadYAML(*pf.configFile); err != nil {
			return fmt.Errorf("failed to load configuration from file %s: %w", *pf.configFile, err)
		}
	}

	if err := applyAllFlags(explicitlySet, pf); err != nil {
		return err
	}

	if len(config.InputPaths) == 0 {
		flag.Usage()
		return fmt.Errorf("no input paths specified")
	}

	return runMerge(programStartTime, *pf.dryRun)
}

// defineFlags registers all CLI flags and returns pointers to their values.
func defineFlags() *parsedFlags {
	pf := &parsedFlags{}
	pf.output = flag.String("out", "", "")
	flag.StringVar(pf.output, "o", "", "")
	pf.log = flag.String("log", "", "")
	flag.StringVar(pf.log, "l", "", "")
	pf.configFile = flag.String("config", "", "")
	pf.completions = flag.String("completions", "", "")
	pf.version = flag.Bool("version", false, "")
	flag.BoolVar(pf.version, "v", false, "")
	pf.dryRun = flag.Bool("dry-run", false, "")
	pf.inputFlags = new(stringSliceFlag)
	flag.Var(pf.inputFlags, "in", "")
	flag.Var(pf.inputFlags, "i", "")
	pf.excludeFlags = new(stringSliceFlag)
	flag.Var(pf.excludeFlags, "exclude", "")
	flag.Var(pf.excludeFlags, "e", "")
	pf.aliasFlags = new(stringSliceFlag)
	flag.Var(pf.aliasFlags, "alias", "")
	pf.ignoreFile = flag.String("ignore-file", "", "")
	pf.ignoreArchives = flag.Bool("ignore-archives", false, "")
	pf.followSymlinks = flag.Bool("follow-symlinks", false, "")
	pf.writeTimestamp = flag.Bool("write-timestamp", false, "")
	flag.BoolVar(pf.writeTimestamp, "t", false, "")
	pf.stripTimestamp = flag.Bool("strip-timestamp", false, "")
	flag.BoolVar(pf.stripTimestamp, "s", false, "")
	pf.writeLevel = flag.Bool("write-level", false, "")
	pf.stripLevel = flag.Bool("strip-level", false, "")
	pf.writeBlockAlias = flag.Bool("write-block-alias", false, "")
	flag.BoolVar(pf.writeBlockAlias, "b", false, "")
	pf.writeLineAlias = flag.Bool("write-line-alias", false, "")
	flag.BoolVar(pf.writeLineAlias, "a", false, "")
	pf.since = flag.String("since", "", "")
	pf.until = flag.String("until", "", "")
	pf.ignoreTimezone = flag.Bool("ignore-timezone", false, "")
	pf.minTsLen = flag.Int("min-ts-len", 0, "")
	pf.tsSearchLimit = flag.Int("ts-search-limit", 0, "")
	pf.bufRead = flag.Int("buf-read", 0, "")
	pf.bufWrite = flag.Int("buf-write", 0, "")
	pf.metrics = flag.Bool("metrics", false, "")
	pf.profile = flag.Bool("profile", false, "")
	pf.progress = flag.Bool("progress", false, "")
	flag.BoolVar(pf.progress, "p", false, "")
	flag.Usage = func() { printUsage(flag.CommandLine.Output()) }
	return pf
}

func printUsage(w interface{ Write([]byte) (int, error) }) {
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

func handleCompletions(shell string) error {
	switch shell {
	case "bash":
		generateBashCompletion(os.Stdout)
	case "zsh":
		generateZshCompletion(os.Stdout)
	case "fish":
		generateFishCompletion(os.Stdout)
	case "powershell":
		generatePowershellCompletion(os.Stdout)
	default:
		return fmt.Errorf("unknown shell %q for --completions (supported: bash, zsh, fish, powershell)", shell)
	}
	return nil
}

// applyAllFlags applies explicitly-set CLI flag values to the global config.
func applyAllFlags(explicitlySet map[string]bool, pf *parsedFlags) error {
	if err := applyFileFlag(explicitlySet, "out", "o", *pf.output, &config.OutputFile, os.Stdout); err != nil {
		return err
	}
	if err := applyFileFlag(explicitlySet, "log", "l", *pf.log, &config.LogFile, os.Stderr); err != nil {
		return err
	}
	if err := applyMergeFlags(explicitlySet, pf); err != nil {
		return err
	}
	applyParseFlags(explicitlySet, pf)
	return applyListFilesFlags(explicitlySet, pf)
}

// applyFileFlag updates *dest to a new WritableFile only when the flag was explicitly set.
func applyFileFlag(explicitlySet map[string]bool, key1, key2, value string, dest **fsutil.WritableFile, defaultFile *os.File) error {
	if !explicitlySet[key1] && !explicitlySet[key2] {
		return nil
	}
	if value != "" {
		*dest = &fsutil.WritableFile{Path: value}
		return (*dest).Initialize()
	}
	*dest = &fsutil.WritableFile{File: defaultFile}
	return nil
}

// overrideBool sets *dest = value when key1 or key2 appears in explicitlySet.
func overrideBool(explicitlySet map[string]bool, key1, key2 string, value bool, dest *bool) {
	if explicitlySet[key1] || explicitlySet[key2] {
		*dest = value
	}
}

// overrideInt sets *dest = value when key appears in explicitlySet.
func overrideInt(explicitlySet map[string]bool, key string, value int, dest *int) {
	if explicitlySet[key] {
		*dest = value
	}
}

func applyMergeFlags(explicitlySet map[string]bool, pf *parsedFlags) error {
	c := config.MergeConfig
	overrideBool(explicitlySet, "write-timestamp", "t", *pf.writeTimestamp, &c.WriteTimestampPerLine)
	overrideBool(explicitlySet, "strip-timestamp", "s", *pf.stripTimestamp, &c.StripOriginalTimestamp)
	overrideBool(explicitlySet, "write-level", "", *pf.writeLevel, &c.WriteLevelPerLine)
	overrideBool(explicitlySet, "strip-level", "", *pf.stripLevel, &c.StripOriginalLevel)
	overrideBool(explicitlySet, "write-block-alias", "b", *pf.writeBlockAlias, &c.WriteAliasPerBlock)
	overrideBool(explicitlySet, "write-line-alias", "a", *pf.writeLineAlias, &c.WriteAliasPerLine)
	overrideBool(explicitlySet, "metrics", "", *pf.metrics, &c.MetricsTreeEnabled)
	overrideBool(explicitlySet, "profile", "", *pf.profile, &config.ProfilingEnabled)
	overrideBool(explicitlySet, "progress", "p", *pf.progress, &config.PrintProgressConfig.PrintProgressEnabled)
	overrideInt(explicitlySet, "buf-read", *pf.bufRead, &c.BufferSizeForRead)
	overrideInt(explicitlySet, "buf-write", *pf.bufWrite, &c.BufferSizeForWrite)
	if explicitlySet["since"] {
		ts, err := time.Parse(time.RFC3339Nano, *pf.since)
		if err != nil {
			return fmt.Errorf("failed to parse --since value %q: %w", *pf.since, err)
		}
		c.MinTimestamp = logtime.Timestamp(ts.UnixNano())
	}
	if explicitlySet["until"] {
		ts, err := time.Parse(time.RFC3339Nano, *pf.until)
		if err != nil {
			return fmt.Errorf("failed to parse --until value %q: %w", *pf.until, err)
		}
		c.MaxTimestamp = logtime.Timestamp(ts.UnixNano())
	}
	return nil
}

func applyParseFlags(explicitlySet map[string]bool, pf *parsedFlags) {
	c := config.ParseTimestampConfig
	overrideBool(explicitlySet, "ignore-timezone", "", *pf.ignoreTimezone, &c.IgnoreTimezoneInfo)
	overrideInt(explicitlySet, "min-ts-len", *pf.minTsLen, &c.ShortestTimestampLen)
	overrideInt(explicitlySet, "ts-search-limit", *pf.tsSearchLimit, &c.TimestampSearchEndIndex)
}

func applyListFilesFlags(explicitlySet map[string]bool, pf *parsedFlags) error {
	c := config.ListFilesConfig
	overrideBool(explicitlySet, "ignore-archives", "", *pf.ignoreArchives, &c.IgnoreArchives)
	overrideBool(explicitlySet, "follow-symlinks", "", *pf.followSymlinks, &c.FollowSymlinks)
	if explicitlySet["ignore-file"] {
		c.IgnoreFile = *pf.ignoreFile
	}
	c.IgnorePatterns = append(c.IgnorePatterns, []string(*pf.excludeFlags)...)
	for _, a := range []string(*pf.aliasFlags) {
		parts := strings.SplitN(a, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid --alias format %q, expected pattern=alias", a)
		}
		c.FileAliases[parts[0]] = parts[1]
	}
	if args := flag.Args(); len(args) > 0 {
		config.InputPaths = args
	}
	config.InputPaths = append(config.InputPaths, []string(*pf.inputFlags)...)
	if c.IgnoreFile != "" {
		lines, err := readFilterFile(c.IgnoreFile)
		if err != nil {
			return err
		}
		c.IgnorePatterns = append(lines, c.IgnorePatterns...)
	}
	if c.IgnoreArchives {
		archiveGlobs := []string{"*.zip", "*.tar", "*.gz", "*.rar", "*.7z", "*.tgz", "*.bz2", "*.tbz2", "*.xz", "*.txz"}
		c.IgnorePatterns = append(c.IgnorePatterns, archiveGlobs...)
	}
	return nil
}

// runMerge executes the merge: list files, optionally dry-run, process, and print metrics.
func runMerge(programStartTime time.Time, dryRun bool) (err error) {
	if config.ProfilingEnabled {
		stop := startCPUProfile(config.LogFile)
		defer stop()
	}

	outputFile := config.OutputFile
	logFile := config.LogFile

	appMetrics = metrics.NewMetrics()
	appMetrics.Tree = metrics.NewMetricsTree()
	appMetrics.Tree.Enabled = config.MergeConfig.MetricsTreeEnabled

	files, listErr := fsutil.ListFiles(
		config.InputPaths,
		config.ListFilesConfig,
		appMetrics.ListFilesMetrics,
		config.MergeConfig.BufferSizeForRead,
		config.ParseTimestampConfig.ShortestTimestampLen,
		logFile,
	)
	if listErr != nil {
		return fmt.Errorf("failed to list files: %w", listErr)
	}

	if dryRun {
		for _, f := range files {
			fmt.Fprintln(outputFile, f.File.Name())
			_ = f.Close()
		}
		return nil
	}

	configureFileMetrics(files, config.MergeConfig.MetricsTreeEnabled)

	progressFiles := toProgressFiles(files)
	go metrics.PrintProgressPeriodically(config.PrintProgressConfig, progressFiles, programStartTime)
	defer func() {
		if err == nil {
			metrics.PrintProgress(config.PrintProgressConfig, progressFiles, programStartTime)
		}
	}()

	writer := bufio.NewWriterSize(outputFile, config.MergeConfig.BufferSizeForWrite)
	defer func() {
		_ = writer.Flush()
		_ = outputFile.Close()
	}()

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

	for _, f := range files {
		appMetrics.MergeMetrics.Merge(f.MergeMetrics)
		appMetrics.Tree.Merge(f.Metrics)
	}
	appMetrics.Tree.Root.Metric.Duration = time.Since(programStartTime).Nanoseconds()

	if config.ProfilingEnabled {
		writeMemProfile(config.LogFile)
	}

	elapsedTime := time.Since(programStartTime)
	configYAML, yamlErr := config.ToYAML()
	if yamlErr != nil {
		configYAML = fmt.Sprintf("Failed to convert configuration to YAML: %v", yamlErr)
	}
	appMetrics.PrintMetrics(config.LogFile, configYAML, programStartTime, elapsedTime, err)

	return err
}

// startCPUProfile starts CPU profiling and returns a stop function.
func startCPUProfile(logFile *fsutil.WritableFile) func() {
	cpuFile, err := os.Create("out/cpu.prof")
	if err != nil {
		fmt.Fprintf(logFile, "could not create CPU profile: %v\n", err)
		return func() {}
	}
	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		fmt.Fprintf(logFile, "could not start CPU profile: %v\n", err)
		_ = cpuFile.Close()
		return func() {}
	}
	return func() {
		pprof.StopCPUProfile()
		_ = cpuFile.Close()
	}
}

// writeMemProfile writes a heap profile to out/mem.prof.
func writeMemProfile(logFile *fsutil.WritableFile) {
	memFile, err := os.Create("out/mem.prof")
	if err != nil {
		fmt.Fprintf(logFile, "could not create memory profile: %v\n", err)
		return
	}
	defer func() { _ = memFile.Close() }()
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		fmt.Fprintf(logFile, "could not write memory profile: %v\n", err)
	}
}

// configureFileMetrics enables or disables per-file metrics trees.
// When metrics are disabled, the Metrics field is nilled to eliminate
// function-call overhead on disabled metrics.
func configureFileMetrics(files []*fsutil.FileHandle, metricsEnabled bool) {
	for _, f := range files {
		if metricsEnabled {
			f.Metrics.Enabled = true
		} else {
			f.Metrics = nil
			f.MergeMetrics = metrics.NewMergeMetricsLite()
		}
	}
}

// toProgressFiles converts a slice of FileHandle to the ProgressFile interface slice.
func toProgressFiles(files []*fsutil.FileHandle) []metrics.ProgressFile {
	progressFiles := make([]metrics.ProgressFile, len(files))
	for i, f := range files {
		progressFiles[i] = f
	}
	return progressFiles
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

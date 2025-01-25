package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

// TODO: Streamline config infra. Now it requires 6 changes to add a single field.

// Effective configuration values after loading the config file.
var (
	InputPath string
	Stdout    *os.File
	Stderr    *os.File

	EnableMetricsCollection bool
	EnableProfiling         bool

	WriteSourceNamesPerBlock bool
	WriteSourceNamesPerLine  bool
	WriteTimestampPerLine    bool

	MinTimestamp       Timestamp
	MaxTimestamp       Timestamp
	IgnoreTimezoneInfo bool

	ShortestTimestampLen    int
	TimestampSearchEndIndex int

	BufferSizeForRead  int
	BufferSizeForWrite int

	ExcludedStrictSuffixes  []string
	IncludedStrictSuffixes  []string
	ExcludedLenientSuffixes []string
	IncludedLenientSuffixes []string

	SourceNameAliases map[string]string
)

// defaultConfig Defaults set here. Overrides loaded from config file.
var defaultConfig = AppConfig{
	InputPath:  "",
	OutputPath: "out/stdout.log",
	LogPath:    "out/stderr.log",

	EnableMetricsCollection: true,
	EnableProfiling:         false,

	WriteSourceNamesPerBlock: true,
	WriteSourceNamesPerLine:  false,
	WriteTimestampPerLine:    false,

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

	SourceNameAliases: map[string]string{},
}

type AppConfig struct {
	InputPath  string
	OutputPath string
	LogPath    string

	EnableMetricsCollection bool
	EnableProfiling         bool

	WriteSourceNamesPerBlock bool
	WriteSourceNamesPerLine  bool
	WriteTimestampPerLine    bool

	MinTimestamp       Timestamp
	MaxTimestamp       Timestamp
	IgnoreTimezoneInfo bool

	ShortestTimestampLen    int
	TimestampSearchEndIndex int

	BufferSizeForRead  int
	BufferSizeForWrite int

	ExcludedStrictSuffixes  []string
	IncludedStrictSuffixes  []string
	ExcludedLenientSuffixes []string
	IncludedLenientSuffixes []string

	SourceNameAliases map[string]string
}

type YamlConfig struct {
	InputPath  *string `yaml:"InputPath"`
	OutputPath *string `yaml:"OutputPath"`
	LogPath    *string `yaml:"LogPath"`

	EnableMetricsCollection *bool `yaml:"EnableMetricsCollection"`
	EnableProfiling         *bool `yaml:"EnableProfiling"`

	WriteSourceNamesPerBlock *bool `yaml:"WriteSourceNamesPerBlock"`
	WriteSourceNamesPerLine  *bool `yaml:"WriteSourceNamesPerLine"`
	WriteTimestampPerLine    *bool `yaml:"WriteTimestampPerLine"`

	MinTimestamp       *string `yaml:"MinTimestamp"`
	MaxTimestamp       *string `yaml:"MaxTimestamp"`
	IgnoreTimezoneInfo *bool   `yaml:"IgnoreTimezoneInfo"`

	ShortestTimestampLen    *int `yaml:"ShortestTimestampLen"`
	TimestampSearchEndIndex *int `yaml:"TimestampSearchEndIndex"`

	BufferSizeForRead  *int `yaml:"BufferSizeForRead"`
	BufferSizeForWrite *int `yaml:"BufferSizeForWrite"`

	ExcludedStrictSuffixes  *[]string `yaml:"ExcludedStrictSuffixes"`
	IncludedStrictSuffixes  *[]string `yaml:"IncludedStrictSuffixes"`
	ExcludedLenientSuffixes *[]string `yaml:"ExcludedLenientSuffixes"`
	IncludedLenientSuffixes *[]string `yaml:"IncludedLenientSuffixes"`

	SourceNameAliases *map[string]string `yaml:"SourceNameAliases"`
}

func loadConfigFromYaml(yamlPath string) error {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", yamlPath, err)
	}

	yamlConfig := &YamlConfig{}
	err = yaml.Unmarshal(data, yamlConfig)
	if err != nil {
		return fmt.Errorf("failed to unmarshal yaml file %s: %w", yamlPath, err)
	}

	if yamlConfig.InputPath != nil {
		defaultConfig.InputPath = *yamlConfig.InputPath
	}

	if yamlConfig.OutputPath != nil {
		defaultConfig.OutputPath = *yamlConfig.OutputPath
	}

	if yamlConfig.LogPath != nil {
		defaultConfig.LogPath = *yamlConfig.LogPath
	}

	if yamlConfig.EnableMetricsCollection != nil {
		defaultConfig.EnableMetricsCollection = *yamlConfig.EnableMetricsCollection
	}

	if yamlConfig.EnableProfiling != nil {
		defaultConfig.EnableProfiling = *yamlConfig.EnableProfiling
	}

	if yamlConfig.WriteSourceNamesPerBlock != nil {
		defaultConfig.WriteSourceNamesPerBlock = *yamlConfig.WriteSourceNamesPerBlock
	}

	if yamlConfig.WriteSourceNamesPerLine != nil {
		defaultConfig.WriteSourceNamesPerLine = *yamlConfig.WriteSourceNamesPerLine
	}

	if yamlConfig.WriteTimestampPerLine != nil {
		defaultConfig.WriteTimestampPerLine = *yamlConfig.WriteTimestampPerLine
	}

	if yamlConfig.MinTimestamp != nil {
		ts, err := NewTimestampFromString(*yamlConfig.MinTimestamp)
		if err != nil {
			return fmt.Errorf("failed to parse MinTimestamp from file %s: %w", yamlPath, err)
		}
		defaultConfig.MinTimestamp = ts
	}

	if yamlConfig.MaxTimestamp != nil {
		ts, err := NewTimestampFromString(*yamlConfig.MaxTimestamp)
		if err != nil {
			return fmt.Errorf("failed to parse MaxTimestamp: from file %s: %w", yamlPath, err)
		}
		defaultConfig.MaxTimestamp = ts
	}

	if yamlConfig.IgnoreTimezoneInfo != nil {
		defaultConfig.IgnoreTimezoneInfo = *yamlConfig.IgnoreTimezoneInfo
	}

	if yamlConfig.ShortestTimestampLen != nil {
		defaultConfig.ShortestTimestampLen = *yamlConfig.ShortestTimestampLen
	}

	if yamlConfig.TimestampSearchEndIndex != nil {
		defaultConfig.TimestampSearchEndIndex = *yamlConfig.TimestampSearchEndIndex
	}

	if yamlConfig.BufferSizeForRead != nil {
		defaultConfig.BufferSizeForRead = *yamlConfig.BufferSizeForRead
	}

	if yamlConfig.BufferSizeForWrite != nil {
		defaultConfig.BufferSizeForWrite = *yamlConfig.BufferSizeForWrite
	}

	if yamlConfig.ExcludedStrictSuffixes != nil {
		defaultConfig.ExcludedStrictSuffixes = *yamlConfig.ExcludedStrictSuffixes
	}

	if yamlConfig.IncludedStrictSuffixes != nil {
		defaultConfig.IncludedStrictSuffixes = *yamlConfig.IncludedStrictSuffixes
	}

	if yamlConfig.ExcludedLenientSuffixes != nil {
		defaultConfig.ExcludedLenientSuffixes = *yamlConfig.ExcludedLenientSuffixes
	}

	if yamlConfig.IncludedLenientSuffixes != nil {
		defaultConfig.IncludedLenientSuffixes = *yamlConfig.IncludedLenientSuffixes
	}

	if yamlConfig.SourceNameAliases != nil {
		defaultConfig.SourceNameAliases = *yamlConfig.SourceNameAliases
	}

	return LoadConfigValuesToVariables()
}

func LoadConfigValuesToVariables() error {
	InputPath = defaultConfig.InputPath

	f, err := createFile(defaultConfig.OutputPath, os.Stdout)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	Stdout = f

	f, err = createFile(defaultConfig.LogPath, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	Stderr = f

	EnableMetricsCollection = defaultConfig.EnableMetricsCollection
	EnableProfiling = defaultConfig.EnableProfiling
	WriteSourceNamesPerBlock = defaultConfig.WriteSourceNamesPerBlock
	WriteSourceNamesPerLine = defaultConfig.WriteSourceNamesPerLine
	WriteTimestampPerLine = defaultConfig.WriteTimestampPerLine
	MinTimestamp = defaultConfig.MinTimestamp
	MaxTimestamp = defaultConfig.MaxTimestamp
	IgnoreTimezoneInfo = defaultConfig.IgnoreTimezoneInfo
	ShortestTimestampLen = defaultConfig.ShortestTimestampLen
	TimestampSearchEndIndex = defaultConfig.TimestampSearchEndIndex
	BufferSizeForRead = defaultConfig.BufferSizeForRead
	BufferSizeForWrite = defaultConfig.BufferSizeForWrite
	ExcludedStrictSuffixes = defaultConfig.ExcludedStrictSuffixes
	IncludedStrictSuffixes = defaultConfig.IncludedStrictSuffixes
	ExcludedLenientSuffixes = defaultConfig.ExcludedLenientSuffixes
	IncludedLenientSuffixes = defaultConfig.IncludedLenientSuffixes
	SourceNameAliases = defaultConfig.SourceNameAliases

	return nil
}

func createFile(path string, fallback *os.File) (*os.File, error) {
	if path == "" {
		return fallback, nil
	}

	err := os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create directory for file %s: %v", path, err)
	}

	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("could not create file %s: %v", path, err)
	}

	return f, nil
}

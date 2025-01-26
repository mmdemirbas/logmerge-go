package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type AppConfig struct {
	OutputFile *os.File
	LogFile    *os.File

	ProfilingEnabled bool

	ListFilesConfig      *ListFilesConfig
	ParseTimestampConfig *ParseTimestampConfig
	MergeConfig          *MergeConfig
}

// TODO: Reflect the same nested config structure in YAML

type YamlConfig struct {
	OutputPath       *string `yaml:"OutputPath"`
	LogPath          *string `yaml:"LogPath"`
	ProfilingEnabled *bool   `yaml:"ProfilingEnabled"`

	InputPath               *string            `yaml:"InputPath"`
	ExcludedStrictSuffixes  *[]string          `yaml:"ExcludedStrictSuffixes"`
	IncludedStrictSuffixes  *[]string          `yaml:"IncludedStrictSuffixes"`
	ExcludedLenientSuffixes *[]string          `yaml:"ExcludedLenientSuffixes"`
	IncludedLenientSuffixes *[]string          `yaml:"IncludedLenientSuffixes"`
	FileAliases             *map[string]string `yaml:"FileAliases"`

	ShortestTimestampLen    *int  `yaml:"ShortestTimestampLen"`
	IgnoreTimezoneInfo      *bool `yaml:"IgnoreTimezoneInfo"`
	TimestampSearchEndIndex *int  `yaml:"TimestampSearchEndIndex"`

	MetricsTreeEnabled    *bool   `yaml:"MetricsTreeEnabled"`
	WriteAliasPerBlock    *bool   `yaml:"WriteAliasPerBlock"`
	WriteAliasPerLine     *bool   `yaml:"WriteAliasPerLine"`
	WriteTimestampPerLine *bool   `yaml:"WriteTimestampPerLine"`
	MinTimestamp          *string `yaml:"MinTimestamp"`
	MaxTimestamp          *string `yaml:"MaxTimestamp"`
	BufferSizeForRead     *int    `yaml:"BufferSizeForRead"`
	BufferSizeForWrite    *int    `yaml:"BufferSizeForWrite"`
}

func NewYamlConfig(yamlPath string) (*YamlConfig, error) {
	c := &YamlConfig{}

	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", yamlPath, err)
	}

	err = yaml.Unmarshal(data, &c)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal yaml file %s: %w", yamlPath, err)
	}

	return c, nil
}

func (c *AppConfig) LoadAppConfig(yml *YamlConfig) error {

	// OutputPath -> OutputFile
	outputFile, err := createFile(yml.OutputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	if outputFile != nil {
		c.OutputFile = outputFile
	}

	// LogPath -> LogFile
	logFile, err := createFile(yml.LogPath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	if logFile != nil {
		c.LogFile = logFile
	}

	// MetricsTreeEnabled
	if yml.MetricsTreeEnabled != nil {
		c.MergeConfig.MetricsTreeEnabled = *yml.MetricsTreeEnabled
	}

	// ProfilingEnabled
	if yml.ProfilingEnabled != nil {
		c.ProfilingEnabled = *yml.ProfilingEnabled
	}

	// WriteAliasPerBlock
	if yml.WriteAliasPerBlock != nil {
		c.MergeConfig.WriteAliasPerBlock = *yml.WriteAliasPerBlock
	}

	// WriteAliasPerLine
	if yml.WriteAliasPerLine != nil {
		c.MergeConfig.WriteAliasPerLine = *yml.WriteAliasPerLine
	}

	// WriteTimestampPerLine
	if yml.WriteTimestampPerLine != nil {
		c.MergeConfig.WriteTimestampPerLine = *yml.WriteTimestampPerLine
	}

	// MinTimestamp
	if yml.MinTimestamp != nil {
		ts, err := NewTimestampFromString(*yml.MinTimestamp)
		if err != nil {
			return fmt.Errorf("failed to parse MinTimestamp <%s>: %w", *yml.MinTimestamp, err)
		}
		c.MergeConfig.MinTimestamp = ts
	}

	// MaxTimestamp
	if yml.MaxTimestamp != nil {
		ts, err := NewTimestampFromString(*yml.MaxTimestamp)
		if err != nil {
			return fmt.Errorf("failed to parse MaxTimestamp <%s>: %w", *yml.MaxTimestamp, err)
		}
		c.MergeConfig.MaxTimestamp = ts
	}

	// IgnoreTimezoneInfo
	if yml.IgnoreTimezoneInfo != nil {
		c.ParseTimestampConfig.IgnoreTimezoneInfo = *yml.IgnoreTimezoneInfo
	}

	// ShortestTimestampLen
	if yml.ShortestTimestampLen != nil {
		c.ParseTimestampConfig.ShortestTimestampLen = *yml.ShortestTimestampLen
	}

	// TimestampSearchEndIndex
	if yml.TimestampSearchEndIndex != nil {
		c.ParseTimestampConfig.TimestampSearchEndIndex = *yml.TimestampSearchEndIndex
	}

	// BufferSizeForRead
	if yml.BufferSizeForRead != nil {
		c.MergeConfig.BufferSizeForRead = *yml.BufferSizeForRead
	}

	// BufferSizeForWrite
	if yml.BufferSizeForWrite != nil {
		c.MergeConfig.BufferSizeForWrite = *yml.BufferSizeForWrite
	}

	lfc := c.ListFilesConfig
	if lfc == nil {
		lfc = &ListFilesConfig{}
		c.ListFilesConfig = lfc
	}

	// InputPath
	if yml.InputPath != nil {
		lfc.InputPath = *yml.InputPath
	}

	// ExcludedStrictSuffixes
	if yml.ExcludedStrictSuffixes != nil {
		lfc.ExcludedStrictSuffixes = *yml.ExcludedStrictSuffixes
	}

	// IncludedStrictSuffixes
	if yml.IncludedStrictSuffixes != nil {
		lfc.IncludedStrictSuffixes = *yml.IncludedStrictSuffixes
	}

	// ExcludedLenientSuffixes
	if yml.ExcludedLenientSuffixes != nil {
		lfc.ExcludedLenientSuffixes = *yml.ExcludedLenientSuffixes
	}

	// IncludedLenientSuffixes
	if yml.IncludedLenientSuffixes != nil {
		lfc.IncludedLenientSuffixes = *yml.IncludedLenientSuffixes
	}

	// FileAliases
	if yml.FileAliases != nil {
		lfc.FileAliases = *yml.FileAliases
	}

	return nil
}

func createFile(path *string) (*os.File, error) {
	if path == nil || *path == "" {
		return nil, nil
	}

	err := os.MkdirAll(filepath.Dir(*path), os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create directory for file %s: %v", *path, err)
	}

	f, err := os.Create(*path)
	if err != nil {
		return nil, fmt.Errorf("could not create file %s: %v", *path, err)
	}

	return f, nil
}

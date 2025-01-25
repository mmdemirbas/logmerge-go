package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type AppConfig struct {
	InputPath  string
	OutputPath string
	LogPath    string

	Stdout *os.File
	Stderr *os.File

	EnableMetricsCollection bool
	EnableProfiling         bool

	WriteAliasPerBlock    bool
	WriteAliasPerLine     bool
	WriteTimestampPerLine bool

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

	FileAliases map[string]string
}

type YamlConfig struct {
	InputPath  *string `yaml:"InputPath"`
	OutputPath *string `yaml:"OutputPath"`
	LogPath    *string `yaml:"LogPath"`

	EnableMetricsCollection *bool `yaml:"EnableMetricsCollection"`
	EnableProfiling         *bool `yaml:"EnableProfiling"`

	WriteAliasPerBlock    *bool `yaml:"WriteAliasPerBlock"`
	WriteAliasPerLine     *bool `yaml:"WriteAliasPerLine"`
	WriteTimestampPerLine *bool `yaml:"WriteTimestampPerLine"`

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

	FileAliases *map[string]string `yaml:"FileAliases"`
}

func (c *YamlConfig) LoadYamlConfig(yamlPath string) error {
	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", yamlPath, err)
	}

	err = yaml.Unmarshal(data, &c)
	if err != nil {
		return fmt.Errorf("failed to unmarshal yaml file %s: %w", yamlPath, err)
	}

	return nil
}

func (c *AppConfig) LoadAppConfig(yamlConfig *YamlConfig) error {
	// InputPath
	if yamlConfig.InputPath != nil {
		c.InputPath = *yamlConfig.InputPath
	}

	// OutputPath -> Stdout
	if yamlConfig.OutputPath != nil {
		c.OutputPath = *yamlConfig.OutputPath
	}
	f, err := createFile(c.OutputPath, os.Stdout)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	c.Stdout = f

	// LogPath -> Stderr
	if yamlConfig.LogPath != nil {
		c.LogPath = *yamlConfig.LogPath
	}
	f, err = createFile(c.LogPath, os.Stderr)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	c.Stderr = f

	// EnableMetricsCollection
	if yamlConfig.EnableMetricsCollection != nil {
		c.EnableMetricsCollection = *yamlConfig.EnableMetricsCollection
	}

	// EnableProfiling
	if yamlConfig.EnableProfiling != nil {
		c.EnableProfiling = *yamlConfig.EnableProfiling
	}

	// WriteAliasPerBlock
	if yamlConfig.WriteAliasPerBlock != nil {
		c.WriteAliasPerBlock = *yamlConfig.WriteAliasPerBlock
	}

	// WriteAliasPerLine
	if yamlConfig.WriteAliasPerLine != nil {
		c.WriteAliasPerLine = *yamlConfig.WriteAliasPerLine
	}

	// WriteTimestampPerLine
	if yamlConfig.WriteTimestampPerLine != nil {
		c.WriteTimestampPerLine = *yamlConfig.WriteTimestampPerLine
	}

	// MinTimestamp
	if yamlConfig.MinTimestamp != nil {
		ts, err := NewTimestampFromString(*yamlConfig.MinTimestamp)
		if err != nil {
			return fmt.Errorf("failed to parse MinTimestamp <%s>: %w", *yamlConfig.MinTimestamp, err)
		}
		c.MinTimestamp = ts
	}

	// MaxTimestamp
	if yamlConfig.MaxTimestamp != nil {
		ts, err := NewTimestampFromString(*yamlConfig.MaxTimestamp)
		if err != nil {
			return fmt.Errorf("failed to parse MaxTimestamp <%s>: %w", *yamlConfig.MaxTimestamp, err)
		}
		c.MaxTimestamp = ts
	}

	// IgnoreTimezoneInfo
	if yamlConfig.IgnoreTimezoneInfo != nil {
		c.IgnoreTimezoneInfo = *yamlConfig.IgnoreTimezoneInfo
	}

	// ShortestTimestampLen
	if yamlConfig.ShortestTimestampLen != nil {
		c.ShortestTimestampLen = *yamlConfig.ShortestTimestampLen
	}

	// TimestampSearchEndIndex
	if yamlConfig.TimestampSearchEndIndex != nil {
		c.TimestampSearchEndIndex = *yamlConfig.TimestampSearchEndIndex
	}

	// BufferSizeForRead
	if yamlConfig.BufferSizeForRead != nil {
		c.BufferSizeForRead = *yamlConfig.BufferSizeForRead
	}

	// BufferSizeForWrite
	if yamlConfig.BufferSizeForWrite != nil {
		c.BufferSizeForWrite = *yamlConfig.BufferSizeForWrite
	}

	// ExcludedStrictSuffixes
	if yamlConfig.ExcludedStrictSuffixes != nil {
		c.ExcludedStrictSuffixes = *yamlConfig.ExcludedStrictSuffixes
	}

	// IncludedStrictSuffixes
	if yamlConfig.IncludedStrictSuffixes != nil {
		c.IncludedStrictSuffixes = *yamlConfig.IncludedStrictSuffixes
	}

	// ExcludedLenientSuffixes
	if yamlConfig.ExcludedLenientSuffixes != nil {
		c.ExcludedLenientSuffixes = *yamlConfig.ExcludedLenientSuffixes
	}

	// IncludedLenientSuffixes
	if yamlConfig.IncludedLenientSuffixes != nil {
		c.IncludedLenientSuffixes = *yamlConfig.IncludedLenientSuffixes
	}

	// FileAliases
	if yamlConfig.FileAliases != nil {
		c.FileAliases = *yamlConfig.FileAliases
	}

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

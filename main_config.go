package main

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

type MainConfig struct {
	OutputFile           *WritableFile         `yaml:"OutputFile"`
	LogFile              *WritableFile         `yaml:"LogFile"`
	ProfilingEnabled     bool                  `yaml:"ProfilingEnabled"`
	ListFilesConfig      *ListFilesConfig      `yaml:"ListFilesConfig"`
	ParseTimestampConfig *ParseTimestampConfig `yaml:"ParseTimestampConfig"`
	MergeConfig          *MergeConfig          `yaml:"MergeConfig"`
	PrintProgressConfig  *PrintProgressConfig  `yaml:"PrintProgressConfig"`
}

func (c *MainConfig) LoadYAML(yamlPath string) error {
	file, err := os.ReadFile(yamlPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	err = yaml.Unmarshal(file, c)
	if err != nil {
		return fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	return nil
}

func (c *MainConfig) ToYAML() (string, error) {
	data, err := yaml.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to marshal YAML: %w", err)
	}
	return string(data), nil
}

// region: WritableFile

type WritableFile struct {
	File *os.File
}

func (f *WritableFile) Write(p []byte) (n int, err error) {
	return f.File.Write(p)
}

func (f *WritableFile) Close() error {
	return f.File.Close()
}

func (f *WritableFile) MarshalYAML() (interface{}, error) {
	if f.File == nil {
		return "", nil
	}
	return f.File.Name(), nil
}

func (f *WritableFile) UnmarshalYAML(value *yaml.Node) error {
	var path string
	if err := value.Decode(&path); err != nil {
		return fmt.Errorf("failed to decode file path: %w", err)
	}

	if path == "" {
		// leave default value
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(path), os.ModePerm); err != nil {
		return fmt.Errorf("could not create directory for %s: %v", path, err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("could not create file %s: %v", path, err)
	}

	f.File = file
	return nil
}

// endregion: WritableFile

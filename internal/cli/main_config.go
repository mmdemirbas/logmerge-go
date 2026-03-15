package cli

import (
	"fmt"
	"os"

	"github.com/mmdemirbas/logmerge/internal/core"
	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
	"gopkg.in/yaml.v3"
)

type MainConfig struct {
	InputPaths           []string                      `yaml:"InputPaths"`
	OutputFile           *fsutil.WritableFile          `yaml:"OutputFile"`
	LogFile              *fsutil.WritableFile          `yaml:"LogFile"`
	ProfilingEnabled     bool                          `yaml:"ProfilingEnabled"`
	ListFilesConfig      *fsutil.ListFilesConfig       `yaml:"ListFilesConfig"`
	ParseTimestampConfig *logtime.ParseTimestampConfig `yaml:"ParseTimestampConfig"`
	MergeConfig          *core.MergeConfig             `yaml:"MergeConfig"`
	PrintProgressConfig  *metrics.PrintProgressConfig  `yaml:"PrintProgressConfig"`
}

// LoadYAML reads a YAML configuration file and unmarshals it into c,
// initializing output and log file handles.
func (c *MainConfig) LoadYAML(yamlPath string) error {
	file, err := os.ReadFile(yamlPath)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	err = yaml.Unmarshal(file, c)
	if err != nil {
		return fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	if err := c.OutputFile.Initialize(); err != nil {
		return err
	}
	if err := c.LogFile.Initialize(); err != nil {
		return err
	}

	return nil
}

// ToYAML serializes the configuration to a YAML string.
func (c *MainConfig) ToYAML() (yamlStr string, err error) {
	data, err := yaml.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("failed to marshal YAML: %w", err)
	}
	return string(data), nil
}

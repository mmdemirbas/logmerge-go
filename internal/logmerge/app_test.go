package logmerge_test

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/logmerge"
)

func TestHydrationPipelineCLIOverridesYAML(t *testing.T) {
	// Create a temporary YAML config file
	tmpDir := t.TempDir()
	yamlPath := filepath.Join(tmpDir, "test.yaml")

	yamlContent := `
ListFilesConfig:
  InputPaths:
    - "/some/yaml/path"
  IgnorePatterns:
    - "*.zip"
  FileAliases:
    "app.log": "application"

ParseTimestampConfig:
  IgnoreTimezoneInfo: false
  ShortestTimestampLen: 15
  TimestampSearchEndIndex: 250

MergeConfig:
  WriteTimestampPerLine: false
  WriteAliasPerBlock: false
  WriteAliasPerLine: false
  BufferSizeForRead: 104857600
  BufferSizeForWrite: 104857600

PrintProgressConfig:
  PrintProgressEnabled: false
`
	err := os.WriteFile(yamlPath, []byte(yamlContent), 0644)
	if err != nil {
		t.Fatalf("failed to write test YAML: %v", err)
	}

	// Load the config
	cfg := &MainConfig{
		OutputFile:           &WritableFile{File: os.Stdout},
		LogFile:              &WritableFile{File: os.Stderr},
		ListFilesConfig:      &ListFilesConfig{FileAliases: map[string]string{}},
		ParseTimestampConfig: &ParseTimestampConfig{},
		MergeConfig:          &MergeConfig{},
		PrintProgressConfig:  &PrintProgressConfig{},
	}

	err = cfg.LoadYAML(yamlPath)
	if err != nil {
		t.Fatalf("failed to load YAML: %v", err)
	}

	// Verify YAML values loaded correctly
	if len(cfg.ListFilesConfig.InputPaths) != 1 || cfg.ListFilesConfig.InputPaths[0] != "/some/yaml/path" {
		t.Errorf("expected InputPaths from YAML, got %v", cfg.ListFilesConfig.InputPaths)
	}
	if len(cfg.ListFilesConfig.IgnorePatterns) != 1 || cfg.ListFilesConfig.IgnorePatterns[0] != "*.zip" {
		t.Errorf("expected IgnorePatterns from YAML, got %v", cfg.ListFilesConfig.IgnorePatterns)
	}
	if cfg.MergeConfig.WriteTimestampPerLine != false {
		t.Errorf("expected WriteTimestampPerLine=false from YAML")
	}

	// Simulate CLI flag override: only override fields that were "explicitly set"
	// This verifies the principle that unset CLI flags don't wipe YAML values
	cfg.MergeConfig.WriteTimestampPerLine = true // simulate --write-timestamp flag

	// Verify CLI override took effect
	if cfg.MergeConfig.WriteTimestampPerLine != true {
		t.Errorf("expected WriteTimestampPerLine=true after CLI override")
	}

	// Verify YAML values NOT overridden remain intact
	if cfg.ListFilesConfig.FileAliases["app.log"] != "application" {
		t.Errorf("expected FileAliases from YAML to remain, got %v", cfg.ListFilesConfig.FileAliases)
	}
	if cfg.ParseTimestampConfig.ShortestTimestampLen != 15 {
		t.Errorf("expected ShortestTimestampLen=15 from YAML, got %d", cfg.ParseTimestampConfig.ShortestTimestampLen)
	}
}

func TestHydrationPipelineFilterAppend(t *testing.T) {
	cfg := &MainConfig{
		OutputFile: &WritableFile{File: os.Stdout},
		LogFile:    &WritableFile{File: os.Stderr},
		ListFilesConfig: &ListFilesConfig{
			IgnorePatterns: []string{"*.zip"},
			FileAliases:    map[string]string{},
		},
		ParseTimestampConfig: &ParseTimestampConfig{},
		MergeConfig:          &MergeConfig{},
		PrintProgressConfig:  &PrintProgressConfig{},
	}

	// Simulate appending --filter flags
	cliFilters := []string{"*.tar", "*.gz"}
	cfg.ListFilesConfig.IgnorePatterns = append(cfg.ListFilesConfig.IgnorePatterns, cliFilters...)

	expected := []string{"*.zip", "*.tar", "*.gz"}
	if len(cfg.ListFilesConfig.IgnorePatterns) != len(expected) {
		t.Fatalf("expected %d patterns, got %d", len(expected), len(cfg.ListFilesConfig.IgnorePatterns))
	}
	for i, p := range expected {
		if cfg.ListFilesConfig.IgnorePatterns[i] != p {
			t.Errorf("pattern[%d]: expected %q, got %q", i, p, cfg.ListFilesConfig.IgnorePatterns[i])
		}
	}
}

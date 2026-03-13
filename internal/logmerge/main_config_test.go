package logmerge_test

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/logmerge"
)

func TestToYAML(t *testing.T) {
	cfg := &MainConfig{
		InputPaths:           []string{"/var/log"},
		OutputFile:           &WritableFile{File: os.Stdout},
		LogFile:              &WritableFile{File: os.Stderr},
		ListFilesConfig:      &ListFilesConfig{IgnorePatterns: []string{"*.zip"}, FileAliases: map[string]string{}},
		ParseTimestampConfig: &ParseTimestampConfig{ShortestTimestampLen: 15, TimestampSearchEndIndex: 250},
		MergeConfig:          &MergeConfig{WriteTimestampPerLine: true},
		PrintProgressConfig:  &PrintProgressConfig{PeriodMillis: 500},
	}

	yamlStr, err := cfg.ToYAML()
	if err != nil {
		t.Fatalf("ToYAML failed: %v", err)
	}

	if yamlStr == "" {
		t.Error("expected non-empty YAML string")
	}

	// Verify key fields are present
	if !contains(yamlStr, "InputPaths") {
		t.Error("expected 'InputPaths' in YAML output")
	}
	if !contains(yamlStr, "/var/log") {
		t.Error("expected '/var/log' in YAML output")
	}
	if !contains(yamlStr, "WriteTimestampPerLine: true") {
		t.Error("expected 'WriteTimestampPerLine: true' in YAML output")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestToYAML_RoundTrip(t *testing.T) {
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "out.log")
	logPath := filepath.Join(tmpDir, "log.log")

	cfg := &MainConfig{
		InputPaths:           []string{"/tmp/logs"},
		OutputFile:           &WritableFile{Path: outPath},
		LogFile:              &WritableFile{Path: logPath},
		ListFilesConfig:      &ListFilesConfig{IgnorePatterns: []string{"*.bak"}, FileAliases: map[string]string{"app.log": "app"}},
		ParseTimestampConfig: &ParseTimestampConfig{ShortestTimestampLen: 15, TimestampSearchEndIndex: 250},
		MergeConfig:          &MergeConfig{BufferSizeForRead: 1024},
		PrintProgressConfig:  &PrintProgressConfig{},
	}

	yamlStr, err := cfg.ToYAML()
	if err != nil {
		t.Fatalf("ToYAML failed: %v", err)
	}

	// Write to temp file and reload
	yamlPath := filepath.Join(tmpDir, "roundtrip.yaml")
	os.WriteFile(yamlPath, []byte(yamlStr), 0644)

	cfg2 := &MainConfig{
		OutputFile:           &WritableFile{File: os.Stdout},
		LogFile:              &WritableFile{File: os.Stderr},
		ListFilesConfig:      &ListFilesConfig{FileAliases: map[string]string{}},
		ParseTimestampConfig: &ParseTimestampConfig{},
		MergeConfig:          &MergeConfig{},
		PrintProgressConfig:  &PrintProgressConfig{},
	}

	err = cfg2.LoadYAML(yamlPath)
	if err != nil {
		t.Fatalf("LoadYAML failed: %v", err)
	}

	if len(cfg2.InputPaths) != 1 || cfg2.InputPaths[0] != "/tmp/logs" {
		t.Errorf("expected InputPaths [/tmp/logs], got %v", cfg2.InputPaths)
	}
	if cfg2.MergeConfig.BufferSizeForRead != 1024 {
		t.Errorf("expected BufferSizeForRead=1024, got %d", cfg2.MergeConfig.BufferSizeForRead)
	}
}

func TestWritableFile_Initialize_EmptyPath(t *testing.T) {
	wf := &WritableFile{File: os.Stdout}
	err := wf.Initialize()
	if err != nil {
		t.Fatalf("Initialize with empty path should not fail: %v", err)
	}
	// File should remain as os.Stdout
	if wf.File != os.Stdout {
		t.Error("expected File to remain os.Stdout when Path is empty")
	}
}

func TestWritableFile_Initialize_CreatesFile(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "subdir", "output.log")

	wf := &WritableFile{Path: path}
	err := wf.Initialize()
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	defer wf.Close()

	// File should exist
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("expected file to be created")
	}

	// Should be writable
	n, err := wf.Write([]byte("test"))
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if n != 4 {
		t.Errorf("expected 4 bytes written, got %d", n)
	}
}

func TestLoadYAML_InvalidPath(t *testing.T) {
	cfg := &MainConfig{
		OutputFile:           &WritableFile{File: os.Stdout},
		LogFile:              &WritableFile{File: os.Stderr},
		ListFilesConfig:      &ListFilesConfig{FileAliases: map[string]string{}},
		ParseTimestampConfig: &ParseTimestampConfig{},
		MergeConfig:          &MergeConfig{},
		PrintProgressConfig:  &PrintProgressConfig{},
	}

	err := cfg.LoadYAML("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent YAML file")
	}
}

func TestLoadYAML_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "bad.yaml")
	os.WriteFile(path, []byte("{{{{not yaml"), 0644)

	cfg := &MainConfig{
		OutputFile:           &WritableFile{File: os.Stdout},
		LogFile:              &WritableFile{File: os.Stderr},
		ListFilesConfig:      &ListFilesConfig{FileAliases: map[string]string{}},
		ParseTimestampConfig: &ParseTimestampConfig{},
		MergeConfig:          &MergeConfig{},
		PrintProgressConfig:  &PrintProgressConfig{},
	}

	err := cfg.LoadYAML(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

package fsutil

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

type WritableFile struct {
	Path string
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
	if err := value.Decode(&f.Path); err != nil {
		return fmt.Errorf("failed to decode file path: %w", err)
	}
	return nil
}

// Initialize creates the file at Path (and its parent directories). If Path is
// empty, the existing File handle (e.g. os.Stdout) is left unchanged.
func (f *WritableFile) Initialize() error {
	if f.Path == "" {
		// leave default value (e.g., os.Stdout/os.Stderr)
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(f.Path), 0750); err != nil {
		return fmt.Errorf("could not create directory for %s: %v", f.Path, err)
	}

	file, err := os.Create(f.Path)
	if err != nil {
		return fmt.Errorf("could not create file %s: %v", f.Path, err)
	}

	f.File = file
	return nil
}

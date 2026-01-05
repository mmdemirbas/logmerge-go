package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type ListFilesConfig struct {
	InputPath          string            `yaml:"InputPath"`
	ExcludedSuffixes   []string          `yaml:"ExcludedSuffixes"`
	IncludedSuffixes   []string          `yaml:"IncludedSuffixes"`
	ExcludedSubstrings []string          `yaml:"ExcludedSubstrings"`
	IncludedSubstrings []string          `yaml:"IncludedSubstrings"`
	FileAliases        map[string]string `yaml:"FileAliases"`
}

type ListFilesMetrics struct {
	DirsScanned  int64
	FilesScanned int64
	FilesMatched int64
	MatchedFiles []string
}

func NewListFilesMetrics() *ListFilesMetrics {
	return &ListFilesMetrics{
		DirsScanned:  0,
		FilesScanned: 0,
		FilesMatched: 0,
		MatchedFiles: make([]string, 0),
	}
}

func ListFiles(c *ListFilesConfig, m *ListFilesMetrics, totalBufferSize int, minBufferSizePerFile int, logFile *WritableFile) (files []*FileHandle, err error) {
	fileList, err := listFilePaths(c, m)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %v", err)
	}

	if len(fileList) == 0 {
		return nil, fmt.Errorf("no files found")
	}

	perFileBufferSize := FastMax(minBufferSizePerFile, totalBufferSize/len(fileList))
	maxAliasLen := 0

	files = make([]*FileHandle, len(fileList))
	files = files[:0]

	for _, file := range fileList {
		alias, err := getAlias(c, file)
		if err != nil {
			return nil, err
		}

		f, err := os.Open(file)
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(logFile, "failed to open file %s: %v\n", file, err)
			continue
		}

		file, err := NewFileHandle(f, alias, perFileBufferSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create handle for file %v: %v", file, err)
		}

		file.AliasForBlock = []byte(fmt.Sprintf("\n--- %s ---\n", file.Alias))
		aliasLen := len(file.Alias)
		if maxAliasLen < aliasLen {
			maxAliasLen = aliasLen
		}

		files = append(files, file)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no fileList to merge")
	}

	// pad source names to max length
	for _, file := range files {
		file.AliasForLine = []byte(fmt.Sprintf("%-*s - ", maxAliasLen, file.Alias))
	}

	return files, nil
}

func listFilePaths(c *ListFilesConfig, m *ListFilesMetrics) (files []string, err error) {
	basePath := c.InputPath
	if basePath == "" {
		return nil, fmt.Errorf("input path is empty")
	}

	stat, err := os.Stat(basePath)

	switch {
	case err != nil:
		return nil, fmt.Errorf("could not stat %s: %v", basePath, err)

	case stat.IsDir():
		err = filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return fmt.Errorf("could not walk %s: %v", path, err)
			}
			if info.IsDir() {
				m.DirsScanned++
			} else {
				visitFile(c, m, path, &files)
			}
			return nil
		})
	default:
		visitFile(c, m, basePath, &files)
	}
	return files, err
}

func visitFile(c *ListFilesConfig, m *ListFilesMetrics, path string, files *[]string) {
	m.FilesScanned++
	if ShouldIncludeFile(c, path) {
		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, path)
		*files = append(*files, path)
	}
}

func getAlias(c *ListFilesConfig, file string) (string, error) {
	relative, err := filepath.Rel(c.InputPath, file)
	if err != nil {
		return "", fmt.Errorf("failed to calculate relative path for file %s: %v", file, err)
	}

	mappedAlias, ok := (c.FileAliases)[relative]
	if ok {
		return mappedAlias, nil
	} else {
		return relative, nil
	}
}

func ShouldIncludeFile(c *ListFilesConfig, filePath string) bool {
	_, fileName := filepath.Split(filePath)
	lowerName := strings.ToLower(fileName)
	return !hasSuffix(lowerName, c.ExcludedSuffixes...) &&
		(len(c.IncludedSuffixes) == 0 || hasSuffix(lowerName, c.IncludedSuffixes...)) &&
		!hasSubstring(lowerName, c.ExcludedSubstrings...) &&
		(len(c.IncludedSubstrings) == 0 || hasSubstring(lowerName, c.IncludedSubstrings...))
}

func hasSubstring(s string, suffices ...string) bool {
	for _, suffix := range suffices {
		lowerSuffix := strings.ToLower(suffix)
		if strings.Contains(s, lowerSuffix) {
			return true
		}
	}
	return false
}

func hasSuffix(s string, suffices ...string) bool {
	for _, suffix := range suffices {
		lowerSuffix := strings.ToLower(suffix)
		if strings.HasSuffix(s, lowerSuffix) {
			return true
		}
	}
	return false
}

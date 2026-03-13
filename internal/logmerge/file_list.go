package logmerge

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

type ListFilesConfig struct {
	InputPaths     []string          `yaml:"InputPaths"`
	IgnorePatterns []string          `yaml:"IgnorePatterns"`
	FileAliases    map[string]string `yaml:"FileAliases"`
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
	matcher := NewMatcher(c.IgnorePatterns)

	fileList, err := listFilePaths(c, m, matcher)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %v", err)
	}

	if len(fileList) == 0 {
		return nil, fmt.Errorf("no files found")
	}

	perFileBufferSize := max(minBufferSizePerFile, totalBufferSize/len(fileList))
	maxAliasLen := 0

	files = make([]*FileHandle, 0, len(fileList))

	for _, filePath := range fileList {
		alias := GetAlias(c, filePath)

		f, err := os.Open(filePath)
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(logFile, "failed to open file %s: %v\n", filePath, err)
			continue
		}

		fh, err := NewFileHandle(f, alias, perFileBufferSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create handle for file %v: %v", filePath, err)
		}

		fh.AliasForBlock = []byte(fmt.Sprintf("\n--- %s ---\n", fh.Alias))
		aliasLen := len(fh.Alias)
		if maxAliasLen < aliasLen {
			maxAliasLen = aliasLen
		}

		files = append(files, fh)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files to merge")
	}

	// pad source names to max length
	for _, file := range files {
		file.AliasForLine = []byte(fmt.Sprintf("%-*s - ", maxAliasLen, file.Alias))
	}

	return files, nil
}

func listFilePaths(c *ListFilesConfig, m *ListFilesMetrics, matcher *Matcher) (files []string, err error) {
	if len(c.InputPaths) == 0 {
		return nil, fmt.Errorf("no input paths specified")
	}

	for _, basePath := range c.InputPaths {
		stat, statErr := os.Stat(basePath)

		switch {
		case statErr != nil:
			return nil, fmt.Errorf("could not stat %s: %v", basePath, statErr)

		case stat.IsDir():
			walkErr := filepath.WalkDir(basePath, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return fmt.Errorf("could not walk %s: %v", path, err)
				}
				if d.IsDir() {
					m.DirsScanned++
				} else {
					visitFile(matcher, m, path, &files)
				}
				return nil
			})
			if walkErr != nil {
				return nil, walkErr
			}

		default:
			visitFile(matcher, m, basePath, &files)
		}
	}
	return files, nil
}

func visitFile(matcher *Matcher, m *ListFilesMetrics, path string, files *[]string) {
	m.FilesScanned++
	if matcher.ShouldInclude(path) {
		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, path)
		*files = append(*files, path)
	}
}

func GetAlias(c *ListFilesConfig, file string) string {
	// Try pattern-based matching from FileAliases
	for pattern, alias := range c.FileAliases {
		matched, _ := filepath.Match(pattern, file)
		if matched {
			return alias
		}
		// Also try matching against just the filename
		_, name := filepath.Split(file)
		matched, _ = filepath.Match(pattern, name)
		if matched {
			return alias
		}
		// Try matching against relative path segments
		if pattern == file {
			return alias
		}
	}

	// Fall back to the file path itself
	// Try to make it relative to the first input path
	if len(c.InputPaths) > 0 {
		for _, inputPath := range c.InputPaths {
			relative, err := filepath.Rel(inputPath, file)
			if err == nil {
				// Check for exact match in aliases
				if alias, ok := c.FileAliases[relative]; ok {
					return alias
				}
				return relative
			}
		}
	}
	return file
}

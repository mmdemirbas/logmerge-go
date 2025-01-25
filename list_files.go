package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TODO: Think about global metric access

func ListFiles(c *AppConfig) (files []*FileHandle, err error) {
	fileList, err := listFilePaths(c)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %v", err)
	}

	perFileBufferSize := max(c.TimestampSearchEndIndex, c.BufferSizeForRead/len(fileList))
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
			fmt.Fprintf(c.Stderr, "failed to open file %s: %v\n", file, err)
			continue
		}

		file, err := NewFileHandle(f, alias, perFileBufferSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create handle for file %v: %v", file, err)
		}

		if c.WriteAliasPerBlock {
			file.AliasForBlock = fmt.Sprintf("\n--- %s ---\n", file.Alias)
		}

		if c.WriteAliasPerLine {
			aliasLen := len(file.Alias)
			if maxAliasLen < aliasLen {
				maxAliasLen = aliasLen
			}
		}

		files = append(files, file)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no fileList to merge")
	}

	if c.WriteAliasPerLine {
		// pad source names to max length
		for _, file := range files {
			file.AliasForLine = fmt.Sprintf("%-*s - ", maxAliasLen, file.Alias)
		}
	}

	return files, nil
}

func listFilePaths(c *AppConfig) (files []string, err error) {
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
				DirsScanned++
			} else {
				visitFile(c, path, &files)
			}
			return nil
		})
	default:
		visitFile(c, basePath, &files)
	}
	return files, err
}

func visitFile(c *AppConfig, path string, files *[]string) {
	FilesScanned++
	if ShouldIncludeFile(c, path) {
		FilesMatched++
		MatchedFiles = append(MatchedFiles, path)
		*files = append(*files, path)
	}
}

func getAlias(c *AppConfig, file string) (string, error) {
	// TODO: Consider measuring overhead of each features separately (overheadOfWriteAliases etc)
	if c.WriteAliasPerBlock || c.WriteAliasPerLine {
		relative, err := filepath.Rel(c.InputPath, file)
		if err != nil {
			return "", fmt.Errorf("failed to calculate relative path for file %s: %v", file, err)
		}

		mappedAlias, ok := c.FileAliases[relative]
		if ok {
			return mappedAlias, nil
		} else {
			return relative, nil
		}
	}
	return "", nil
}

func ShouldIncludeFile(c *AppConfig, filePath string) bool {
	_, fileName := filepath.Split(filePath)
	lowerName := strings.ToLower(fileName)
	return !hasSuffix(lowerName, c.ExcludedStrictSuffixes...) &&
		(len(c.IncludedStrictSuffixes) == 0 || hasSuffix(lowerName, c.IncludedStrictSuffixes...)) &&
		!hasLenientSuffix(lowerName, c.ExcludedLenientSuffixes...) &&
		(len(c.IncludedLenientSuffixes) == 0 || hasLenientSuffix(lowerName, c.IncludedLenientSuffixes...))
}

func hasLenientSuffix(s string, suffices ...string) bool {
	if hasSuffix(s, suffices...) {
		return true
	}
	for _, suffix := range suffices {
		if strings.Contains(s, suffix+".") {
			return true
		}
	}
	return false
}

func hasSuffix(s string, suffices ...string) bool {
	for _, suffix := range suffices {
		if strings.HasSuffix(s, suffix) {
			return true
		}
	}
	return false
}

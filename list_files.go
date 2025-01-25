package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// TODO: Pass list-file related configs as a struct for better testability

// TODO: Think about global metric access

func ListFiles(basePath string) (files []string, err error) {
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
				FilesScanned++
				if ShouldIncludeFile(path) {
					FilesMatched++
					files = append(files, path)
				}
			}
			return nil
		})
	default:
		FilesScanned++
		if ShouldIncludeFile(basePath) {
			FilesMatched++
			files = append(files, basePath)
		}
	}
	MatchedFiles = append(MatchedFiles, files...)
	return files, err
}

func ShouldIncludeFile(filePath string) bool {
	_, fileName := filepath.Split(filePath)
	lowerName := strings.ToLower(fileName)
	return !hasSuffix(lowerName, ExcludedStrictSuffixes...) &&
		(len(IncludedStrictSuffixes) == 0 || hasSuffix(lowerName, IncludedStrictSuffixes...)) &&
		!hasLenientSuffix(lowerName, ExcludedLenientSuffixes...) &&
		(len(IncludedLenientSuffixes) == 0 || hasLenientSuffix(lowerName, IncludedLenientSuffixes...))
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

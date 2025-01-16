package main

import (
	"os"
	"path/filepath"
	"strings"
)

var (
	excludedStrictSuffixes  = []string{".zip", ".tar", ".gz", ".rar", ".7z", ".tgz", ".bz2", ".tbz2", ".xz", ".txz"}
	includedStrictSuffixes  = []string{}
	excludedLenientSuffixes = []string{}
	includedLenientSuffixes = []string{".log", ".err", ".error", ".warn", ".warning", ".info", ".txt", ".out", ".debug", ".trace"}
)

func listFiles(basePath string) ([]string, error) {
	var (
		files []string
		err   error
	)
	printDuration("listFiles", func() {
		stat, err := os.Stat(basePath)
		if err != nil {
			return
		}
		switch {
		case stat.IsDir():
			err = filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && ShouldIncludeFile(path) {
					files = append(files, path)
				}
				return nil
			})
		case ShouldIncludeFile(basePath):
			files = append(files, basePath)
		}
		printFileList(files)
		printFileStats(files)
	})
	return files, err
}

func ShouldIncludeFile(filePath string) bool {
	_, fileName := filepath.Split(filePath)
	lowerName := strings.ToLower(fileName)
	return !hasSuffix(lowerName, excludedStrictSuffixes...) &&
		(len(includedStrictSuffixes) == 0 || hasSuffix(lowerName, includedStrictSuffixes...)) &&
		!hasLenientSuffix(lowerName, excludedLenientSuffixes...) &&
		(len(includedLenientSuffixes) == 0 || hasLenientSuffix(lowerName, includedLenientSuffixes...))
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

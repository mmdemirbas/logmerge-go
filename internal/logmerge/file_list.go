package logmerge

import (
	"archive/zip"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
)

type ListFilesConfig struct {
	IgnoreFile     string            `yaml:"IgnoreFile"`
	IgnorePatterns []string          `yaml:"IgnorePatterns"`
	IgnoreArchives bool              `yaml:"IgnoreArchives"`
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

func ListFiles(inputPaths []string, c *ListFilesConfig, m *ListFilesMetrics, totalBufferSize int, minBufferSizePerFile int, logFile *WritableFile) (files []*FileHandle, err error) {
	matcher := NewMatcher(c.IgnorePatterns)

	vfiles, err := listVirtualFiles(inputPaths, m, matcher, logFile)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %v", err)
	}

	if len(vfiles) == 0 {
		return nil, fmt.Errorf("no files found")
	}

	perFileBufferSize := max(minBufferSizePerFile, totalBufferSize/len(vfiles))
	maxAliasLen := 0

	files = make([]*FileHandle, 0, len(vfiles))

	for _, vf := range vfiles {
		alias := GetAlias(inputPaths, c, vf.Name())

		fh, err := NewFileHandle(vf, alias, perFileBufferSize)
		if err != nil {
			return nil, fmt.Errorf("failed to create handle for file %v: %v", vf.Name(), err)
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

func listVirtualFiles(inputPaths []string, m *ListFilesMetrics, matcher *Matcher, logFile *WritableFile) ([]VirtualFile, error) {
	if len(inputPaths) == 0 {
		return nil, fmt.Errorf("no input paths specified")
	}

	var vfiles []VirtualFile

	for _, basePath := range inputPaths {
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
					visitVirtualFile(matcher, m, path, &vfiles, logFile)
				}
				return nil
			})
			if walkErr != nil {
				return nil, walkErr
			}

		default:
			visitVirtualFile(matcher, m, basePath, &vfiles, logFile)
		}
	}
	return vfiles, nil
}

func visitVirtualFile(matcher *Matcher, m *ListFilesMetrics, path string, vfiles *[]VirtualFile, logFile *WritableFile) {
	m.FilesScanned++
	if !matcher.ShouldInclude(path) {
		return
	}

	ext := strings.ToLower(filepath.Ext(path))

	switch ext {
	case ".gz":
		vf, err := openGzFile(path)
		if err != nil {
			fmt.Fprintf(logFile, "failed to open gz file %s: %v\n", path, err)
			return
		}
		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, path)
		*vfiles = append(*vfiles, vf)

	case ".zip":
		entries, err := openZipFile(path, matcher, m)
		if err != nil {
			fmt.Fprintf(logFile, "failed to open zip file %s: %v\n", path, err)
			return
		}
		*vfiles = append(*vfiles, entries...)

	default:
		f, err := os.Open(path)
		if err != nil {
			fmt.Fprintf(logFile, "failed to open file %s: %v\n", path, err)
			return
		}
		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, path)
		*vfiles = append(*vfiles, &OsFile{F: f})
	}
}

// gzFile wraps a gzip reader as a VirtualFile.
type gzFile struct {
	reader *gzip.Reader
	file   *os.File
	name   string
	size   int64 // compressed size
}

func (g *gzFile) Read(p []byte) (int, error) { return g.reader.Read(p) }
func (g *gzFile) Close() error {
	g.reader.Close()
	return g.file.Close()
}
func (g *gzFile) Name() string { return g.name }
func (g *gzFile) Size() int64  { return g.size }

func openGzFile(path string) (VirtualFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gr, err := gzip.NewReader(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	info, _ := f.Stat()
	var size int64
	if info != nil {
		size = info.Size()
	}
	return &gzFile{reader: gr, file: f, name: path, size: size}, nil
}

// sharedZipReader manages the lifecycle of the underlying zip file.
// The zip.ReadCloser is only closed when all entries have been closed.
type sharedZipReader struct {
	zr    *zip.ReadCloser
	count int32
}

func (s *sharedZipReader) release() error {
	if atomic.AddInt32(&s.count, -1) == 0 {
		return s.zr.Close()
	}
	return nil
}

// zipEntryFile wraps a zip entry as a VirtualFile.
type zipEntryFile struct {
	reader io.ReadCloser
	shared *sharedZipReader
	name   string
	size   int64
}

func (z *zipEntryFile) Read(p []byte) (int, error) { return z.reader.Read(p) }
func (z *zipEntryFile) Close() error {
	err := z.reader.Close()
	if err2 := z.shared.release(); err == nil {
		err = err2
	}
	return err
}
func (z *zipEntryFile) Name() string { return z.name }
func (z *zipEntryFile) Size() int64  { return z.size }

func openZipFile(path string, matcher *Matcher, m *ListFilesMetrics) ([]VirtualFile, error) {
	zr, err := zip.OpenReader(path)
	if err != nil {
		return nil, err
	}

	var entries []VirtualFile
	// First pass: find which entries are included
	var included []*zip.File
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		virtualPath := path + "!/" + f.Name
		if matcher.ShouldInclude(virtualPath) {
			included = append(included, f)
		}
	}

	if len(included) == 0 {
		zr.Close()
		return nil, nil
	}

	shared := &sharedZipReader{zr: zr, count: int32(len(included))}

	for _, f := range included {
		virtualPath := path + "!/" + f.Name
		rc, err := f.Open()
		if err != nil {
			shared.release() // decrement if we fail to open one
			continue
		}
		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, virtualPath)
		entries = append(entries, &zipEntryFile{
			reader: rc,
			shared: shared,
			name:   virtualPath,
			size:   int64(f.UncompressedSize64),
		})
	}

	return entries, nil
}

func GetAlias(inputPaths []string, c *ListFilesConfig, virtualPath string) string {
	// Try pattern-based matching from FileAliases
	for pattern, alias := range c.FileAliases {
		matched, _ := filepath.Match(pattern, virtualPath)
		if matched {
			return alias
		}
		// Also try matching against just the filename
		_, name := filepath.Split(virtualPath)
		matched, _ = filepath.Match(pattern, name)
		if matched {
			return alias
		}
	}

	// Fall back: try to make it relative to the first input path
	for _, inputPath := range inputPaths {
		relative, err := filepath.Rel(inputPath, virtualPath)
		if err == nil {
			// Check for exact match in aliases
			if alias, ok := c.FileAliases[relative]; ok {
				return alias
			}
			return relative
		}
	}
	return virtualPath
}

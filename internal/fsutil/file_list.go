package fsutil

import (
	"archive/tar"
	"archive/zip"
	bytespkg "bytes"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/mmdemirbas/logmerge/internal/metrics"
	"github.com/ulikunitz/xz"
)

type ListFilesConfig struct {
	IgnoreFile     string            `yaml:"IgnoreFile"`
	IgnorePatterns []string          `yaml:"IgnorePatterns"`
	IgnoreArchives bool              `yaml:"IgnoreArchives"`
	FollowSymlinks bool              `yaml:"FollowSymlinks"`
	FileAliases    map[string]string `yaml:"FileAliases"`
}

// ListFiles discovers files from inputPaths (files or directories), applies ignore
// patterns, transparently opens archives, and returns a FileHandle per discovered file.
func ListFiles(inputPaths []string, c *ListFilesConfig, m *metrics.ListFilesMetrics, totalBufferSize int, minBufferSizePerFile int, logFile *WritableFile) (files []*FileHandle, err error) {
	matcher := NewMatcher(c.IgnorePatterns)

	vfiles, err := listVirtualFiles(inputPaths, c, m, matcher, logFile)
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

func listVirtualFiles(inputPaths []string, c *ListFilesConfig, m *metrics.ListFilesMetrics, matcher *Matcher, logFile *WritableFile) ([]VirtualFile, error) {
	if len(inputPaths) == 0 {
		return nil, fmt.Errorf("no input paths specified")
	}

	var vfiles []VirtualFile
	// Track visited real paths to avoid infinite symlink loops
	var visited map[string]bool
	if c.FollowSymlinks {
		visited = make(map[string]bool)
	}

	for _, basePath := range inputPaths {
		stat, statErr := os.Stat(basePath) // os.Stat follows symlinks
		if statErr != nil {
			return nil, fmt.Errorf("could not stat %s: %v", basePath, statErr)
		}

		if stat.IsDir() {
			// Resolve the base path only if it is itself a symlink.
			// filepath.WalkDir does not follow symlinked roots, so we
			// must resolve them. But we avoid resolving regular paths
			// to preserve the original path in file listings/aliases.
			walkPath := basePath
			if lstat, err := os.Lstat(basePath); err == nil && lstat.Mode()&os.ModeSymlink != 0 {
				if resolved, err := filepath.EvalSymlinks(basePath); err == nil {
					walkPath = resolved
				}
			}
			if c.FollowSymlinks {
				if realPath, err := filepath.EvalSymlinks(walkPath); err == nil {
					visited[realPath] = true
				}
			}
			err := walkDir(walkPath, c.FollowSymlinks, visited, m, matcher, &vfiles, logFile)
			if err != nil {
				return nil, err
			}
		} else {
			visitVirtualFile(matcher, m, basePath, &vfiles, logFile)
		}
	}
	return vfiles, nil
}

// walkDir traverses a directory. When followSymlinks is true, symlinked
// directories are resolved and walked recursively, with loop detection
// via the visited set of real paths.
func walkDir(dirPath string, followSymlinks bool, visited map[string]bool, m *metrics.ListFilesMetrics, matcher *Matcher, vfiles *[]VirtualFile, logFile *WritableFile) error {
	return filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("could not walk %s: %v", path, err)
		}

		isSymlink := d.Type()&os.ModeSymlink != 0

		// For symlinks encountered during traversal, resolve to determine
		// if target is a file or directory.
		if isSymlink {
			info, err := os.Stat(path) // follows symlink
			if err != nil {
				fmt.Fprintf(logFile, "could not stat symlink %s: %v\n", path, err)
				return nil // skip broken symlinks
			}
			if info.IsDir() {
				if !followSymlinks {
					return nil // skip symlinked directories
				}
				realPath, err := filepath.EvalSymlinks(path)
				if err != nil {
					fmt.Fprintf(logFile, "could not resolve symlink %s: %v\n", path, err)
					return nil
				}
				if visited[realPath] {
					fmt.Fprintf(logFile, "skipping symlink loop: %s -> %s\n", path, realPath)
					return nil
				}
				visited[realPath] = true
				return walkDir(realPath, followSymlinks, visited, m, matcher, vfiles, logFile)
			}
			// Symlink to a file — treat as regular file
			visitVirtualFile(matcher, m, path, vfiles, logFile)
			return nil
		}

		if d.IsDir() {
			m.DirsScanned++
		} else {
			visitVirtualFile(matcher, m, path, vfiles, logFile)
		}
		return nil
	})
}

func visitVirtualFile(matcher *Matcher, m *metrics.ListFilesMetrics, path string, vfiles *[]VirtualFile, logFile *WritableFile) {
	m.FilesScanned++
	if !matcher.ShouldInclude(path) {
		return
	}
	entries, err := openVirtualEntries(path, strings.ToLower(path), matcher, m)
	if err != nil {
		fmt.Fprintf(logFile, "failed to open %s: %v\n", path, err)
		return
	}
	*vfiles = append(*vfiles, entries...)
}

// openVirtualEntries dispatches on compound extensions (tar.*) first, then
// delegates single-extension files to openSingleVirtualFile.
func openVirtualEntries(path, lower string, matcher *Matcher, m *metrics.ListFilesMetrics) ([]VirtualFile, error) {
	switch {
	case strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz"):
		return openTarFile(path, matcher, m, decompressGzip)
	case strings.HasSuffix(lower, ".tar.bz2") || strings.HasSuffix(lower, ".tbz2"):
		return openTarFile(path, matcher, m, decompressBzip2)
	case strings.HasSuffix(lower, ".tar.xz") || strings.HasSuffix(lower, ".txz"):
		return openTarFile(path, matcher, m, decompressXz)
	default:
		return openSingleVirtualFile(path, strings.ToLower(filepath.Ext(path)), matcher, m)
	}
}

// singleFileOpeners maps single compressed-file extensions to their openers.
var singleFileOpeners = map[string]func(string) (VirtualFile, error){
	".gz":  openGzFile,
	".bz2": openBz2File,
	".xz":  openXzFile,
}

// openSingleVirtualFile opens a file by its single extension.
func openSingleVirtualFile(path, ext string, matcher *Matcher, m *metrics.ListFilesMetrics) ([]VirtualFile, error) {
	if opener, ok := singleFileOpeners[ext]; ok {
		vf, err := opener(path)
		if err != nil {
			return nil, err
		}
		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, path)
		return []VirtualFile{vf}, nil
	}
	switch ext {
	case ".tar":
		return openTarFile(path, matcher, m, nil)
	case ".zip":
		return openZipFile(path, matcher, m)
	default:
		return openPlainFile(path, m)
	}
}

// openPlainFile opens a regular (non-archive) file and records it in metrics.
func openPlainFile(path string, m *metrics.ListFilesMetrics) ([]VirtualFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	m.FilesMatched++
	m.MatchedFiles = append(m.MatchedFiles, path)
	return []VirtualFile{&OsFile{F: f}}, nil
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
	rerr := g.reader.Close()
	ferr := g.file.Close()
	if rerr != nil {
		return rerr
	}
	return ferr
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
		_ = f.Close()
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

func openZipFile(path string, matcher *Matcher, m *metrics.ListFilesMetrics) ([]VirtualFile, error) {
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
		_ = zr.Close()
		return nil, nil
	}

	shared := &sharedZipReader{zr: zr, count: int32(len(included))}

	for _, f := range included {
		virtualPath := path + "!/" + f.Name
		rc, err := f.Open()
		if err != nil {
			_ = shared.release() // decrement if we fail to open one
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

// bz2File wraps a bzip2 reader as a VirtualFile.
type bz2File struct {
	reader io.Reader
	file   *os.File
	name   string
	size   int64 // compressed size
}

func (b *bz2File) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *bz2File) Close() error               { return b.file.Close() }
func (b *bz2File) Name() string               { return b.name }
func (b *bz2File) Size() int64                { return b.size }

func openBz2File(path string) (VirtualFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	var size int64
	if info != nil {
		size = info.Size()
	}
	return &bz2File{reader: bzip2.NewReader(f), file: f, name: path, size: size}, nil
}

// xzFile wraps an xz reader as a VirtualFile.
type xzFile struct {
	reader io.Reader
	file   *os.File
	name   string
	size   int64 // compressed size
}

func (x *xzFile) Read(p []byte) (int, error) { return x.reader.Read(p) }
func (x *xzFile) Close() error               { return x.file.Close() }
func (x *xzFile) Name() string               { return x.name }
func (x *xzFile) Size() int64                { return x.size }

func openXzFile(path string) (VirtualFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	xr, err := xz.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	info, _ := f.Stat()
	var size int64
	if info != nil {
		size = info.Size()
	}
	return &xzFile{reader: xr, file: f, name: path, size: size}, nil
}

// tarEntryFile wraps an in-memory tar entry as a VirtualFile.
type tarEntryFile struct {
	reader *bytespkg.Reader
	name   string
	size   int64
}

func (t *tarEntryFile) Read(p []byte) (int, error) { return t.reader.Read(p) }
func (t *tarEntryFile) Close() error               { return nil }
func (t *tarEntryFile) Name() string               { return t.name }
func (t *tarEntryFile) Size() int64                { return t.size }

// decompressor creates a decompressing reader wrapping the given file.
type decompressor func(*os.File) (io.Reader, error)

func decompressGzip(f *os.File) (io.Reader, error)  { return gzip.NewReader(f) }
func decompressBzip2(f *os.File) (io.Reader, error) { return bzip2.NewReader(f), nil }
func decompressXz(f *os.File) (io.Reader, error)    { return xz.NewReader(f) }

func openTarFile(path string, matcher *Matcher, m *metrics.ListFilesMetrics, decomp decompressor) ([]VirtualFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	var r io.Reader = f
	if decomp != nil {
		r, err = decomp(f)
		if err != nil {
			return nil, err
		}
	}

	tr := tar.NewReader(r)
	var entries []VirtualFile

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry: %v", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}

		virtualPath := path + "!/" + hdr.Name
		if !matcher.ShouldInclude(virtualPath) {
			continue
		}

		data, err := io.ReadAll(tr)
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry %s: %v", hdr.Name, err)
		}

		m.FilesMatched++
		m.MatchedFiles = append(m.MatchedFiles, virtualPath)
		entries = append(entries, &tarEntryFile{
			reader: bytespkg.NewReader(data),
			name:   virtualPath,
			size:   hdr.Size,
		})
	}

	return entries, nil
}

// GetAlias returns the display alias for a file. It checks FileAliases glob patterns
// first, then falls back to the path relative to the first matching input path.
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

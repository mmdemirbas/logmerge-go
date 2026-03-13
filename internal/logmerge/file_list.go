package logmerge

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

	"github.com/ulikunitz/xz"
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

	lower := strings.ToLower(path)

	// Check compound extensions first (tar.gz, tar.bz2, tar.xz)
	switch {
	case strings.HasSuffix(lower, ".tar.gz") || strings.HasSuffix(lower, ".tgz"):
		entries, err := openTarFile(path, matcher, m, decompressGzip)
		if err != nil {
			fmt.Fprintf(logFile, "failed to open tar.gz file %s: %v\n", path, err)
			return
		}
		*vfiles = append(*vfiles, entries...)

	case strings.HasSuffix(lower, ".tar.bz2") || strings.HasSuffix(lower, ".tbz2"):
		entries, err := openTarFile(path, matcher, m, decompressBzip2)
		if err != nil {
			fmt.Fprintf(logFile, "failed to open tar.bz2 file %s: %v\n", path, err)
			return
		}
		*vfiles = append(*vfiles, entries...)

	case strings.HasSuffix(lower, ".tar.xz") || strings.HasSuffix(lower, ".txz"):
		entries, err := openTarFile(path, matcher, m, decompressXz)
		if err != nil {
			fmt.Fprintf(logFile, "failed to open tar.xz file %s: %v\n", path, err)
			return
		}
		*vfiles = append(*vfiles, entries...)

	default:
		// Single extension
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

		case ".bz2":
			vf, err := openBz2File(path)
			if err != nil {
				fmt.Fprintf(logFile, "failed to open bz2 file %s: %v\n", path, err)
				return
			}
			m.FilesMatched++
			m.MatchedFiles = append(m.MatchedFiles, path)
			*vfiles = append(*vfiles, vf)

		case ".xz":
			vf, err := openXzFile(path)
			if err != nil {
				fmt.Fprintf(logFile, "failed to open xz file %s: %v\n", path, err)
				return
			}
			m.FilesMatched++
			m.MatchedFiles = append(m.MatchedFiles, path)
			*vfiles = append(*vfiles, vf)

		case ".tar":
			entries, err := openTarFile(path, matcher, m, nil)
			if err != nil {
				fmt.Fprintf(logFile, "failed to open tar file %s: %v\n", path, err)
				return
			}
			*vfiles = append(*vfiles, entries...)

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
		f.Close()
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

func openTarFile(path string, matcher *Matcher, m *ListFilesMetrics, decomp decompressor) ([]VirtualFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

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

package fsutil

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mmdemirbas/logmerge/internal/container"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
)

var newline = []byte{'\n'}

// VirtualFile abstracts a readable file, supporting transparent decompression.
type VirtualFile interface {
	io.ReadCloser
	Name() string
	Size() int64
}

// OsFile wraps a standard *os.File as a VirtualFile.
type OsFile struct {
	F *os.File
}

func (f *OsFile) Read(p []byte) (int, error) { return f.F.Read(p) }
func (f *OsFile) Close() error               { return f.F.Close() }
func (f *OsFile) Name() string               { return f.F.Name() }
func (f *OsFile) Size() int64 {
	info, err := f.F.Stat()
	if err != nil {
		return 0
	}
	return info.Size()
}

type FileHandle struct {
	File                VirtualFile
	Alias               []byte
	AliasForBlock       []byte
	AliasForLine        []byte
	Size                int64                 // Size of the file in bytes
	BytesRead           int64                 // Number of bytes read from the file
	Done                bool                  // Whether the file has been fully read
	Buffer              *container.RingBuffer // Buffer for reading the file
	LineTimestampParsed bool                  // Whether the timestamp for the current line has been parsed
	LineTimestamp       logtime.Timestamp     // The timestamp for the current line
	LineTimestampStart  int                   // Byte offset where the timestamp section starts (including leading delimiters)
	LineTimestampEnd    int                   // Byte offset where the timestamp section ends (including trailing delimiters)
	LineLevel           byte                  // Detected log level for the current line (loglevel.Level)
	LineLevelStart      int                   // Byte offset where the level token starts
	LineLevelEnd        int                   // Byte offset where the level token ends
	BlockTimestamp      logtime.Timestamp     // The timestamp for the current block, i.e. the last non-zero timestamp
	Metrics             *metrics.MetricsTree
	MergeMetrics        *metrics.MergeMetrics
	eofReached          bool // true after the underlying reader returns io.EOF
}

// NewFileHandle creates a FileHandle wrapping the given VirtualFile with its own
// read buffer and per-file metrics.
func NewFileHandle(file VirtualFile, alias string, bufferSize int) (fh *FileHandle, err error) {
	return &FileHandle{
		File:                file,
		Alias:               []byte(alias),
		Size:                file.Size(),
		BytesRead:           0,
		Buffer:              container.NewRingBuffer(bufferSize),
		Done:                false,
		LineTimestampParsed: false,
		LineTimestamp:       logtime.ZeroTimestamp,
		BlockTimestamp:      logtime.ZeroTimestamp,
		Metrics:             metrics.NewMetricsTree(),
		MergeMetrics:        metrics.NewMergeMetrics(),
	}, nil
}

// FillBuffer reads data from the underlying file into the ring buffer.
func (r *FileHandle) FillBuffer() error {
	if r.eofReached {
		return nil
	}
	n, err := r.Buffer.Fill(r.File)
	r.BytesRead += int64(n)
	if err == io.EOF {
		r.eofReached = true
		return nil
	}
	return err
}

// SkipLine advances past the current line in the buffer without writing it,
// returning the number of bytes skipped and the EOL length.
func (r *FileHandle) SkipLine() (bytesCount int, eolLength int, err error) {
	var (
		n               = 0
		latestCharWasCR = false
		eol             = container.None
	)
	mt := r.Metrics // cache nil check

	for !r.Buffer.IsEmpty() {
		n, eol = r.Buffer.SkipNextLineSlice(&latestCharWasCR)
		bytesCount += n
		if eol != container.None {
			break
		}

		if r.Buffer.IsEmpty() {
			var startTime time.Time
			if mt != nil {
				startTime = mt.Start("FillBuffer")
			}
			err = r.FillBuffer()
			if err != nil {
				err = fmt.Errorf("failed to fill buffer: %v", err)
				return
			}
			if mt != nil {
				mt.Stop(startTime)
			}
		}
	}

	switch eol {
	case container.None:
	case container.CR, container.LF:
		eolLength = 1
	case container.CRLF:
		eolLength = 2
	}
	return
}

// WriteLine writes the current line from the buffer to the writer, ensuring
// it ends with a newline. Updates byte and line-length metrics.
func (r *FileHandle) WriteLine(m *metrics.MergeMetrics, writer *bufio.Writer) error {
	var (
		count           = 0
		latestCharWasCR = false
		eol             = container.None
		chunk           []byte
		err             error = nil
	)
	mt := r.Metrics // cache nil check — avoids 10 method calls per line when nil

	for !r.Buffer.IsEmpty() || latestCharWasCR {
		var startTime time.Time
		if mt != nil {
			startTime = mt.Start("PeekNextLineSlice")
		}
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		if mt != nil {
			mt.Stop(startTime)
		}

		if chunk != nil {
			if mt != nil {
				startTime = mt.Start("WriteLinePartial")
			}
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				if eol == container.CRLF && len(chunk) == 1 && chunk[0] == '\n' {
					r.Buffer.Skip(1)
				} else {
					r.Buffer.Skip(n)
				}
				count += n
			}
			if mt != nil {
				mt.Stop(startTime)
			}
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != container.None {
			break
		}

		if r.Buffer.IsEmpty() && !latestCharWasCR {
			if mt != nil {
				startTime = mt.Start("FillBuffer")
			}
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			if mt != nil {
				mt.Stop(startTime)
			}
		}
	}

	// Ensure \n is written at the end of the line
	if eol != container.LF && eol != container.CRLF {
		var startTime time.Time
		if mt != nil {
			startTime = mt.Start("WriteMissingNewline")
		}
		_, err = writer.Write(newline)
		m.BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		if mt != nil {
			mt.Stop(startTime)
		}
	}

	lineLengthWithoutEol := count
	switch eol {
	case container.None:
	case container.CR, container.LF:
		lineLengthWithoutEol -= 1
	case container.CRLF:
		lineLengthWithoutEol -= 2
	}

	m.BytesWrittenForRawData += int64(count)
	m.LineLengths.UpdateBucketCount(lineLengthWithoutEol)

	return nil
}

// Close closes the underlying VirtualFile.
func (r *FileHandle) Close() error {
	return r.File.Close()
}

func (r *FileHandle) GetBytesRead() int64 { return r.BytesRead }
func (r *FileHandle) GetFileSize() int64  { return r.Size }
func (r *FileHandle) IsDone() bool        { return r.Done }

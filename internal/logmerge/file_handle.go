package logmerge

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

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
	Size                int64       // Size of the file in bytes
	BytesRead           int64       // Number of bytes read from the file
	Done                bool        // Whether the file has been fully read
	Buffer              *RingBuffer // Buffer for reading the file
	LineTimestampParsed bool        // Whether the timestamp for the current line has been parsed
	LineTimestamp       Timestamp   // The timestamp for the current line
	BlockTimestamp      Timestamp   // The timestamp for the current block, i.e. the last non-zero timestamp
	Metrics             *MetricsTree
	MergeMetrics        *MergeMetrics
}

// NewFileHandle creates a FileHandle wrapping the given VirtualFile with its own
// read buffer and per-file metrics.
func NewFileHandle(file VirtualFile, alias string, bufferSize int) (fh *FileHandle, err error) {
	return &FileHandle{
		File:                file,
		Alias:               []byte(alias),
		Size:                file.Size(),
		BytesRead:           0,
		Buffer:              NewRingBuffer(bufferSize),
		Done:                false,
		LineTimestampParsed: false,
		LineTimestamp:       ZeroTimestamp,
		BlockTimestamp:      ZeroTimestamp,
		Metrics:             NewMetricsTree(),
		MergeMetrics:        NewMergeMetrics(),
	}, nil
}

// FillBuffer reads data from the underlying file into the ring buffer.
func (r *FileHandle) FillBuffer() error {
	n, err := r.Buffer.Fill(r.File)
	if err == io.EOF {
		return nil
	}

	r.BytesRead += int64(n)
	return err
}

// SkipLine advances past the current line in the buffer without writing it,
// returning the number of bytes skipped and the EOL length.
func (r *FileHandle) SkipLine() (bytesCount int, eolLength int, err error) {
	var (
		n               = 0
		latestCharWasCR = false
		eol             = None
	)

	for !r.Buffer.IsEmpty() {
		n, eol = r.Buffer.SkipNextLineSlice(&latestCharWasCR)
		bytesCount += n
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := r.Metrics.Start("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				err = fmt.Errorf("failed to fill buffer: %v", err)
				return
			}
			r.Metrics.Stop(startTime)
		}
	}

	switch eol {
	case None:
	case CR, LF:
		eolLength = 1
	case CRLF:
		eolLength = 2
	}
	return
}

// WriteLine writes the current line from the buffer to the writer, ensuring
// it ends with a newline. Updates byte and line-length metrics.
func (r *FileHandle) WriteLine(m *MergeMetrics, writer *bufio.Writer) error {
	var (
		count           = 0
		latestCharWasCR = false
		eol             = None
		chunk           []byte
		err             error = nil
	)

	for !r.Buffer.IsEmpty() || latestCharWasCR {
		startTime := r.Metrics.Start("PeekNextLineSlice")
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		r.Metrics.Stop(startTime)

		if chunk != nil {
			startTime = r.Metrics.Start("WriteLinePartial")
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				if eol == CRLF && len(chunk) == 1 && chunk[0] == '\n' {
					// This chunk is the '\n' part of a split CRLF.
					// The '\n' was returned by PeekNextLineSlice but not
					// consumed — skip it so it doesn't leak into the next line.
					r.Buffer.Skip(1)
				} else {
					r.Buffer.Skip(n)
				}
				count += n
			}
			r.Metrics.Stop(startTime)
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() && !latestCharWasCR {
			startTime := r.Metrics.Start("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			r.Metrics.Stop(startTime)
		}
	}

	// Ensure \n is written at the end of the line
	if eol != LF && eol != CRLF {
		startTime := r.Metrics.Start("WriteMissingNewline")
		_, err = writer.Write(newline)
		m.BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		r.Metrics.Stop(startTime)
	}

	lineLengthWithoutEol := count
	switch eol {
	case None:
	case CR, LF:
		lineLengthWithoutEol -= 1
	case CRLF:
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

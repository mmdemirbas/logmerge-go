package logmerge

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type FileHandle struct {
	File                *os.File
	Alias               []byte
	AliasForBlock       []byte
	AliasForLine        []byte
	Size                int          // Size of the file in bytes
	BytesRead           int          // Number of bytes read from the file
	Done                bool         // Whether the file has been fully read
	Buffer              *RingBuffer  // Buffer for reading the file
	LineTimestampParsed bool         // Whether the timestamp for the current line has been parsed
	LineTimestamp       Timestamp    // The timestamp for the current line
	BlockTimestamp      Timestamp    // The timestamp for the current block, i.e. the last non-zero timestamp
	Metrics             *MetricsTree // Local metrics for thread-safe tracking
}

func NewFileHandle(file *os.File, alias string, bufferSize int) (*FileHandle, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %v", alias, err)
	}
	fileSize := int(fileInfo.Size())
	return &FileHandle{
		File:                file,
		Alias:               []byte(alias),
		Size:                fileSize,
		BytesRead:           0,
		Buffer:              NewRingBuffer(bufferSize),
		Done:                false,
		LineTimestampParsed: false,
		LineTimestamp:       ZeroTimestamp,
		BlockTimestamp:      ZeroTimestamp,
		Metrics:             NewMetricsTree(),
	}, nil
}

func (r *FileHandle) FillBuffer() error {
	n, err := r.Buffer.Fill(r.File)
	if err == io.EOF {
		return nil
	}

	r.BytesRead += n
	return err
}

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
					// this chunk is just the '\n' part of a split CRLF
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

func (r *FileHandle) Close() error {
	return r.File.Close()
}

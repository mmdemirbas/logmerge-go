package main

import (
	"fmt"
	"io"
	"os"
)

type FileHandle struct {
	File            *os.File
	Alias           []byte
	AliasForBlock   []byte
	AliasForLine    []byte
	Size            int
	BytesRead       int
	Done            bool
	Buffer          *RingBuffer
	TimestampParsed bool
	Timestamp       Timestamp
}

func NewFileHandle(file *os.File, alias string, bufferSize int) (*FileHandle, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %v", alias, err)
	}
	fileSize := int(fileInfo.Size())
	return &FileHandle{
		File:   file,
		Alias:  []byte(alias),
		Size:   fileSize,
		Buffer: NewRingBuffer(bufferSize),
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
			startTime := GlobalMetricsTree.Start("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				err = fmt.Errorf("failed to fill buffer: %v", err)
				return
			}
			GlobalMetricsTree.Stop(startTime)
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

func (r *FileHandle) WriteLine(m *MergeMetrics, writer *BufferedWriter) error {
	var (
		count           = 0
		latestCharWasCR = false
		eol             = None
		chunk           []byte
		err             error = nil
	)

	for !r.Buffer.IsEmpty() {
		startTime := GlobalMetricsTree.Start("PeekNextLineSlice")
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		GlobalMetricsTree.Stop(startTime)

		if chunk != nil {
			startTime = GlobalMetricsTree.Start("WriteLinePartial")
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				r.Buffer.Skip(n)
				count += n
			}
			GlobalMetricsTree.Stop(startTime)
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := GlobalMetricsTree.Start("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			GlobalMetricsTree.Stop(startTime)
		}
	}

	// Ensure \n is written at the end of the line
	if eol != LF && eol != CRLF {
		startTime := GlobalMetricsTree.Start("WriteMissingNewline")
		_, err = writer.Write(newline)
		m.BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		GlobalMetricsTree.Stop(startTime)
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

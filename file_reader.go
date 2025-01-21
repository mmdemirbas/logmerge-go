package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type FileReader struct {
	File       *os.File
	Buffer     *RingBuffer
	SourceName string
	FileSize   int
	BytesRead  int
	eof        bool
}

// NewFileReader creates a new FileReader.
func NewFileReader(file *os.File, sourceName string, bufferSize int) (*FileReader, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %v", sourceName, err)
	}
	fileSize := int(fileInfo.Size())
	return &FileReader{
		File:       file,
		Buffer:     NewRingBuffer(bufferSize),
		SourceName: sourceName,
		FileSize:   fileSize,
	}, nil
}

// FillBuffer reads data from the file into the buffer to fill the empty slots.
func (r *FileReader) FillBuffer() error {
	if r.eof {
		return nil
	}
	if r.Buffer.IsFull() {
		return nil
	}

	n, err := r.Buffer.Fill(r.File)
	BytesRead += int64(n)
	r.BytesRead += n

	if err == io.EOF || n == 0 {
		r.eof = true
		return nil
	}
	return err
}

func (r *FileReader) WriteLine(writer *bufio.Writer) error {
	var (
		count                 = 0
		latestCharWasCR       = false
		eol                   = NIL
		err             error = nil
	)

	for !r.Buffer.IsEmpty() {
		startWriteLinePartial := MeasureStart("WriteLinePartial")
		eol, err = r.Buffer.WriteLinePartial(writer, &count, &latestCharWasCR)
		TotalWriteOutputDuration += MeasureSince(startWriteLinePartial)

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != NIL {
			break
		}

		startOfFillBuffer := MeasureStart("FillBuffer")
		err = r.FillBuffer()
		TotalFillBufferDuration += MeasureSince(startOfFillBuffer)

		if err != nil {
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
	}

	// Ensure \n is written at the end of the line
	startOfWriteMissingNewline := MeasureStart("WriteMissingNewline")
	if eol != LF && eol != CRLF {
		// Write the last line
		err = writer.WriteByte('\n')
		BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
	}
	TotalWriteOutputDuration += MeasureSince(startOfWriteMissingNewline)

	lineLengthWithoutEol := count
	switch eol {
	case NIL:
	case CR, LF:
		lineLengthWithoutEol -= 1
	case CRLF:
		lineLengthWithoutEol -= 2
	}

	LinesRead++
	BytesWrittenForRawData += int64(count)
	MaxLineLength = max(MaxLineLength, lineLengthWithoutEol)
	UpdateBucketCount(lineLengthWithoutEol, LineLengthBucketLevels, LineLengthBucketValues)

	return nil
}

// Close closes the file.
func (r *FileReader) Close() error {
	return r.File.Close()
}

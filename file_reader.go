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
	n, err := r.Buffer.Fill(r.File)
	if err == io.EOF {
		return nil
	}

	BytesRead += int64(n)
	r.BytesRead += n
	return err
}

func (r *FileReader) WriteLine(writer *bufio.Writer) error {
	var (
		count           = 0
		latestCharWasCR = false
		eol             = None
		chunk           []byte
		err             error = nil
	)

	for !r.Buffer.IsEmpty() {
		startPeekNextLineSlice := MeasureStart("PeekNextLineSlice")
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		MeasureSince(startPeekNextLineSlice)

		if chunk != nil {
			startWriteLinePartial := MeasureStart("WriteLinePartial")
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				r.Buffer.Skip(n)
				count += n
			}
			TotalWriteOutputDuration += MeasureSince(startWriteLinePartial)
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != None {
			break
		}

		// TODO: Fill only if empty
		if r.Buffer.IsEmpty() {
			startOfFillBuffer := MeasureStart("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			TotalFillBufferDuration += MeasureSince(startOfFillBuffer)
		}
	}

	// Ensure \n is written at the end of the line
	if eol != LF && eol != CRLF {
		startOfWriteMissingNewline := MeasureStart("WriteMissingNewline")
		err = writer.WriteByte('\n')
		BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		TotalWriteOutputDuration += MeasureSince(startOfWriteMissingNewline)
	}

	lineLengthWithoutEol := count
	switch eol {
	case None:
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

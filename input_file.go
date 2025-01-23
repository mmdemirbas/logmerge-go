package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

type InputFile struct {
	File               *os.File
	Buffer             *RingBuffer
	CurrentTimestamp   time.Time
	SourceName         string
	SourceNamePerBlock string
	SourceNamePerLine  string
	FileSize           int // TODO: unused
}

func NewInputFile(file *os.File, sourceName string, bufferSize int) (*InputFile, error) {
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %v", sourceName, err)
	}
	fileSize := int(fileInfo.Size())
	return &InputFile{
		File:       file,
		Buffer:     NewRingBuffer(bufferSize),
		SourceName: sourceName,
		FileSize:   fileSize,
	}, nil
}

// FillBuffer reads data from the file into the buffer to fill the empty slots.
func (r *InputFile) FillBuffer() error {
	n, err := r.Buffer.Fill(r.File)
	if err == io.EOF {
		return nil
	}

	BytesRead += int64(n)
	return err
}

func (r *InputFile) SkipLine() error {
	var (
		count                 = 0
		n                     = 0
		latestCharWasCR       = false
		eol                   = None
		err             error = nil
	)

	for !r.Buffer.IsEmpty() {
		n, eol = r.Buffer.GetNextLineSliceLen(&latestCharWasCR)
		r.Buffer.Skip(n)
		count += n
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := MeasureStart("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			FillBufferMetric.MeasureSince(startTime)
		}
	}

	lineLengthWithoutEol := count
	switch eol {
	case None:
	case CR, LF:
		lineLengthWithoutEol -= 1
	case CRLF:
		lineLengthWithoutEol -= 2
	}

	LineLengths.UpdateBucketCount(lineLengthWithoutEol)
	return nil
}

func (r *InputFile) WriteLine(writer *bufio.Writer) error {
	var (
		count           = 0
		latestCharWasCR = false
		eol             = None
		chunk           []byte
		err             error = nil
	)

	for !r.Buffer.IsEmpty() {
		startTime := MeasureStart("PeekNextLineSlice")
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		PeekNextLineSliceMetric.MeasureSince(startTime)

		if chunk != nil {
			startTime = MeasureStart("WriteLinePartial")
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				r.Buffer.Skip(n)
				count += n
			}
			WriteOutputMetric.MeasureSince(startTime)
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := MeasureStart("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			FillBufferMetric.MeasureSince(startTime)
		}
	}

	// Ensure \n is written at the end of the line
	if eol != LF && eol != CRLF {
		startTime := MeasureStart("WriteMissingNewline")
		err = writer.WriteByte('\n')
		BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		WriteOutputMetric.MeasureSince(startTime)
	}

	lineLengthWithoutEol := count
	switch eol {
	case None:
	case CR, LF:
		lineLengthWithoutEol -= 1
	case CRLF:
		lineLengthWithoutEol -= 2
	}

	BytesWrittenForRawData += int64(count)
	LineLengths.UpdateBucketCount(lineLengthWithoutEol)

	return nil
}

// Close closes the file.
func (r *InputFile) Close() error {
	return r.File.Close()
}

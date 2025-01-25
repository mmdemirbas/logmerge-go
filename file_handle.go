package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type FileHandle struct {
	File            *os.File
	Alias           string
	AliasForBlock   string
	AliasForLine    string
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
		Alias:  alias,
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

func (r *FileHandle) SkipLine(c *AppConfig) error {
	var (
		lineLengthWithoutEol       = 0
		n                          = 0
		latestCharWasCR            = false
		eol                        = None
		err                  error = nil
	)

	for !r.Buffer.IsEmpty() {
		n, eol = r.Buffer.SkipNextLineSlice(&latestCharWasCR)
		lineLengthWithoutEol += n
		BytesReadAndSkipped += int64(n)
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := MeasureStart(c, "FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			FillBufferMetric.MeasureSince(c, startTime)
		}
	}

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

func (r *FileHandle) WriteLine(c *AppConfig, writer *bufio.Writer) error {
	var (
		count           = 0
		latestCharWasCR = false
		eol             = None
		chunk           []byte
		err             error = nil
	)

	for !r.Buffer.IsEmpty() {
		startTime := MeasureStart(c, "PeekNextLineSlice")
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		PeekNextLineSliceMetric.MeasureSince(c, startTime)

		if chunk != nil {
			startTime = MeasureStart(c, "WriteLinePartial")
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				r.Buffer.Skip(n)
				count += n
			}
			WriteOutputMetric.MeasureSince(c, startTime)
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := MeasureStart(c, "FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			FillBufferMetric.MeasureSince(c, startTime)
		}
	}

	// Ensure \n is written at the end of the line
	if eol != LF && eol != CRLF {
		startTime := MeasureStart(c, "WriteMissingNewline")
		err = writer.WriteByte('\n')
		BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		WriteOutputMetric.MeasureSince(c, startTime)
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

func (r *FileHandle) Close() error {
	return r.File.Close()
}

func (r *FileHandle) UpdateTimestamp(c *AppConfig, timestampBuffer []byte) error {
	bufLen := r.Buffer.Len()
	if bufLen < c.TimestampSearchEndIndex {
		startTime := MeasureStart(c, "FillBuffer")
		err := r.FillBuffer()
		if err != nil {
			r.TimestampParsed = false
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
		FillBufferMetric.MeasureSince(c, startTime)

		if bufLen == 0 && r.Buffer.IsEmpty() {
			r.TimestampParsed = false
			return nil
		}
	}

	startTime := MeasureStart(c, "BufferAsSlice")
	buf := r.Buffer.AsSlice(timestampBuffer)
	BufferAsSliceMetric.MeasureSince(c, startTime)

	startTime = MeasureStart(c, "ParseTimestamp")
	timestamp := ParseTimestamp(c, buf)
	ParseTimestampMetric.MeasureSince(c, startTime)

	if timestamp == noTimestamp {
		LinesWithoutTimestamps++
	} else {
		LinesWithTimestamps++
	}

	r.TimestampParsed = true
	r.Timestamp = timestamp
	return nil
}

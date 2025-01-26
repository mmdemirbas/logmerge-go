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

func (r *FileHandle) SkipLine(m *MergeMetrics) (bytesCount int, eolLength int, err error) {
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
			startTime := m.MetricsTree.MeasureStart("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				err = fmt.Errorf("failed to fill buffer: %v", err)
				return
			}
			m.FillBufferMetric.MeasureSince(startTime)
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

	for !r.Buffer.IsEmpty() {
		startTime := m.MetricsTree.MeasureStart("PeekNextLineSlice")
		chunk, eol = r.Buffer.PeekNextLineSlice(&latestCharWasCR)
		m.PeekNextLineSliceMetric.MeasureSince(startTime)

		if chunk != nil {
			startTime = m.MetricsTree.MeasureStart("WriteLinePartial")
			var n int
			n, err = writer.Write(chunk)
			if err == nil {
				r.Buffer.Skip(n)
				count += n
			}
			m.WriteOutputMetric.MeasureSince(startTime)
		}

		if err != nil {
			return fmt.Errorf("failed to write line to output: %v", err)
		}
		if eol != None {
			break
		}

		if r.Buffer.IsEmpty() {
			startTime := m.MetricsTree.MeasureStart("FillBuffer")
			err = r.FillBuffer()
			if err != nil {
				return fmt.Errorf("failed to fill buffer: %v", err)
			}
			m.FillBufferMetric.MeasureSince(startTime)
		}
	}

	// Ensure \n is written at the end of the line
	if eol != LF && eol != CRLF {
		startTime := m.MetricsTree.MeasureStart("WriteMissingNewline")
		err = writer.WriteByte('\n')
		m.BytesWrittenForMissingNewlines++
		if err != nil {
			return fmt.Errorf("failed to write newline: %v", err)
		}
		m.WriteOutputMetric.MeasureSince(startTime)
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

var parseTimestampBuffer []byte

// TODO: Isolate MergeMetrics, ParseTimestampConfig, ParseTimestampMetrics
func (r *FileHandle) UpdateTimestamp(m *MergeMetrics, pc *ParseTimestampConfig, pm *ParseTimestampMetrics) error {
	bufLen := r.Buffer.Len()
	if bufLen < pc.TimestampSearchEndIndex {
		startTime := m.MetricsTree.MeasureStart("FillBuffer")
		err := r.FillBuffer()
		if err != nil {
			r.TimestampParsed = false
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
		m.FillBufferMetric.MeasureSince(startTime)

		if bufLen == 0 && r.Buffer.IsEmpty() {
			r.TimestampParsed = false
			return nil
		}
	}

	if parseTimestampBuffer == nil {
		parseTimestampBuffer = make([]byte, pc.TimestampSearchEndIndex)
	}

	startTime := m.MetricsTree.MeasureStart("BufferAsSlice")
	buf := r.Buffer.AsSlice(parseTimestampBuffer)
	m.BufferAsSliceMetric.MeasureSince(startTime)

	startTime = m.MetricsTree.MeasureStart("ParseTimestamp")
	timestamp := ParseTimestamp(pc, pm, buf)
	m.ParseTimestampMetric.MeasureSince(startTime)

	if timestamp == ZeroTimestamp {
		m.LinesWithoutTimestamps++
	} else {
		m.LinesWithTimestamps++
	}

	r.TimestampParsed = true
	r.Timestamp = timestamp
	return nil
}

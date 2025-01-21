package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
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
func NewFileReader(file *os.File, sourceName string, bufferSize int) *FileReader {
	fileInfo, err := file.Stat()
	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "failed to get file info: %v\n", err)
		return nil
	}
	fileSize := int(fileInfo.Size())
	return &FileReader{
		File:       file,
		Buffer:     NewRingBuffer(bufferSize),
		SourceName: sourceName,
		FileSize:   fileSize,
	}
}

// FillBuffer reads data from the file into the buffer to fill the empty slots.
func (r *FileReader) FillBuffer() error {
	if r.eof {
		if enableDebugLogging {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(
				os.Stderr,
				"%-33s EOF   %.2f%% %11d / %11d - current: %s -> %.2f%% %11d / %11d\n",
				time.Now().Format(time.RFC3339Nano),
				100.0*float64(BytesRead)/float64(ExpectedBytesToRead),
				BytesRead,
				ExpectedBytesToRead,
				r.SourceName,
				100.0*float64(r.BytesRead)/float64(r.FileSize),
				r.BytesRead,
				r.FileSize,
			)
		}
		return nil
	}
	if r.Buffer.IsFull() {
		if enableDebugLogging {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(
				os.Stderr,
				"%-33s FULL  %.2f%% %11d / %11d - current: %s -> %.2f%% %11d / %11d\n",
				time.Now().Format(time.RFC3339Nano),
				100.0*float64(BytesRead)/float64(ExpectedBytesToRead),
				BytesRead,
				ExpectedBytesToRead,
				r.SourceName,
				100.0*float64(r.BytesRead)/float64(r.FileSize),
				r.BytesRead,
				r.FileSize,
			)
		}
		return nil
	}

	n, err := r.Buffer.Fill(r.File)
	BytesRead += int64(n)
	r.BytesRead += n
	if enableDebugLogging {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(
			os.Stderr,
			"%-33s READ  %.2f%% %11d / %11d - current: %s -> %.2f%% %11d / %11d\n",
			time.Now().Format(time.RFC3339Nano),
			100.0*float64(BytesRead)/float64(ExpectedBytesToRead),
			BytesRead,
			ExpectedBytesToRead,
			r.SourceName,
			100.0*float64(r.BytesRead)/float64(r.FileSize),
			r.BytesRead,
			r.FileSize,
		)
	}
	if err == io.EOF || n == 0 {
		r.eof = true
		return nil
	}
	return err
}

func (r *FileReader) WriteLine(writer *bufio.Writer) {
	n := 0
	crFound := false
	lfFound := false

	for !r.Buffer.IsEmpty() {
		beforeBufferWriteLine := MeasureStart("WriteLinePartial")
		eol, err := r.Buffer.WriteLinePartial(writer, &n, &crFound, &lfFound)
		RB_WriteLineDuration += MeasureSince(beforeBufferWriteLine)
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "failed to write raw lines to output: %v\n", err)
			break
		}
		if eol {
			break
		}

		beforeFillBuffer := MeasureStart("FillBuffer2")
		err = r.FillBuffer()
		RB_FillBufferDuration += MeasureSince(beforeFillBuffer)
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "failed to refill buffer: %v\n", err)
		}
	}

	// Ensure \n is written at the end of the line
	if !lfFound {
		// Write the last line
		err := writer.WriteByte('\n')
		BytesWrittenForMissingNewlines++
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "failed to write last new line character to output: %v\n", err)
		}
	}

	lineLengthWithoutEol := n
	if lfFound {
		if crFound {
			lineLengthWithoutEol -= 2
		} else {
			lineLengthWithoutEol -= 1
		}
	}

	LinesRead++
	BytesWrittenForRawData += int64(n)
	MaxLineLength = max(MaxLineLength, lineLengthWithoutEol)
	UpdateBucketCount(lineLengthWithoutEol, LineLengthBucketLevels, LineLengthBucketValues)
}

// Close closes the file.
func (r *FileReader) Close() error {
	return r.File.Close()
}

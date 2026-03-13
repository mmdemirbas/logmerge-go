package logmerge_test

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/mmdemirbas/logmerge/internal/logmerge"
)

var nullLog = &logmerge.WritableFile{File: os.NewFile(0, os.DevNull)}

func BenchmarkProcessFiles_Saturation(b *testing.B) {
	// configuration
	numFiles := 20
	linesPerFile := 10000
	lineTemplate := "2026-03-13 01:21:22.000000000 [INFO] Simulation log line %d\n"

	// Prepare mock files in a temporary directory
	tmpDir, err := os.MkdirTemp("", "logmerge-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	var files []*logmerge.FileHandle
	var osFiles []*os.File // keep references for seeking
	var totalBytes int64

	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("%s/file-%d.log", tmpDir, i)
		f, _ := os.Create(path)

		writer := bufio.NewWriter(f)
		for j := 0; j < linesPerFile; j++ {
			// Offset timestamps slightly so they interleave
			ts := 1741825282000000000 + int64(j*1000000000) + int64(i*100)
			line := fmt.Sprintf(lineTemplate, ts)
			n, _ := writer.WriteString(line)
			totalBytes += int64(n)
		}
		writer.Flush()
		f.Seek(0, 0)

		handle, _ := logmerge.NewFileHandle(&logmerge.OsFile{F: f}, fmt.Sprintf("file-%d", i), 1024*1024)
		files = append(files, handle)
		osFiles = append(osFiles, f)
	}

	config := &logmerge.MergeConfig{
		BufferSizeForWrite: 1024 * 1024,
	}

	b.ResetTimer()
	b.SetBytes(totalBytes) // This enables MB/s reporting
	for i := 0; i < b.N; i++ {
		// Reset handles for each iteration
		for j, h := range files {
			osFiles[j].Seek(0, 0)
			h.Done = false
			h.BytesRead = 0
			h.LineTimestampParsed = false
			h.Buffer = logmerge.NewRingBuffer(1024 * 1024)
		}

		out := io.Discard // Bypass actual disk I/O to measure pure CPU/Logic overhead
		writer := bufio.NewWriter(out)

		_ = logmerge.ProcessFiles(
			config,
			logmerge.NewMergeMetrics(),
			files,
			writer,
			&logmerge.WritableFile{File: os.Stderr},
			func(f *logmerge.FileHandle) error {
				return logmerge.UpdateTimestamp(&logmerge.ParseTimestampConfig{
					ShortestTimestampLen:    15,
					TimestampSearchEndIndex: 250,
				}, f)
			},
		)
		writer.Flush()
	}
}

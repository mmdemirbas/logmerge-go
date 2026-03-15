package core_test

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/mmdemirbas/logmerge/internal/core"
	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
)

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

	paths := make([]string, numFiles)
	var totalBytes int64

	for i := 0; i < numFiles; i++ {
		path := fmt.Sprintf("%s/file-%d.log", tmpDir, i)
		paths[i] = path
		f, err := os.Create(path)
		if err != nil {
			b.Fatal(err)
		}

		writer := bufio.NewWriter(f)
		for j := 0; j < linesPerFile; j++ {
			// Offset timestamps slightly so they interleave
			ts := 1741825282000000000 + int64(j*1000000000) + int64(i*100)
			line := fmt.Sprintf(lineTemplate, ts)
			n, _ := writer.WriteString(line)
			totalBytes += int64(n)
		}
		writer.Flush()
		f.Close()
	}

	config := &core.MergeConfig{
		BufferSizeForWrite: 1024 * 1024,
		MaxTimestamp:       logtime.Timestamp(1<<63 - 1),
	}

	tsConfig := &logtime.ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	b.ResetTimer()
	b.SetBytes(totalBytes) // This enables MB/s reporting
	for i := 0; i < b.N; i++ {
		// Reopen files from disk each iteration since ProcessFiles closes them
		files := make([]*fsutil.FileHandle, numFiles)
		for j, path := range paths {
			f, err := os.Open(path)
			if err != nil {
				b.Fatal(err)
			}
			files[j], err = fsutil.NewFileHandle(&fsutil.OsFile{F: f}, fmt.Sprintf("file-%d", j), 1024*1024)
			if err != nil {
				b.Fatal(err)
			}
		}

		out := io.Discard // Bypass actual disk I/O to measure pure CPU/Logic overhead
		writer := bufio.NewWriter(out)

		err := core.ProcessFiles(
			config,
			metrics.NewMergeMetrics(),
			files,
			writer,
			&fsutil.WritableFile{File: os.Stderr},
			func(f *fsutil.FileHandle) error {
				return core.UpdateTimestamp(tsConfig, f)
			},
		)
		if err != nil {
			b.Fatal(err)
		}
		writer.Flush()
	}
}

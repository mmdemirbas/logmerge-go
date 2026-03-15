package core_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/core"
	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
)

// --- MinHeap benchmarks ---

func BenchmarkMinHeap_PushPop_20(b *testing.B) {
	const n = 20
	files := make([]*fsutil.FileHandle, n)
	for i := range files {
		files[i] = &fsutil.FileHandle{
			LineTimestamp: logtime.Timestamp(1741825282000000000 + int64(i)*1000000000),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := NewMinHeap(n)
		for _, f := range files {
			h.Push(f)
		}
		for h.Len() > 0 {
			h.Pop()
		}
	}
}

func BenchmarkMinHeap_PushPop_100(b *testing.B) {
	const n = 100
	files := make([]*fsutil.FileHandle, n)
	for i := range files {
		files[i] = &fsutil.FileHandle{
			LineTimestamp: logtime.Timestamp(1741825282000000000 + int64(i)*1000000000),
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		h := NewMinHeap(n)
		for _, f := range files {
			h.Push(f)
		}
		for h.Len() > 0 {
			h.Pop()
		}
	}
}

func BenchmarkMinHeap_SteadyState(b *testing.B) {
	// Simulates the merge loop: pop min, update timestamp, push back
	const n = 20
	h := NewMinHeap(n)
	files := make([]*fsutil.FileHandle, n)
	for i := range files {
		files[i] = &fsutil.FileHandle{
			LineTimestamp: logtime.Timestamp(1741825282000000000 + int64(i)*1000000000),
		}
		h.Push(files[i])
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := h.Pop()
		f.LineTimestamp += logtime.Timestamp(1000000000) // advance 1 second
		h.Push(f)
	}
}

// --- UpdateTimestamp benchmarks ---

func BenchmarkUpdateTimestamp_NumericAtStart(b *testing.B) {
	content := "2026-03-13 01:21:22.000000000 [INFO] Simulation log line 174182528200\n"
	vf := newMemFile("test.log", content)
	fh, _ := fsutil.NewFileHandle(vf, "test", 4096)
	fh.Metrics = nil
	c := &logtime.ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fh.Buffer.Skip(fh.Buffer.Len())
		vf.(*memFile).Reader = bytes.NewReader([]byte(content))
		fh.FillBuffer()
		UpdateTimestamp(c, fh, false)
	}
}

func BenchmarkUpdateTimestamp_WithStripPositions(b *testing.B) {
	content := "2026-03-13 01:21:22.000000000 [INFO] Simulation log line 174182528200\n"
	vf := newMemFile("test.log", content)
	fh, _ := fsutil.NewFileHandle(vf, "test", 4096)
	fh.Metrics = nil
	c := &logtime.ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fh.Buffer.Skip(fh.Buffer.Len())
		vf.(*memFile).Reader = bytes.NewReader([]byte(content))
		fh.FillBuffer()
		UpdateTimestamp(c, fh, true)
	}
}

// --- ProcessFiles with different configurations ---

func BenchmarkProcessFiles_NoTimestampWrite(b *testing.B) {
	benchProcessFiles(b, &MergeConfig{
		MaxTimestamp:       ^logtime.Timestamp(0),
		BufferSizeForWrite: 64 * 1024,
	})
}

func BenchmarkProcessFiles_WithTimestampWrite(b *testing.B) {
	benchProcessFiles(b, &MergeConfig{
		MaxTimestamp:          ^logtime.Timestamp(0),
		BufferSizeForWrite:    64 * 1024,
		WriteTimestampPerLine: true,
	})
}

func BenchmarkProcessFiles_WithStrip(b *testing.B) {
	benchProcessFiles(b, &MergeConfig{
		MaxTimestamp:           ^logtime.Timestamp(0),
		BufferSizeForWrite:     64 * 1024,
		StripOriginalTimestamp: true,
	})
}

func BenchmarkProcessFiles_WithStripAndTimestamp(b *testing.B) {
	benchProcessFiles(b, &MergeConfig{
		MaxTimestamp:           ^logtime.Timestamp(0),
		BufferSizeForWrite:     64 * 1024,
		StripOriginalTimestamp: true,
		WriteTimestampPerLine:  true,
	})
}

func BenchmarkProcessFiles_SingleFile(b *testing.B) {
	benchProcessFilesN(b, 1, 200000, &MergeConfig{
		MaxTimestamp:       ^logtime.Timestamp(0),
		BufferSizeForWrite: 64 * 1024,
	})
}

func BenchmarkProcessFiles_ManyFiles(b *testing.B) {
	benchProcessFilesN(b, 100, 2000, &MergeConfig{
		MaxTimestamp:       ^logtime.Timestamp(0),
		BufferSizeForWrite: 64 * 1024,
	})
}

func benchProcessFiles(b *testing.B, config *MergeConfig) {
	benchProcessFilesN(b, 20, 10000, config)
}

func benchProcessFilesN(b *testing.B, numFiles, linesPerFile int, config *MergeConfig) {
	b.Helper()
	lineTemplate := "2026-03-13 01:21:22.000000000 [INFO] Simulation log line %d\n"

	contents := make([][]byte, numFiles)
	var totalBytes int64
	for i := range contents {
		var buf bytes.Buffer
		for j := 0; j < linesPerFile; j++ {
			ts := 1741825282000000000 + int64(j*1000000000) + int64(i*100)
			line := []byte(fmt.Sprintf(lineTemplate, ts))
			buf.Write(line)
		}
		contents[i] = buf.Bytes()
		totalBytes += int64(len(contents[i]))
	}

	tsConfig := &logtime.ParseTimestampConfig{
		ShortestTimestampLen:    15,
		TimestampSearchEndIndex: 250,
	}

	b.ResetTimer()
	b.SetBytes(totalBytes)
	for i := 0; i < b.N; i++ {
		files := make([]*fsutil.FileHandle, numFiles)
		for j := range files {
			vf := newMemFile(fmt.Sprintf("file-%d", j), string(contents[j]))
			files[j], _ = fsutil.NewFileHandle(vf, fmt.Sprintf("file-%d", j), 1024*1024)
			files[j].Metrics = nil
			files[j].MergeMetrics = metrics.NewMergeMetricsLite()
		}

		writer := bufio.NewWriter(io.Discard)
		m := metrics.NewMergeMetricsLite()
		logFile := &bytes.Buffer{}

		strip := config.StripOriginalTimestamp
		ProcessFiles(
			config,
			m,
			files,
			writer,
			logFile,
			func(f *fsutil.FileHandle) error {
				return UpdateTimestamp(tsConfig, f, strip)
			},
		)
		writer.Flush()
	}
}

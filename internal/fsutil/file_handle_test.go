package fsutil_test

import (
	"bufio"
	"bytes"
	"strings"
	"testing"
	"time"

	. "github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/metrics"
	"github.com/mmdemirbas/logmerge/internal/testutil"
)

func newMemFile(name, content string) VirtualFile {
	b := []byte(content)
	return &memFile{Reader: bytes.NewReader(b), name: name, size: int64(len(b))}
}

type memFile struct {
	*bytes.Reader
	name string
	size int64
}

func (m *memFile) Close() error { return nil }
func (m *memFile) Name() string { return m.name }
func (m *memFile) Size() int64  { return m.size }

func TestFillBuffer_ReadsData(t *testing.T) {
	content := "2024-01-15 10:00:00 hello world\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)

	err := fh.FillBuffer()
	if err != nil {
		t.Fatalf("FillBuffer failed: %v", err)
	}

	if fh.BytesRead == 0 {
		t.Error("expected BytesRead > 0 after FillBuffer")
	}
	if fh.Buffer.IsEmpty() {
		t.Error("expected buffer to have data after FillBuffer")
	}
}

func TestFillBuffer_EmptyFile(t *testing.T) {
	vf := newMemFile("empty.log", "")
	fh, _ := NewFileHandle(vf, "empty", 4096)

	err := fh.FillBuffer()
	if err != nil {
		t.Fatalf("FillBuffer failed: %v", err)
	}

	if fh.BytesRead != 0 {
		t.Errorf("expected BytesRead=0 for empty file, got %d", fh.BytesRead)
	}
}

func TestFillBuffer_MultipleFills(t *testing.T) {
	// Use content larger than a small buffer to require multiple fills
	content := strings.Repeat("abcdefghij", 20) + "\n"
	vf := newMemFile("big.log", content)
	fh, _ := NewFileHandle(vf, "big", 64) // small buffer

	err := fh.FillBuffer()
	if err != nil {
		t.Fatalf("first FillBuffer failed: %v", err)
	}
	firstRead := fh.BytesRead

	// Drain some buffer space
	fh.Buffer.Skip(32)

	err = fh.FillBuffer()
	if err != nil {
		t.Fatalf("second FillBuffer failed: %v", err)
	}

	if fh.BytesRead <= firstRead {
		t.Errorf("expected more bytes after second fill, first=%d total=%d", firstRead, fh.BytesRead)
	}
}

func TestSkipLine_SingleLine(t *testing.T) {
	content := "line one\nline two\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	bytesCount, eolLen, err := fh.SkipLine()
	if err != nil {
		t.Fatalf("SkipLine failed: %v", err)
	}

	// "line one\n" = 9 bytes
	if bytesCount != 9 {
		t.Errorf("expected 9 bytes skipped, got %d", bytesCount)
	}
	if eolLen != 1 {
		t.Errorf("expected eolLength=1 for LF, got %d", eolLen)
	}
}

func TestSkipLine_CRLF(t *testing.T) {
	content := "line one\r\nline two\r\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	bytesCount, eolLen, err := fh.SkipLine()
	if err != nil {
		t.Fatalf("SkipLine failed: %v", err)
	}

	// "line one\r\n" = 10 bytes
	if bytesCount != 10 {
		t.Errorf("expected 10 bytes skipped, got %d", bytesCount)
	}
	if eolLen != 2 {
		t.Errorf("expected eolLength=2 for CRLF, got %d", eolLen)
	}
}

func TestWriteLine_Basic(t *testing.T) {
	content := "hello world\nsecond line\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()

	err := fh.WriteLine(m, writer)
	if err != nil {
		t.Fatalf("WriteLine failed: %v", err)
	}
	writer.Flush()

	testutil.AssertEquals(t, "hello world\n", buf.String())
}

func TestWriteLine_NoTrailingNewline(t *testing.T) {
	content := "no newline at end"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()

	err := fh.WriteLine(m, writer)
	if err != nil {
		t.Fatalf("WriteLine failed: %v", err)
	}
	writer.Flush()

	// Should add a missing newline
	if !strings.HasSuffix(buf.String(), "\n") {
		t.Errorf("expected trailing newline, got: %q", buf.String())
	}
	if m.BytesWrittenForMissingNewlines != 1 {
		t.Errorf("expected BytesWrittenForMissingNewlines=1, got %d", m.BytesWrittenForMissingNewlines)
	}
}

func TestWriteLine_UpdatesMetrics(t *testing.T) {
	content := "abcdef\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()

	fh.WriteLine(m, writer)
	writer.Flush()

	if m.BytesWrittenForRawData != 7 { // "abcdef\n" = 7 bytes
		t.Errorf("expected BytesWrittenForRawData=7, got %d", m.BytesWrittenForRawData)
	}
}

func TestWriteLine_MultipleLines(t *testing.T) {
	content := "first\nsecond\nthird\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()

	// Write three lines
	for i := 0; i < 3; i++ {
		err := fh.WriteLine(m, writer)
		if err != nil {
			t.Fatalf("WriteLine %d failed: %v", i, err)
		}
	}
	writer.Flush()

	testutil.AssertEquals(t, content, buf.String())
}

func TestWriteLine_CRAtEOF(t *testing.T) {
	// File ending with \r (CR as the very last byte, no LF follows).
	// This previously caused an infinite loop in WriteLine because
	// PeekNextLineSlice checks empty buffer before latestCharWasCR,
	// leaving the flag set forever.
	content := "hello\r"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)
	fh.FillBuffer()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()

	// Run in goroutine with timeout to detect infinite loop
	done := make(chan error, 1)
	go func() {
		done <- fh.WriteLine(m, writer)
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WriteLine failed: %v", err)
		}
		writer.Flush()
		// CR line ending: the \r is written as content, then \n appended
		testutil.AssertEquals(t, "hello\r\n", buf.String())
	case <-timer.C:
		t.Fatal("WriteLine hung — infinite loop on CR at EOF")
	}
}

func TestClose(t *testing.T) {
	vf := newMemFile("test.log", "data")
	fh, _ := NewFileHandle(vf, "test", 4096)

	err := fh.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestSkipLine_LineLongerThanBuffer(t *testing.T) {
	// Line is 17 bytes ("abcdefghijklmnop\n"), buffer is 8 bytes.
	// This exercises the fill-in-the-middle path of SkipLine.
	content := "abcdefghijklmnop\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 8)
	fh.FillBuffer()

	bytesCount, eolLen, err := fh.SkipLine()
	if err != nil {
		t.Fatalf("SkipLine failed: %v", err)
	}

	testutil.AssertEquals(t, 17, bytesCount)
	testutil.AssertEquals(t, 1, eolLen)
}

func TestSkipLine_CRLFSplitAcrossBufferFill(t *testing.T) {
	// Buffer size 6, content "abcd\r\nefgh\n" (11 bytes).
	// The \r\n may split across buffer fills.
	content := "abcd\r\nefgh\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 6)
	fh.FillBuffer()

	bytesCount, eolLen, err := fh.SkipLine()
	if err != nil {
		t.Fatalf("SkipLine failed: %v", err)
	}

	// "abcd\r\n" = 6 bytes, eol length = 2 (CRLF)
	testutil.AssertEquals(t, 6, bytesCount)
	testutil.AssertEquals(t, 2, eolLen)
}

func TestWriteLine_LineLongerThanBuffer(t *testing.T) {
	// Line is 17 bytes, buffer is 8 bytes. Verify full line appears in output.
	content := "abcdefghijklmnop\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 8)
	fh.FillBuffer()

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()

	// Run in goroutine with timeout to detect potential infinite loop
	done := make(chan error, 1)
	go func() {
		done <- fh.WriteLine(m, writer)
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WriteLine failed: %v", err)
		}
		writer.Flush()
		testutil.AssertEquals(t, "abcdefghijklmnop\n", buf.String())
	case <-timer.C:
		t.Fatal("WriteLine hung — possible infinite loop on line longer than buffer")
	}
}

func TestFillBuffer_EofReachedFlag(t *testing.T) {
	// After the first FillBuffer reads all data, subsequent calls should
	// return nil immediately (eofReached flag).
	content := "short\n"
	vf := newMemFile("test.log", content)
	fh, _ := NewFileHandle(vf, "test", 4096)

	err := fh.FillBuffer()
	if err != nil {
		t.Fatalf("first FillBuffer failed: %v", err)
	}
	firstRead := fh.BytesRead

	// Second fill should hit EOF and set the flag
	err = fh.FillBuffer()
	if err != nil {
		t.Fatalf("second FillBuffer failed: %v", err)
	}

	// Third fill should be a no-op because eofReached is already set
	err = fh.FillBuffer()
	if err != nil {
		t.Fatalf("third FillBuffer failed: %v", err)
	}
	thirdRead := fh.BytesRead

	// BytesRead should not increase after EOF is reached
	if thirdRead != firstRead {
		t.Errorf("expected BytesRead to stay at %d after EOF, got %d", firstRead, thirdRead)
	}
}

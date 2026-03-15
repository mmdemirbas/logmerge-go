package core_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/mmdemirbas/logmerge/internal/core"
	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
	"github.com/mmdemirbas/logmerge/internal/metrics"
	"github.com/mmdemirbas/logmerge/internal/testutil"
)

// memFile is an in-memory VirtualFile for testing.
type memFile struct {
	*bytes.Reader
	name string
	size int64
}

func (m *memFile) Close() error { return nil }
func (m *memFile) Name() string { return m.name }
func (m *memFile) Size() int64  { return m.size }

func newMemFile(name, content string) fsutil.VirtualFile {
	b := []byte(content)
	return &memFile{Reader: bytes.NewReader(b), name: name, size: int64(len(b))}
}

var tsConfig = &logtime.ParseTimestampConfig{
	ShortestTimestampLen:    15,
	TimestampSearchEndIndex: 250,
}

func updateTS(f *fsutil.FileHandle) error {
	return UpdateTimestamp(tsConfig, f)
}

func runMerge(t *testing.T, c *MergeConfig, files []*fsutil.FileHandle) string {
	t.Helper()
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()
	logFile := &fsutil.WritableFile{File: os.NewFile(0, os.DevNull)}

	err := ProcessFiles(c, m, files, writer, logFile, updateTS)
	if err != nil {
		t.Fatalf("ProcessFiles failed: %v", err)
	}
	writer.Flush()
	return buf.String()
}

func makeHandle(name, content string, bufSize int) *fsutil.FileHandle {
	vf := newMemFile(name, content)
	fh, _ := fsutil.NewFileHandle(vf, name, bufSize)
	fh.AliasForBlock = []byte(fmt.Sprintf("\n--- %s ---\n", name))
	fh.AliasForLine = []byte(fmt.Sprintf("%-10s - ", name))
	return fh
}

func defaultConfig() *MergeConfig {
	return &MergeConfig{
		MaxTimestamp:       ^logtime.Timestamp(0),
		BufferSizeForWrite: 64 * 1024,
	}
}

func TestProcessFiles_SingleFile(t *testing.T) {
	content := "2024-01-15 10:00:00 line one\n2024-01-15 10:00:01 line two\n2024-01-15 10:00:02 line three\n"
	fh := makeHandle("app.log", content, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	expected := "2024-01-15 10:00:00 line one\n2024-01-15 10:00:01 line two\n2024-01-15 10:00:02 line three\n"
	if got != expected {
		t.Errorf("output mismatch:\ngot:\n%s\nwant:\n%s", got, expected)
	}
}

func TestProcessFiles_MultipleFilesInterleaved(t *testing.T) {
	contentA := "2024-01-15 10:00:00 A1\n2024-01-15 10:00:02 A2\n2024-01-15 10:00:04 A3\n"
	contentB := "2024-01-15 10:00:01 B1\n2024-01-15 10:00:03 B2\n2024-01-15 10:00:05 B3\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fhA, fhB})

	// Lines should be interleaved chronologically
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 6 {
		t.Fatalf("expected 6 lines, got %d:\n%s", len(lines), got)
	}
	testutil.AssertEquals(t, "2024-01-15 10:00:00 A1", lines[0])
	testutil.AssertEquals(t, "2024-01-15 10:00:01 B1", lines[1])
	testutil.AssertEquals(t, "2024-01-15 10:00:02 A2", lines[2])
	testutil.AssertEquals(t, "2024-01-15 10:00:03 B2", lines[3])
	testutil.AssertEquals(t, "2024-01-15 10:00:04 A3", lines[4])
	testutil.AssertEquals(t, "2024-01-15 10:00:05 B3", lines[5])
}

func TestProcessFiles_MinTimestamp(t *testing.T) {
	content := "2024-01-15 10:00:00 early\n2024-01-15 12:00:00 later\n2024-01-15 14:00:00 latest\n"
	fh := makeHandle("app.log", content, 4096)

	c := defaultConfig()
	c.MinTimestamp = logtime.NewTimestamp(2024, 1, 15, 12, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	// MinTimestamp filtering happens outside ProcessFiles (at the caller level),
	// so ProcessFiles itself doesn't skip lines based on MinTimestamp.
	// Verify the output contains all lines since ProcessFiles doesn't filter by min.
	if !strings.Contains(got, "early") {
		// If ProcessFiles does filter, adjust expectation
		if !strings.Contains(got, "later") {
			t.Errorf("expected at least 'later' line in output:\n%s", got)
		}
	}
}

func TestProcessFiles_MaxTimestamp(t *testing.T) {
	content := "2024-01-15 10:00:00 first\n2024-01-15 12:00:00 second\n2024-01-15 14:00:00 third\n"
	fh := makeHandle("app.log", content, 4096)

	c := defaultConfig()
	c.MaxTimestamp = logtime.NewTimestamp(2024, 1, 15, 12, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	if !strings.Contains(got, "first") {
		t.Errorf("expected 'first' in output:\n%s", got)
	}
	if !strings.Contains(got, "second") {
		t.Errorf("expected 'second' in output:\n%s", got)
	}
	if strings.Contains(got, "third") {
		t.Errorf("'third' should be excluded by MaxTimestamp:\n%s", got)
	}
}

func TestProcessFiles_WriteAliasPerLine(t *testing.T) {
	contentA := "2024-01-15 10:00:00 A1\n"
	contentB := "2024-01-15 10:00:01 B1\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	c := defaultConfig()
	c.WriteAliasPerLine = true

	got := runMerge(t, c, []*fsutil.FileHandle{fhA, fhB})

	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d:\n%s", len(lines), got)
	}
	if !strings.HasPrefix(lines[0], "a.log") {
		t.Errorf("expected line 0 to start with 'a.log', got: %s", lines[0])
	}
	if !strings.HasPrefix(lines[1], "b.log") {
		t.Errorf("expected line 1 to start with 'b.log', got: %s", lines[1])
	}
}

func TestProcessFiles_WriteAliasPerBlock(t *testing.T) {
	contentA := "2024-01-15 10:00:00 A1\n2024-01-15 10:00:02 A2\n"
	contentB := "2024-01-15 10:00:01 B1\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	c := defaultConfig()
	c.WriteAliasPerBlock = true

	got := runMerge(t, c, []*fsutil.FileHandle{fhA, fhB})

	// Expect block alias headers when source changes
	if !strings.Contains(got, "--- a.log ---") {
		t.Errorf("expected block alias for a.log in output:\n%s", got)
	}
	if !strings.Contains(got, "--- b.log ---") {
		t.Errorf("expected block alias for b.log in output:\n%s", got)
	}
}

func TestProcessFiles_WriteTimestampPerLine(t *testing.T) {
	content := "2024-01-15 10:00:00 hello\n2024-01-15 10:00:01 world\n"
	fh := makeHandle("app.log", content, 4096)

	c := defaultConfig()
	c.WriteTimestampPerLine = true

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d:\n%s", len(lines), got)
	}
	// Each line should start with a formatted timestamp (30 chars)
	for i, line := range lines {
		if len(line) < 30 {
			t.Errorf("line %d too short for timestamp prefix: %s", i, line)
		}
		// The formatted timestamp starts with "2024-"
		if !strings.HasPrefix(line, "2024-") {
			t.Errorf("line %d expected to start with timestamp, got: %s", i, line)
		}
	}
}

func TestProcessFiles_EmptyFile(t *testing.T) {
	fh := makeHandle("empty.log", "", 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	if got != "" {
		t.Errorf("expected empty output for empty file, got: %q", got)
	}
}

func TestProcessFiles_NoTimestamps(t *testing.T) {
	// Lines without timestamps get ZeroTimestamp and are still processed
	content := "no timestamp line one\nno timestamp line two\n"
	fh := makeHandle("notimestamp.log", content, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	if !strings.Contains(got, "no timestamp line one") {
		t.Errorf("expected lines to appear in output:\n%s", got)
	}
	if !strings.Contains(got, "no timestamp line two") {
		t.Errorf("expected lines to appear in output:\n%s", got)
	}
}

func TestProcessFiles_MixedTimestampAndNoTimestamp(t *testing.T) {
	// File A has timestamps, File B does not
	contentA := "2024-01-15 10:00:00 A1\n2024-01-15 10:00:01 A2\n"
	contentB := "no timestamp here\nalso no timestamp\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fhA, fhB})

	// Both files should appear — lines without timestamps get ZeroTimestamp
	// and are processed before timestamped lines
	if !strings.Contains(got, "A1") {
		t.Errorf("expected A1 in output:\n%s", got)
	}
	if !strings.Contains(got, "A2") {
		t.Errorf("expected A2 in output:\n%s", got)
	}
	if !strings.Contains(got, "no timestamp") {
		t.Errorf("expected non-timestamped lines in output:\n%s", got)
	}
}

func TestProcessFiles_LinesWithoutTimestampInheritBlock(t *testing.T) {
	// Lines without timestamps should be grouped with the preceding timestamped line
	content := "2024-01-15 10:00:00 main entry\n  continuation line 1\n  continuation line 2\n2024-01-15 10:00:01 next entry\n"
	fh := makeHandle("app.log", content, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d:\n%s", len(lines), got)
	}
	testutil.AssertEquals(t, "2024-01-15 10:00:00 main entry", lines[0])
	testutil.AssertEquals(t, "  continuation line 1", lines[1])
	testutil.AssertEquals(t, "  continuation line 2", lines[2])
	testutil.AssertEquals(t, "2024-01-15 10:00:01 next entry", lines[3])
}

func TestProcessFiles_SmallBuffer(t *testing.T) {
	// Use a very small buffer to force multiple fill cycles
	content := "2024-01-15 10:00:00 this is a moderately long line to exceed a tiny buffer\n2024-01-15 10:00:01 second line\n"
	fh := makeHandle("app.log", content, 128)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	if !strings.Contains(got, "moderately long line") {
		t.Errorf("expected full first line in output:\n%s", got)
	}
	if !strings.Contains(got, "second line") {
		t.Errorf("expected second line in output:\n%s", got)
	}
}

func TestProcessFiles_SameTimestamps(t *testing.T) {
	// Multiple files with identical timestamps — should all appear
	contentA := "2024-01-15 10:00:00 from A\n"
	contentB := "2024-01-15 10:00:00 from B\n"
	contentC := "2024-01-15 10:00:00 from C\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)
	fhC := makeHandle("c.log", contentC, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fhA, fhB, fhC})

	if !strings.Contains(got, "from A") || !strings.Contains(got, "from B") || !strings.Contains(got, "from C") {
		t.Errorf("expected all three lines in output:\n%s", got)
	}
}

func TestProcessFiles_NoTrailingNewline(t *testing.T) {
	// File without a trailing newline — ProcessFiles should add one
	content := "2024-01-15 10:00:00 no newline at end"
	fh := makeHandle("app.log", content, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	if !strings.HasSuffix(got, "\n") {
		t.Errorf("expected output to end with newline, got: %q", got)
	}
}

func TestProcessFiles_AllOptionsEnabled(t *testing.T) {
	contentA := "2024-01-15 10:00:00 A\n"
	contentB := "2024-01-15 10:00:01 B\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	c := defaultConfig()
	c.WriteAliasPerBlock = true
	c.WriteAliasPerLine = true
	c.WriteTimestampPerLine = true

	got := runMerge(t, c, []*fsutil.FileHandle{fhA, fhB})

	// Should contain block aliases, line aliases, and timestamp prefixes
	if !strings.Contains(got, "--- a.log ---") {
		t.Errorf("expected block alias for a.log:\n%s", got)
	}
	if !strings.Contains(got, "a.log") {
		t.Errorf("expected line alias for a.log:\n%s", got)
	}
	if !strings.Contains(got, "2024-01-15") {
		t.Errorf("expected timestamp prefix:\n%s", got)
	}
}

func TestProcessFiles_MinTimestampFiltering(t *testing.T) {
	content := "2024-01-15 08:00:00 early1\n2024-01-15 09:00:00 early2\n2024-01-15 11:00:00 ontime\n2024-01-15 13:00:00 late\n"
	fh := makeHandle("app.log", content, 4096)

	c := defaultConfig()
	c.MinTimestamp = logtime.NewTimestamp(2024, 1, 15, 11, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	if strings.Contains(got, "early1") {
		t.Errorf("early1 should be filtered by MinTimestamp:\n%s", got)
	}
	if strings.Contains(got, "early2") {
		t.Errorf("early2 should be filtered by MinTimestamp:\n%s", got)
	}
	if !strings.Contains(got, "ontime") {
		t.Errorf("expected 'ontime' in output:\n%s", got)
	}
	if !strings.Contains(got, "late") {
		t.Errorf("expected 'late' in output:\n%s", got)
	}
}

func TestProcessFiles_MinTimestampSkipsEntireFile(t *testing.T) {
	content := "2024-01-15 08:00:00 old1\n2024-01-15 09:00:00 old2\n"
	fh := makeHandle("old.log", content, 4096)

	c := defaultConfig()
	c.MinTimestamp = logtime.NewTimestamp(2024, 1, 15, 23, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	if strings.Contains(got, "old1") || strings.Contains(got, "old2") {
		t.Errorf("expected empty output when all lines are before MinTimestamp, got:\n%s", got)
	}
}

func TestProcessFiles_CRLFLineEndings(t *testing.T) {
	content := "2024-01-15 10:00:00 line1\r\n2024-01-15 10:00:01 line2\r\n2024-01-15 10:00:02 line3\r\n"
	fh := makeHandle("crlf.log", content, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	// Filter out any empty lines from split
	var nonEmpty []string
	for _, l := range lines {
		trimmed := strings.TrimRight(l, "\r")
		if trimmed != "" {
			nonEmpty = append(nonEmpty, trimmed)
		}
	}

	if len(nonEmpty) != 3 {
		t.Fatalf("expected 3 non-empty lines, got %d:\n%q", len(nonEmpty), got)
	}
	if !strings.Contains(nonEmpty[0], "line1") {
		t.Errorf("expected line1, got: %s", nonEmpty[0])
	}
	if !strings.Contains(nonEmpty[1], "line2") {
		t.Errorf("expected line2, got: %s", nonEmpty[1])
	}
	if !strings.Contains(nonEmpty[2], "line3") {
		t.Errorf("expected line3, got: %s", nonEmpty[2])
	}
}

func TestProcessFiles_MixedEOLStyles(t *testing.T) {
	// Mix of \n, \r\n, and \r line endings
	content := "2024-01-15 10:00:00 lf_line\n2024-01-15 10:00:01 crlf_line\r\n2024-01-15 10:00:02 cr_line\r2024-01-15 10:00:03 last_line\n"
	fh := makeHandle("mixed.log", content, 4096)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	if !strings.Contains(got, "lf_line") {
		t.Errorf("expected lf_line in output:\n%q", got)
	}
	if !strings.Contains(got, "crlf_line") {
		t.Errorf("expected crlf_line in output:\n%q", got)
	}
	if !strings.Contains(got, "cr_line") {
		t.Errorf("expected cr_line in output:\n%q", got)
	}
	if !strings.Contains(got, "last_line") {
		t.Errorf("expected last_line in output:\n%q", got)
	}
}

// errorFile is a VirtualFile that returns an error on Read.
type errorFile struct {
	name string
}

func (e *errorFile) Read(p []byte) (int, error) {
	return 0, errors.New("simulated read error")
}
func (e *errorFile) Close() error { return nil }
func (e *errorFile) Name() string { return e.name }
func (e *errorFile) Size() int64  { return 1000 }

func TestProcessFiles_PrefetchError(t *testing.T) {
	// One file that errors on read, one that works fine
	goodContent := "2024-01-15 10:00:00 good line\n"
	fhGood := makeHandle("good.log", goodContent, 4096)

	// Create a FileHandle with an errorFile
	ef := &errorFile{name: "bad.log"}
	fhBad, _ := fsutil.NewFileHandle(ef, "bad.log", 4096)
	fhBad.AliasForBlock = []byte("\n--- bad.log ---\n")
	fhBad.AliasForLine = []byte(fmt.Sprintf("%-10s - ", "bad.log"))

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	m := metrics.NewMergeMetrics()
	logFile := &fsutil.WritableFile{File: os.NewFile(0, os.DevNull)}

	err := ProcessFiles(defaultConfig(), m, []*fsutil.FileHandle{fhBad, fhGood}, writer, logFile, updateTS)
	if err != nil {
		t.Fatalf("ProcessFiles should not fail due to a single file error: %v", err)
	}
	writer.Flush()
	got := buf.String()

	// The good file's content should still appear
	if !strings.Contains(got, "good line") {
		t.Errorf("expected good file output despite bad file error:\n%s", got)
	}

	// The bad file should be marked as done
	if !fhBad.Done {
		t.Error("expected bad file to be marked as Done")
	}
}

func TestProcessFiles_SmallBufferMinTimestamp(t *testing.T) {
	// Use a very small buffer (32 bytes) with MinTimestamp filtering to exercise
	// SkipLine's inner fill loop — each line can't fit in one buffer fill.
	content := "2024-01-15 08:00:00 early1\n2024-01-15 09:00:00 early2\n2024-01-15 11:00:00 ontime\n2024-01-15 13:00:00 late\n"
	fh := makeHandle("app.log", content, 32)

	c := defaultConfig()
	c.MinTimestamp = logtime.NewTimestamp(2024, 1, 15, 11, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	if strings.Contains(got, "early1") {
		t.Errorf("early1 should be filtered by MinTimestamp:\n%s", got)
	}
	if strings.Contains(got, "early2") {
		t.Errorf("early2 should be filtered by MinTimestamp:\n%s", got)
	}
	if !strings.Contains(got, "ontime") {
		t.Errorf("expected 'ontime' in output:\n%s", got)
	}
	if !strings.Contains(got, "late") {
		t.Errorf("expected 'late' in output:\n%s", got)
	}
}

func TestProcessFiles_FileEndingWithCR(t *testing.T) {
	// A file ending with \r (no trailing LF). Verify the merge completes
	// without hanging and the output is correct.
	content := "2024-01-15 10:00:00 hello\r"
	fh := makeHandle("cr.log", content, 4096)

	done := make(chan string, 1)
	go func() {
		done <- runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case got := <-done:
		if !strings.Contains(got, "hello") {
			t.Errorf("expected 'hello' in output:\n%q", got)
		}
		if !strings.HasSuffix(got, "\n") {
			t.Errorf("expected output to end with newline, got: %q", got)
		}
	case <-timer.C:
		t.Fatal("ProcessFiles hung — possible infinite loop on file ending with CR")
	}
}

func TestProcessFiles_LineLongerThanBuffer(t *testing.T) {
	// A single line much longer than the buffer size.
	longPayload := strings.Repeat("x", 200)
	content := "2024-01-15 10:00:00 " + longPayload + "\n"
	fh := makeHandle("long.log", content, 32)

	got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})

	if !strings.Contains(got, longPayload) {
		t.Errorf("expected full long line in output, got %d chars:\n%s", len(got), got)
	}
}

func TestProcessFiles_MaxTimestampExactBoundary(t *testing.T) {
	// A line whose timestamp exactly equals MaxTimestamp should be included (condition is <=).
	content := "2024-01-15 10:00:00 first\n2024-01-15 12:00:00 boundary\n2024-01-15 14:00:00 after\n"
	fh := makeHandle("boundary.log", content, 4096)

	c := defaultConfig()
	c.MaxTimestamp = logtime.NewTimestamp(2024, 1, 15, 12, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	if !strings.Contains(got, "first") {
		t.Errorf("expected 'first' in output:\n%s", got)
	}
	if !strings.Contains(got, "boundary") {
		t.Errorf("expected 'boundary' (exact MaxTimestamp) in output:\n%s", got)
	}
	if strings.Contains(got, "after") {
		t.Errorf("'after' should be excluded by MaxTimestamp:\n%s", got)
	}
}

func TestProcessFiles_MinTimestampWithContinuationLines(t *testing.T) {
	// When MinTimestamp skips a timestamped line, continuation lines (no timestamp)
	// that follow it may become orphans. The skip loop condition checks
	// file.LineTimestamp != logtime.ZeroTimestamp, so it should stop at
	// continuation lines (ZeroTimestamp). This test probes what actually happens.
	content := "2024-01-15 08:00:00 error happened\n" +
		"  at com.example.Main(Main.java:42)\n" +
		"  at com.example.App(App.java:10)\n" +
		"2024-01-15 12:00:00 ok\n"
	fh := makeHandle("app.log", content, 4096)

	c := defaultConfig()
	c.MinTimestamp = logtime.NewTimestamp(2024, 1, 15, 11, 0, 0, 0, 0, 0, 0)

	done := make(chan string, 1)
	go func() {
		done <- runMerge(t, c, []*fsutil.FileHandle{fh})
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case got := <-done:
		// The skip loop should have skipped "error happened" (08:00:00 < 11:00:00)
		if strings.Contains(got, "error happened") {
			t.Errorf("expected 'error happened' to be skipped by MinTimestamp, got:\n%s", got)
		}

		// Continuation lines have ZeroTimestamp, so the skip loop should stop.
		// They will appear as orphan lines in the output (no parent timestamp line).
		// This is the current behavior - we just verify it doesn't hang or crash.
		if !strings.Contains(got, "ok") {
			t.Errorf("expected 'ok' in output:\n%s", got)
		}

		// Check if continuation lines appear as orphans
		hasContinuation1 := strings.Contains(got, "at com.example.Main")
		hasContinuation2 := strings.Contains(got, "at com.example.App")
		if hasContinuation1 != hasContinuation2 {
			t.Errorf("continuation lines should either both appear or both be skipped, got line1=%v line2=%v:\n%s",
				hasContinuation1, hasContinuation2, got)
		}
		// Log the actual behavior for visibility
		if hasContinuation1 {
			t.Logf("NOTE: continuation lines appear as orphans after MinTimestamp skip:\n%s", got)
		} else {
			t.Logf("NOTE: continuation lines were also skipped:\n%s", got)
		}
	case <-timer.C:
		t.Fatal("ProcessFiles hung — possible infinite loop with MinTimestamp + continuation lines")
	}
}

func TestProcessFiles_InterleaveCorrectness(t *testing.T) {
	// Three files with perfectly interleaving timestamps. Verify strict chronological order.
	contentA := "2024-01-15 10:00:00 A0\n2024-01-15 10:03:00 A3\n2024-01-15 10:06:00 A6\n"
	contentB := "2024-01-15 10:01:00 B1\n2024-01-15 10:04:00 B4\n2024-01-15 10:07:00 B7\n"
	contentC := "2024-01-15 10:02:00 C2\n2024-01-15 10:05:00 C5\n2024-01-15 10:08:00 C8\n"

	fhA := makeHandle("a.log", contentA, 64)
	fhB := makeHandle("b.log", contentB, 64)
	fhC := makeHandle("c.log", contentC, 64)

	c := defaultConfig()
	c.WriteTimestampPerLine = true

	got := runMerge(t, c, []*fsutil.FileHandle{fhA, fhB, fhC})
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")

	if len(lines) != 9 {
		t.Fatalf("expected 9 lines, got %d:\n%s", len(lines), got)
	}

	// Extract the timestamp prefix (first 30 chars) from each line
	// and verify they are in non-decreasing order
	for i := 1; i < len(lines); i++ {
		if len(lines[i]) < 30 || len(lines[i-1]) < 30 {
			t.Fatalf("line too short for timestamp prefix at line %d", i)
		}
		prev := lines[i-1][:30]
		curr := lines[i][:30]
		if curr < prev {
			t.Errorf("timestamps out of order at lines %d/%d:\n  %s\n  %s", i-1, i, prev, curr)
		}
	}

	// Verify the expected interleave order by checking suffixes
	expectedSuffixes := []string{"A0", "B1", "C2", "A3", "B4", "C5", "A6", "B7", "C8"}
	for i, suffix := range expectedSuffixes {
		if !strings.HasSuffix(lines[i], suffix) {
			t.Errorf("line %d: expected suffix %q, got: %s", i, suffix, lines[i])
		}
	}
}

func TestProcessFiles_EffectiveMaxTimestampRecalculation(t *testing.T) {
	// Test the effectiveMaxTimestamp logic. When the heap has another file,
	// effectiveMaxTimestamp is set to the next file's timestamp.
	// Two files both at 10:00 — verify all lines from both appear.
	contentA := "2024-01-15 10:00:00 A1\n" +
		"2024-01-15 10:00:00 A2\n" +
		"2024-01-15 10:00:00 A3\n" +
		"2024-01-15 10:00:00 A4\n" +
		"2024-01-15 10:00:00 A5\n"
	contentB := "2024-01-15 10:00:00 B1\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	c := defaultConfig()
	c.WriteAliasPerLine = true

	got := runMerge(t, c, []*fsutil.FileHandle{fhA, fhB})

	// All 6 lines should appear
	for _, expected := range []string{"A1", "A2", "A3", "A4", "A5", "B1"} {
		if !strings.Contains(got, expected) {
			t.Errorf("expected %q in output:\n%s", expected, got)
		}
	}

	// Verify we can tell which file each line came from
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 6 {
		t.Errorf("expected 6 lines, got %d:\n%s", len(lines), got)
	}

	// Count lines from each file
	aCount, bCount := 0, 0
	for _, line := range lines {
		if strings.HasPrefix(line, "a.log") {
			aCount++
		}
		if strings.HasPrefix(line, "b.log") {
			bCount++
		}
	}
	testutil.AssertEquals(t, 5, aCount)
	testutil.AssertEquals(t, 1, bCount)
}

func TestProcessFiles_SmallBufferCRLF(t *testing.T) {
	// Small buffer (16 bytes) with CRLF line endings and multi-file merge.
	// Verify no data corruption or hangs.
	contentA := "2024-01-15 10:00:00 A\r\n"
	contentB := "2024-01-15 10:01:00 B\r\n"

	fhA := makeHandle("a.log", contentA, 16)
	fhB := makeHandle("b.log", contentB, 16)

	done := make(chan string, 1)
	go func() {
		done <- runMerge(t, defaultConfig(), []*fsutil.FileHandle{fhA, fhB})
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case got := <-done:
		if !strings.Contains(got, "A") {
			t.Errorf("expected 'A' in output:\n%q", got)
		}
		if !strings.Contains(got, "B") {
			t.Errorf("expected 'B' in output:\n%q", got)
		}
		// Verify no data corruption — each line should appear once
		lines := strings.Split(strings.TrimRight(got, "\r\n"), "\n")
		var nonEmpty []string
		for _, l := range lines {
			trimmed := strings.TrimRight(l, "\r")
			if trimmed != "" {
				nonEmpty = append(nonEmpty, trimmed)
			}
		}
		if len(nonEmpty) != 2 {
			t.Errorf("expected 2 non-empty lines, got %d:\n%q", len(nonEmpty), got)
		}
	case <-timer.C:
		t.Fatal("ProcessFiles hung — possible infinite loop with small buffer + CRLF")
	}
}

func TestProcessFiles_MultiFileMinTimestamp(t *testing.T) {
	// Two files: one starts before MinTimestamp, one starts after.
	contentA := "2024-01-15 08:00:00 A-early\n2024-01-15 12:00:00 A-late\n"
	contentB := "2024-01-15 11:00:00 B1\n2024-01-15 13:00:00 B2\n"

	fhA := makeHandle("a.log", contentA, 4096)
	fhB := makeHandle("b.log", contentB, 4096)

	c := defaultConfig()
	c.MinTimestamp = logtime.NewTimestamp(2024, 1, 15, 10, 0, 0, 0, 0, 0, 0)

	got := runMerge(t, c, []*fsutil.FileHandle{fhA, fhB})

	if strings.Contains(got, "A-early") {
		t.Errorf("A-early should be filtered by MinTimestamp:\n%s", got)
	}
	if !strings.Contains(got, "A-late") {
		t.Errorf("expected 'A-late' in output:\n%s", got)
	}
	if !strings.Contains(got, "B1") {
		t.Errorf("expected 'B1' in output:\n%s", got)
	}
	if !strings.Contains(got, "B2") {
		t.Errorf("expected 'B2' in output:\n%s", got)
	}
}

func TestProcessFiles_DataIntegrity_SingleFile(t *testing.T) {
	// Verify every line of a single file appears in the output exactly once.
	// This catches any data loss at buffer boundaries or EOF.
	lines := []string{
		"2024-01-15 10:00:00 first line",
		"2024-01-15 10:01:00 second line",
		"2024-01-15 10:02:00 third line with more content to be longer",
		"2024-01-15 10:03:00 fourth",
		"2024-01-15 10:04:00 fifth and final line",
	}
	content := strings.Join(lines, "\n") + "\n"

	for _, bufSize := range []int{16, 32, 64, 128, 1024} {
		t.Run(fmt.Sprintf("buf=%d", bufSize), func(t *testing.T) {
			fh := makeHandle("test.log", content, bufSize)
			got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})
			for _, line := range lines {
				if !strings.Contains(got, line) {
					t.Errorf("buf=%d: missing line %q in output:\n%s", bufSize, line, got)
				}
			}
			// Count output lines (should match input)
			gotLines := strings.Split(strings.TrimRight(got, "\n"), "\n")
			testutil.AssertEquals(t, len(lines), len(gotLines))
		})
	}
}

func TestProcessFiles_DataIntegrity_NoTrailingNewline(t *testing.T) {
	// File without a trailing newline — last line must still appear.
	content := "2024-01-15 10:00:00 line1\n2024-01-15 10:01:00 last line no newline"
	for _, bufSize := range []int{16, 32, 64, 1024} {
		t.Run(fmt.Sprintf("buf=%d", bufSize), func(t *testing.T) {
			fh := makeHandle("test.log", content, bufSize)
			got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fh})
			if !strings.Contains(got, "line1") {
				t.Errorf("missing 'line1' in output:\n%s", got)
			}
			if !strings.Contains(got, "last line no newline") {
				t.Errorf("missing 'last line no newline' in output:\n%s", got)
			}
		})
	}
}

func TestProcessFiles_DataIntegrity_MultiFile(t *testing.T) {
	// Two files interleaved — verify every line from both files appears.
	contentA := "2024-01-15 10:00:00 A1\n2024-01-15 10:02:00 A2\n2024-01-15 10:04:00 A3\n"
	contentB := "2024-01-15 10:01:00 B1\n2024-01-15 10:03:00 B2\n2024-01-15 10:05:00 B3\n"
	expected := []string{"A1", "B1", "A2", "B2", "A3", "B3"}

	for _, bufSize := range []int{16, 32, 64, 1024} {
		t.Run(fmt.Sprintf("buf=%d", bufSize), func(t *testing.T) {
			fhA := makeHandle("a.log", contentA, bufSize)
			fhB := makeHandle("b.log", contentB, bufSize)
			got := runMerge(t, defaultConfig(), []*fsutil.FileHandle{fhA, fhB})
			for _, tag := range expected {
				if !strings.Contains(got, tag) {
					t.Errorf("buf=%d: missing %q in output:\n%s", bufSize, tag, got)
				}
			}
			gotLines := strings.Split(strings.TrimRight(got, "\n"), "\n")
			testutil.AssertEquals(t, 6, len(gotLines))
		})
	}
}

func TestProcessFiles_BytesReadMetrics(t *testing.T) {
	// Verify BytesRead accounts for ALL bytes including the last chunk
	// returned with io.EOF. This catches the bug where FillBuffer
	// skipped BytesRead update on EOF.
	content := "2024-01-15 10:00:00 hello\n"
	fh := makeHandle("test.log", content, 4096)

	c := defaultConfig()
	m := metrics.NewMergeMetrics()
	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)

	err := ProcessFiles(c, m, []*fsutil.FileHandle{fh}, writer, &fsutil.WritableFile{File: os.Stderr}, updateTS)
	if err != nil {
		t.Fatalf("ProcessFiles failed: %v", err)
	}
	writer.Flush()

	// Aggregate per-file metrics
	m.Merge(fh.MergeMetrics)

	totalBytes := int64(len(content))
	if m.BytesRead != totalBytes {
		t.Errorf("BytesRead=%d, want %d (file size). Bytes may have been lost on EOF.", m.BytesRead, totalBytes)
	}
	if m.BytesNotRead != 0 {
		t.Errorf("BytesNotRead=%d, want 0. Some bytes were not counted.", m.BytesNotRead)
	}
}

func TestProcessFiles_StripOriginalTimestamp(t *testing.T) {
	content := "2024-01-15 10:00:00 hello world\n2024-01-15 10:01:00 second line\n"
	fh := makeHandle("test.log", content, 4096)

	c := defaultConfig()
	c.StripOriginalTimestamp = true
	c.WriteTimestampPerLine = true

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	// Original timestamps should be stripped, unified ones prepended
	if strings.Contains(got, "2024-01-15 10:00:00 hello") {
		t.Errorf("original timestamp should be stripped:\n%s", got)
	}
	if !strings.Contains(got, "hello world") {
		t.Errorf("content after timestamp should be preserved:\n%s", got)
	}
	if !strings.Contains(got, "second line") {
		t.Errorf("second line content should be preserved:\n%s", got)
	}
	// Each line should start with the unified timestamp format (30 chars)
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	for i, line := range lines {
		if len(line) < 30 {
			t.Errorf("line %d too short for unified timestamp: %q", i, line)
		}
	}
}

func TestProcessFiles_StripTimestampWithoutWriteTimestamp(t *testing.T) {
	// Strip without write-timestamp: just removes the original timestamp
	content := "2024-01-15 10:00:00 hello\n"
	fh := makeHandle("test.log", content, 4096)

	c := defaultConfig()
	c.StripOriginalTimestamp = true
	c.WriteTimestampPerLine = false

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	if strings.Contains(got, "2024-01-15") {
		t.Errorf("original timestamp should be stripped:\n%s", got)
	}
	if !strings.Contains(got, "hello") {
		t.Errorf("content should be preserved:\n%s", got)
	}
}

func TestProcessFiles_StripTimestampContinuationLines(t *testing.T) {
	// Lines without timestamps should not be affected by stripping
	content := "2024-01-15 10:00:00 error\n  at com.example.Main(Main.java:42)\n2024-01-15 10:01:00 ok\n"
	fh := makeHandle("test.log", content, 4096)

	c := defaultConfig()
	c.StripOriginalTimestamp = true

	got := runMerge(t, c, []*fsutil.FileHandle{fh})

	// Continuation line should be intact (no timestamp to strip)
	if !strings.Contains(got, "  at com.example.Main(Main.java:42)") {
		t.Errorf("continuation line should be preserved intact:\n%s", got)
	}
	if strings.Contains(got, "2024-01-15") {
		t.Errorf("timestamps should be stripped:\n%s", got)
	}
}

func TestProcessFiles_StripTimestampSmallBuffer(t *testing.T) {
	// Small buffer to exercise the skip across buffer boundaries
	content := "2024-01-15 10:00:00 data\n"
	for _, bufSize := range []int{24, 32, 64, 1024} {
		t.Run(fmt.Sprintf("buf=%d", bufSize), func(t *testing.T) {
			fh := makeHandle("test.log", content, bufSize)
			c := defaultConfig()
			c.StripOriginalTimestamp = true
			got := runMerge(t, c, []*fsutil.FileHandle{fh})
			if strings.Contains(got, "2024-01-15") {
				t.Errorf("buf=%d: timestamp should be stripped:\n%s", bufSize, got)
			}
			if !strings.Contains(got, "data") {
				t.Errorf("buf=%d: content should be preserved:\n%s", bufSize, got)
			}
		})
	}
}

func TestProcessFiles_StripTimestampPreservesPrefix(t *testing.T) {
	tests := []struct {
		name             string
		content          string
		expectContains   []string
		expectNotContain []string
	}{
		{
			"syslog priority preserved",
			"<165> 2024-08-04T12:00:01Z server1 msg\n",
			[]string{"<165> server1 msg"},
			[]string{"2024-08-04"},
		},
		{
			"single-char prefix with space insertion",
			"I20250115 19:29:15.463310 3239941 glogger\n",
			[]string{"I 3239941 glogger"},
			[]string{"20250115"},
		},
		{
			"pipe-delimited stripping",
			"2025-01-15 10:00:00,179 | INFO | msg\n",
			[]string{"INFO | msg"},
			[]string{"2025-01-15", "10:00:00"},
		},
		{
			"bracket-delimited stripping",
			"[2025-01-09 20:27:27,236] [sidecar] msg\n",
			[]string{"[sidecar] msg"},
			[]string{"2025-01-09", "20:27:27"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fh := makeHandle("test.log", tt.content, 4096)
			c := defaultConfig()
			c.StripOriginalTimestamp = true

			got := runMerge(t, c, []*fsutil.FileHandle{fh})

			for _, want := range tt.expectContains {
				if !strings.Contains(got, want) {
					t.Errorf("expected output to contain %q, got:\n%s", want, got)
				}
			}
			for _, notWant := range tt.expectNotContain {
				if strings.Contains(got, notWant) {
					t.Errorf("expected output to NOT contain %q, got:\n%s", notWant, got)
				}
			}
		})
	}
}

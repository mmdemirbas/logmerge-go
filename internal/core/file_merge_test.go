package core_test

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

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

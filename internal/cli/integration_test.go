package cli_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// buildBinary builds the logmerge binary and returns its path.
// It uses t.TempDir so the binary is automatically cleaned up.
func buildBinary(t *testing.T) string {
	t.Helper()
	binPath := filepath.Join(t.TempDir(), "logmerge")
	cmd := exec.Command("go", "build", "-o", binPath, "github.com/mmdemirbas/logmerge/cmd/logmerge")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build binary: %v\n%s", err, out)
	}
	return binPath
}

// writeFile creates a file with the given content inside dir.
func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write %s: %v", path, err)
	}
	return path
}

// runLogmerge runs the binary with the given args and returns stdout, stderr, and any error.
func runLogmerge(t *testing.T, bin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var outBuf, errBuf strings.Builder
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf
	err = cmd.Run()
	return outBuf.String(), errBuf.String(), err
}

func TestIntegration_SingleFile(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log",
		"2024-01-15 10:00:00 first line\n"+
			"2024-01-15 10:00:01 second line\n"+
			"2024-01-15 10:00:02 third line\n")

	outPath := filepath.Join(dir, "out.log")
	stdout, _, err := runLogmerge(t, bin,
		"-o", outPath,
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v\nstdout: %s", err, stdout)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("expected 3 lines, got %d:\n%s", len(lines), got)
	}
	if !strings.Contains(lines[0], "first line") {
		t.Errorf("unexpected first line: %s", lines[0])
	}
}

func TestIntegration_MultipleFiles(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "a.log",
		"2024-01-15 10:00:00 A1\n"+
			"2024-01-15 10:00:02 A2\n"+
			"2024-01-15 10:00:04 A3\n")

	writeFile(t, dir, "b.log",
		"2024-01-15 10:00:01 B1\n"+
			"2024-01-15 10:00:03 B2\n"+
			"2024-01-15 10:00:05 B3\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		filepath.Join(dir, "a.log"),
		filepath.Join(dir, "b.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 6 {
		t.Fatalf("expected 6 lines, got %d:\n%s", len(lines), got)
	}

	// Verify chronological interleaving
	expected := []string{"A1", "B1", "A2", "B2", "A3", "B3"}
	for i, exp := range expected {
		if !strings.Contains(lines[i], exp) {
			t.Errorf("line %d: expected to contain %q, got %q", i, exp, lines[i])
		}
	}
}

func TestIntegration_DirectoryInput(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "x.log", "2024-01-15 10:00:00 from X\n")
	writeFile(t, dir, "y.log", "2024-01-15 10:00:01 from Y\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-i", "out.log", // ignore the output file itself
		dir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)
	if !strings.Contains(output, "from X") || !strings.Contains(output, "from Y") {
		t.Errorf("expected both files in output:\n%s", output)
	}
}

func TestIntegration_WriteTimestamp(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log",
		"2024-01-15 10:00:00 hello\n"+
			"2024-01-15 10:00:01 world\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-t",
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")

	// Each line should have an extra timestamp prefix (30 chars) before the raw line
	for i, line := range lines {
		// Timestamp prefix is "YYYY-MM-DD HH:MM:SS.nnnnnnnnn "
		if len(line) < 30 {
			t.Errorf("line %d too short: %q", i, line)
		}
		if !strings.HasPrefix(line, "2024-01-15") {
			t.Errorf("line %d expected timestamp prefix, got: %q", i, line)
		}
	}
}

func TestIntegration_WriteLineAlias(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "a.log", "2024-01-15 10:00:00 A\n")
	writeFile(t, dir, "b.log", "2024-01-15 10:00:01 B\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-a",
		"-i", "out.log",
		dir) // pass directory so aliases are relative filenames
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d:\n%s", len(lines), got)
	}

	// Each line should be prefixed with the file alias (relative to dir)
	if !strings.Contains(lines[0], "a.log") {
		t.Errorf("line 0 should contain alias 'a.log': %q", lines[0])
	}
	if !strings.Contains(lines[1], "b.log") {
		t.Errorf("line 1 should contain alias 'b.log': %q", lines[1])
	}
}

func TestIntegration_WriteBlockAlias(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "a.log",
		"2024-01-15 10:00:00 A1\n"+
			"2024-01-15 10:00:02 A2\n")
	writeFile(t, dir, "b.log",
		"2024-01-15 10:00:01 B1\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-b",
		"-i", "out.log",
		dir) // pass directory so aliases are relative filenames
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)
	if !strings.Contains(output, "--- a.log ---") {
		t.Errorf("expected block separator for a.log:\n%s", output)
	}
	if !strings.Contains(output, "--- b.log ---") {
		t.Errorf("expected block separator for b.log:\n%s", output)
	}
}

func TestIntegration_MaxTimestamp(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log",
		"2024-01-15 10:00:00 before\n"+
			"2024-01-15 12:00:00 middle\n"+
			"2024-01-15 14:00:00 after\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"--until", "2024-01-15T12:00:00Z",
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)
	if !strings.Contains(output, "before") {
		t.Errorf("expected 'before' in output:\n%s", output)
	}
	if !strings.Contains(output, "middle") {
		t.Errorf("expected 'middle' in output:\n%s", output)
	}
	if strings.Contains(output, "after") {
		t.Errorf("'after' should be excluded by --until:\n%s", output)
	}
}

func TestIntegration_IgnorePattern(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log", "2024-01-15 10:00:00 app line\n")
	writeFile(t, dir, "debug.log", "2024-01-15 10:00:01 debug line\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-i", "debug*",
		"-i", "out.log",
		dir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)
	if !strings.Contains(output, "app line") {
		t.Errorf("expected 'app line' in output:\n%s", output)
	}
	if strings.Contains(output, "debug line") {
		t.Errorf("'debug line' should be excluded by ignore pattern:\n%s", output)
	}
}

func TestIntegration_IgnoreFile(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log", "2024-01-15 10:00:00 app\n")
	writeFile(t, dir, "tmp.log", "2024-01-15 10:00:01 tmp\n")
	writeFile(t, dir, ".logignore", "tmp*\nout.log\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"--ignore-file", filepath.Join(dir, ".logignore"),
		"-i", "out.log",
		"-i", ".logignore",
		dir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)
	if !strings.Contains(output, "app") {
		t.Errorf("expected 'app' in output:\n%s", output)
	}
	if strings.Contains(output, "tmp") {
		t.Errorf("'tmp' should be excluded by ignore file:\n%s", output)
	}
}

func TestIntegration_Alias(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log", "2024-01-15 10:00:00 hello\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-a",
		"--alias", "*.log=APPLICATION",
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	if !strings.Contains(string(got), "APPLICATION") {
		t.Errorf("expected alias 'APPLICATION' in output:\n%s", got)
	}
}

func TestIntegration_YAMLConfig(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log",
		"2024-01-15 10:00:00 hello\n"+
			"2024-01-15 10:00:01 world\n")

	outPath := filepath.Join(dir, "out.log")
	logPath := filepath.Join(dir, "metrics.log")

	writeFile(t, dir, "config.yaml", `
OutputFile: "`+outPath+`"
LogFile: "`+logPath+`"
MergeConfig:
  WriteAliasPerLine: true
  BufferSizeForRead: 1048576
  BufferSizeForWrite: 1048576
`)

	_, _, err := runLogmerge(t, bin,
		"--config", filepath.Join(dir, "config.yaml"),
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)
	// With a single file as input, the alias is "." (relative to itself).
	// Verify that WriteAliasPerLine is working by checking for the " - " separator.
	if !strings.Contains(output, " - ") {
		t.Errorf("expected line alias separator from WriteAliasPerLine:\n%s", output)
	}
	if !strings.Contains(output, "hello") || !strings.Contains(output, "world") {
		t.Errorf("expected log content in output:\n%s", output)
	}
}

func TestIntegration_Completions(t *testing.T) {
	bin := buildBinary(t)

	for _, shell := range []string{"bash", "zsh", "fish", "powershell"} {
		t.Run(shell, func(t *testing.T) {
			stdout, _, err := runLogmerge(t, bin, "--completions", shell)
			if err != nil {
				t.Fatalf("--completions %s failed: %v", shell, err)
			}
			if stdout == "" {
				t.Errorf("expected non-empty completion output for %s", shell)
			}
		})
	}
}

func TestIntegration_CompletionsInvalidShell(t *testing.T) {
	bin := buildBinary(t)

	_, _, err := runLogmerge(t, bin, "--completions", "invalid")
	if err == nil {
		t.Error("expected error for invalid shell name")
	}
}

func TestIntegration_NoInputPaths(t *testing.T) {
	bin := buildBinary(t)

	_, _, err := runLogmerge(t, bin)
	if err == nil {
		t.Error("expected error when no input paths provided")
	}
}

func TestIntegration_NonexistentInput(t *testing.T) {
	bin := buildBinary(t)

	_, _, err := runLogmerge(t, bin, "/nonexistent/path/file.log")
	if err == nil {
		t.Error("expected error for nonexistent input path")
	}
}

func TestIntegration_CombinedFlags(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "a.log",
		"2024-01-15 10:00:00 A1\n"+
			"2024-01-15 10:00:02 A2\n")
	writeFile(t, dir, "b.log",
		"2024-01-15 10:00:01 B1\n"+
			"2024-01-15 10:00:03 B2\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-t", "-a", "-b",
		"-i", "out.log",
		dir) // pass directory so aliases are relative filenames
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)

	// Should have all three: timestamp prefix, line alias, block alias
	if !strings.Contains(output, "--- a.log ---") {
		t.Errorf("expected block alias:\n%s", output)
	}
	if !strings.Contains(output, "a.log") {
		t.Errorf("expected line alias:\n%s", output)
	}
	// Timestamp prefix starts with year
	if !strings.Contains(output, "2024-01-15") {
		t.Errorf("expected timestamp prefix:\n%s", output)
	}
}

func TestIntegration_ContinuationLines(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log",
		"2024-01-15 10:00:00 main entry\n"+
			"  stack trace line 1\n"+
			"  stack trace line 2\n"+
			"2024-01-15 10:00:01 next entry\n")

	outPath := filepath.Join(dir, "out.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines (including continuations), got %d:\n%s", len(lines), got)
	}
	if !strings.Contains(lines[1], "stack trace line 1") {
		t.Errorf("expected continuation line preserved: %q", lines[1])
	}
}

func TestIntegration_LogFile(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log", "2024-01-15 10:00:00 hello\n")

	outPath := filepath.Join(dir, "out.log")
	logPath := filepath.Join(dir, "metrics.log")
	_, _, err := runLogmerge(t, bin,
		"-o", outPath,
		"-l", logPath,
		filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	// Log file should contain metrics summary
	logContent, _ := os.ReadFile(logPath)
	if !strings.Contains(string(logContent), "SUMMARY") {
		t.Errorf("expected metrics summary in log file:\n%s", logContent)
	}
}

func TestIntegration_StdoutOutput(t *testing.T) {
	bin := buildBinary(t)
	dir := t.TempDir()

	writeFile(t, dir, "app.log", "2024-01-15 10:00:00 hello stdout\n")

	// No -o flag: output goes to stdout
	stdout, _, err := runLogmerge(t, bin, filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	if !strings.Contains(stdout, "hello stdout") {
		t.Errorf("expected output on stdout:\n%s", stdout)
	}
}

// --- Tests against real example directories ---

// examplesDir returns the path to the examples/ directory relative to this test file.
func examplesDir(t *testing.T) string {
	t.Helper()
	// This test file is at internal/cli/integration_test.go
	// Examples are at ../../examples/
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(wd, "..", "..", "examples")
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Skipf("examples directory not found at %s", dir)
	}
	return dir
}

func TestIntegration_ExamplesSmall_GoldenOutput(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)
	inputDir := filepath.Join(examples, "small")
	goldenPath := filepath.Join(examples, "small.log")

	golden, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("cannot read golden file: %v", err)
	}

	outPath := filepath.Join(t.TempDir(), "out.log")
	_, stderr, err := runLogmerge(t, bin,
		"-o", outPath,
		"-t", // write unified timestamp (matches golden file format)
		"-a", // write per-line alias (matches golden file format)
		inputDir)
	if err != nil {
		t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
	}

	got, _ := os.ReadFile(outPath)
	if string(got) != string(golden) {
		gotLines := strings.Split(string(got), "\n")
		goldenLines := strings.Split(string(golden), "\n")
		for i := 0; i < len(gotLines) && i < len(goldenLines); i++ {
			if gotLines[i] != goldenLines[i] {
				t.Errorf("first difference at line %d:\n  got:    %q\n  golden: %q", i+1, gotLines[i], goldenLines[i])
				break
			}
		}
		if len(gotLines) != len(goldenLines) {
			t.Errorf("line count: got %d, golden %d", len(gotLines), len(goldenLines))
		}
	}
}

func TestIntegration_ExamplesSmall_Deterministic(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)
	inputDir := filepath.Join(examples, "small")

	run := func() string {
		t.Helper()
		outPath := filepath.Join(t.TempDir(), "out.log")
		_, _, err := runLogmerge(t, bin, "-o", outPath, "-t", inputDir)
		if err != nil {
			t.Fatalf("logmerge failed: %v", err)
		}
		got, _ := os.ReadFile(outPath)
		return string(got)
	}

	out1 := run()
	out2 := run()
	if out1 != out2 {
		t.Error("output is not deterministic across runs")
	}
}

func TestIntegration_ExamplesSmall_ChronologicalOrder(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)
	inputDir := filepath.Join(examples, "small")

	outPath := filepath.Join(t.TempDir(), "out.log")
	_, _, err := runLogmerge(t, bin, "-o", outPath, "-t", inputDir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")

	// Each line starts with a 30-char timestamp; verify non-decreasing order
	var prev string
	for i, line := range lines {
		if len(line) < 30 {
			t.Errorf("line %d too short: %q", i, line)
			continue
		}
		ts := line[:30]
		if ts < prev {
			t.Errorf("line %d: timestamp went backwards:\n  prev: %s\n  curr: %s", i, prev, ts)
		}
		prev = ts
	}
}

func TestIntegration_ExamplesSmall_NoDataLoss(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)
	inputDir := filepath.Join(examples, "small")

	outPath := filepath.Join(t.TempDir(), "out.log")
	_, _, err := runLogmerge(t, bin, "-o", outPath, inputDir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	output := string(got)

	// Read source files and verify every line appears in output
	for _, name := range []string{"file1.log", "file2.log"} {
		src, _ := os.ReadFile(filepath.Join(inputDir, name))
		for _, line := range strings.Split(strings.TrimRight(string(src), "\n"), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if !strings.Contains(output, line) {
				t.Errorf("line from %s not in output: %q", name, line)
			}
		}
	}
}

func TestIntegration_ExamplesSmall_StripTimestamp(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)
	inputDir := filepath.Join(examples, "small")

	outPath := filepath.Join(t.TempDir(), "out.log")
	_, _, err := runLogmerge(t, bin, "-o", outPath, "-s", inputDir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")

	for i, line := range lines {
		if strings.Contains(line, "2025-01-01") {
			t.Errorf("line %d: original timestamp not stripped: %q", i, line)
		}
	}

	// Content must be preserved
	output := string(got)
	for _, want := range []string{"Zero", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine"} {
		if !strings.Contains(output, want) {
			t.Errorf("missing content %q after stripping", want)
		}
	}
}

func TestIntegration_ExamplesSmall_StripWithTimestamp(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)
	inputDir := filepath.Join(examples, "small")

	outPath := filepath.Join(t.TempDir(), "out.log")
	_, _, err := runLogmerge(t, bin, "-o", outPath, "-s", "-t", inputDir)
	if err != nil {
		t.Fatalf("logmerge failed: %v", err)
	}

	got, _ := os.ReadFile(outPath)
	lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")

	for i, line := range lines {
		if len(line) < 30 {
			t.Errorf("line %d too short for unified timestamp: %q", i, line)
			continue
		}
		// After the 30-char unified timestamp, original should be stripped
		rest := line[30:]
		if strings.Contains(rest, "2025-01-01") {
			t.Errorf("line %d: original timestamp not stripped from rest: %q", i, line)
		}
	}
}

// discoverExampleDirs returns all subdirectories under examples/, excluding "small"
// which has its own dedicated tests above.
func discoverExampleDirs(t *testing.T) []string {
	t.Helper()
	examples := examplesDir(t)
	entries, err := os.ReadDir(examples)
	if err != nil {
		t.Fatalf("failed to read examples dir: %v", err)
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && e.Name() != "small" {
			dirs = append(dirs, e.Name())
		}
	}
	if len(dirs) == 0 {
		t.Skip("no example directories found (besides small)")
	}
	return dirs
}

func TestIntegration_ExampleDirs_BasicMerge(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)

	for _, dirName := range discoverExampleDirs(t) {
		t.Run(dirName, func(t *testing.T) {
			inputDir := filepath.Join(examples, dirName)
			outPath := filepath.Join(t.TempDir(), "out.log")
			_, stderr, err := runLogmerge(t, bin,
				"-o", outPath,
				"--ignore-archives",
				inputDir)
			if err != nil {
				t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
			}

			got, _ := os.ReadFile(outPath)
			lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
			if len(lines) < 10 {
				t.Fatalf("expected substantial output, got %d lines", len(lines))
			}
		})
	}
}

func TestIntegration_ExampleDirs_WithTimestamp(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)

	for _, dirName := range discoverExampleDirs(t) {
		t.Run(dirName, func(t *testing.T) {
			inputDir := filepath.Join(examples, dirName)
			outPath := filepath.Join(t.TempDir(), "out.log")
			_, stderr, err := runLogmerge(t, bin,
				"-o", outPath,
				"-t",
				"--ignore-archives",
				inputDir)
			if err != nil {
				t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
			}

			got, _ := os.ReadFile(outPath)
			lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
			if len(lines) < 10 {
				t.Fatalf("expected substantial output, got %d lines", len(lines))
			}

			// With -t, most lines should have a unified timestamp prefix
			withTS := 0
			for _, line := range lines {
				if len(line) >= 19 && line[4] == '-' && line[10] == ' ' {
					withTS++
				}
			}
			ratio := float64(withTS) / float64(len(lines))
			if ratio < 0.5 {
				t.Errorf("only %.0f%% of %d lines have timestamps (expected >50%%)", ratio*100, len(lines))
			}
		})
	}
}

func TestIntegration_ExampleDirs_WithStrip(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)

	for _, dirName := range discoverExampleDirs(t) {
		t.Run(dirName, func(t *testing.T) {
			inputDir := filepath.Join(examples, dirName)
			outPath := filepath.Join(t.TempDir(), "out.log")
			_, stderr, err := runLogmerge(t, bin,
				"-o", outPath,
				"-s",
				"--ignore-archives",
				inputDir)
			if err != nil {
				t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
			}

			got, _ := os.ReadFile(outPath)
			lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
			if len(lines) < 10 {
				t.Fatalf("expected substantial output, got %d lines", len(lines))
			}
		})
	}
}

func TestIntegration_ExampleDirs_WithStripAndTimestamp(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)

	for _, dirName := range discoverExampleDirs(t) {
		t.Run(dirName, func(t *testing.T) {
			inputDir := filepath.Join(examples, dirName)
			outPath := filepath.Join(t.TempDir(), "out.log")
			_, stderr, err := runLogmerge(t, bin,
				"-o", outPath,
				"-t", "-s",
				"--ignore-archives",
				inputDir)
			if err != nil {
				t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
			}

			got, _ := os.ReadFile(outPath)
			lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
			if len(lines) < 10 {
				t.Fatalf("expected substantial output, got %d lines", len(lines))
			}
		})
	}
}

func TestIntegration_ExampleDirs_WithBlockAlias(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)

	for _, dirName := range discoverExampleDirs(t) {
		t.Run(dirName, func(t *testing.T) {
			inputDir := filepath.Join(examples, dirName)
			outPath := filepath.Join(t.TempDir(), "out.log")
			_, stderr, err := runLogmerge(t, bin,
				"-o", outPath,
				"-b",
				"--ignore-archives",
				inputDir)
			if err != nil {
				t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
			}

			got, _ := os.ReadFile(outPath)
			output := string(got)
			// Block aliases should produce "---" separators
			if !strings.Contains(output, "---") {
				t.Errorf("expected block alias separators in output")
			}
		})
	}
}

func TestIntegration_ExampleDirs_AllFlagsCombined(t *testing.T) {
	bin := buildBinary(t)
	examples := examplesDir(t)

	for _, dirName := range discoverExampleDirs(t) {
		t.Run(dirName, func(t *testing.T) {
			inputDir := filepath.Join(examples, dirName)
			outPath := filepath.Join(t.TempDir(), "out.log")
			_, stderr, err := runLogmerge(t, bin,
				"-o", outPath,
				"-t", "-s", "-a", "-b",
				"--ignore-archives",
				inputDir)
			if err != nil {
				t.Fatalf("logmerge failed: %v\nstderr: %s", err, stderr)
			}

			got, _ := os.ReadFile(outPath)
			lines := strings.Split(strings.TrimRight(string(got), "\n"), "\n")
			if len(lines) < 10 {
				t.Fatalf("expected substantial output, got %d lines", len(lines))
			}
		})
	}
}

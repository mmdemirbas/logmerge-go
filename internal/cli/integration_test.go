package logmerge_test

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

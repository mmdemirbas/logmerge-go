package cli

import (
	bytespkg "bytes"
	"strings"
	"testing"
)

func TestGenerateBashCompletion(t *testing.T) {
	var buf bytespkg.Buffer
	generateBashCompletion(&buf)
	out := buf.String()

	if out == "" {
		t.Fatal("expected non-empty bash completion output")
	}
	for _, keyword := range []string{"_logmerge", "COMPREPLY", "complete", "--out", "--completions"} {
		if !strings.Contains(out, keyword) {
			t.Errorf("bash completion missing keyword %q", keyword)
		}
	}
}

func TestGenerateZshCompletion(t *testing.T) {
	var buf bytespkg.Buffer
	generateZshCompletion(&buf)
	out := buf.String()

	if out == "" {
		t.Fatal("expected non-empty zsh completion output")
	}
	for _, keyword := range []string{"#compdef", "_logmerge", "_arguments", "--out", "--completions"} {
		if !strings.Contains(out, keyword) {
			t.Errorf("zsh completion missing keyword %q", keyword)
		}
	}
}

func TestGenerateFishCompletion(t *testing.T) {
	var buf bytespkg.Buffer
	generateFishCompletion(&buf)
	out := buf.String()

	if out == "" {
		t.Fatal("expected non-empty fish completion output")
	}
	for _, keyword := range []string{"complete -c logmerge", "out", "completions"} {
		if !strings.Contains(out, keyword) {
			t.Errorf("fish completion missing keyword %q", keyword)
		}
	}
}

func TestGeneratePowershellCompletion(t *testing.T) {
	var buf bytespkg.Buffer
	generatePowershellCompletion(&buf)
	out := buf.String()

	if out == "" {
		t.Fatal("expected non-empty powershell completion output")
	}
	for _, keyword := range []string{"Register-ArgumentCompleter", "logmerge", "--out", "--completions", "DirectorySeparatorChar"} {
		if !strings.Contains(out, keyword) {
			t.Errorf("powershell completion missing keyword %q", keyword)
		}
	}
}

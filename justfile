#!/usr/bin/env just --justfile

BINARY_NAME := "logmerge"

# Paths

BIN_DIR := "bin"
OUT_DIR := "out"
LOG_DIR := "log"
CPU_PROF_FILE := "cpu.prof"
MEM_PROF_FILE := "mem.prof"
TRACE_FILE := "trace.out"
OUTPUT_FILE := "output.log"

# Test params

# Display this help message
@help:
    echo "Usage: just <recipe-name>"
    echo ""
    just --list --unsorted

# clean test build
[group("common")]
all: clean test build

# Clean binaries and profiling data
[group("common")]
clean:
    rm -rf {{ BIN_DIR }} {{ OUT_DIR }}

# go build flags:
# CGO_ENABLED=0    => Disable CGO (dynamic linking). This will create a fully static binary.
# -ldflags="-s -w" => Strip the debug information from the binary.

# Build binaries
[group("common")]
build:
    mkdir -p {{ BIN_DIR }}
    CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -o {{ BIN_DIR }}/{{ BINARY_NAME }}-macos-arm64 *.go
    CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -ldflags="-s -w" -o {{ BIN_DIR }}/{{ BINARY_NAME }}-windows-amd64.exe *.go

# Run unit tests
[group("test")]
test:
    go test -v ./...

# Run benchmark tests and capture CPU and memory profiles
[group("test")]
benchmark:
    mkdir -p {{ OUT_DIR }}
    go test -bench=. -benchmem ./... -run=^$ \
      -benchtime=5s \
      -cpuprofile={{ OUT_DIR }}/{{ CPU_PROF_FILE }} \
      -memprofile={{ OUT_DIR }}/{{ MEM_PROF_FILE }} \
      -trace={{ OUT_DIR }}/{{ TRACE_FILE }}
    mv logmerge.test {{ OUT_DIR }}/logmerge.test || true

# Run application
[group("run")]
run stderrName="stderr":
    mkdir -p {{ LOG_DIR }} {{ BIN_DIR }} {{ OUT_DIR }}
    go build -ldflags="-s -w" -o {{ BIN_DIR }}/{{ BINARY_NAME }} *.go
    {{ BIN_DIR }}/{{ BINARY_NAME }} "" "" {{ LOG_DIR }}/{{ stderrName }}.err

#    go run $(ls *.go | grep -v _test.go)

# Run application with profiling and capture CPU and memory profiles
[group("run")]
profile stderrName="stderr":
    ENABLE_PPROF=true just run {{ stderrName }}

# Run application with GC tracing and capture CPU and memory profiles
[group("run")]
trace stderrName="stderr":
    GODEBUG=gctrace=1 just run {{ stderrName }}

# Browse captured CPU profile in a web browser
[group("inspect")]
inspect-cpu:
    go tool pprof -http=:8080 {{ OUT_DIR }}/{{ CPU_PROF_FILE }}

# Browse captured memory profile in a web browser
[group("inspect")]
inspect-mem:
    go tool pprof -http=:8080 {{ OUT_DIR }}/{{ MEM_PROF_FILE }}

# View execution trace
[group("inspect")]
inspect-trace:
    echo "Launching trace viewer..."
    go tool trace {{ OUT_DIR }}/{{ TRACE_FILE }}

# Browse captured CPU profile in an interactive shell
[group("inspect")]
inspect-cpu-interactive:
    go tool pprof {{ OUT_DIR }}/{{ CPU_PROF_FILE }}

# Browse captured memory profile in an interactive shell
[group("inspect")]
inspect-mem-interactive:
    go tool pprof {{ OUT_DIR }}/{{ MEM_PROF_FILE }}

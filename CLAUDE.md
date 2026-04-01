# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LogMerge is a high-performance sequential log aggregator in Go that merges multiple massive log files into a single chronologically-ordered stream (~34 GB/s throughput on M1 Max). Module: `github.com/mmdemirbas/logmerge`. Go 1.26+.

## Build & Development Commands

Uses [go-task](https://taskfile.dev/) runner with `CGO_ENABLED=0`:

```bash
task all                # Clean, test, build
task build              # Build to build/logmerge (stripped binary, version injected via ldflags)
task build-pgo          # Build with Profile-Guided Optimization (requires prior `task bench`)
task test               # Run all tests
task test:v             # Run all tests with verbose output
task bench              # Run benchmarks with CPU/mem/trace profiling to out/
task run                # Build and run with conf/default.yaml
task dist               # Cross-compile with PGO for Darwin/Linux/Windows x amd64/arm64
task profile            # Run benchmarks and open CPU flame graph
task inspect-cpu        # CPU profile browser (web)
task inspect-mem        # Memory profile browser (web)
task inspect-trace      # Execution trace viewer
task trace              # Run with GC tracing (GODEBUG=gctrace=1)
task install-completions       # Install shell completions (bash/zsh/fish)
```

Running a single test or benchmark directly:
```bash
go test -run TestName ./internal/core/
go test -bench BenchmarkName -benchmem ./internal/core/ -run=^$
```

## Architecture

### Data Flow

```
main() -> os.Exit(run())
  run() -> cli.Run()
    |- Parse CLI flags (with YAML config override via positional arg)
    |- ListFiles() -> discover/filter log files, allocate per-file buffers
    |- Parallel prefetch -> UpdateTimestamp() for all files concurrently
    |- ProcessFiles() -> min-heap merge (sequential output, parallel prefetch)
    '- Print aggregated metrics
```

### Package Structure

```
cmd/logmerge/main.go          # Thin entry point: os.Exit(run()) pattern
internal/
  cli/                         # CLI orchestration, flag parsing, YAML config, shell completions
    app.go                     #   Run() entry point, flag definitions, config merging
    main_config.go             #   YAML config struct and loading
    completions.go             #   Shell completion generation (bash/zsh/fish/powershell)
    integration_test.go        #   End-to-end binary tests
  core/                        # Merge engine (hot path)
    file_merge.go              #   Two-stage merge: parallel timestamp prefetch + sequential min-heap
    min_heap.go                #   Cache-optimized min-heap with contiguous heapEntry structs
    update_timestamp.go        #   Timestamp extraction from next line in buffer
  container/                   # Generic data structures
    ring_buffer.go             #   Circular I/O buffer with realignment, handles CR/LF/CRLF
  fsutil/                      # File system utilities
    file_handle.go             #   Per-file state: os.File, RingBuffer, parsed timestamp, metrics
    file_list.go               #   Recursive directory walking with gitignore-style filters
    filter.go                  #   Include/exclude pattern matching
    writable_file.go           #   Output file abstraction (stdout or file path)
  logtime/                     # Timestamp handling
    timestamp.go               #   uint64 nanosecond representation, fixed 30-byte formatted output
    timestamp_parse.go         #   Detection/parsing within first N bytes; multiple date formats
  loglevel/                    # Log level detection and filtering
    level.go                   #   Log level parsing from line content
  metrics/                     # Observability
    main_metrics.go            #   Top-level aggregated metrics
    merge_metrics.go           #   Per-merge-pass metrics
    list_metrics.go            #   File listing metrics
    progress.go                #   Periodic progress display
  testutil/                    # Shared test helpers
    assertions.go              #   assertEquals, assertNotEquals, isDeepEqual
```

### Performance Design Principles

These are intentional and should be preserved:

1. **Lock-free concurrency** — Per-file localized metrics, no shared mutable state in hot path
2. **Cache-friendly data structures** — Min-heap entries are contiguous structs, not pointer-heavy
3. **Branch-optimized parsing** — Linear code paths preferred over bitwise tricks for CPU branch predictor
4. **Zero-allocation hot path** — Buffers pre-allocated, no GC pressure during merge
5. **Ring buffer realignment** — Maximizes contiguous space to reduce I/O fragmentation
6. **Conditional parallelism** — Parallel workers only when `files > NumCPU && files > 4`

### Configuration

YAML-based config (`conf/default.yaml`) with CLI flag overrides (flags win). Key sections:
- **InputPaths** — directories/files to scan
- **OutputFile / LogFile** — merged output and stats destinations
- **ListFilesConfig** — gitignore-style ignore patterns, file aliases, archive handling
- **ParseTimestampConfig** — timestamp search window, timezone handling, minimum length
- **MergeConfig** — buffer sizes (default 100MB), time range filtering, alias/timestamp/level output modes
- **PrintProgressConfig** — periodic progress display interval

### Testing

- Black-box tests (separate `_test` packages) with custom assertion helpers in `internal/testutil/`
- Table-driven test patterns throughout
- Integration tests in `cli/` that build and run the actual binary
- CI runs on Linux, macOS, and Windows with `-race` flag
- External dependencies: `gopkg.in/yaml.v3`, `github.com/ulikunitz/xz`

## Coding Conventions

- **`os.Exit(run())` in main** — keeps `main()` thin, defers execute properly, entry point is testable
- **Errors to stderr, data to stdout** — never mix them. Return errors from libraries, never `log.Fatal` outside `main()`
- **Wrap errors with context** — `fmt.Errorf("doing X for %q: %w", name, err)`
- **No dead code** — unused types, params, functions get deleted, not commented out
- **Comments explain why, not what** — don't restate the code
- **No `fmt.Sprintf` in hot loops** — pre-compute or use `strings.Builder`
- **Profile first, optimize second** — benchmark before and after every perf claim
- **Stdlib first** — exhaust stdlib before reaching for third-party code
- **Cross-platform** — `filepath.Separator`, `path.Match`, line ending awareness (`\r\n` vs `\n`)
- **`t.Helper()`** on all test helper functions; table-driven tests with descriptive name fields

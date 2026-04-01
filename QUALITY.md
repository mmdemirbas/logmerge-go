# Quality Findings

Generated: 2026-04-01  
Tool: golangci-lint (errcheck, govet, staticcheck, gosec, gocognit, cyclop, funlen, gocritic, unused)  
Total: 93 issues

## Priority Order

Issues ordered by: correctness > security > noise-to-value ratio.

### P1 — Correctness (staticcheck, unused)
| # | File | Line | Linter | Issue |
|---|------|------|--------|-------|
| 1 | internal/loglevel/level.go | 117 | staticcheck | SA4004: surrounding loop is unconditionally terminated |
| 2 | internal/fsutil/archive_test.go | 326 | staticcheck | SA9003: empty branch |
| 3 | internal/container/ring_buffer_bench_test.go | 13 | unused | var sinkInt is unused |

### P2 — Style/Correctness (gocritic)
| # | File | Line | Linter | Issue |
|---|------|------|--------|-------|
| 4 | internal/container/ring_buffer.go | 98 | gocritic | commentFormatting: missing space after `//` |
| 5 | internal/logtime/timestamp.go | 38 | gocritic | captLocal: `M` should not be capitalized |
| 6 | internal/logtime/timestamp_parse.go | 94 | gocritic | ifElseChain: rewrite if-else to switch |
| 7 | internal/logtime/timestamp_parse.go | 261 | gocritic | ifElseChain: rewrite if-else to switch |

### P3 — Security (gosec)
| # | File | Line | Linter | Issue |
|---|------|------|--------|-------|
| 8  | internal/cli/app_test.go | 45 | gosec | G306: WriteFile perm > 0600 |
| 9  | internal/cli/integration_test.go | 21 | gosec | G204: subprocess with variable |
| 10 | internal/cli/integration_test.go | 33 | gosec | G306: WriteFile perm > 0600 |
| 11 | internal/cli/integration_test.go | 43 | gosec | G204: subprocess with variable |
| 12 | internal/fsutil/file_list.go | 256 | gosec | G304: file inclusion via variable |
| 13 | internal/fsutil/file_list.go | 285 | gosec | G304: file inclusion via variable |
| 14 | internal/fsutil/file_list.go | 395 | gosec | G304: file inclusion via variable |
| 15 | internal/fsutil/writable_file.go | 46 | gosec | G301: dir perm > 0750 |

### P4 — Error Handling (errcheck) — production code first
| # | File | Line | Linter | Issue |
|---|------|------|--------|-------|
| 16 | internal/core/file_merge.go | 134 | errcheck | file.Close unchecked |
| 17 | internal/core/file_merge.go | 144 | errcheck | file.Close unchecked |
| 18 | internal/cli/app.go | 353 | errcheck | cpuFile.Close unchecked |
| 19 | internal/cli/app.go | 384 | errcheck | fmt.Fprintln unchecked |
| 20 | internal/cli/app.go | 416 | errcheck | outputFile.Close unchecked |
| 21 | internal/cli/app.go | 443 | errcheck | memFile.Close unchecked |
| 22 | internal/cli/completions.go | 9 | errcheck | fmt.Fprint unchecked |
| 23 | internal/cli/completions.go | 74 | errcheck | fmt.Fprint unchecked |
| 24 | internal/cli/completions.go | 110 | errcheck | fmt.Fprint unchecked |
| 25 | internal/fsutil/writable_file.go | ? | errcheck | fmt.Fprintf unchecked (multiple) |
| 26 | internal/metrics/main_metrics.go | 249 | errcheck | fmt.Fprintf unchecked |
| 27 | internal/metrics/main_metrics.go | 250 | errcheck | fmt.Fprintf unchecked |
| -- | (test files) | various | errcheck | ~38 unchecked errors in test/bench files |

### P5 — Complexity (gocognit, cyclop, funlen)
High complexity in production code — refactor candidates:

| # | File | Func | Linter | Value |
|---|------|------|--------|-------|
| 28 | internal/logtime/timestamp_parse.go | tryParseTimestamp | gocognit | 142 |
| 29 | internal/cli/app.go | Run | gocognit | 86 |
| 30 | internal/core/file_merge.go | writeLine | gocognit | 65 |
| 31 | internal/core/file_merge.go | sequentialProcessFiles | gocognit | 64 |
| 32 | internal/fsutil/file_handle.go | WriteLine | gocognit | 48 |
| 33 | internal/logtime/timestamp_parse.go | parseCtimeFrom | gocognit | 39 |
| 34 | internal/logtime/timestamp_parse.go | parseMonthName | gocognit | 38 |
| 35 | internal/fsutil/file_list.go | visitVirtualFile | gocognit | 31 |
| 36 | internal/loglevel/level.go | matchLevelWord | gocognit | 32 |
| 37 | internal/loglevel/level.go | ParseLevel | gocognit | 31 |
| 38 | internal/fsutil/file_list.go | listVirtualFiles | gocognit | 26 |
| 39 | internal/fsutil/file_list.go | walkDir | gocognit | 26 |
| 40 | internal/fsutil/filter.go | matchSegments | gocognit | 26 |
| 41 | internal/logtime/timestamp_parse.go | computeStripBounds | cyclop | 13 |
| 42 | internal/logtime/timestamp_parse.go | ParseTimestampWithEnd | cyclop | 11 |
| 43 | internal/logtime/timestamp_parse.go | ParseTimestampForStrip | cyclop | 11 |
| 44 | internal/container/ring_buffer.go | Fill | cyclop | 11 |
| 45 | internal/metrics/main_metrics.go | PrintMetrics | funlen | 107 stmts |
| 46 | internal/container/ring_buffer_test.go | TestRingBuffer | funlen | 567 lines |
| -- | (test files) | various | gocognit/cyclop/funlen | complexity in tests |

## Status

- [x] P1 (3 issues) — fixed (commit f652577)
- [x] P2 (4 issues) — fixed (commit 2c46b68)
- [x] P3 (8 issues) — fixed (commit eec4e4a)
- [x] P4 errcheck prod (12 issues) — fixed (commit 1f7adb1)
- [x] P4 errcheck tests (~38 issues) — fixed (commit 1f7adb1)
- [ ] P5 complexity — 10 remaining (1 funlen, 9 gocognit):
  - metrics/main_metrics.go: PrintMetrics funlen:107
  - cli/app.go: Run gocognit:86
  - core/file_merge.go: sequentialProcessFiles gocognit:69, writeLine gocognit:65
  - fsutil/file_handle.go: (*FileHandle).WriteLine gocognit:48
  - fsutil/file_list.go: listVirtualFiles:26, walkDir:26, visitVirtualFile:31
  - fsutil/filter.go: matchSegments gocognit:26

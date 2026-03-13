# LogMerge: High-Performance Sequential Log Aggregator

LogMerge is a specialized systems tool designed to merge multiple, massive log files into a single
chronologically ordered stream. It is engineered for extreme throughput, leveraging modern CPU
architectures and Go's concurrency primitives.

## 📖 Usage

LogMerge supports both CLI flags and an optional YAML configuration file. CLI flags override YAML
values, and input paths are passed as positional arguments.

### 1. Basic Execution

```bash
# Merge all log files in a directory
./logmerge /var/log/myapp

# Merge multiple paths, output to file
./logmerge -o merged.log /var/log/app1 /var/log/app2

# Use a YAML config as a base, override with flags
./logmerge --config config.yaml -o merged.log /var/log/myapp
```

### 2. Filtering Examples

```bash
# Ignore backup and temp files using gitignore-style patterns
./logmerge -i "*.bak" -i "*temp*" /var/log/myapp

# Auto-ignore all archive files (.zip, .gz, .tar, etc.)
./logmerge --ignore-archives /var/log/myapp

# Load ignore patterns from a file
./logmerge --ignore-file .logmergeignore /var/log/myapp

# Combine: ignore archives + custom patterns
./logmerge --ignore-archives -i "*.old" /var/log/myapp
```

### 3. Formatting Examples

```bash
# Prepend timestamps and show file separators
./logmerge -t -b /var/log/myapp

# Prepend file alias to every line
./logmerge -a --alias "console.log=driver" --alias "*/worker/*.log=worker" /var/log/myapp

# Filter by time range
./logmerge --since 2025-01-17T13:00:00Z --until 2025-01-17T14:00:00Z /var/log/myapp
```

### 4. YAML Configuration

For complex setups, use a YAML config file. See `conf/example.yaml` for all available fields.

```yaml
InputPaths:
  - /var/log/myapp
  - /var/log/myapp-worker

OutputFile: /tmp/merged.log

ListFilesConfig:
  IgnoreArchives: true
  IgnorePatterns:
    - "*temp*"
    - "*.bak"
  FileAliases:
    "console.log": "driver"
    "*/worker/*.log": "worker"

MergeConfig:
  WriteTimestampPerLine: true
  WriteAliasPerBlock: true
  MinTimestamp: "2025-01-17T00:00:00Z"
  MaxTimestamp: "2025-01-18T00:00:00Z"
```

### 5. Key Flags

* **`-i, --ignore <pattern>`**: Gitignore-style glob patterns to exclude files. Repeatable. Prefix with `!` to negate.
* **`--ignore-archives`**: Shorthand to ignore `.zip`, `.gz`, `.tar`, `.rar`, `.7z`, `.tgz`, `.bz2`, `.tbz2`, `.xz`, `.txz`.
* **`--config <path>`**: Load a YAML configuration file as base. CLI flags override YAML values.
* **`--buf-read <bytes>`**: Increase for high-latency storage (network mounts). Default: 100 MB.

## 📦 Transparent Decompression

LogMerge automatically handles compressed files during file discovery:

- **`.gz` files**: Decompressed on-the-fly using Go's `compress/gzip` reader.
- **`.zip` files**: Each entry inside the archive is treated as an individual log file. Entries are
  filtered through the same ignore patterns, and a reference-counted reader ensures safe concurrent
  access.

Archive files are processed transparently — no extraction step is needed. Virtual paths for zip
entries use the format `path/to/archive.zip!/entry/name.log`, which also works with `--alias` and
`--ignore` patterns.

To skip compressed files entirely, use `--ignore-archives`.

## 📝 Output Preview

The following example demonstrates how LogMerge handles interleaved files, filename aliases, and
lines without explicit timestamps (e.g., stack traces or multi-line logs).

```text
--- auth-service.log ---
2026-03-13 01:21:22.000000000 [INFO] User "admin" logged in from 192.168.1.5
2026-03-13 01:21:22.105000000 [DEBUG] Initializing session cache...

--- database.log ---
2026-03-13 01:21:22.500000000 [INFO] Query: SELECT * FROM users WHERE id = 1
2026-03-13 01:21:22.505000000 [ERROR] Connection timeout:
at db.connector.connect (connection.go:45)
at main.main (main.go:12)
caused by: context deadline exceeded

--- auth-service.log ---
2026-03-13 01:21:23.000000000 [INFO] Session expired for user "guest"
```

### 💡 Key Output Features

**Block Aliases**: When WriteAliasPerBlock is enabled, the source filename is prepended with ---
separators whenever the stream switches files.

**Orphan Handling**: Notice the stack trace under database.log. Because those lines have no
timestamp, LogMerge correctly keeps them immediately following the parent log line that generated
them, maintaining the logical context.

**Precision**: Timestamps are normalized to nanosecond precision (padding with zeros where
necessary) to ensure a standard width.

## 🚀 Performance Highlights

Current benchmarks on Apple M1 Max hardware:

- **Throughput:** ~34 GB/s (In-memory simulation)
- **Memory Profile:** Near-zero allocations in the hot path.
- **Scaling:** Parallel pre-fetching utilizes all available CPU cores.

## 🛠 Architectural Optimizations

### 1. Cache-Friendly Min-Heap

The core merge logic uses a specialized `MinHeap`. To avoid "pointer chasing" and L1 cache misses,
the heap entries store the `Timestamp` contiguously with the `FileHandle` pointer, ensuring the
CPU's branch predictor and prefetcher can operate at peak efficiency.

### 2. SIMD-Accelerated Searching

Instead of sequential byte-scanning, the tool utilizes assembly-optimized primitives (
`bytes.IndexAny`). On ARM64 (M1 Max), this leverages **NEON** vector instructions to scan for
newlines across 16-32 bytes in a single clock cycle.

### 3. Lock-Free Concurrency

Following a "Localize & Aggregate" pattern, the metrics and parsing state are localized to each
`FileHandle`. This eliminates global mutex contention and data races, allowing the CPU-intensive
timestamp parsing phase to scale linearly with the number of cores.

### 4. Branch-Optimized Parsing

Timestamp parsing has been restructured to favor linear execution. By utilizing standard branching
patterns that the CPU's **Branch Prediction Unit (BPU)** can easily learn, we achieved a ~6x
performance increase over traditional bitwise-logic hacks.

### 5. Profile-Guided Optimization (PGO)

The build pipeline supports **PGO**, allowing the Go compiler to use real-world CPU profiles to make
aggressive inlining and devirtualization decisions.

## 🔨 Development

📋 Prerequisites:

- **Go:** 1.21+
- **Task:** (Optional) for build automation

Tasks are managed via `go-task`.

```bash
# Run benchmarks and generate CPU/Mem profiles
task benchmark

# Build a PGO-optimized binary for the current OS
task build-pgo

# Run cross-compilation for Linux, Darwin, and Windows
task dist
```

## ⚖️ License

MIT

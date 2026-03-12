# LogMerge: High-Performance Sequential Log Aggregator

LogMerge is a specialized systems tool designed to merge multiple, massive log files into a single
chronologically ordered stream. It is engineered for extreme throughput, leveraging modern CPU
architectures and Go's concurrency primitives.

## 📖 Usage

LogMerge is driven by a YAML configuration file. This allows for precise control over I/O buffers
and timestamp discovery.

### 1. Basic Execution

```bash
./logmerge config.yaml

```

### 2. Configuration Example (`config.yaml`)

```yaml
OutputFile:
  Path: "out/merged.log"
LogFile:
  Path: "out/process.log"

ListFilesConfig:
  InputPath: "/var/logs/app-cluster"
  IncludedSubstrings: [ ".log" ]
  ExcludedSuffixes: [ ".gz", ".zip" ]

MergeConfig:
  BufferSizeForRead: 104857600  # 100MB per file
  BufferSizeForWrite: 104857600 # 100MB output buffer
  WriteTimestampPerLine: true
  WriteAliasPerBlock: true

ParseTimestampConfig:
  ShortestTimestampLen: 15
  TimestampSearchEndIndex: 250

```

### 3. Key Parameters

* **InputPath:** Directory containing the logs to be merged.
* **BufferSizeForRead:** Increase this for high-latency storage (network mounts).
* **WriteAliasPerBlock:** Prepends the source filename whenever the stream switches between files.

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

# TASK.md

Open work items. Ordered by priority: correctness > user-visible quality > performance > refactoring.

---

## P1 — Correctness

### Timestamp-less entry retention
Lines without a parsable timestamp (continuation lines, stack traces) must be retained regardless
of `MinTimestamp` / `MaxTimestamp` filtering. Silently dropping them is data loss.

### Default local timezone
If a log timestamp carries no timezone info, assume the local machine timezone instead of UTC.
Treating ambiguous timestamps as UTC causes ordering jumps when merging logs from servers in
different timezones.

---

## P2 — User-Visible Features

### Mod-time fallback
If a file contains zero parsable timestamps, use the OS file modification time as a placement
anchor so the file still appears in roughly the right position in the merged output.

### Custom timestamp output format
Let users specify the output timestamp format (ISO 8601, Unix epoch, RFC 3339, etc.) instead of
the hardcoded `YYYY-MM-DD HH:MM:SS.nnnnnnnnn`.

### Custom source name template
Go beyond `--alias pat=name` with a format template that can embed file path, directory, or
hostname extracted from the path.

### Threshold ordering (slop factor)
Allow breaking strict chronological order up to a configurable threshold (e.g. 1 second) to keep
logs from the same source together. Reduces interleaving jitter.

---

## P3 — Performance / Scaling

### FD limit handling
When merging thousands of files (e.g. 2000 Spark executor logs) the process may hit `ulimit -n`.
Implement external merge sort: merge chunks into temp files, then merge the results.

### Parallel / concurrent reading
The merge loop reads one file at a time (with initial-timestamp prefetch). True concurrent reading
during the merge pass could improve throughput on high-latency / network storage.

### Parallel merge (fan-in)
The heap loop is single-threaded. For very high file counts, a multi-stage fan-in could help.
Stub code already exists but is unused — evaluate before implementing.

---

## P4 — Code Quality

### Simplify the merge loop
`sequentialProcessFiles` mixes I/O, timestamp parsing, alias formatting, and heap management.
Extract output formatting into a separate layer to make it testable and extensible.

---

## P5 — Testing

### Realistic test fixtures
Add a curated archive of real-world log samples (varied formats, timezones, continuation lines)
to complement the unit tests and expose gaps in timestamp parsing and filtering.

### Manual end-to-end review
Walk through all CLI flags and config options against real log data to surface UX gaps, missing
validation, and unexpected behavior.

---

## Parking Lot (evaluate before committing)

- Loser tree as an alternative to the current min-heap — profile first to confirm the heap is
  actually a bottleneck before replacing it.

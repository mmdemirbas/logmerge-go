# TODO

Ideas and future work. Not in priority order. Some may never be implemented.

## Data integrity

- **Timestamp-less entry retention** — lines without a parsable timestamp (continuation lines, stack
  traces) should always be included regardless of `MinTimestamp` filtering. Discarding a stack trace
  because its continuation lines have no timestamp is a data loss bug.

- **Mod-time fallback** — if a file contains zero parsable timestamps, use the OS file modification
  time as a placement anchor so it at least appears in roughly the right position in the merged
  output.

## Output formatting

- **Custom timestamp output format** — allow specifying the output timestamp format (ISO 8601, Unix
  epoch, RFC3339, etc.) instead of the fixed `YYYY-MM-DD HH:MM:SS.nnnnnnnnn`.

- **Custom source name template** — go beyond `--alias pat=name` with a format template that can
  include the file path, directory, hostname extracted from path, etc.

- **Default local timezone** — if a log timestamp has no timezone info, assume the local machine's
  timezone instead of treating it as UTC. Prevents ordering jumps when merging logs from servers in
  different timezones.

- **Threshold ordering (slop factor)** — allow breaking strict chronological order in favor of
  keeping logs from the same source together, up to a configurable threshold (e.g., 1 second).
  Reduces interleaving jitter and makes output easier to read.

## Performance & scaling

- **Parallel merge** — the merge loop is sequential (single-threaded heap). For very high file
  counts or high-latency storage (network mounts), a multi-stage fan-in could help. Stub code
  exists but is unused.

- **FD limit handling** — when merging thousands of files (e.g., 2000 Spark executor logs), the
  process may hit `ulimit -n`. Implement external merge sort: merge chunks into intermediate temp
  files, then merge the results.

- **Concurrent file reading** — the merge loop reads one file at a time (with prefetch for initial
  timestamps). True concurrent reading during merge could improve throughput on high-latency storage.

## Code quality

- **Simplify the merge loop** — `sequentialProcessFiles` handles I/O, timestamp parsing, alias
  formatting, and heap management in one place. Extracting output formatting into a separate layer
  would make the code easier to extend and test.

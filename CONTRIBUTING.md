# Contributing to LogMerge

Contributions are welcome. Here's how to get started.

## Reporting issues

Open a [GitHub issue](https://github.com/mmdemirbas/logmerge/issues) with:
- What you expected to happen
- What actually happened
- A minimal input that reproduces the problem (a few log lines is ideal)

## Submitting changes

1. Fork the repository
2. Create a feature branch (`git checkout -b my-feature`)
3. Make your changes
4. Run tests: `go test -count=1 ./...`
5. Run the integration tests against the example data — they catch regressions that unit tests miss
6. Open a pull request

## Development setup

Requires Go 1.21+ and [Task](https://taskfile.dev/) (optional, for convenience commands).

```bash
# Run all tests
go test -count=1 ./...

# Build
go build -o logmerge ./cmd/logmerge

# Run benchmarks
go test ./internal/core/ -bench=. -benchmem -run='^$'
```

## Code style

- Follow standard Go conventions (`go fmt`, `go vet`)
- Keep the hot path zero-allocation — avoid heap allocations in the merge loop
- Add tests for new functionality, including edge cases
- Performance matters — run benchmarks before and after changes to the merge pipeline

## What makes a good contribution

- Bug fixes with a reproducing test case
- New timestamp format support (with test coverage)
- New log level format detection (with positive and negative test cases)
- Documentation improvements
- Performance improvements backed by benchmark data

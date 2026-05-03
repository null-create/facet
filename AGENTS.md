# AGENTS.md

## Commands

- `make test` — Tests with `-race` detector (matches CI)
- `make proto` — Generate protobuf code from `proto/store.proto`
- `make bench` — Run benchmarks (10s benchtime)

## Structure

- Go library (`package facet`), not executable — no `main.go`
- `proto/store.pb.go` is auto-generated, do not edit

## Dependencies

- `github.com/google/btree v1.1.3` — B-tree for timestamp range queries
- `golang.org/x/exp` — (not used currently, can be removed)

## CI

- Execution order: proto → `go vet` → tests (with `-race`) → build
- Requires: `protobuf-compiler`, `protoc-gen-go`

# AGENTS.md

## Commands

- `make test` — Tests with `-race` detector (matches CI)
- `make proto` — Generate protobuf code from `proto/store.proto`
- `make bench` — Run benchmarks (10s benchtime)

## Structure

- Go library (`package facet`), not executable — no `main.go`
- `proto/store.pb.go` is auto-generated, do not edit

## CI

- Execution order: proto → `go vet` → tests (with `-race`) → build
- Requires: `protobuf-compiler`, `protoc-gen-go`

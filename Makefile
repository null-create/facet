.PHONY: all build test bench clean proto help

# Variables
BINARY_NAME=facet
PROTO_DIR=proto

# Default target
all: proto build test

## help: Display available commands
help:
	@echo "Facet - Available Commands:"
	@echo ""
	@echo "  make build       - Build the binary"
	@echo "  make test        - Run all tests"
	@echo "  make bench       - Run benchmarks"
	@echo "  make proto       - Generate protobuf code"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make run         - Build and run demo"
	@echo ""

## proto: Generate Go code from protobuf files
proto:
	@echo "Generating protobuf code..."
	@protoc --go_out=. --go_opt=paths=source_relative $(PROTO_DIR)/*.proto
	@echo "✓ Protobuf code generated"

## build: Build the binary
build: proto
	@echo "Building $(BINARY_NAME)..."
	@go build -o bin/$(BINARY_NAME) .
	@echo "✓ Build complete: bin/$(BINARY_NAME)"

## run: Build and run the demo
run: build
	@./bin/$(BINARY_NAME)

## test: Run all tests
test:
	@echo "Running tests..."
	@go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	@echo "✓ Tests passed"

## bench: Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=10s
	@echo "✓ Benchmarks complete"

## bench-cpu: Run benchmarks with CPU profiling
bench-cpu:
	@echo "Running benchmarks with CPU profiling..."
	@go test -bench=. -benchmem -benchtime=10s -cpuprofile=cpu.prof
	@echo "✓ CPU profile saved to cpu.prof"
	@echo "Analyze with: go tool pprof cpu.prof"

## bench-mem: Run benchmarks with memory profiling
bench-mem:
	@echo "Running benchmarks with memory profiling..."
	@go test -bench=. -benchmem -benchtime=10s -memprofile=mem.prof
	@echo "✓ Memory profile saved to mem.prof"
	@echo "Analyze with: go tool pprof mem.prof"

## bench-all: Run benchmarks with all profiling
bench-all:
	@echo "Running benchmarks with full profiling..."
	@go test -bench=. -benchmem -benchtime=10s \
		-cpuprofile=cpu.prof \
		-memprofile=mem.prof \
		-mutexprofile=mutex.prof \
		-blockprofile=block.prof
	@echo "✓ All profiles saved"
	@echo "CPU:   go tool pprof cpu.prof"
	@echo "Mem:   go tool pprof mem.prof"
	@echo "Mutex: go tool pprof mutex.prof"
	@echo "Block: go tool pprof block.prof"

## bench-trace: Run benchmarks with execution trace
bench-trace:
	@echo "Running benchmarks with trace..."
	@go test -bench=BenchmarkMixedWorkload -benchtime=10s -trace=trace.out
	@echo "✓ Trace saved to trace.out"
	@echo "View with: go tool trace trace.out"

## clean: Remove build artifacts and test data
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -rf data/
	@rm -rf test_data_*
	@echo "✓ Cleaned"
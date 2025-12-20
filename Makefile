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
	@go test -v ./...
	@echo "✓ Tests passed"

## bench: Run benchmarks
bench:
	@echo "Running benchmarks..."
	@go test -bench=. -benchmem -benchtime=3s
	@echo "✓ Benchmarks complete"

## clean: Remove build artifacts and test data
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -rf data/
	@rm -rf test_data_*
	@echo "✓ Cleaned"
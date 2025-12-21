# Facet

```
   ___                 _
  / __\__ _  ___ ___  | |_
 / _\/ _` |/ __/ _ \ | __|
/ / | (_| | (_|  __/ | |_
\/   \__,_|\___\___|  \__|

Multi-faceted key-value storage
```

A high-performance, in-memory key-value store with support for multi-dimensional compound keys, efficient partial queries, TTL expiration, persistent storage, and timestamp range queries.

## Features

- **Structured Compound Keys** - Type-safe multi-dimensional keys (TenantID, UserID, Resource, Timestamp)
- **Efficient Partial Queries** - Query by any subset of key dimensions in O(k) time instead of O(n)
- **Multi-Index Architecture** - Secondary indexes for fast filtering without full scans
- **TTL/Expiration** - Automatic cleanup of expired entries with min-heap optimization
- **Persistence** - Durable storage with Write-Ahead Log (WAL) and snapshots using Protocol Buffers
- **Range Queries** - B-tree index for efficient timestamp range queries
- **Thread-Safe** - Concurrent reads and writes using RWMutex
- **Zero External Dependencies** - Core uses only Go standard library (persistence requires protobuf)

## Quick Start

```go
package main

import (
    "fmt"
    "time"

    "github.com/null-create/facet"
)

func main() {
    // Create store with persistence
    store, _ := facet.NewStore("./data")
    defer store.Close()

    // Create a compound key
    key := facet.CompoundKey{
        TenantID:  1,
        UserID:    100,
        Resource:  "profile",
        Timestamp: time.Now().Unix(),
    }

    // Store a value with 60-second TTL
    store.Set(key, "user profile data", 60*time.Second)

    // Exact lookup
    if val, ok := store.Get(key); ok {
        fmt.Println("Found:", val)
    }

    // Partial query - find all entries for tenant 1
    results := store.Query(facet.WithTenant(1))
    fmt.Printf("Found %d entries for tenant 1\n", len(results))

    // Range query - events from last hour
    now := time.Now().Unix()
    oneHourAgo := now - 3600
    results = store.RangeQuery(oneHourAgo, now, facet.WithTenant(1))

    // Delete all entries for a tenant
    deleted := store.Delete(facet.WithTenant(1))
    fmt.Printf("Deleted %d entries\n", deleted)

    // Create snapshot
    store.CreateSnapshot()
}
```

## Installation

```bash
# Clone the repository
git clone https://github.com/null-create/facet.git
cd facet

# Install Protocol Buffer compiler (for persistence)
brew install protobuf  # macOS
# or: sudo apt-get install protobuf-compiler  # Linux

# Install Go protobuf plugin
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

# Generate protobuf code
protoc --go_out=. --go_opt=paths=source_relative proto/store.proto

# Download dependencies
go mod tidy

# Run the demo
go run main.go

# Run benchmarks
go test -bench=. -benchmem

# Run tests
go test -v
```

## Performance

Benchmarked on AMD Ryzen 5 5600X (6-core) running Linux:

### Core Operations

- **Exact lookups**: ~99 ns per operation (single-thread), ~43 ns (parallel)
- **Write operations**: ~2.4 μs per operation (without WAL)
- **Partial queries**: 260 ns - 312 μs (depends on selectivity)
- **Range queries**: ~26-190 μs (depends on result size)
- **Throughput**:
  - Reads: 10M ops/sec (single-thread), 23M ops/sec (parallel)
  - Writes: 417k ops/sec (single-thread), 714k ops/sec (parallel)
  - Mixed workload: 2.2M ops/sec

### Real-World Performance

| Operation                     | Latency | Speedup vs Full Scan |
| ----------------------------- | ------- | -------------------- |
| Query tenant (1000 results)   | 312 μs  | **4.9x faster**      |
| Query user (10 results)       | 260 ns  | **5,800x faster**    |
| String key scan (Redis-style) | 1.52 ms | Baseline (slowest)   |

**Key Insight**: Queries scale with result size (k), not database size (n), achieving O(k) complexity.

### vs. Traditional String-Based Stores

Partial key queries are **5-5,800x faster** than Redis SCAN-style pattern matching:

- Highly selective queries (0.01% of data): **5,800x faster**
- Moderate queries (1% of data): **5x faster**
- Uses indexed lookups O(k) instead of full scans O(n)

### Scalability

| Dataset Size | Query Time (1% results) | Confirms O(k)        |
| ------------ | ----------------------- | -------------------- |
| 1K entries   | 1.3 μs                  | ✅ Baseline          |
| 10K entries  | 15.6 μs                 | ✅ ~12x (10x data)   |
| 100K entries | 310 μs                  | ✅ ~238x (100x data) |

Time scales linearly with **result set size**, not total database size.

### With Persistence (WAL)

⚠️ WAL adds ~10-50 μs per write due to fsync() to disk. For benchmarking in-memory performance, WAL can be disabled.

See [DOCUMENTATION.md](DOCUMENTATION.md) for detailed benchmark results and methodology.

## Use Cases

- **Multi-tenant caching** - Efficiently invalidate all cache entries for a tenant
- **Session management** - Query sessions by user, device, or session type with TTL
- **API gateway caching** - Cache by user + endpoint + version with fast invalidation
- **Time-series data** - Store and query by sensor + timestamp combinations
- **Event tracking** - Range queries for events within time windows
- **Feature flags** - Tenant-specific configuration with expiration

## Architecture

Facet uses a **multi-index architecture** with several key innovations:

1. **Primary Store** - Hash map keyed by full compound key hash (FNV-1a)
2. **Secondary Indexes** - Separate indexes for TenantID, UserID, and Resource
3. **Timestamp B-tree** - Ordered index for efficient range queries
4. **TTL Min-Heap** - Efficient tracking of expiration times
5. **Query Optimizer** - Selects the most selective index for each query
6. **WAL + Snapshots** - Durable persistence with Protocol Buffers

See [DOCUMENTATION.md](DOCUMENTATION.md) for detailed implementation information.

## Limitations

- **In-memory only** - Data stored in RAM (persistence for durability, not capacity)
- **Single-node** - No distribution (yet)
- **Fixed schema** - Key structure is predefined
- **Memory overhead** - ~4x due to multiple indexes (~170 bytes per entry with all features)

## Project Structure

```
facet/
├── README.md           # This file
├── DOCUMENTATION.md    # Detailed technical documentation
├── main.go             # Core implementation
├── main_test.go        # Benchmark tests
├── persistence.go      # Protobuf I/O helpers
├── proto/
│   ├── store.proto     # Protocol Buffer schema
│   └── store.pb.go     # Generated Go code
├── data/               # Runtime data directory
│   ├── wal.log         # Write-ahead log
│   └── snapshot.pb     # Snapshot file
└── LICENSE             # MIT License
```

## API Reference

### Types

```go
type CompoundKey struct {
    TenantID  uint64
    UserID    uint64
    Resource  string
    Timestamp int64
}

type PartialKey struct {
    TenantID  *uint64
    UserID    *uint64
    Resource  *string
    Timestamp *int64
}
```

### Store Methods

```go
// Create a new store with persistence
func NewStore(dataDir string) (*Store, error)

// Set a value with optional TTL (0 = no expiration)
func (s *Store) Set(key CompoundKey, value interface{}, ttl time.Duration)

// Get a value by exact key
func (s *Store) Get(key CompoundKey) (interface{}, bool)

// Query by partial key pattern
func (s *Store) Query(partial PartialKey) map[CompoundKey]interface{}

// Range query by timestamp with optional filters
func (s *Store) RangeQuery(startTime, endTime int64, partial PartialKey) map[CompoundKey]interface{}

// Delete entries matching pattern
func (s *Store) Delete(partial PartialKey) int

// Create snapshot of current state
func (s *Store) CreateSnapshot() error

// Get store statistics
func (s *Store) Stats() map[string]int

// Close store and flush to disk
func (s *Store) Close() error
```

### Helper Functions

```go
// Create partial keys for common queries
func WithTenant(id uint64) PartialKey
func WithUser(id uint64) PartialKey
func WithResource(name string) PartialKey
func WithTenantAndUser(tenantID, userID uint64) PartialKey
```

## Advanced Features

### TTL/Expiration

```go
// Set with 5-minute TTL
store.Set(key, value, 5*time.Minute)

// No expiration
store.Set(key, value, 0)
```

Expired entries are automatically cleaned up by a background goroutine every second. The min-heap ensures O(1) checking of the next expiration time.

### Persistence

Facet uses a combination of Write-Ahead Log (WAL) and periodic snapshots:

- **WAL**: Every write operation is durably logged using Protocol Buffers
- **Snapshots**: Periodic full state dumps for fast recovery
- **Recovery**: On startup, load latest snapshot + replay WAL

```go
// Manual snapshot creation
store.CreateSnapshot()

// Automatic recovery on startup
store, err := facet.NewStore("./data")  // Loads snapshot + replays WAL
```

### Range Queries

Efficiently query by timestamp ranges using the B-tree index:

```go
// All events in the last hour
now := time.Now().Unix()
results := store.RangeQuery(now-3600, now, facet.PartialKey{})

// Tenant 1's events in the last hour
results = store.RangeQuery(now-3600, now, facet.WithTenant(1))

// Specific user's events in a date range
startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
endDate := time.Date(2024, 1, 31, 23, 59, 59, 0, time.UTC).Unix()
results = store.RangeQuery(startDate, endDate, facet.WithUser(12345))
```

## Contributing

Contributions are welcome! Areas for improvement:

- [ ] Distributed operation support
- [ ] Custom key schemas via generics
- [ ] Additional index types (geospatial, full-text)
- [ ] Compression for snapshots
- [ ] Metrics/observability hooks
- [ ] HTTP/gRPC API server

## License

MIT License

## References

- [FNV Hash Function](http://www.isthe.com/chongo/tech/comp/fnv/)
- [Inverted Index Theory](https://en.wikipedia.org/wiki/Inverted_index)
- [Protocol Buffers](https://protobuf.dev/)
- [Go Sync Primitives](https://pkg.go.dev/sync)

## Author

Created as an exploration of efficient partial key queries for key-value stores and the limitations of string-based key concatenation in systems like Redis and Memcached.

## Acknowledgments

Inspired by the need for better partial key query support in distributed caches and the desire to leverage structured, type-safe keys for multi-dimensional data access patterns.

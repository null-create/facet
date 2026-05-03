package facet

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

// Temp dir for testing data
var tmpDir = fmt.Sprintf("./test_data_%d", time.Now().UnixNano())

// Helper to create test stores for benchmark tests
func createBenchTestStore(t *testing.B) *Store {
	store, err := NewStoreWithOpts("", StoreOpts{
		WalEnabled:       false,
		SnapshotsEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// Helper to create test stores for standard tests
func createTestStore(t *testing.T) *Store {
	store, err := NewStoreWithOpts("", StoreOpts{
		WalEnabled:       false,
		SnapshotsEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// BenchmarkHash measures hash creation performance
func BenchmarkHash(b *testing.B) {
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := CompoundKey{
			TenantID:  uint64(i),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		key.Hash()
	}
}

// BenchmarkSet measures write performance without TTL
func BenchmarkSet(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now + int64(i), // Incremental timestamp avoids calling time.Now() in hot loop
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
}

// BenchmarkSetWithTTL measures write performance with TTL
func BenchmarkSetWithTTL(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now + int64(i), // Incremental timestamp avoids calling time.Now() in hot loop
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 60*time.Second)
	}
}

// BenchmarkSetParallel measures concurrent write performance
func BenchmarkSetParallel(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := CompoundKey{
				TenantID:  uint64(i % 100),
				UserID:    uint64(i),
				Resource:  fmt.Sprintf("resource_%d", i%10),
				Timestamp: now + int64(i), // Incremental timestamp avoids calling time.Now() in hot loop
			}
			store.Set(key, fmt.Sprintf("value_%d", i), 0)
			i++
		}
	})
}

// BenchmarkGet measures exact key lookup performance
func BenchmarkGet(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate
	keys := make([]CompoundKey, 10000)
	for i := range 10000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		keys[i] = key
		keys[i].Hash() // Hash once!
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		key := keys[i%10000]
		store.Get(key)
	}
}

// BenchmarkGetParallel measures concurrent read performance
func BenchmarkGetParallel(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate
	keys := make([]CompoundKey, 10000)
	for i := range 10000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		keys[i] = key
		keys[i].Hash() // Hash once!
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := keys[i%10000]
			store.Get(key)
			i++
		}
	})
}

// BenchmarkQueryByTenant measures partial query performance (indexed field)
func BenchmarkQueryByTenant(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()

	// Prepopulate with 100k entries across 100 tenants
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now,
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Each tenant has ~1000 entries (1% of total)
		tenantID := uint64(i % 100)
		store.Query(PartialKey{TenantID: &tenantID}, func(_ CompoundKey, _ any) bool {
			return true
		})
	}
}

// BenchmarkQueryByUser measures partial query with different selectivity
func BenchmarkQueryByUser(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()

	// Prepopulate
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i % 10000),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now,
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Each user has ~10 entries (0.01% of total)
		userID := uint64(i % 10000)
		store.Query(PartialKey{UserID: &userID}, func(_ CompoundKey, _ any) bool {
			return true
		})
	}
}

// BenchmarkQueryByResource measures query on less selective index
func BenchmarkQueryByResource(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate with 1000 resources (~100 entries each from 100k total)
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%1000),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Each resource has ~100 entries (0.1% of total) - realistic selectivity
		resource := fmt.Sprintf("resource_%d", i%1000)
		store.Query(PartialKey{Resource: &resource}, func(_ CompoundKey, _ any) bool {
			return true
		})
	}
}

// BenchmarkQueryTenantAndUser measures compound partial query
func BenchmarkQueryTenantAndUser(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()

	// Prepopulate
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i % 10000),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now,
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Very selective - typically 1-10 entries
		tenantID := uint64(i % 100)
		userID := uint64(i % 10000)
		store.Query(PartialKey{TenantID: &tenantID, UserID: &userID}, func(_ CompoundKey, _ any) bool {
			return true
		})
	}
}

// BenchmarkRangeQuery measures timestamp range query performance
func BenchmarkRangeQuery(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate with timestamps spread over time
	baseTime := time.Now().Unix()
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: baseTime + int64(i), // Sequential timestamps
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Query 1000 entries (1% of data) with tenant filter (realistic)
		start := baseTime + int64(i%99000)
		end := start + 1000
		tenantID := uint64(i % 100)
		store.RangeQuery(start, end, PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
			return true
		})
	}
}

// BenchmarkRangeQueryWithFilter measures range query with partial key filter
func BenchmarkRangeQueryWithFilter(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate
	baseTime := time.Now().Unix()
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: baseTime + int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		// Query 1000 entries for specific tenant (~10 results)
		start := baseTime + int64(i%99000)
		end := start + 1000
		tenantID := uint64(i % 100)
		store.RangeQuery(start, end, PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
			return true
		})
	}
}

// BenchmarkDelete measures deletion performance
func BenchmarkDelete(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()

	// Prepopulate
	for i := range 100000 {
		key := CompoundKey{
			TenantID:  uint64(i % 10),
			UserID:    uint64(i),
			Resource:  "resource",
			Timestamp: now,
		}
		store.Set(key, "value", 0)
	}

	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		tenantID := uint64(i % 10)
		store.Delete(PartialKey{TenantID: &tenantID})
	}
}

// BenchmarkKeyHash measures hashing performance
func BenchmarkKeyHash(b *testing.B) {
	key := CompoundKey{
		TenantID:  12345,
		UserID:    67890,
		Resource:  "orders",
		Timestamp: time.Now().Unix(),
	}

	b.ResetTimer()
	for b.Loop() {
		key.Hash()
	}
}





// Benchmark for mixed workload (realistic usage)
func BenchmarkMixedWorkload(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()

	// Prepopulate
	for i := range 10000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now,
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	// Fast pseudo-random using LCG (Linear Congruential Generator)
	// Avoids slow rand.Intn() calls in hot loop
	seed := uint64(42)
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		seed = seed*1664525 + 1013904223 // LCG constants
		op := seed % 100

		switch {
		case op < 70: // 70% reads
			seed = seed*1664525 + 1013904223
			key := CompoundKey{
				TenantID:  uint64(seed % 100),
				UserID:    uint64(seed % 10000),
				Resource:  fmt.Sprintf("resource_%d", seed%10),
				Timestamp: now + int64(seed%10000),
			}
			store.Get(key)

		case op < 90: // 20% queries
			seed = seed*1664525 + 1013904223
			tid := uint64(seed % 100)
			store.Query(WithTenant(tid), func(ck CompoundKey, a any) bool {
				return true
			})

		case op < 98: // 8% writes
			key := CompoundKey{
				TenantID:  uint64(seed % 100),
				UserID:    uint64(i),
				Resource:  fmt.Sprintf("resource_%d", seed%10),
				Timestamp: now + int64(i),
			}
			store.Set(key, fmt.Sprintf("value_%d", i), 0)

		default: // 2% deletes
			seed = seed*1664525 + 1013904223
			tid := uint64(seed % 100)
			store.Delete(WithTenant(tid))
		}
	}
}

// Scale benchmark: Test with different dataset sizes
func BenchmarkScaleTest_1K(b *testing.B)   { benchmarkScale(b, 1000) }
func BenchmarkScaleTest_10K(b *testing.B)  { benchmarkScale(b, 10000) }
func BenchmarkScaleTest_100K(b *testing.B) { benchmarkScale(b, 100000) }

func benchmarkScale(b *testing.B, size int) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate
	for i := range size {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	for i := 0; b.Loop(); i++ {
		tenantID := uint64(i % 100)
		store.Query(PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
			return true
		})
	}
}

// BenchmarkQueryNoIndex measures worst-case query (no usable index)
func BenchmarkQueryNoIndex(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	// Prepopulate - use resource that has many entries but not indexed by resourceHash
	// This forces iteration over all entries
	for i := range 10000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	// Query by a resource that exists but forces scanning all resources
	// This is a worst-case: the resource hash index will be used but with many results
	resource := "resource_5"
	for b.Loop() {
		store.Query(PartialKey{Resource: &resource}, func(ck CompoundKey, a any) bool {
			return true
		})
	}
}

// Benchmarks to verify concurrency safety
func BenchmarkConcurrentReadWrite(b *testing.B) {
	store := createBenchTestStore(b)
	defer store.Close()

	now := time.Now().Unix()

	// Prepopulate
	for i := range 10000 {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: now,
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				// Read
				store.Query(WithTenant(uint64(i%100)), func(ck CompoundKey, a any) bool {
					return true
				})
			} else {
				// Write
				key := CompoundKey{
					TenantID:  uint64(i % 100),
					UserID:    uint64(i),
					Resource:  "resource",
					Timestamp: now + int64(i), // Use incremental timestamp
				}
				store.Set(key, "value", 0)
			}
			i++
		}
	})
}

// ------------------ Unit tests -------------------- //

func TestBasicOperations(t *testing.T) {
	store, err := NewStoreWithOpts("", StoreOpts{
		WalEnabled:       false,
		SnapshotsEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := CompoundKey{
		TenantID:  1,
		UserID:    100,
		Resource:  "profile",
		Timestamp: 12345,
	}

	// Test Set and Get
	store.Set(key, "test_value", 0)

	val, ok := store.Get(key)
	if !ok {
		t.Fatal("Expected to find key")
	}
	if val != "test_value" {
		t.Fatalf("Expected 'test_value', got %v", val)
	}
}

func TestPartialQuery(t *testing.T) {
	store, err := NewStoreWithOpts("", StoreOpts{
		WalEnabled:       false,
		SnapshotsEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Add multiple entries
	for i := range 10 {
		key := CompoundKey{
			TenantID:  1,
			UserID:    uint64(100 + i),
			Resource:  "profile",
			Timestamp: int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	// Query by tenant
	var results QueryResult
	tenantID := uint64(1)
	store.Query(PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
		results.count++
		return true
	})
	if results.count != 10 {
		t.Fatalf("Expected 10 results, got %d", results.count)
	}

	for _, result := range results.values {
		if !strings.Contains(result.(string), "value_") {
			t.Fail()
		}
	}
}

func TestRangeQuery(t *testing.T) {
	store, err := NewStoreWithOpts("", StoreOpts{
		WalEnabled:       false,
		SnapshotsEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Add entries with sequential timestamps
	tenantID := uint64(1)
	baseTime := int64(1000)
	for i := range 20 {
		key := CompoundKey{
			TenantID:  tenantID,
			UserID:    100,
			Resource:  "event",
			Timestamp: baseTime + int64(i*10),
		}
		store.Set(key, fmt.Sprintf("event_%d", i), 0)
	}

	// Query range [1050, 1150] should return 11 entries
	var results QueryResult
	store.RangeQuery(baseTime+50, baseTime+150, PartialKey{}, func(ck CompoundKey, a any) bool {
		results.count++
		return true
	})
	if results.count != 11 {
		t.Fatalf("Expected 11 results in range, got %d", len(results.keys))
	}

	// Query range with filter (should return ALL tenants)
	var results2 QueryResult
	store.RangeQuery(baseTime, baseTime+200, PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
		results2.count++
		return true
	})
	if results2.count != 20 {
		t.Fatalf("Expected 20 results with tenant filter, got %d", results2.count)
	}
}

func TestTTL(t *testing.T) {
	store, err := NewStoreWithOpts("", StoreOpts{
		WalEnabled:       false,
		SnapshotsEnabled: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := CompoundKey{
		TenantID:  1,
		UserID:    100,
		Resource:  "session",
		Timestamp: time.Now().Unix(),
	}

	// Set with 2 second TTL
	store.Set(key, "session_data", 2*time.Second)

	// Should exist immediately
	if _, ok := store.Get(key); !ok {
		t.Fatal("Expected key to exist immediately after set")
	}

	// Wait for expiration
	time.Sleep(3 * time.Second)

	// Should be expired
	if _, ok := store.Get(key); ok {
		t.Fatal("Expected key to be expired after TTL")
	}
}

// func TestLoadSnapshot(t *testing.T) {
// 	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
// 	store, err := NewStore(tmpDir)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	defer func() {
// 		store.Close()
// 		os.RemoveAll(tmpDir)
// 	}()

// 	// Add some entries
// 	for i := range 5 {
// 		key := CompoundKey{
func TestReplayWAL(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Add some entries (will be written to WAL)
	tenantID := uint64(1)
	for i := range 3 {
		key := CompoundKey{
			TenantID:  tenantID,
			UserID:    uint64(200 + i),
			Resource:  "session",
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("session_%d", i), 0)
	}

	// Close store
	store.Close()

	// Create new store (should replay WAL)
	store2, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store2.Close()
		os.RemoveAll(tmpDir)
	}()

	// Verify data was replayed from WAL
	var results QueryResult
	store2.Query(PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
		results.count++
		return results.count < 3
	})
	if results.count != 3 {
		t.Fatalf("Expected 3 entries after replaying WAL, got %d", results.count)
	}
}

func TestEmptyWAL(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()

	// Replay WAL should succeed even if file doesn't exist
	if err := store.replayWAL(); err != nil {
		t.Fatalf("Replaying non-existent WAL should not error: %v", err)
	}
}

func TestDelete(t *testing.T) {
	store := createTestStore(t)
	defer store.Close()

	// Add entries
	tenantID := uint64(1)
	for i := range 5 {
		key := CompoundKey{
			TenantID:  1,
			UserID:    uint64(i),
			Resource:  "profile",
			Timestamp: int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}

	// Delete by tenant
	deleted := store.Delete(PartialKey{TenantID: &tenantID})
	if deleted != 5 {
		t.Fatalf("Expected to delete 5 entries, deleted %d", deleted)
	}

	// Verify deletion
	var results QueryResult
	store.Query(PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
		results.count++
		return results.count == 5
	})
	if len(results.keys) != 0 {
		t.Fatalf("Expected 0 results after deletion, got %d", len(results.keys))
	}
}

func TestConcurrency(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()

	// Run concurrent writes and reads
	done := make(chan bool)

	// TenantID
	tenantID := uint64(1)

	// Writer goroutine
	go func() {
		for i := range 1000 {
			key := CompoundKey{
				TenantID:  tenantID,
				UserID:    uint64(i),
				Resource:  "data",
				Timestamp: time.Now().Unix(),
			}
			store.Set(key, fmt.Sprintf("value_%d", i), 0)
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for range 1000 {
			store.Query(PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
				return true
			})
		}
		done <- true
	}()

	// Wait for both to complete
	<-done
	<-done

	// Verify final state
	var results QueryResult
	store.Query(PartialKey{TenantID: &tenantID}, func(ck CompoundKey, a any) bool {
		results.keys = append(results.keys, ck)
		results.values = append(results.values, a)
		results.count++
		return results.count == 1000
	})
	if len(results.keys) == 0 {
		t.Fatal("Expected entries after concurrent operations")
	}
}

func TestTimestampIndex(t *testing.T) {
	idx := NewTimestampIndex()

	// Add entries
	idx.Add(100, 1)
	idx.Add(200, 2)
	idx.Add(150, 3)
	idx.Add(100, 4) // Same timestamp

	// Range query
	var results []uint64
	idx.RangeQuery(90, 160, func(hashs []uint64) bool {
		results = append(results, hashs...)
		return true
	})
	if len(results) != 3 { // Should get hashes 1, 4, 3
		t.Fatalf("Expected 3 results, got %d", len(results))
	}

	// Remove and query again
	idx.Remove(100, 1)

	var results2 []uint64
	idx.RangeQuery(90, 160, func(hashs []uint64) bool {
		results2 = append(results2, hashs...)
		return true
	})
	if len(results2) != 2 { // Should get hashes 4, 3
		t.Fatalf("Expected 2 results after removal, got %d", len(results))
	}
}

// func TestTTLHeap(t *testing.T) {
// 	heap := NewTTLHeap()

// 	now := time.Now()

// 	// Add items with different expiration times
// 	heap.Push(1, now.Add(5*time.Second))
// 	heap.Push(2, now.Add(2*time.Second))
// 	heap.Push(3, now.Add(8*time.Second))

// 	// Peek should return earliest expiration
// 	earliest, ok := heap.Peek()
// 	if !ok {
// 		t.Fatal("Expected heap to have items")
// 	}

// 	expected := now.Add(2 * time.Second)
// 	if earliest.Unix() != expected.Unix() {
// 		t.Fatalf("Expected earliest expiration at %v, got %v", expected, earliest)
// 	}

// 	// Pop should return in order
// 	hash, expTime, ok := heap.Pop()
// 	if !ok || hash != 2 {
// 		t.Fatalf("Expected to pop hash 2, got %d", hash)
// 	}
// 	if expTime.Unix() != expected.Unix() {
// 		t.Fatalf("Expected expiration time %v, got %v", expected, expTime)
// 	}
// }

package main

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"
)

// Helper to create a test store
func createTestStore(t *testing.B) *Store {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func cleanupTestStore(store *Store, t *testing.B) {
	tmpDir := store.snapshotPath[:len(store.snapshotPath)-len("/snapshot.pb")]
	store.Close()
	os.RemoveAll(tmpDir)
}

// BenchmarkSet measures write performance without TTL
func BenchmarkSet(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	for i := 0; b.Loop(); i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
}

// BenchmarkSetWithTTL measures write performance with TTL
func BenchmarkSetWithTTL(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 60*time.Second)
	}
}

// BenchmarkSetParallel measures concurrent write performance
func BenchmarkSetParallel(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := CompoundKey{
				TenantID:  uint64(i % 100),
				UserID:    uint64(i),
				Resource:  fmt.Sprintf("resource_%d", i%10),
				Timestamp: time.Now().Unix(),
			}
			store.Set(key, fmt.Sprintf("value_%d", i), 0)
			i++
		}
	})
}

// BenchmarkGet measures exact key lookup performance
func BenchmarkGet(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	keys := make([]CompoundKey, 10000)
	for i := 0; i < 10000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		keys[i] = key
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := keys[i%10000]
		store.Get(key)
	}
}

// BenchmarkGetParallel measures concurrent read performance
func BenchmarkGetParallel(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	keys := make([]CompoundKey, 10000)
	for i := 0; i < 10000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		keys[i] = key
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
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate with 100k entries across 100 tenants
	for i := 0; i < 100000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each tenant has ~1000 entries (1% of total)
		store.Query(WithTenant(uint64(i % 100)))
	}
}

// BenchmarkQueryByUser measures partial query with different selectivity
func BenchmarkQueryByUser(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < 100000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i % 10000),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each user has ~10 entries (0.01% of total)
		store.Query(WithUser(uint64(i % 10000)))
	}
}

// BenchmarkQueryByResource measures query on less selective index
func BenchmarkQueryByResource(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < 100000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Each resource has ~10000 entries (10% of total)
		store.Query(WithResource(fmt.Sprintf("resource_%d", i%10)))
	}
}

// BenchmarkQueryTenantAndUser measures compound partial query
func BenchmarkQueryTenantAndUser(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < 100000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i % 10000),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Very selective - typically 1-10 entries
		store.Query(WithTenantAndUser(uint64(i%100), uint64(i%10000)))
	}
}

// BenchmarkRangeQuery measures timestamp range query performance
func BenchmarkRangeQuery(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate with timestamps spread over time
	baseTime := time.Now().Unix()
	for i := 0; i < 100000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: baseTime + int64(i), // Sequential timestamps
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query 1000 entries (1% of data)
		start := baseTime + int64(i%99000)
		end := start + 1000
		store.RangeQuery(start, end, PartialKey{})
	}
}

// BenchmarkRangeQueryWithFilter measures range query with partial key filter
func BenchmarkRangeQueryWithFilter(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	baseTime := time.Now().Unix()
	for i := 0; i < 100000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: baseTime + int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query 1000 entries for specific tenant (~10 results)
		start := baseTime + int64(i%99000)
		end := start + 1000
		store.RangeQuery(start, end, WithTenant(uint64(i%100)))
	}
}

// BenchmarkDelete measures deletion performance
func BenchmarkDelete(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Repopulate for each iteration
		for j := 0; j < 1000; j++ {
			key := CompoundKey{
				TenantID:  uint64(i % 10),
				UserID:    uint64(j),
				Resource:  "resource",
				Timestamp: time.Now().Unix(),
			}
			store.Set(key, "value", 0)
		}
		b.StartTimer()
		
		// Delete by tenant (~100 entries)
		store.Delete(WithTenant(uint64(i % 10)))
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
	for i := 0; i < b.N; i++ {
		key.Hash()
	}
}

// BenchmarkKeyToBytes measures serialization performance
func BenchmarkKeyToBytes(b *testing.B) {
	key := CompoundKey{
		TenantID:  12345,
		UserID:    67890,
		Resource:  "orders",
		Timestamp: time.Now().Unix(),
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key.ToBytes()
	}
}

// Comparative benchmark: Simulate Redis-style string key operations
func BenchmarkStringKeySet(b *testing.B) {
	store := make(map[string]interface{})
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("tenant:%d:user:%d:resource:%s:ts:%d",
			i%100, i, fmt.Sprintf("resource_%d", i%10), time.Now().Unix())
		store[key] = fmt.Sprintf("value_%d", i)
	}
}

// Comparative benchmark: Simulate Redis SCAN for partial matches
func BenchmarkStringKeyScan(b *testing.B) {
	store := make(map[string]interface{})
	
	// Prepopulate
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("tenant:%d:user:%d:resource:%s",
			i%100, i, fmt.Sprintf("resource_%d", i%10))
		store[key] = fmt.Sprintf("value_%d", i)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate SCAN with pattern matching
		pattern := fmt.Sprintf("tenant:%d:", i%100)
		results := make([]interface{}, 0)
		for k, v := range store {
			if len(k) >= len(pattern) && k[:len(pattern)] == pattern {
				results = append(results, v)
			}
		}
	}
}

// BenchmarkTTLCleanup measures TTL cleanup overhead
func BenchmarkTTLCleanup(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate with entries that expire quickly
	for i := 0; i < 10000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  "resource",
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, "value", 100*time.Millisecond)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		time.Sleep(150 * time.Millisecond)
		// Force cleanup
		store.cleanupExpired()
		
		// Repopulate
		for j := 0; j < 1000; j++ {
			key := CompoundKey{
				TenantID:  uint64(j % 100),
				UserID:    uint64(j),
				Resource:  "resource",
				Timestamp: time.Now().Unix(),
			}
			store.Set(key, "value", 100*time.Millisecond)
		}
	}
}

// Benchmark for mixed workload (realistic usage)
func BenchmarkMixedWorkload(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < 10000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		op := rand.Intn(100)
		
		switch {
		case op < 70: // 70% reads
			key := CompoundKey{
				TenantID:  uint64(rand.Intn(100)),
				UserID:    uint64(rand.Intn(10000)),
				Resource:  fmt.Sprintf("resource_%d", rand.Intn(10)),
				Timestamp: int64(rand.Intn(10000)),
			}
			store.Get(key)
			
		case op < 90: // 20% queries
			store.Query(WithTenant(uint64(rand.Intn(100))))
			
		case op < 98: // 8% writes
			key := CompoundKey{
				TenantID:  uint64(rand.Intn(100)),
				UserID:    uint64(i),
				Resource:  fmt.Sprintf("resource_%d", rand.Intn(10)),
				Timestamp: time.Now().Unix(),
			}
			store.Set(key, fmt.Sprintf("value_%d", i), 0)
			
		default: // 2% deletes
			store.Delete(WithTenant(uint64(rand.Intn(100))))
		}
	}
}

// Scale benchmark: Test with different dataset sizes
func BenchmarkScaleTest_1K(b *testing.B)   { benchmarkScale(b, 1000) }
func BenchmarkScaleTest_10K(b *testing.B)  { benchmarkScale(b, 10000) }
func BenchmarkScaleTest_100K(b *testing.B) { benchmarkScale(b, 100000) }

func benchmarkScale(b *testing.B, size int) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < size; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query returns ~1% of dataset
		store.Query(WithTenant(uint64(i % 100)))
	}
}

// Benchmark for worst-case query (no index available)
func BenchmarkQueryNoIndex(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < 10000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	timestamp := int64(5000)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query by timestamp only (not indexed in partial key) - requires filtering
		partial := PartialKey{Timestamp: &timestamp}
		store.Query(partial)
	}
}

// Benchmarks to verify concurrency safety
func BenchmarkConcurrentReadWrite(b *testing.B) {
	store := createTestStore(b)
	defer cleanupTestStore(store, b)
	
	// Prepopulate
	for i := 0; i < 10000; i++ {
		key := CompoundKey{
			TenantID:  uint64(i % 100),
			UserID:    uint64(i),
			Resource:  fmt.Sprintf("resource_%d", i%10),
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				// Read
				store.Query(WithTenant(uint64(i % 100)))
			} else {
				// Write
				key := CompoundKey{
					TenantID:  uint64(i % 100),
					UserID:    uint64(i),
					Resource:  "resource",
					Timestamp: time.Now().Unix(),
				}
				store.Set(key, "value", 0)
			}
			i++
		}
	})
}

// Unit tests

func TestBasicOperations(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()
	
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
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()
	
	// Add multiple entries
	for i := 0; i < 10; i++ {
		key := CompoundKey{
			TenantID:  1,
			UserID:    uint64(100 + i),
			Resource:  "profile",
			Timestamp: int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	// Query by tenant
	results := store.Query(WithTenant(1))
	if len(results) != 10 {
		t.Fatalf("Expected 10 results, got %d", len(results))
	}
}

func TestRangeQuery(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()
	
	// Add entries with sequential timestamps
	baseTime := int64(1000)
	for i := 0; i < 20; i++ {
		key := CompoundKey{
			TenantID:  1,
			UserID:    100,
			Resource:  "event",
			Timestamp: baseTime + int64(i*10),
		}
		store.Set(key, fmt.Sprintf("event_%d", i), 0)
	}
	
	// Query range [1050, 1150] should return 11 entries
	results := store.RangeQuery(baseTime+50, baseTime+150, PartialKey{})
	if len(results) != 11 {
		t.Fatalf("Expected 11 results in range, got %d", len(results))
	}
	
	// Query range with filter
	results = store.RangeQuery(baseTime, baseTime+200, WithTenant(1))
	if len(results) != 20 {
		t.Fatalf("Expected 20 results with tenant filter, got %d", len(results))
	}
}

func TestTTL(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()
	
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

func TestDelete(t *testing.T) {
	tmpDir := fmt.Sprintf("./test_data_%d", time.Now().UnixNano())
	store, err := NewStore(tmpDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		store.Close()
		os.RemoveAll(tmpDir)
	}()
	
	// Add entries
	for i := 0; i < 5; i++ {
		key := CompoundKey{
			TenantID:  1,
			UserID:    uint64(i),
			Resource:  "profile",
			Timestamp: int64(i),
		}
		store.Set(key, fmt.Sprintf("value_%d", i), 0)
	}
	
	// Delete by tenant
	deleted := store.Delete(WithTenant(1))
	if deleted != 5 {
		t.Fatalf("Expected to delete 5 entries, deleted %d", deleted)
	}
	
	// Verify deletion
	results := store.Query(WithTenant(1))
	if len(results) != 0 {
		t.Fatalf("Expected 0 results after deletion, got %d", len(results))
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
	
	// Writer goroutine
	go func() {
		for i := 0; i < 1000; i++ {
			key := CompoundKey{
				TenantID:  1,
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
		for i := 0; i < 1000; i++ {
			store.Query(WithTenant(1))
		}
		done <- true
	}()
	
	// Wait for both to complete
	<-done
	<-done
	
	// Verify final state
	results := store.Query(WithTenant(1))
	if len(results) == 0 {
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
	results := idx.RangeQuery(90, 160)
	if len(results) != 3 { // Should get hashes 1, 4, 3
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	
	// Remove and query again
	idx.Remove(100, 1)
	results = idx.RangeQuery(90, 160)
	if len(results) != 2 { // Should get hashes 4, 3
		t.Fatalf("Expected 2 results after removal, got %d", len(results))
	}
}

func TestTTLHeap(t *testing.T) {
	heap := NewTTLHeap()
	
	now := time.Now()
	
	// Add items with different expiration times
	heap.Push(1, now.Add(5*time.Second))
	heap.Push(2, now.Add(2*time.Second))
	heap.Push(3, now.Add(8*time.Second))
	
	// Peek should return earliest expiration
	earliest, ok := heap.Peek()
	if !ok {
		t.Fatal("Expected heap to have items")
	}
	
	expected := now.Add(2 * time.Second)
	if earliest.Unix() != expected.Unix() {
		t.Fatalf("Expected earliest expiration at %v, got %v", expected, earliest)
	}
	
	// Pop should return in order
	hash, expTime, ok := heap.Pop()
	if !ok || hash != 2 {
		t.Fatalf("Expected to pop hash 2, got %d", hash)
	}
	if expTime.Unix() != expected.Unix() {
		t.Fatalf("Expected expiration time %v, got %v", expected, expTime)
	}
}
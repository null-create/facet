package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// CompoundKey represents a multi-dimensional cache key
type CompoundKey struct {
	TenantID   uint64
	UserID     uint64
	Resource   string
	Timestamp  int64
	KeyBytes   string // Cached serialization
	serialized bool
}

// ToBytes serializes the key for hashing
func (k *CompoundKey) ToBytes() []byte {
	if !k.serialized {
		buf := make([]byte, 16+len(k.Resource)+8)
		binary.BigEndian.PutUint64(buf[0:8], k.TenantID)
		binary.BigEndian.PutUint64(buf[8:16], k.UserID)
		copy(buf[16:], []byte(k.Resource))
		binary.BigEndian.PutUint64(buf[16+len(k.Resource):], uint64(k.Timestamp))
		k.KeyBytes = string(buf)
		k.serialized = true
		return buf
	}
	return []byte(k.KeyBytes)
}

// Hash returns a hash of the complete key
func (k CompoundKey) Hash() uint64 {
	h := fnv.New64a()
	h.Write(k.ToBytes())
	return h.Sum64()
}

// PartialKey represents a query pattern where some fields may be wildcards
type PartialKey struct {
	TenantID  *uint64
	UserID    *uint64
	Resource  *string
	Timestamp *int64
}

// Matches returns true if all non-nil fields in p equal k.
func (p PartialKey) Matches(k CompoundKey) bool {
	if p.TenantID != nil && *p.TenantID != k.TenantID {
		return false
	}
	if p.UserID != nil && *p.UserID != k.UserID {
		return false
	}
	if p.Resource != nil && *p.Resource != k.Resource {
		return false
	}
	if p.Timestamp != nil && *p.Timestamp != k.Timestamp {
		return false
	}
	return true
}

// Store is a concurrent key-value store with partial key query support
type Store struct {
	mu   sync.RWMutex
	data map[uint64]*entry // main data store (keyed by full key hash)

	// Secondary indexes for efficient partial queries
	tenantIndex   map[uint64]map[uint64]bool // tenant -> set of key hashes
	userIndex     map[uint64]map[uint64]bool // user -> set of key hashes
	resourceIndex map[string]map[uint64]bool // resource -> set of key hashes

	// Reverse mapping from hash to full key for lookups
	keyMap map[uint64]CompoundKey

	// B-tree for range queries on timestamp
	timestampIndex *TimestampIndex

	// Persistence
	walFile         *os.File
	walPath         string
	snapshotPath    string
	walOffset       uint64
	walEnabled      bool
	snapShotEnabled bool

	// TTL management
	ttlHeap       *TTLHeap
	cleanupTicker *time.Ticker
	stopCleanup   chan bool
}

type entry struct {
	key        CompoundKey
	value      any
	insertTime time.Time
	ttl        time.Duration // 0 means no expiration
}

// TimestampIndex is a simple B-tree-like structure for range queries
type TimestampIndex struct {
	nodes []*TimestampNode
}

type TimestampNode struct {
	timestamp int64
	keyHashes []uint64
}

func NewTimestampIndex() *TimestampIndex {
	return &TimestampIndex{
		nodes: make([]*TimestampNode, 0),
	}
}

func (idx *TimestampIndex) Add(timestamp int64, keyHash uint64) {
	// Try to append if possible, otherwise search for best insertion point
	n := len(idx.nodes)
	if n == 0 || idx.nodes[n-1].timestamp <= timestamp {
		// fast path: append to end (most writes are monotonic)
		if n > 0 && idx.nodes[n-1].timestamp == timestamp {
			idx.nodes[n-1].keyHashes = append(idx.nodes[n-1].keyHashes, keyHash)
			return
		}
		idx.nodes = append(idx.nodes, &TimestampNode{
			timestamp: timestamp,
			keyHashes: []uint64{keyHash},
		})
		return
	}

	// Binary search for insertion point
	i := sort.Search(len(idx.nodes), func(i int) bool {
		return idx.nodes[i].timestamp >= timestamp
	})

	if i < len(idx.nodes) && idx.nodes[i].timestamp == timestamp {
		// Timestamp exists, add to this node
		idx.nodes[i].keyHashes = append(idx.nodes[i].keyHashes, keyHash)
	} else {
		// Create new node
		node := &TimestampNode{
			timestamp: timestamp,
			keyHashes: []uint64{keyHash},
		}
		// Insert at position i
		idx.nodes = append(idx.nodes, nil)
		copy(idx.nodes[i+1:], idx.nodes[i:])
		idx.nodes[i] = node
	}
}

func (idx *TimestampIndex) Remove(timestamp int64, keyHash uint64) {
	i := sort.Search(len(idx.nodes), func(i int) bool {
		return idx.nodes[i].timestamp >= timestamp
	})

	if i < len(idx.nodes) && idx.nodes[i].timestamp == timestamp {
		node := idx.nodes[i]
		for j, h := range node.keyHashes {
			if h == keyHash {
				node.keyHashes = append(node.keyHashes[:j], node.keyHashes[j+1:]...)
				break
			}
		}
		// Remove node if empty
		if len(node.keyHashes) == 0 {
			idx.nodes = append(idx.nodes[:i], idx.nodes[i+1:]...)
		}
	}
}

func (idx *TimestampIndex) RangeQuery(start, end int64) []uint64 {
	results := make([]uint64, 0)

	// Find start position
	startIdx := sort.Search(len(idx.nodes), func(i int) bool {
		return idx.nodes[i].timestamp >= start
	})

	// Collect all hashes in range
	for i := startIdx; i < len(idx.nodes) && idx.nodes[i].timestamp <= end; i++ {
		results = append(results, idx.nodes[i].keyHashes...)
	}

	return results
}

// TTL Heap for efficient expiration
type TTLHeap struct {
	items []TTLItem
}

type TTLItem struct {
	keyHash    uint64
	expireTime time.Time
}

func NewTTLHeap() *TTLHeap {
	return &TTLHeap{
		items: make([]TTLItem, 0),
	}
}

func (h *TTLHeap) Push(keyHash uint64, expireTime time.Time) {
	h.items = append(h.items, TTLItem{keyHash, expireTime})
	h.bubbleUp(len(h.items) - 1)
}

func (h *TTLHeap) Pop() (uint64, time.Time, bool) {
	if len(h.items) == 0 {
		return 0, time.Time{}, false
	}

	item := h.items[0]
	lastIdx := len(h.items) - 1
	h.items[0] = h.items[lastIdx]
	h.items = h.items[:lastIdx]

	if len(h.items) > 0 {
		h.bubbleDown(0)
	}

	return item.keyHash, item.expireTime, true
}

func (h *TTLHeap) Peek() (time.Time, bool) {
	if len(h.items) == 0 {
		return time.Time{}, false
	}
	return h.items[0].expireTime, true
}

func (h *TTLHeap) bubbleUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if h.items[idx].expireTime.Before(h.items[parent].expireTime) {
			h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
			idx = parent
		} else {
			break
		}
	}
}

func (h *TTLHeap) bubbleDown(idx int) {
	for {
		smallest := idx
		left := 2*idx + 1
		right := 2*idx + 2

		if left < len(h.items) && h.items[left].expireTime.Before(h.items[smallest].expireTime) {
			smallest = left
		}
		if right < len(h.items) && h.items[right].expireTime.Before(h.items[smallest].expireTime) {
			smallest = right
		}

		if smallest != idx {
			h.items[idx], h.items[smallest] = h.items[smallest], h.items[idx]
			idx = smallest
		} else {
			break
		}
	}
}

// NewStore creates a new structured key-value store
// with Write Ahead Logging and snapshots
func NewStore(dataDir string) (*Store, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	store := &Store{
		data:            make(map[uint64]*entry),
		tenantIndex:     make(map[uint64]map[uint64]bool),
		userIndex:       make(map[uint64]map[uint64]bool),
		resourceIndex:   make(map[string]map[uint64]bool),
		keyMap:          make(map[uint64]CompoundKey),
		timestampIndex:  NewTimestampIndex(),
		ttlHeap:         NewTTLHeap(),
		walPath:         filepath.Join(dataDir, "wal.log"),
		snapShotEnabled: true,
		snapshotPath:    filepath.Join(dataDir, "snapshot.pb"),
		stopCleanup:     make(chan bool),
		walEnabled:      true,
	}

	// Load from disk
	if err := store.loadFromDisk(); err != nil {
		return nil, fmt.Errorf("failed to load from disk: %w", err)
	}

	// Open WAL for appending
	walFile, err := os.OpenFile(store.walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	store.walFile = walFile

	// Start TTL cleanup goroutine
	store.startTTLCleanup()

	return store, nil
}

// Optional configurations for the Store
type StoreOpts struct {
	WalEnabled       bool
	WalPath          string
	SnapshotsEnabled bool
	SnapshotPath     string
}

// Create a store with or without WAL or snapshots
func NewStoreWithOpts(dataDir string, opts StoreOpts) (*Store, error) {
	if opts.WalEnabled || opts.SnapshotsEnabled {
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create data directory: %w", err)
		}
	}

	store := &Store{
		data:            make(map[uint64]*entry),
		tenantIndex:     make(map[uint64]map[uint64]bool),
		userIndex:       make(map[uint64]map[uint64]bool),
		resourceIndex:   make(map[string]map[uint64]bool),
		keyMap:          make(map[uint64]CompoundKey),
		timestampIndex:  NewTimestampIndex(),
		ttlHeap:         NewTTLHeap(),
		walEnabled:      opts.WalEnabled,
		walPath:         opts.WalPath,
		snapShotEnabled: opts.SnapshotsEnabled,
		snapshotPath:    opts.SnapshotPath,
		stopCleanup:     make(chan bool),
	}

	// Load from disk if needed
	if store.snapShotEnabled {
		if err := store.loadFromDisk(); err != nil {
			return nil, fmt.Errorf("failed to load from disk: %w", err)
		}
	}

	// Open WAL for appending if needed
	if store.walEnabled {
		walFile, err := os.OpenFile(store.walPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open WAL: %w", err)
		}
		store.walFile = walFile
	}

	// Start TTL cleanup goroutine
	store.startTTLCleanup()

	return store, nil
}

func (s *Store) loadFromDisk() error {
	// Load snapshot if exists
	if s.snapShotEnabled {
		if _, err := os.Stat(s.snapshotPath); err == nil {
			if err := s.loadSnapshot(); err != nil {
				return fmt.Errorf("failed to load snapshot: %w", err)
			}
		}
	}

	// Replay WAL if enabled and exists
	if s.walEnabled {
		if _, err := os.Stat(s.walPath); err == nil {
			if err := s.replayWAL(); err != nil {
				return fmt.Errorf("failed to replay WAL: %w", err)
			}
		}
	}

	return nil
}

func (s *Store) loadSnapshot() error {
	data, err := os.ReadFile(s.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No snapshot file yet
		}
		return fmt.Errorf("failed to read snapshot: %w", err)
	}

	if len(data) == 0 {
		return nil // Empty snapshot
	}

	// Read the data as length-prefixed entries
	buf := bytes.NewReader(data)

	for buf.Len() > 0 {
		// Read entry count (first 4 bytes if this is first read)
		var entryCount uint32
		if buf.Len() == len(data) {
			if err := binary.Read(buf, binary.BigEndian, &entryCount); err != nil {
				return fmt.Errorf("failed to read entry count: %w", err)
			}
		}

		// Read each entry
		for buf.Len() > 0 {
			// Read tenant ID
			var tenantID uint64
			if err := binary.Read(buf, binary.BigEndian, &tenantID); err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("failed to read tenant ID: %w", err)
			}

			// Read user ID
			var userID uint64
			if err := binary.Read(buf, binary.BigEndian, &userID); err != nil {
				return fmt.Errorf("failed to read user ID: %w", err)
			}

			// Read resource (length-prefixed string)
			var resourceLen uint32
			if err := binary.Read(buf, binary.BigEndian, &resourceLen); err != nil {
				return fmt.Errorf("failed to read resource length: %w", err)
			}
			resourceBytes := make([]byte, resourceLen)
			if _, err := io.ReadFull(buf, resourceBytes); err != nil {
				return fmt.Errorf("failed to read resource: %w", err)
			}

			// Read timestamp
			var timestamp int64
			if err := binary.Read(buf, binary.BigEndian, &timestamp); err != nil {
				return fmt.Errorf("failed to read timestamp: %w", err)
			}

			// Read value (length-prefixed)
			var valueLen uint32
			if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
				return fmt.Errorf("failed to read value length: %w", err)
			}
			valueBytes := make([]byte, valueLen)
			if _, err := io.ReadFull(buf, valueBytes); err != nil {
				return fmt.Errorf("failed to read value: %w", err)
			}

			// Read insert time
			var insertTime int64
			if err := binary.Read(buf, binary.BigEndian, &insertTime); err != nil {
				return fmt.Errorf("failed to read insert time: %w", err)
			}

			// Read TTL
			var ttlSeconds int64
			if err := binary.Read(buf, binary.BigEndian, &ttlSeconds); err != nil {
				return fmt.Errorf("failed to read TTL: %w", err)
			}

			// Reconstruct key and entry
			key := CompoundKey{
				TenantID:  tenantID,
				UserID:    userID,
				Resource:  string(resourceBytes),
				Timestamp: timestamp,
			}

			entry := &entry{
				key:        key,
				value:      string(valueBytes),
				insertTime: time.Unix(insertTime, 0),
				ttl:        time.Duration(ttlSeconds) * time.Second,
			}

			// Skip if expired
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}

			// Add to store
			hash := key.Hash()
			s.data[hash] = entry
			s.keyMap[hash] = key

			// Rebuild indexes
			if s.tenantIndex[key.TenantID] == nil {
				s.tenantIndex[key.TenantID] = make(map[uint64]bool)
			}
			s.tenantIndex[key.TenantID][hash] = true

			if s.userIndex[key.UserID] == nil {
				s.userIndex[key.UserID] = make(map[uint64]bool)
			}
			s.userIndex[key.UserID][hash] = true

			if s.resourceIndex[key.Resource] == nil {
				s.resourceIndex[key.Resource] = make(map[uint64]bool)
			}
			s.resourceIndex[key.Resource][hash] = true

			s.timestampIndex.Add(key.Timestamp, hash)

			if entry.ttl > 0 {
				s.ttlHeap.Push(hash, entry.insertTime.Add(entry.ttl))
			}
		}

		break
	}

	return nil
}

func (s *Store) replayWAL() error {
	if !s.walEnabled {
		return nil
	}

	file, err := os.Open(s.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No WAL file yet
		}
		return fmt.Errorf("failed to open WAL: %w", err)
	}
	defer file.Close()

	for {
		// Read length prefix
		var length uint32
		if err := binary.Read(file, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				break // End of file
			}
			return fmt.Errorf("failed to read WAL entry length: %w", err)
		}

		// Read entry data
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return fmt.Errorf("failed to read WAL entry data: %w", err)
		}

		buf := bytes.NewReader(data)

		// Read operation type
		var opType uint32
		if err := binary.Read(buf, binary.BigEndian, &opType); err != nil {
			return fmt.Errorf("failed to read operation type: %w", err)
		}

		// Read key fields
		var tenantID, userID uint64
		if err := binary.Read(buf, binary.BigEndian, &tenantID); err != nil {
			return fmt.Errorf("failed to read tenant ID: %w", err)
		}
		if err := binary.Read(buf, binary.BigEndian, &userID); err != nil {
			return fmt.Errorf("failed to read user ID: %w", err)
		}

		// Read resource (length-prefixed)
		var resourceLen uint32
		if err := binary.Read(buf, binary.BigEndian, &resourceLen); err != nil {
			return fmt.Errorf("failed to read resource length: %w", err)
		}
		resourceBytes := make([]byte, resourceLen)
		if _, err := io.ReadFull(buf, resourceBytes); err != nil {
			return fmt.Errorf("failed to read resource: %w", err)
		}

		var timestamp int64
		if err := binary.Read(buf, binary.BigEndian, &timestamp); err != nil {
			return fmt.Errorf("failed to read timestamp: %w", err)
		}

		key := CompoundKey{
			TenantID:  tenantID,
			UserID:    userID,
			Resource:  string(resourceBytes),
			Timestamp: timestamp,
		}

		// Apply operation based on type
		switch opType {
		case 0: // SET
			// Read value (length-prefixed)
			var valueLen uint32
			if err := binary.Read(buf, binary.BigEndian, &valueLen); err != nil {
				return fmt.Errorf("failed to read value length: %w", err)
			}
			valueBytes := make([]byte, valueLen)
			if _, err := io.ReadFull(buf, valueBytes); err != nil {
				return fmt.Errorf("failed to read value: %w", err)
			}

			// Read TTL
			var ttlSeconds int64
			if err := binary.Read(buf, binary.BigEndian, &ttlSeconds); err != nil {
				return fmt.Errorf("failed to read TTL: %w", err)
			}

			// Read operation timestamp (we can ignore this for replay)
			var opTimestamp int64
			if err := binary.Read(buf, binary.BigEndian, &opTimestamp); err != nil {
				return fmt.Errorf("failed to read operation timestamp: %w", err)
			}

			// Apply the SET operation
			ttl := time.Duration(ttlSeconds) * time.Second
			s.Set(key, string(valueBytes), ttl)

		case 1: // DELETE
			// For DELETE operations, we stored the partial key pattern
			// For simplicity, we'll delete by tenant (most common case)
			partial := WithTenant(tenantID)
			s.Delete(partial)

		default:
			return fmt.Errorf("unknown operation type: %d", opType)
		}

		s.walOffset++
	}

	return nil
}

func (s *Store) startTTLCleanup() {
	s.cleanupTicker = time.NewTicker(1 * time.Second)

	go func() {
		for {
			select {
			case <-s.cleanupTicker.C:
				s.cleanupExpired()
			case <-s.stopCleanup:
				return
			}
		}
	}()
}

func (s *Store) cleanupExpired() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()

	for {
		expireTime, ok := s.ttlHeap.Peek()
		if !ok || expireTime.After(now) {
			break
		}

		keyHash, _, _ := s.ttlHeap.Pop()

		if entry, exists := s.data[keyHash]; exists {
			// Double-check it's actually expired
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				s.deleteInternal(keyHash)
			}
		}
	}
}

// Set stores a value with the given compound key and optional TTL
func (s *Store) Set(key CompoundKey, value any, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hash := key.Hash()

	// Store the entry
	e := &entry{
		key:        key,
		value:      value,
		insertTime: time.Now(),
		ttl:        ttl,
	}
	s.data[hash] = e

	// Update reverse mapping
	s.keyMap[hash] = key

	// Update indexes
	if s.tenantIndex[key.TenantID] == nil {
		s.tenantIndex[key.TenantID] = make(map[uint64]bool)
	}
	s.tenantIndex[key.TenantID][hash] = true

	if s.userIndex[key.UserID] == nil {
		s.userIndex[key.UserID] = make(map[uint64]bool)
	}
	s.userIndex[key.UserID][hash] = true

	if s.resourceIndex[key.Resource] == nil {
		s.resourceIndex[key.Resource] = make(map[uint64]bool)
	}
	s.resourceIndex[key.Resource][hash] = true

	// Update timestamp index
	s.timestampIndex.Add(key.Timestamp, hash)

	// Add to TTL heap if has expiration
	if ttl > 0 {
		s.ttlHeap.Push(hash, e.insertTime.Add(ttl))
	}

	// Write to WAL
	s.writeWAL("SET", key, value, ttl)
}

func (s *Store) writeWAL(op string, key CompoundKey, value interface{}, ttl time.Duration) {
	if s.walFile == nil || !s.walEnabled {
		return // WAL not enabled
	}

	// Determine operation type
	var opType int32
	switch op {
	case "SET":
		opType = 0 // OperationType_SET
	case "DELETE":
		opType = 1 // OperationType_DELETE
	default:
		return
	}

	// Serialize value to bytes
	valueBytes := fmt.Appendf(nil, "%v", value)

	// Create WAL entry (simplified without protobuf dependency)
	// In production, this would use the generated protobuf structs
	var buf bytes.Buffer

	// Write operation type (4 bytes)
	binary.Write(&buf, binary.BigEndian, uint32(opType))

	// Write key fields
	binary.Write(&buf, binary.BigEndian, key.TenantID)
	binary.Write(&buf, binary.BigEndian, key.UserID)

	// Write resource (length-prefixed string)
	resourceBytes := []byte(key.Resource)
	binary.Write(&buf, binary.BigEndian, uint32(len(resourceBytes)))
	buf.Write(resourceBytes)

	binary.Write(&buf, binary.BigEndian, key.Timestamp)

	// Write value (length-prefixed)
	binary.Write(&buf, binary.BigEndian, uint32(len(valueBytes)))
	buf.Write(valueBytes)

	// Write TTL
	binary.Write(&buf, binary.BigEndian, int64(ttl.Seconds()))

	// Write timestamp
	binary.Write(&buf, binary.BigEndian, time.Now().Unix())

	data := buf.Bytes()

	// Write length prefix (4 bytes) + data
	lenBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBuf, uint32(len(data)))

	s.walFile.Write(lenBuf)
	s.walFile.Write(data)
	s.walFile.Sync()

	s.walOffset++
}

// Get retrieves a value by exact compound key
func (s *Store) Get(key CompoundKey) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	hash := key.Hash()
	if entry, ok := s.data[hash]; ok {
		// Check if expired
		if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
			return nil, false
		}
		return entry.value, true
	}
	return nil, false
}

// QueryResult holds query results without copying all data
type QueryResult struct {
	keys   []CompoundKey
	values []any
	count  int
}

// NewQueryResult creates a result with estimated capacity
func NewQueryResult(estimatedSize int) *QueryResult {
	return &QueryResult{
		keys:   make([]CompoundKey, 0, estimatedSize),
		values: make([]any, 0, estimatedSize),
	}
}

// Len returns the number of results
func (qr *QueryResult) Len() int {
	return qr.count
}

// Get returns the key-value pair at index i
func (qr *QueryResult) Get(i int) (CompoundKey, any) {
	if i < 0 || i >= qr.count {
		return CompoundKey{}, nil
	}
	return qr.keys[i], qr.values[i]
}

// Iterate calls fn for each result
func (qr *QueryResult) Iterate(fn func(CompoundKey, any) bool) {
	for i := 0; i < qr.count; i++ {
		if !fn(qr.keys[i], qr.values[i]) {
			break
		}
	}
}

// ToMap converts results to a map (only use if needed for compatibility)
func (qr *QueryResult) ToMap() map[CompoundKey]any {
	results := make(map[CompoundKey]any, qr.count)
	for i := 0; i < qr.count; i++ {
		results[qr.keys[i]] = qr.values[i]
	}
	return results
}

// Query finds all entries matching a partial key pattern
// Returns QueryResult which is much more memory efficient than map
func (s *Store) Query(partial PartialKey) *QueryResult {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Find candidate set using most selective index
	var candidates map[uint64]bool
	var estimatedSize int

	// Choose the most selective index available
	if partial.TenantID != nil {
		candidates = s.tenantIndex[*partial.TenantID]
		estimatedSize = len(candidates)
	} else if partial.UserID != nil {
		candidates = s.userIndex[*partial.UserID]
		estimatedSize = len(candidates)
	} else if partial.Resource != nil {
		candidates = s.resourceIndex[*partial.Resource]
		estimatedSize = len(candidates)
	} else {
		// No index available, scan all keys
		candidates = make(map[uint64]bool)
		for hash := range s.data {
			candidates[hash] = true
		}
		estimatedSize = len(s.data)
	}

	// Pre-allocate with estimated size to avoid reallocations
	result := NewQueryResult(estimatedSize)

	// Filter candidates by remaining criteria
	for hash := range candidates {
		if entry, ok := s.data[hash]; ok {
			// Skip expired entries
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}

			if partial.Matches(entry.key) {
				result.keys = append(result.keys, entry.key)
				result.values = append(result.values, entry.value)
				result.count++
			}
		}
	}

	return result
}

// QueryStream calls fn for each match, returns false to stop early
func (s *Store) QueryStream(partial PartialKey, fn func(CompoundKey, any) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var candidates map[uint64]bool
	if partial.TenantID != nil {
		candidates = s.tenantIndex[*partial.TenantID]
	} else if partial.UserID != nil {
		candidates = s.userIndex[*partial.UserID]
	} else if partial.Resource != nil {
		candidates = s.resourceIndex[*partial.Resource]
	} else {
		candidates = make(map[uint64]bool, len(s.data))
		for h := range s.data {
			candidates[h] = true
		}
	}

	for hash := range candidates {
		if entry, ok := s.data[hash]; ok {
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}
			if partial.Matches(entry.key) {
				if !fn(entry.key, entry.value) {
					return
				}
			}
		}
	}
}

// RangeQuery finds all entries with timestamps in the given range
func (s *Store) RangeQuery(startTime, endTime int64, partial PartialKey) map[CompoundKey]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Get candidates from timestamp index
	candidateHashes := s.timestampIndex.RangeQuery(startTime, endTime)

	results := make(map[CompoundKey]any)
	for _, hash := range candidateHashes {
		if entry, ok := s.data[hash]; ok {
			// Skip expired entries
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}

			// Apply additional filters from partial key
			if partial.Matches(entry.key) {
				results[entry.key] = entry.value
			}
		}
	}

	return results
}

func (s *Store) deleteInternal(hash uint64) {
	entry := s.data[hash]
	if entry == nil {
		return
	}

	key := entry.key

	// Remove from main store
	delete(s.data, hash)
	delete(s.keyMap, hash)

	// Remove from indexes
	delete(s.tenantIndex[key.TenantID], hash)
	delete(s.userIndex[key.UserID], hash)
	delete(s.resourceIndex[key.Resource], hash)

	// Remove from timestamp index
	s.timestampIndex.Remove(key.Timestamp, hash)
}

// Delete removes entries matching a partial key pattern
func (s *Store) Delete(partial PartialKey) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find all matching keys
	var candidates map[uint64]bool
	if partial.TenantID != nil {
		candidates = s.tenantIndex[*partial.TenantID]
	} else if partial.UserID != nil {
		candidates = s.userIndex[*partial.UserID]
	} else if partial.Resource != nil {
		candidates = s.resourceIndex[*partial.Resource]
	} else {
		candidates = make(map[uint64]bool)
		for hash := range s.data {
			candidates[hash] = true
		}
	}

	// Collect hashes to delete
	var toDelete []uint64
	for hash := range candidates {
		if entry, ok := s.data[hash]; ok {
			if partial.Matches(entry.key) {
				toDelete = append(toDelete, hash)
			}
		}
	}

	// Delete entries
	for _, hash := range toDelete {
		s.deleteInternal(hash)
		s.writeWAL("DELETE", s.keyMap[hash], nil, 0)
	}

	return len(toDelete)
}

// CreateSnapshot creates a snapshot of the current store state
func (s *Store) CreateSnapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.snapShotEnabled {
		return nil
	}

	// Write number of entries
	var buf bytes.Buffer
	entryCount := uint32(len(s.data))
	if err := binary.Write(&buf, binary.BigEndian, entryCount); err != nil {
		return fmt.Errorf("failed to write entry count: %w", err)
	}

	// Write each entry
	for _, entry := range s.data {
		// Skip expired entries
		if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
			continue
		}

		// Write tenant ID
		if err := binary.Write(&buf, binary.BigEndian, entry.key.TenantID); err != nil {
			return fmt.Errorf("failed to write tenant ID: %w", err)
		}

		// Write user ID
		if err := binary.Write(&buf, binary.BigEndian, entry.key.UserID); err != nil {
			return fmt.Errorf("failed to write user ID: %w", err)
		}

		// Write resource (length-prefixed string)
		resourceBytes := []byte(entry.key.Resource)
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(resourceBytes))); err != nil {
			return fmt.Errorf("failed to write resource length: %w", err)
		}
		if _, err := buf.Write(resourceBytes); err != nil {
			return fmt.Errorf("failed to write resource: %w", err)
		}

		// Write timestamp
		if err := binary.Write(&buf, binary.BigEndian, entry.key.Timestamp); err != nil {
			return fmt.Errorf("failed to write timestamp: %w", err)
		}

		// Write value (length-prefixed)
		valueBytes := []byte(fmt.Sprintf("%v", entry.value))
		if err := binary.Write(&buf, binary.BigEndian, uint32(len(valueBytes))); err != nil {
			return fmt.Errorf("failed to write value length: %w", err)
		}
		if _, err := buf.Write(valueBytes); err != nil {
			return fmt.Errorf("failed to write value: %w", err)
		}

		// Write insert time
		if err := binary.Write(&buf, binary.BigEndian, entry.insertTime.Unix()); err != nil {
			return fmt.Errorf("failed to write insert time: %w", err)
		}

		// Write TTL
		if err := binary.Write(&buf, binary.BigEndian, int64(entry.ttl.Seconds())); err != nil {
			return fmt.Errorf("failed to write TTL: %w", err)
		}
	}

	// Write to temporary file then rename (atomic)
	tempPath := s.snapshotPath + ".tmp"
	if err := os.WriteFile(tempPath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed to write snapshot file: %w", err)
	}

	if err := os.Rename(tempPath, s.snapshotPath); err != nil {
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	return nil
}

// Close cleanly shuts down the store
func (s *Store) Close() error {
	s.cleanupTicker.Stop()
	s.stopCleanup <- true

	if err := s.CreateSnapshot(); err != nil {
		return err
	}

	if s.walFile != nil {
		s.walFile.Close()
	}

	return nil
}

// Stats returns statistics about the store
func (s *Store) Stats() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return map[string]int{
		"entries":   len(s.data),
		"tenants":   len(s.tenantIndex),
		"users":     len(s.userIndex),
		"resources": len(s.resourceIndex),
	}
}

// Helper functions for creating partial keys
func WithTenant(id uint64) PartialKey {
	return PartialKey{TenantID: &id}
}

func WithUser(id uint64) PartialKey {
	return PartialKey{UserID: &id}
}

func WithResource(name string) PartialKey {
	return PartialKey{Resource: &name}
}

func WithTenantAndUser(tenantID, userID uint64) PartialKey {
	return PartialKey{TenantID: &tenantID, UserID: &userID}
}

// Example usage
func main() {
	fmt.Println("=== Enhanced Structured Key-Value Store Demo ===")

	store, err := NewStore("./data")
	if err != nil {
		fmt.Printf("Error creating store: %v\n", err)
		return
	}
	defer store.Close()

	// Example 1: Set with TTL
	fmt.Println("Example 1: Set entries with TTL")
	for i := range 5 {
		key := CompoundKey{
			TenantID:  1,
			UserID:    uint64(100 + i),
			Resource:  "session",
			Timestamp: time.Now().Unix(),
		}
		store.Set(key, fmt.Sprintf("session_data_%d", i), 5*time.Second)
	}
	fmt.Printf("Added 5 entries with 5-second TTL\n\n")

	// Example 2: Range query
	fmt.Println("Example 2: Range query by timestamp")
	now := time.Now().Unix()
	for i := range 10 {
		key := CompoundKey{
			TenantID:  2,
			UserID:    200,
			Resource:  "event",
			Timestamp: now + int64(i*10), // Events at 10-second intervals
		}
		store.Set(key, fmt.Sprintf("event_%d", i), 0) // No TTL
	}

	results := store.RangeQuery(now, now+50, PartialKey{})
	fmt.Printf("Found %d events in time range [%d, %d]\n", len(results), now, now+50)
	for key := range results {
		fmt.Printf("  - Timestamp: %d\n", key.Timestamp)
	}
	fmt.Println()

	// Example 3: Wait for TTL expiration
	fmt.Println("Example 3: Waiting for TTL expiration...")
	time.Sleep(6 * time.Second)

	queryResults := store.Query(WithTenant(1))
	fmt.Printf("After 6 seconds, tenant 1 has %d entries (should be 0)\n\n", len(queryResults.values))

	// Example 4: Persistence
	fmt.Println("Example 4: Creating snapshot...")
	if err := store.CreateSnapshot(); err != nil {
		fmt.Printf("Error creating snapshot: %v\n", err)
	}

	fmt.Printf("\nFinal stats: %+v\n", store.Stats())
}

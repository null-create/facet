package facet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/btree"
)

// CompoundKey represents a multi-dimensional cache key
type CompoundKey struct {
	TenantID   uint64
	UserID     uint64
	Resource   string
	Timestamp  int64
	hash       uint64 // Cached serialization
	serialized bool
}

// Hash returns a hash of the complete key and
// caches it within CompoundKey
func (k *CompoundKey) Hash() uint64 {
	if !k.serialized {
		var buf [8]byte
		h := fnv.New64a()
		binary.BigEndian.PutUint64(buf[:], k.TenantID)
		h.Write(buf[:])
		binary.BigEndian.PutUint64(buf[:], k.UserID)
		h.Write(buf[:])
		h.Write([]byte(k.Resource))
		binary.BigEndian.PutUint64(buf[:], uint64(k.Timestamp))
		h.Write(buf[:])
		k.hash = h.Sum64()
		k.serialized = true
	}
	return k.hash
}

// NewCompoundKey creates a new compound key with precomputed hash.
// This avoids recomputing the hash on every operation.
func NewCompoundKey(tenantID, userID uint64, resource string, timestamp int64) CompoundKey {
	k := CompoundKey{
		TenantID:  tenantID,
		UserID:    userID,
		Resource:  resource,
		Timestamp: timestamp,
	}
	k.Hash() // Precompute and cache the hash
	return k
}

// hashResource hashes a resource string to uint64 for indexed lookups.
// This allows the resource index to use uint64 keys instead of strings,
// which is faster for map operations.
func hashResource(name string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
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

// QueryCallback is a callback function invoked when a partial
// query match is found
type QueryCallback func(CompoundKey, any) bool

// RangeQueryCallback is a callback function for range queries
type RangeQueryCallback func([]uint64) bool

// Store is a concurrent key-value store with partial key query support
type Store struct {
	mu    sync.RWMutex
	walMu sync.Mutex        // Serializes WAL writes
	data  map[uint64]*entry // main data store (keyed by full key hash)

	// Secondary indexes for efficient partial queries (struct{} reduces memory vs bool)
	tenantIndex       map[uint64]map[uint64]struct{} // tenant -> set of key hashes
	userIndex         map[uint64]map[uint64]struct{} // user -> set of key hashes
	resourceIndex     map[string]map[uint64]struct{} // resource -> set of key hashes (kept for lookup by name)
	resourceHashIndex map[uint64]map[uint64]struct{} // resource hash -> set of key hashes (fast index)

	// Reverse mapping from hash to full key for lookups
	keyMap map[uint64]CompoundKey

	// B-tree for range queries on timestamp
	timestampIndex *TimestampIndex

	// Persistence
	walFile         *os.File
	walPath         string
	walOffset       uint64
	walEnabled      bool
	snapShotEnabled bool
	snapshotPath    string

	// TTL management
	ttlHeap       *TTLHeap
	cleanupTicker *time.Ticker
	stopCleanup   chan bool
}

// entry represents a data object in our Store
type entry struct {
	key        CompoundKey
	value      any
	insertTime time.Time
	ttl        time.Duration // 0 means no expiration
}

// timestampItem is a B-tree item for timestamp-based range queries.
// It implements btree.Item interface for use with github.com/google/btree.
type timestampItem struct {
	timestamp int64
	keyHashes []uint64
}

// Less returns true if this item's timestamp is less than the other's.
// Implements btree.Item interface.
func (t *timestampItem) Less(than btree.Item) bool {
	return t.timestamp < than.(*timestampItem).timestamp
}

// TimestampIndex uses a B-tree for efficient range queries on timestamps.
type TimestampIndex struct {
	mu   sync.RWMutex
	tree *btree.BTree
}

// NewTimestampIndex creates a new timestamp index backed by a B-tree.
// The B-tree degree is set to 32 for good cache performance.
func NewTimestampIndex() *TimestampIndex {
	return &TimestampIndex{
		tree: btree.New(32),
	}
}

// Add inserts or updates a timestamp entry with the given key hash.
// If the timestamp already exists, the hash is appended to the existing slice.
func (idx *TimestampIndex) Add(timestamp int64, keyHash uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	item := &timestampItem{timestamp: timestamp}
	existing := idx.tree.Get(item)
	if existing != nil {
		// Timestamp exists, append to existing hashes
			ti := existing.(*timestampItem)
		ti.keyHashes = append(ti.keyHashes, keyHash)
	} else {
		// New timestamp, insert fresh item
		idx.tree.ReplaceOrInsert(&timestampItem{
			timestamp: timestamp,
			keyHashes: []uint64{keyHash},
		})
	}
}

// Remove deletes a key hash from the given timestamp entry.
// If no hashes remain for that timestamp, the entry is removed entirely.
func (idx *TimestampIndex) Remove(timestamp int64, keyHash uint64) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	item := &timestampItem{timestamp: timestamp}
	existing := idx.tree.Get(item)
	if existing == nil {
		return
	}

	ti := existing.(*timestampItem)
	// Remove keyHash from slice
	for i, h := range ti.keyHashes {
		if h == keyHash {
			ti.keyHashes = append(ti.keyHashes[:i], ti.keyHashes[i+1:]...)
			break
		}
	}
	// Remove entry if no hashes remain
	if len(ti.keyHashes) == 0 {
		idx.tree.Delete(item)
	}
}

// RangeQuery calls fn for each set of key hashes within [start, end].
// fn returns false to stop iteration early.
func (idx *TimestampIndex) RangeQuery(start, end int64, fn RangeQueryCallback) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	startItem := &timestampItem{timestamp: start}

	// AscendGreaterOrEqual iterates from start, stopping when fn returns false
	idx.tree.AscendGreaterOrEqual(startItem, func(i btree.Item) bool {
		ti := i.(*timestampItem)
		if ti.timestamp > end {
			return false // Stop: past the end of range
		}
		return fn(ti.keyHashes)
	})
}

// TTL Heap for efficient expiration
// Note: This heap is accessed ONLY from:
//   - Store.Set (under s.mu lock)
//   - Store.cleanupExpired (single goroutine, under s.mu lock)
//
// Therefore no internal locking is needed.
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
		data:              make(map[uint64]*entry),
		tenantIndex:       make(map[uint64]map[uint64]struct{}),
		userIndex:         make(map[uint64]map[uint64]struct{}),
		resourceIndex:     make(map[string]map[uint64]struct{}),
		resourceHashIndex: make(map[uint64]map[uint64]struct{}),
		keyMap:            make(map[uint64]CompoundKey),
		timestampIndex:    NewTimestampIndex(),
		ttlHeap:           NewTTLHeap(),
		walPath:           filepath.Join(dataDir, "wal.log"),
		snapShotEnabled:   true,
		snapshotPath:      filepath.Join(dataDir, "snapshot.pb"),
		stopCleanup:       make(chan bool),
		walEnabled:        true,
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
		data:              make(map[uint64]*entry),
		tenantIndex:       make(map[uint64]map[uint64]struct{}),
		userIndex:         make(map[uint64]map[uint64]struct{}),
		resourceIndex:     make(map[string]map[uint64]struct{}),
		resourceHashIndex: make(map[uint64]map[uint64]struct{}),
		keyMap:            make(map[uint64]CompoundKey),
		timestampIndex:    NewTimestampIndex(),
		ttlHeap:           NewTTLHeap(),
		walEnabled:        opts.WalEnabled,
		walPath:           opts.WalPath,
		snapShotEnabled:   opts.SnapshotsEnabled,
		snapshotPath:      opts.SnapshotPath,
		stopCleanup:       make(chan bool),
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
				s.tenantIndex[key.TenantID] = make(map[uint64]struct{})
			}
			s.tenantIndex[key.TenantID][hash] = struct{}{}

			if s.userIndex[key.UserID] == nil {
				s.userIndex[key.UserID] = make(map[uint64]struct{})
			}
			s.userIndex[key.UserID][hash] = struct{}{}

			if s.resourceIndex[key.Resource] == nil {
				s.resourceIndex[key.Resource] = make(map[uint64]struct{})
			}
			s.resourceIndex[key.Resource][hash] = struct{}{}

			// Rebuild resource hash index (fast uint64 lookup)
			resourceHash := hashResource(key.Resource)
			if s.resourceHashIndex[resourceHash] == nil {
				s.resourceHashIndex[resourceHash] = make(map[uint64]struct{})
			}
			s.resourceHashIndex[resourceHash][hash] = struct{}{}

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
				s.mu.Unlock()
				s.deleteInternal(keyHash) // handles lock
				s.mu.Lock()
			}
		}
	}
	s.mu.Unlock()
}

// Set stores a value with the given compound key and optional TTL
func (s *Store) Set(key CompoundKey, value any, ttl time.Duration) {
	s.mu.Lock()

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
		s.tenantIndex[key.TenantID] = make(map[uint64]struct{})
	}
	s.tenantIndex[key.TenantID][hash] = struct{}{}

	if s.userIndex[key.UserID] == nil {
		s.userIndex[key.UserID] = make(map[uint64]struct{})
	}
	s.userIndex[key.UserID][hash] = struct{}{}

	if s.resourceIndex[key.Resource] == nil {
		s.resourceIndex[key.Resource] = make(map[uint64]struct{})
	}
	s.resourceIndex[key.Resource][hash] = struct{}{}

	// Update resource hash index (fast uint64-based lookup)
	resourceHash := hashResource(key.Resource)
	if s.resourceHashIndex[resourceHash] == nil {
		s.resourceHashIndex[resourceHash] = make(map[uint64]struct{})
	}
	s.resourceHashIndex[resourceHash][hash] = struct{}{}

	// Write to WAL (while holding store lock for consistency)
	s.writeWAL("SET", key, value, ttl)

	// Add to TTL heap if has expiration
	if ttl > 0 {
		s.ttlHeap.Push(hash, e.insertTime.Add(ttl))
	}

	s.mu.Unlock()

	// Update timestamp index (handles its own lock)
	s.timestampIndex.Add(key.Timestamp, hash)
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

	// Serialize value to bytes - avoid fmt for common string case
	var valueBytes []byte
	if s, ok := value.(string); ok {
		valueBytes = []byte(s)
	} else {
		valueBytes = fmt.Appendf(nil, "%v", value)
	}

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

	s.walMu.Lock()
	s.walFile.Write(lenBuf)
	s.walFile.Write(data)
	s.walFile.Sync()
	s.walOffset++
	s.walMu.Unlock()
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
func (qr *QueryResult) Iterate(fn QueryCallback) {
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

// Query finds all entries matching a partial key pattern and
// calls fn for each match which returns false to stop early
func (s *Store) Query(partial PartialKey, fn QueryCallback) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var candidates map[uint64]struct{}
	var indexedField string

	// Select index with smallest candidate set for better performance
	// Check each non-nil field and pick the one with fewest entries
	minSize := -1
	if partial.TenantID != nil {
		if idx, ok := s.tenantIndex[*partial.TenantID]; ok {
			if minSize == -1 || len(idx) < minSize {
				minSize = len(idx)
				candidates = idx
				indexedField = "tenant"
			}
		}
	}
	if partial.UserID != nil {
		if idx, ok := s.userIndex[*partial.UserID]; ok {
			if minSize == -1 || len(idx) < minSize {
				minSize = len(idx)
				candidates = idx
				indexedField = "user"
			}
		}
	}
	if partial.Resource != nil {
		resourceHash := hashResource(*partial.Resource)
		if idx, ok := s.resourceHashIndex[resourceHash]; ok {
			if minSize == -1 || len(idx) < minSize {
				minSize = len(idx)
				candidates = idx
				indexedField = "resource"
			}
		}
	}

	if indexedField == "" {
		// fallback: just check everything if no candidates are found in indicies
		for _, entry := range s.data {
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}
			if partial.Matches(entry.key) {
				if !fn(entry.key, entry.value) {
					return
				}
			}
		}
		return
	}

	// Build a simplified match check: skip the field we already indexed by
	checkTenant := indexedField != "tenant" && partial.TenantID != nil
	checkUser := indexedField != "user" && partial.UserID != nil
	checkResource := indexedField != "resource" && partial.Resource != nil
	checkTimestamp := partial.Timestamp != nil

	for hash := range candidates {
		if entry, ok := s.data[hash]; ok {
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}
			// Only check fields not covered by our index selection
			if checkTenant && *partial.TenantID != entry.key.TenantID {
				continue
			}
			if checkUser && *partial.UserID != entry.key.UserID {
				continue
			}
			if checkResource && *partial.Resource != entry.key.Resource {
				continue
			}
			if checkTimestamp && *partial.Timestamp != entry.key.Timestamp {
				continue
			}
			if !fn(entry.key, entry.value) {
				return
			}
		}
	}
}

// RangeQuery finds all entries with timestamps in the given range.
// It collects all candidate hashes first (under timestampIndex lock),
// then processes them under a single store read lock for efficiency.
func (s *Store) RangeQuery(startTime, endTime int64, partial PartialKey, fn QueryCallback) {
	// Collect all candidate hashes from timestamp index
	var allHashes []uint64
	s.timestampIndex.RangeQuery(startTime, endTime, func(hashes []uint64) bool {
		allHashes = append(allHashes, hashes...)
		return true
	})

	// Process candidates under a single read lock
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, hash := range allHashes {
		if entry, ok := s.data[hash]; ok {
			// Skip expired entries
			if entry.ttl > 0 && time.Since(entry.insertTime) >= entry.ttl {
				continue
			}

			// Apply additional filters from partial key
			if partial.Matches(entry.key) {
				if !fn(entry.key, entry.value) {
					return
				}
			}
		}
	}
}

func (s *Store) deleteInternal(hash uint64) {
	s.mu.Lock()
	entry := s.data[hash]
	if entry == nil {
		s.mu.Unlock()
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

	// Remove from resource hash index
	resourceHash := hashResource(key.Resource)
	delete(s.resourceHashIndex[resourceHash], hash)

	s.mu.Unlock()

	// Remove from timestamp index (handles own lock)
	s.timestampIndex.Remove(key.Timestamp, hash)
}

// Delete removes entries matching a partial key pattern
func (s *Store) Delete(partial PartialKey) int {
	s.mu.Lock()

	// Find all matching keys with optimal index selection
	var candidates map[uint64]struct{}
	minSize := -1
	if partial.TenantID != nil {
		if idx, ok := s.tenantIndex[*partial.TenantID]; ok {
			if minSize == -1 || len(idx) < minSize {
				minSize = len(idx)
				candidates = idx
			}
		}
	}
	if partial.UserID != nil {
		if idx, ok := s.userIndex[*partial.UserID]; ok {
			if minSize == -1 || len(idx) < minSize {
				minSize = len(idx)
				candidates = idx
			}
		}
	}
	if partial.Resource != nil {
		resourceHash := hashResource(*partial.Resource)
		if idx, ok := s.resourceHashIndex[resourceHash]; ok {
			if minSize == -1 || len(idx) < minSize {
				minSize = len(idx)
				candidates = idx
			}
		}
	}
	if candidates == nil {
		candidates = make(map[uint64]struct{})
		for hash := range s.data {
			candidates[hash] = struct{}{}
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

	s.mu.Unlock()

	// Delete entries
	for _, hash := range toDelete {
		s.deleteInternal(hash) // has own lock

		s.mu.Lock()
		s.writeWAL("DELETE", s.keyMap[hash], nil, 0)
		s.mu.Unlock()
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
		var valueBytes []byte
		if s, ok := entry.value.(string); ok {
			valueBytes = []byte(s)
		} else {
			valueBytes = []byte(fmt.Sprintf("%v", entry.value))
		}
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

// Close cleanly shuts down the store and empties
// data and index objects
func (s *Store) Close() error {
	s.cleanupTicker.Stop()
	s.stopCleanup <- true

	if err := s.CreateSnapshot(); err != nil {
		return err
	}

	if s.walFile != nil {
		if err := s.walFile.Close(); err != nil {
			return err
		}
	}

	s.data = nil
	s.resourceIndex = nil
	s.resourceHashIndex = nil
	s.userIndex = nil
	s.timestampIndex = nil
	return nil
}

// Stats returns statistics about the store
func (s *Store) Stats() map[string]int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return map[string]int{
		"entries":         len(s.data),
		"tenants":         len(s.tenantIndex),
		"users":           len(s.userIndex),
		"resources":       len(s.resourceIndex),
		"resourceHashes": len(s.resourceHashIndex),
	}
}

// Helper functions for creating partial keys

func WithTenant(id uint64) PartialKey {
	return PartialKey{TenantID: &id}
}

func WithTenantAndUser(tenantID, userID uint64) PartialKey {
	return PartialKey{TenantID: &tenantID, UserID: &userID}
}

func WithTenantAndResource(tenantID uint64, resource string) PartialKey {
	return PartialKey{TenantID: &tenantID, Resource: &resource}
}

func WithUser(id uint64) PartialKey {
	return PartialKey{UserID: &id}
}

func WithUserAndResource(userID uint64, resource string) PartialKey {
	return PartialKey{UserID: &userID, Resource: &resource}
}

func WithResource(name string) PartialKey {
	return PartialKey{Resource: &name}
}

// // Example usage
// func main() {
// 	fmt.Println("=== Enhanced Structured Key-Value Store Demo ===")

// 	store, err := NewStore("./data")
// 	if err != nil {
// 		fmt.Printf("Error creating store: %v\n", err)
// 		return
// 	}
// 	defer store.Close()

// 	// Example 1: Set with TTL
// 	fmt.Println("Example 1: Set entries with TTL")
// 	for i := range 5 {
// 		key := CompoundKey{
// 			TenantID:  1,
// 			UserID:    uint64(100 + i),
// 			Resource:  "session",
// 			Timestamp: time.Now().Unix(),
// 		}
// 		store.Set(key, fmt.Sprintf("session_data_%d", i), 5*time.Second)
// 	}
// 	fmt.Printf("Added 5 entries with 5-second TTL\n\n")

// 	// Example 2: Range query
// 	fmt.Println("Example 2: Range query by timestamp")
// 	now := time.Now().Unix()
// 	for i := range 10 {
// 		key := CompoundKey{
// 			TenantID:  2,
// 			UserID:    200,
// 			Resource:  "event",
// 			Timestamp: now + int64(i*10), // Events at 10-second intervals
// 		}
// 		store.Set(key, fmt.Sprintf("event_%d", i), 0) // No TTL
// 	}

// 	results := store.RangeQuery(now, now+50, PartialKey{})
// 	fmt.Printf("Found %d events in time range [%d, %d]\n", len(results), now, now+50)
// 	for key := range results {
// 		fmt.Printf("  - Timestamp: %d\n", key.Timestamp)
// 	}
// 	fmt.Println()

// 	// Example 3: Wait for TTL expiration
// 	fmt.Println("Example 3: Waiting for TTL expiration...")
// 	time.Sleep(6 * time.Second)

// 	// Query with results limit
// 	const limit = 100
// 	keys := make([]CompoundKey, 0, limit)
// 	values := make([]any, 0, limit)
// 	store.Query(WithTenant(1), func(k CompoundKey, v any) bool {
// 		keys = append(keys, k)
// 		values = append(values, v)
// 		return len(keys) < limit
// 	})

// 	fmt.Printf("After 6 seconds, tenant 1 has %d entries (should be 0)\n\n", len(values))

// 	// Example 4: Persistence
// 	fmt.Println("Example 4: Creating snapshot...")
// 	if err := store.CreateSnapshot(); err != nil {
// 		fmt.Printf("Error creating snapshot: %v\n", err)
// 	}

// 	fmt.Printf("\nFinal stats: %+v\n", store.Stats())
// }

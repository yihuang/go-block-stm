package block_stm

import (
	"sync"
)

// OptimizedSecondaryStore is an optimized version of secondary BTree using bitmap + sync.Map
type OptimizedSecondaryStore[V any] struct {
	mu      sync.RWMutex
	bitmap  Bitmap                  // marks which TxnIndex have data
	data    sync.Map                // TxnIndex -> secondaryDataItem[V]
	maxIdx  TxnIndex                // current maximum index for quick lookup
	version TxnVersion              // current maximum version (index + incarnation)
}

// NewOptimizedSecondaryStore creates a new optimized secondary store
func NewOptimizedSecondaryStore[V any]() *OptimizedSecondaryStore[V] {
	return &OptimizedSecondaryStore[V]{
		bitmap:  Bitmap{},
		maxIdx:  -1,
		version: InvalidTxnVersion,
	}
}

// Read returns the value and version for the closest txn less than or equal to the given txn
func (s *OptimizedSecondaryStore[V]) Read(txn TxnIndex) (V, TxnVersion, bool) {
	if txn <= 0 {
		var zero V
		return zero, InvalidTxnVersion, false
	}

	// Convert txn to uint32 for bitmap operations
	target := uint32(txn)
	if target == 0 {
		var zero V
		return zero, InvalidTxnVersion, false
	}

	// We need to find index < txn (exclusive)
	// PreviousValue returns the greatest value that is less than target
	// So we call PreviousValue(txn) to find index < txn
	// No need to subtract 1

	// Keep searching until we find valid data or exhaust all possibilities
	for {
		// Find the closest index <= target that has data in bitmap
		s.mu.RLock()
		prev, found := s.bitmap.PreviousValue(target)
		s.mu.RUnlock()

		if !found {
			var zero V
			return zero, InvalidTxnVersion, false
		}

		// Try to get the data from sync.Map
		idx := TxnIndex(prev)
		if item, ok := s.data.Load(idx); ok {
			dataItem := item.(secondaryDataItem[V])
			if dataItem.Estimate {
				return dataItem.Value, dataItem.Version(), true
			}
			return dataItem.Value, dataItem.Version(), false
		}

		// Data not found in sync.Map (concurrent modification or deletion)
		// Continue searching with the next possible index
		if prev == 0 {
			break
		}
		target = prev
	}

	var zero V
	return zero, InvalidTxnVersion, false
}


// Write writes a value with the given version
func (s *OptimizedSecondaryStore[V]) Write(value V, version TxnVersion) {
	idx := version.Index

	// Set the bit in bitmap (protected by mutex)
	s.mu.Lock()
	s.bitmap.Set(uint32(idx))

	// Update max index and version if needed
	if idx > s.maxIdx {
		s.maxIdx = idx
		s.version = version
	} else if idx == s.maxIdx && version.Incarnation > s.version.Incarnation {
		s.version = version
	}
	s.mu.Unlock()

	// Store the data in sync.Map (no mutex needed)
	item := secondaryDataItem[V]{
		Index:       idx,
		Incarnation: version.Incarnation,
		Value:       value,
		Estimate:    false,
	}
	s.data.Store(idx, item)
}

// WriteEstimate writes an estimate for the given txn
func (s *OptimizedSecondaryStore[V]) WriteEstimate(txn TxnIndex) {
	// Set the bit in bitmap (protected by mutex)
	s.mu.Lock()
	s.bitmap.Set(uint32(txn))

	// Update max index if needed (estimates don't affect version)
	if txn > s.maxIdx {
		s.maxIdx = txn
		// For estimates, we don't update version as it's not a real value
	}
	s.mu.Unlock()

	// Store the estimate in sync.Map (no mutex needed)
	item := secondaryDataItem[V]{
		Index:    txn,
		Estimate: true,
	}
	s.data.Store(txn, item)
}

// Delete marks the key as deleted at the given txn
func (s *OptimizedSecondaryStore[V]) Delete(txn TxnIndex) {
	// First, remove any existing entry from sync.Map
	s.data.Delete(txn)

	// Then set the bit in bitmap and update state (protected by mutex)
	s.mu.Lock()
	defer s.mu.Unlock()

	s.bitmap.Set(uint32(txn))

	// Update max index if needed
	if txn > s.maxIdx {
		s.maxIdx = txn
		// For deletions at new max index, we need to find the previous version
		// We'll update version lazily when needed
		s.version = InvalidTxnVersion
	} else if txn == s.maxIdx {
		// If deleting at current max index, find the new max version
		s.version = InvalidTxnVersion
	}
}

// Max returns the maximum item in the store
func (s *OptimizedSecondaryStore[V]) Max() (secondaryDataItem[V], bool) {
	s.mu.RLock()
	maxIdx := s.maxIdx
	s.mu.RUnlock()

	if maxIdx < 0 {
		return secondaryDataItem[V]{}, false
	}

	// Try to get data at maxIdx
	if item, ok := s.data.Load(maxIdx); ok {
		return item.(secondaryDataItem[V]), true
	}

	// If no data at maxIdx (e.g., deletion), find the previous index with data
	// We need to check bitmap for the actual maximum index with data
	s.mu.RLock()
	max, found := s.bitmap.Max()
	s.mu.RUnlock()

	if !found {
		return secondaryDataItem[V]{}, false
	}

	// Try to get data at the actual maximum index in bitmap
	maxIdx = TxnIndex(max)
	if item, ok := s.data.Load(maxIdx); ok {
		return item.(secondaryDataItem[V]), true
	}

	// If no data at the maximum bitmap index, we need to search backward
	// This is similar to Read but we want the maximum item with data
	var maxItem secondaryDataItem[V]
	s.mu.RLock()
	prev := max
	for {
		// Try to get data at current index
		if item, ok := s.data.Load(TxnIndex(prev)); ok {
			maxItem = item.(secondaryDataItem[V])
			s.mu.RUnlock()
			return maxItem, true
		}

		// Find previous index in bitmap
		prev, found = s.bitmap.PreviousValue(prev)
		if !found {
			s.mu.RUnlock()
			return secondaryDataItem[V]{}, false
		}
	}
}

// Scan iterates over all items in the store
func (s *OptimizedSecondaryStore[V]) Scan(iter func(item secondaryDataItem[V]) bool) {
	// Get all indices from bitmap (protected by mutex)
	s.mu.RLock()
	indices := make([]uint32, 0, s.bitmap.Count())
	s.bitmap.Range(func(x uint32) {
		indices = append(indices, x)
	})
	s.mu.RUnlock()

	// Iterate through indices and load data (no mutex needed for sync.Map)
	for _, idx := range indices {
		if item, ok := s.data.Load(TxnIndex(idx)); ok {
			if !iter(item.(secondaryDataItem[V])) {
				break
			}
		}
	}
}

// Size returns the number of items in the store
func (s *OptimizedSecondaryStore[V]) Size() int {
	s.mu.RLock()
	size := s.bitmap.Count()
	s.mu.RUnlock()
	return size
}
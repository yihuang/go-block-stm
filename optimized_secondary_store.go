package block_stm

import (
	"sync"

	"github.com/kelindar/bitmap"
)

// OptimizedSecondaryStore is an optimized version of secondary BTree using bitmap + sync.Map
type OptimizedSecondaryStore[V any] struct {
	mu      sync.RWMutex
	bitmap  bitmap.Bitmap           // marks which TxnIndex have data
	data    sync.Map                // TxnIndex -> secondaryDataItem[V]
	maxIdx  TxnIndex                // current maximum index for quick lookup
	version TxnVersion              // current maximum version (index + incarnation)
}

// NewOptimizedSecondaryStore creates a new optimized secondary store
func NewOptimizedSecondaryStore[V any]() *OptimizedSecondaryStore[V] {
	return &OptimizedSecondaryStore[V]{
		bitmap:  bitmap.Bitmap{},
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

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Keep searching until we find valid data or exhaust all possibilities
	// We need to find the closest index < txn that has valid data in sync.Map
	// Use a set to track indices we've already checked
	checked := make(map[TxnIndex]bool)
	currentTxn := txn

	for {
		// Find the closest index < currentTxn that has data in bitmap
		// and hasn't been checked yet
		var foundIdx TxnIndex = -1
		s.bitmap.Range(func(x uint32) {
			idx := TxnIndex(x)
			if idx < currentTxn && !checked[idx] {
				if foundIdx < 0 || idx > foundIdx {
					foundIdx = idx
				}
			}
		})

		if foundIdx < 0 {
			break
		}

		// Mark this index as checked
		checked[foundIdx] = true

		// Try to get the data from sync.Map
		if item, ok := s.data.Load(foundIdx); ok {
			dataItem := item.(secondaryDataItem[V])
			if dataItem.Estimate {
				return dataItem.Value, dataItem.Version(), true
			}
			return dataItem.Value, dataItem.Version(), false
		}

		// Data not found in sync.Map (concurrent modification or deletion)
		// Continue searching, but don't update currentTxn since we want to
		// check all indices < original txn
	}

	var zero V
	return zero, InvalidTxnVersion, false
}

// findClosestIndex finds the closest index <= txn that has data in bitmap
func (s *OptimizedSecondaryStore[V]) findClosestIndex(txn TxnIndex) TxnIndex {
	return s.findClosestIndexLessThan(txn)
}

// findClosestIndexLessThan finds the closest index < txn (exclusive) that has data in bitmap
func (s *OptimizedSecondaryStore[V]) findClosestIndexLessThan(txn TxnIndex) TxnIndex {
	// Convert txn to uint32 for bitmap operations
	target := uint32(txn)
	if target == 0 {
		return -1
	}
	// Look for index < txn (exclusive)
	target = target - 1

	// We need to find the maximum set bit that is <= target
	// Since bitmap doesn't have a Prev method, we'll use Range and track the result
	var found uint32 = 0
	hasFound := false

	s.bitmap.Range(func(x uint32) {
		if x <= target && x > found {
			found = x
			hasFound = true
		}
	})

	if !hasFound {
		return -1
	}
	return TxnIndex(found)
}

// Write writes a value with the given version
func (s *OptimizedSecondaryStore[V]) Write(value V, version TxnVersion) {
	s.mu.Lock()
	defer s.mu.Unlock()

	idx := version.Index
	// Set the bit in bitmap
	s.bitmap.Set(uint32(idx))

	// Store the data in sync.Map
	item := secondaryDataItem[V]{
		Index:       idx,
		Incarnation: version.Incarnation,
		Value:       value,
		Estimate:    false,
	}
	s.data.Store(idx, item)

	// Update max index and version if needed
	if idx > s.maxIdx {
		s.maxIdx = idx
		s.version = version
	} else if idx == s.maxIdx && version.Incarnation > s.version.Incarnation {
		s.version = version
	}
}

// WriteEstimate writes an estimate for the given txn
func (s *OptimizedSecondaryStore[V]) WriteEstimate(txn TxnIndex) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Set the bit in bitmap
	s.bitmap.Set(uint32(txn))

	// Store the estimate in sync.Map
	item := secondaryDataItem[V]{
		Index:    txn,
		Estimate: true,
	}
	s.data.Store(txn, item)

	// Update max index if needed (estimates don't affect version)
	if txn > s.maxIdx {
		s.maxIdx = txn
		// For estimates, we don't update version as it's not a real value
	}
}

// Delete marks the key as deleted at the given txn
func (s *OptimizedSecondaryStore[V]) Delete(txn TxnIndex) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Set the bit in bitmap
	s.bitmap.Set(uint32(txn))

	// For Delete, we need to remove any existing entry at this txn
	// and ensure no value is stored (following BTree.Delete behavior)
	s.data.Delete(txn)

	// Update max index if needed
	if txn > s.maxIdx {
		s.maxIdx = txn
		// For deletions at new max index, we need to find the previous version
		// Search for the closest index < txn that has data
		s.version = InvalidTxnVersion
		var prevIdx TxnIndex = -1
		s.bitmap.Range(func(x uint32) {
			idx := TxnIndex(x)
			if idx < txn && idx > prevIdx {
				prevIdx = idx
			}
		})
		if prevIdx >= 0 {
			if item, ok := s.data.Load(prevIdx); ok {
				s.version = item.(secondaryDataItem[V]).Version()
			}
		}
	} else if txn == s.maxIdx {
		// If deleting at current max index, find the new max version
		s.version = InvalidTxnVersion
		var prevIdx TxnIndex = -1
		s.bitmap.Range(func(x uint32) {
			idx := TxnIndex(x)
			if idx < txn && idx > prevIdx {
				prevIdx = idx
			}
		})
		if prevIdx >= 0 {
			if item, ok := s.data.Load(prevIdx); ok {
				s.version = item.(secondaryDataItem[V]).Version()
			}
		}
	}
}

// Max returns the maximum item in the store
func (s *OptimizedSecondaryStore[V]) Max() (secondaryDataItem[V], bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.maxIdx < 0 {
		return secondaryDataItem[V]{}, false
	}

	// Try to get data at maxIdx
	if item, ok := s.data.Load(s.maxIdx); ok {
		return item.(secondaryDataItem[V]), true
	}

	// If no data at maxIdx (e.g., deletion), find the previous index with data
	var maxItem secondaryDataItem[V]
	found := false
	s.bitmap.Range(func(x uint32) {
		idx := TxnIndex(x)
		if idx <= s.maxIdx && idx > maxItem.Index {
			if item, ok := s.data.Load(idx); ok {
				maxItem = item.(secondaryDataItem[V])
				found = true
			}
		}
	})

	return maxItem, found
}

// Scan iterates over all items in the store
func (s *OptimizedSecondaryStore[V]) Scan(iter func(item secondaryDataItem[V]) bool) {
	s.mu.RLock()
	// Get all indices from bitmap
	indices := make([]uint32, 0, s.bitmap.Count())
	s.bitmap.Range(func(x uint32) {
		indices = append(indices, x)
	})
	s.mu.RUnlock()

	// Iterate through indices and load data
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
	defer s.mu.RUnlock()
	return s.bitmap.Count()
}
package block_stm

import (
	"bytes"

	storetypes "cosmossdk.io/store/types"
)

const (
	// Since we do copy-on-write a lot, smaller degree means smaller allocations
	OuterBTreeDegree = 4
	InnerBTreeDegree = 4
)

type MVData = GMVData[[]byte]

func NewMVData() *MVData {
	return NewGMVData(BytesIsZero, BytesLen)
}

type GMVData[V any] struct {
	BTree[dataItem[V]]
	isZero   func(V) bool
	valueLen func(V) int
}

func NewMVStore(key storetypes.StoreKey) MVStore {
	switch key.(type) {
	case *storetypes.ObjectStoreKey:
		return NewGMVData(ObjIsZero, ObjLen)
	default:
		return NewGMVData(BytesIsZero, BytesLen)
	}
}

func NewGMVData[V any](isZero func(V) bool, valueLen func(V) int) *GMVData[V] {
	return &GMVData[V]{
		BTree:    *NewBTree(KeyItemLess[dataItem[V]], OuterBTreeDegree),
		isZero:   isZero,
		valueLen: valueLen,
	}
}

// getStore returns `nil` if not found
func (d *GMVData[V]) getStore(key Key) *OptimizedSecondaryStore[V] {
	outer, _ := d.Get(dataItem[V]{Key: key})
	return outer.Store
}

// getStoreOrDefault set a new store atomically if not found.
func (d *GMVData[V]) getStoreOrDefault(key Key) *OptimizedSecondaryStore[V] {
	return d.GetOrDefault(dataItem[V]{Key: key}, func(item *dataItem[V]) {
		if item.Store == nil {
			item.Store = NewOptimizedSecondaryStore[V]()
		}
	}).Store
}

func (d *GMVData[V]) Write(key Key, value V, version TxnVersion) {
	store := d.getStoreOrDefault(key)
	store.Write(value, version)
}

func (d *GMVData[V]) WriteEstimate(key Key, txn TxnIndex) {
	store := d.getStoreOrDefault(key)
	store.WriteEstimate(txn)
}

func (d *GMVData[V]) Delete(key Key, txn TxnIndex) {
	store := d.getStoreOrDefault(key)
	store.Delete(txn)
}

// Read returns the value and the version of the value that's less than the given txn.
// If the key is not found, returns `(nil, InvalidTxnVersion, false)`.
// If the key is found but value is an estimate, returns `(nil, BlockingTxn, true)`.
// If the key is found, returns `(value, version, false)`, `value` can be `nil` which means deleted.
func (d *GMVData[V]) Read(key Key, txn TxnIndex) (V, TxnVersion, bool) {
	var zero V
	if txn == 0 {
		return zero, InvalidTxnVersion, false
	}

	store := d.getStore(key)
	if store == nil {
		return zero, InvalidTxnVersion, false
	}

	return store.Read(txn)
}

func (d *GMVData[V]) Iterator(
	opts IteratorOptions, txn TxnIndex,
	waitFn func(TxnIndex),
) *MVIterator[V] {
	return NewMVIterator(opts, txn, d.Iter(), waitFn)
}

// ValidateReadSet validates the read descriptors,
// returns true if valid.
func (d *GMVData[V]) ValidateReadSet(txn TxnIndex, rs *ReadSet) bool {
	for _, desc := range rs.Reads {
		_, version, estimate := d.Read(desc.Key, txn)
		if estimate {
			// previously read entry from data, now ESTIMATE
			return false
		}
		if version != desc.Version {
			// previously read entry from data, now NOT_FOUND,
			// or read some entry, but not the same version as before
			return false
		}
	}

	for _, desc := range rs.Iterators {
		if !d.validateIterator(desc, txn) {
			return false
		}
	}

	return true
}

// validateIterator validates the iteration descriptor by replaying and compare the recorded reads.
// returns true if valid.
func (d *GMVData[V]) validateIterator(desc IteratorDescriptor, txn TxnIndex) bool {
	it := NewMVIterator(desc.IteratorOptions, txn, d.Iter(), nil)
	defer it.Close()

	var i int
	for ; it.Valid(); it.Next() {
		if desc.Stop != nil {
			if BytesBeyond(it.Key(), desc.Stop, desc.Ascending) {
				break
			}
		}

		if i >= len(desc.Reads) {
			return false
		}

		read := desc.Reads[i]
		if read.Version != it.Version() || !bytes.Equal(read.Key, it.Key()) {
			return false
		}

		i++
	}

	// we read an estimate value, fail the validation.
	if it.ReadEstimateValue() {
		return false
	}

	return i == len(desc.Reads)
}

func (d *GMVData[V]) Snapshot() (snapshot []GKVPair[V]) {
	d.SnapshotTo(func(key Key, value V) bool {
		snapshot = append(snapshot, GKVPair[V]{key, value})
		return true
	})
	return
}

func (d *GMVData[V]) SnapshotTo(cb func(Key, V) bool) {
	d.Scan(func(outer dataItem[V]) bool {
		item, ok := outer.Store.Max()
		if !ok {
			return true
		}

		if item.Estimate {
			return true
		}

		return cb(outer.Key, item.Value)
	})
}

func (d *GMVData[V]) SnapshotToStore(store storetypes.Store) {
	kv := store.(storetypes.GKVStore[V])
	d.SnapshotTo(func(key Key, value V) bool {
		if d.isZero(value) {
			kv.Delete(key)
		} else {
			kv.Set(key, value)
		}
		return true
	})
}

type GKVPair[V any] struct {
	Key   Key
	Value V
}
type KVPair = GKVPair[[]byte]

type dataItem[V any] struct {
	Key   Key
	Store *OptimizedSecondaryStore[V]
}

var _ KeyItem = dataItem[[]byte]{}

func (item dataItem[V]) GetKey() []byte {
	return item.Key
}

type secondaryDataItem[V any] struct {
	Index       TxnIndex
	Incarnation Incarnation
	Value       V
	Estimate    bool
}

func secondaryLesser[V any](a, b secondaryDataItem[V]) bool {
	return a.Index < b.Index
}

func (item secondaryDataItem[V]) Version() TxnVersion {
	return TxnVersion{Index: item.Index, Incarnation: item.Incarnation}
}

// seekClosestTxn returns the closest txn that's less than the given txn.
func seekClosestTxn[V any](tree *BTree[secondaryDataItem[V]], txn TxnIndex) (secondaryDataItem[V], bool) {
	return tree.ReverseSeek(secondaryDataItem[V]{Index: txn - 1})
}

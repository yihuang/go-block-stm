package block_stm

import (
	storetypes "cosmossdk.io/store/types"
	"github.com/tidwall/btree"
)

// MVIterator is an iterator for a multi-versioned store.
type MVIterator[V any] struct {
	BTreeIteratorG[dataItem[V]]
	txn TxnIndex

	// cache current found value and version
	value   V
	version TxnVersion

	// record the observed reads during iteration during execution
	reads []ReadDescriptor
	// blocking call to wait for dependent transaction to finish, `nil` in validation mode
	waitFn func(TxnIndex)
	// signal the validation to fail
	readEstimateValue bool
}

var _ storetypes.Iterator = (*MVIterator[[]byte])(nil)

func NewMVIterator[V any](
	opts IteratorOptions, txn TxnIndex, iter btree.IterG[dataItem[V]],
	waitFn func(TxnIndex),
) *MVIterator[V] {
	it := &MVIterator[V]{
		BTreeIteratorG: *NewBTreeIteratorG(
			dataItem[V]{Key: opts.Start},
			dataItem[V]{Key: opts.End},
			iter,
			opts.Ascending,
		),
		txn:    txn,
		waitFn: waitFn,
	}
	it.resolveValue()
	return it
}

// Executing returns if the iterator is running in execution mode.
func (it *MVIterator[V]) Executing() bool {
	return it.waitFn != nil
}

func (it *MVIterator[V]) Next() {
	it.BTreeIteratorG.Next()
	it.resolveValue()
}

func (it *MVIterator[V]) Value() V {
	return it.value
}

func (it *MVIterator[V]) Version() TxnVersion {
	return it.version
}

func (it *MVIterator[V]) Reads() []ReadDescriptor {
	return it.reads
}

func (it *MVIterator[V]) ReadEstimateValue() bool {
	return it.readEstimateValue
}

// resolveValue skips the non-exist values in the iterator based on the txn index, and caches the first existing one.
func (it *MVIterator[V]) resolveValue() {
	inner := &it.BTreeIteratorG
	for ; inner.Valid(); inner.Next() {
		v, ok := it.resolveValueInner(inner.Item().Store)
		if !ok {
			// abort the iterator
			it.valid = false
			// signal the validation to fail
			it.readEstimateValue = true
			return
		}
		if v == nil {
			continue
		}

		it.value = v.Value
		it.version = v.Version()
		if it.Executing() {
			it.reads = append(it.reads, ReadDescriptor{
				Key:     inner.Item().Key,
				Version: it.version,
			})
		}
		return
	}
}

// resolveValueInner loop until we find a value that is not an estimate,
// wait for dependency if gets an ESTIMATE.
// returns:
// - (nil, true) if the value is not found
// - (nil, false) if the value is an estimate and we should fail the validation
// - (v, true) if the value is found
func (it *MVIterator[V]) resolveValueInner(store *OptimizedSecondaryStore[V]) (*secondaryDataItem[V], bool) {
	for {
		value, version, estimate := store.Read(it.txn)
		if !version.Valid() {
			// value not found
			return nil, true
		}

		if estimate {
			if it.Executing() {
				it.waitFn(version.Index)
				continue
			}
			// in validation mode, it should fail validation immediately
			return nil, false
		}

		return &secondaryDataItem[V]{
			Index:       version.Index,
			Incarnation: version.Incarnation,
			Value:       value,
			Estimate:    false,
		}, true
	}
}

// The following methods are required by storetypes.Iterator interface
func (it *MVIterator[V]) Domain() (start, end []byte) {
	return it.BTreeIteratorG.Domain()
}

func (it *MVIterator[V]) Valid() bool {
	return it.BTreeIteratorG.Valid()
}

func (it *MVIterator[V]) Key() []byte {
	return it.BTreeIteratorG.Item().Key
}

func (it *MVIterator[V]) ValueBytes() []byte {
	if v, ok := any(it.value).([]byte); ok {
		return v
	}
	return nil
}

func (it *MVIterator[V]) Close() error {
	it.BTreeIteratorG.Close()
	return nil
}

func (it *MVIterator[V]) Error() error {
	return nil
}

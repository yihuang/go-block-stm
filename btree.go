package block_stm

import (
	"sync/atomic"

	"github.com/tidwall/btree"
)

// BTree wraps an atomic pointer to an unsafe btree.BTreeG
type BTree[T any] struct {
	atomic.Pointer[btree.BTreeG[T]]
}

// NewBTree returns a new BTree.
func NewBTree[T any](less func(a, b T) bool, degree int) *BTree[T] {
	tree := btree.NewBTreeGOptions(less, btree.Options{
		NoLocks:  true,
		ReadOnly: true,
		Degree:   degree,
	})
	t := &BTree[T]{}
	t.Store(tree)
	return t
}

func (bt *BTree[T]) Get(item T) (result T, ok bool) {
	return bt.Load().Get(item)
}

func (bt *BTree[T]) GetOrDefault(item T, fillDefaults func(*T)) T {
	for {
		t := bt.Load()
		result, ok := t.Get(item)
		if ok {
			return result
		}
		fillDefaults(&item)
		c := t.Copy()
		c.Set(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return item
		}
	}
}

func (bt *BTree[T]) Set(item T) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.Set(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Delete(item T) (prev T, ok bool) {
	for {
		t := bt.Load()
		c := t.Copy()
		prev, ok = c.Delete(item)
		c.Freeze()
		if bt.CompareAndSwap(t, c) {
			return
		}
	}
}

func (bt *BTree[T]) Scan(iter func(item T) bool) {
	bt.Load().Scan(iter)
}

func (bt *BTree[T]) Min() (T, bool) {
	return bt.Load().Min()
}

func (bt *BTree[T]) Iter() btree.IterG[T] {
	return bt.Load().Iter()
}

func (bt *BTree[T]) Seek(item T) (result T, ok bool) {
	iter := bt.Iter()
	if !iter.Seek(item) {
		iter.Release()
		return
	}

	result = iter.Item()
	ok = true
	iter.Release()
	return
}

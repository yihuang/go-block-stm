package block_stm

import (
	"math/bits"

	"github.com/kelindar/bitmap"
)

type Bitmap struct {
	bm     bitmap.Bitmap
	offset uint32 // all zero prefixes
}

func (dst *Bitmap) Set(x uint32) {
	if dst.offset == 0 && len(dst.bm) == 0 {
		// initialize
		dst.offset = x &^ 0x3F
		// Ensure bm has at least one block
		blkIdx := int((x - dst.offset) >> 6)
		if blkIdx >= len(dst.bm) {
			dst.bm = make(bitmap.Bitmap, blkIdx+1)
		}
	} else if x < dst.offset {
		// grow blocks backward
		blkGrow := int(dst.offset>>6 - x>>6)
		bm := make(bitmap.Bitmap, len(dst.bm)+blkGrow)
		copy(bm[blkGrow:], dst.bm)
		dst.bm = bm
		dst.offset -= uint32(blkGrow << 6)
	} else {
		// Ensure bm has enough blocks
		blkIdx := int((x - dst.offset) >> 6)
		if blkIdx >= len(dst.bm) {
			bm := make(bitmap.Bitmap, blkIdx+1)
			copy(bm, dst.bm)
			dst.bm = bm
		}
	}

	dst.bm.Set(x - dst.offset)
}

func (dst *Bitmap) Remove(x uint32) {
	if x >= dst.offset {
		dst.bm.Remove(x - dst.offset)
	}
}

func (dst *Bitmap) Contains(x uint32) (contains bool) {
	if x >= dst.offset {
		contains = dst.bm.Contains(x - dst.offset)
	}
	return
}

func (dst Bitmap) Min() (uint32, bool) {
	min, found := dst.bm.Min()
	if found {
		min += dst.offset
	}
	return min, found
}

func (dst Bitmap) Max() (uint32, bool) {
	max, found := dst.bm.Max()
	if found {
		max += dst.offset
	}
	return max, found
}

func (dst Bitmap) CountTo(until uint32) int {
	if until <= dst.offset {
		return 0
	}
	return dst.bm.CountTo(until - dst.offset)
}

func (dst Bitmap) Count() int {
	return dst.bm.Count()
}

func (dst Bitmap) Range(fn func(x uint32)) {
	dst.bm.Range(func(x uint32) {
		fn(x + dst.offset)
	})
}

func (dst Bitmap) PreviousValue(x uint32) (uint32, bool) {
	if x <= dst.offset {
		return 0, false
	}
	x -= dst.offset

	blkAt := int(x >> 6)
	bitAt := int(x % 64)
	if blkAt >= len(dst.bm) {
		// find in next block
		blkAt = len(dst.bm) - 1
	} else if bitAt == 0 {
		// find in next block
		blkAt--
	} else {
		bitAt--

		// find in current block first
		blk := dst.bm[blkAt] << uint(63-bitAt)
		if blk != 0 {
			return uint32(blkAt<<6+bitAt-bits.LeadingZeros64(blk)) + dst.offset, true
		}

		blkAt--
	}

	for ; blkAt >= 0; blkAt-- {
		blk := dst.bm[blkAt]
		if blk != 0 {
			return uint32(blkAt<<6+63-bits.LeadingZeros64(blk)) + dst.offset, true
		}
	}

	return 0, false
}

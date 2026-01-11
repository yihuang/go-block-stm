package block_stm

import (
	"math"
	"testing"

	"github.com/test-go/testify/require"
)

func collectSet(bm Bitmap) []uint32 {
	var result []uint32
	bm.Range(func(x uint32) {
		result = append(result, x)
	})
	return result
}

func TestBitmap(t *testing.T) {
	v1 := uint32(64*3 + 2)
	v2 := uint32(64*2 + 5)
	// v3 := uint32(64)
	// v4 := uint32(2)
	// Test cases for Bitmap operations
	bm := Bitmap{}
	bm.Set(v1)
	require.Equal(t, []uint32{v1}, collectSet(bm))

	// grow backward
	bm.Set(v2)
	require.Equal(t, []uint32{v2, v1}, collectSet(bm))

	// seek
	n, found := bm.PreviousValue(v1)
	require.True(t, found)
	require.Equal(t, v2, n)

	n, found = bm.PreviousValue(v1 + 1)
	require.True(t, found)
	require.Equal(t, v1, n)

	n, found = bm.PreviousValue(v2)
	require.False(t, found)

	// seek out of range
	n, found = bm.PreviousValue(math.MaxUint32)
	require.True(t, found)
	require.Equal(t, v1, n)
}

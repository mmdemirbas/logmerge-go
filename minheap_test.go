package main_test

import (
	"container/heap"
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestMinHeap(t *testing.T) {
	h := &MinHeap{}
	heap.Init(h)
	heap.Push(h, &LogLine{Ordinal: [2]uint64{123, 456}})
	heap.Push(h, &LogLine{Ordinal: [2]uint64{456, 789}})
	if (*h)[0].Ordinal[0] > (*h)[1].Ordinal[0] {
		t.Errorf("MinHeap does not maintain order")
	}
}

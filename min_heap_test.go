package main_test

import (
	"container/heap"
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestMinHeap(t *testing.T) {
	h := &MinHeap{}
	heap.Init(h)
	heap.Push(h, &FileHandle{Timestamp: newTime(0)})
	heap.Push(h, &FileHandle{Timestamp: newTime(-1)})
	if (*h)[0].Timestamp > (*h)[1].Timestamp {
		t.Errorf("MinHeap does not maintain order")
	}
}

func newTime(addMinutes int) Timestamp {
	return NewTimestamp(2025, 1, 1, 0, addMinutes, 0, 0, 0, 0, 0)
}

// TODO: Add more tests to cover all cases

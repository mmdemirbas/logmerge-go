package main_test

import (
	"container/heap"
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestMinHeap(t *testing.T) {
	h := &MinHeap{}
	heap.Init(h)
	heap.Push(h, &FileReader{Timestamp: newTime(0)})
	heap.Push(h, &FileReader{Timestamp: newTime(-1)})
	if (*h)[0].Timestamp > (*h)[1].Timestamp {
		t.Errorf("MinHeap does not maintain order")
	}
}

func newTime(addMinutes int) MyTime {
	return NewMyTime(2025, 1, 1, 0, addMinutes, 0, 0, 0, 0, 0)
}

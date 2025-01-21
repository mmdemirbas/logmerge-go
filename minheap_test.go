package main_test

import (
	"container/heap"
	. "github.com/mmdemirbas/logmerge"
	"testing"
	"time"
)

func TestMinHeap(t *testing.T) {
	h := &MinHeap{}
	heap.Init(h)
	heap.Push(h, &LinePrefix{Timestamp: time.Now()})
	heap.Push(h, &LinePrefix{Timestamp: time.Now().Add(-time.Minute)})
	if (*h)[0].Timestamp.After((*h)[1].Timestamp) {
		t.Errorf("MinHeap does not maintain order")
	}
}

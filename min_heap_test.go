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
	heap.Push(h, &InputFile{CurrentTimestamp: now(0)})
	heap.Push(h, &InputFile{CurrentTimestamp: now(-1)})
	if (*h)[0].CurrentTimestamp.After(*(*h)[1].CurrentTimestamp) {
		t.Errorf("MinHeap does not maintain order")
	}
}

func now(addMinutes int) *time.Time {
	t := time.Now().Add(time.Minute * time.Duration(addMinutes))
	return &t
}

package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestMinHeap(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		h := NewMinHeap(10)
		h.Push(&FileHandle{Timestamp: newTime(100)})
		h.Push(&FileHandle{Timestamp: newTime(50)})
		h.Push(&FileHandle{Timestamp: newTime(150)})
		h.Push(&FileHandle{Timestamp: newTime(75)})
		h.Push(&FileHandle{Timestamp: newTime(125)})

		if h.Pop().Timestamp != newTime(50) {
			t.Error("Expected 50")
		}
		if h.Pop().Timestamp != newTime(75) {
			t.Error("Expected 75")
		}
		if h.Pop().Timestamp != newTime(100) {
			t.Error("Expected 100")
		}
		if h.Pop().Timestamp != newTime(125) {
			t.Error("Expected 125")
		}
		if h.Pop().Timestamp != newTime(150) {
			t.Error("Expected 150")
		}
		if h.Pop() != nil {
			t.Error("Expected nil")
		}
	})

	t.Run("2", func(t *testing.T) {
		h := NewMinHeap(10)
		h.Push(&FileHandle{Timestamp: newTime(100)})
		h.Push(&FileHandle{Timestamp: newTime(50)})
		h.Push(&FileHandle{Timestamp: newTime(150)})
		if h.Pop().Timestamp != newTime(50) {
			t.Error("Expected 50")
		}
		if h.Pop().Timestamp != newTime(100) {
			t.Error("Expected 100")
		}
		h.Push(&FileHandle{Timestamp: newTime(75)})
		if h.Pop().Timestamp != newTime(75) {
			t.Error("Expected 75")
		}
		h.Push(&FileHandle{Timestamp: newTime(200)})
		if h.Pop().Timestamp != newTime(150) {
			t.Error("Expected 150")
		}
		if h.Pop().Timestamp != newTime(200) {
			t.Error("Expected 200")
		}
		if h.Pop() != nil {
			t.Error("Expected nil")
		}
	})
}

func newTime(nanos int) Timestamp {
	return NewTimestamp(1970, 1, 1, 0, 0, 0, nanos, 0, 0, 0)
}

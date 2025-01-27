package main_test

import (
	. "github.com/mmdemirbas/logmerge"
	"testing"
)

func TestMinHeap(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		h := NewMinHeap(3)
		h.Push(&FileHandle{Timestamp: newTime(0)})
		h.Push(&FileHandle{Timestamp: newTime(-1)})
		h.Push(&FileHandle{Timestamp: newTime(1)})
		h.Push(&FileHandle{Timestamp: newTime(-2)})
		h.Push(&FileHandle{Timestamp: newTime(2)})

		if h.Pop().Timestamp != newTime(-2) {
			t.Error("Expected -2")
		}
		if h.Pop().Timestamp != newTime(-1) {
			t.Error("Expected -1")
		}
		if h.Pop().Timestamp != newTime(0) {
			t.Error("Expected 0")
		}
		if h.Pop().Timestamp != newTime(1) {
			t.Error("Expected 1")
		}
		if h.Pop().Timestamp != newTime(2) {
			t.Error("Expected 2")
		}
		if h.Pop() != nil {
			t.Error("Expected nil")
		}
	})

	t.Run("2", func(t *testing.T) {
		h := NewMinHeap(3)
		h.Push(&FileHandle{Timestamp: newTime(0)})
		h.Push(&FileHandle{Timestamp: newTime(-1)})
		h.Push(&FileHandle{Timestamp: newTime(1)})
		if h.Pop().Timestamp != newTime(-1) {
			t.Error("Expected -1")
		}
		if h.Pop().Timestamp != newTime(0) {
			t.Error("Expected 0")
		}
		h.Push(&FileHandle{Timestamp: newTime(-2)})
		if h.Pop().Timestamp != newTime(-2) {
			t.Error("Expected -2")
		}
		h.Push(&FileHandle{Timestamp: newTime(2)})
		if h.Pop().Timestamp != newTime(1) {
			t.Error("Expected 1")
		}
		if h.Pop().Timestamp != newTime(2) {
			t.Error("Expected 2")
		}
		if h.Pop() != nil {
			t.Error("Expected nil")
		}
	})
}

func newTime(addMinutes int) Timestamp {
	return NewTimestamp(2025, 1, 1, 0, addMinutes, 0, 0, 0, 0, 0)
}

package core_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/core"
	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
)

func TestMinHeap(t *testing.T) {
	t.Run("1", func(t *testing.T) {
		h := NewMinHeap(10)
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(100)})
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(50)})
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(150)})
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(75)})
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(125)})

		if h.Pop().LineTimestamp != newTime(50) {
			t.Error("Expected 50")
		}
		if h.Pop().LineTimestamp != newTime(75) {
			t.Error("Expected 75")
		}
		if h.Pop().LineTimestamp != newTime(100) {
			t.Error("Expected 100")
		}
		if h.Pop().LineTimestamp != newTime(125) {
			t.Error("Expected 125")
		}
		if h.Pop().LineTimestamp != newTime(150) {
			t.Error("Expected 150")
		}
		if h.Pop() != nil {
			t.Error("Expected nil")
		}
	})

	t.Run("2", func(t *testing.T) {
		h := NewMinHeap(10)
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(100)})
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(50)})
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(150)})
		if h.Pop().LineTimestamp != newTime(50) {
			t.Error("Expected 50")
		}
		if h.Pop().LineTimestamp != newTime(100) {
			t.Error("Expected 100")
		}
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(75)})
		if h.Pop().LineTimestamp != newTime(75) {
			t.Error("Expected 75")
		}
		h.Push(&fsutil.FileHandle{LineTimestamp: newTime(200)})
		if h.Pop().LineTimestamp != newTime(150) {
			t.Error("Expected 150")
		}
		if h.Pop().LineTimestamp != newTime(200) {
			t.Error("Expected 200")
		}
		if h.Pop() != nil {
			t.Error("Expected nil")
		}
	})
}

func TestMinHeap_EqualTimestamps(t *testing.T) {
	h := NewMinHeap(10)
	ts := newTime(42)

	// Push multiple elements with the same timestamp; tag each via Alias.
	fh0 := &fsutil.FileHandle{LineTimestamp: ts, Alias: []byte("f0")}
	fh1 := &fsutil.FileHandle{LineTimestamp: ts, Alias: []byte("f1")}
	fh2 := &fsutil.FileHandle{LineTimestamp: ts, Alias: []byte("f2")}

	h.Push(fh0)
	h.Push(fh1)
	h.Push(fh2)

	// All three should come back; all with the same timestamp.
	seen := make(map[string]bool)
	for i := 0; i < 3; i++ {
		got := h.Pop()
		if got == nil {
			t.Fatalf("unexpected nil at pop %d", i)
		}
		if got.LineTimestamp != ts {
			t.Errorf("expected timestamp %v, got %v", ts, got.LineTimestamp)
		}
		seen[string(got.Alias)] = true
	}

	if len(seen) != 3 {
		t.Errorf("expected 3 distinct elements, got %d", len(seen))
	}
	for _, name := range []string{"f0", "f1", "f2"} {
		if !seen[name] {
			t.Errorf("missing element %s", name)
		}
	}
	if h.Pop() != nil {
		t.Error("expected nil after draining")
	}
}

func TestMinHeap_SingleElement(t *testing.T) {
	h := NewMinHeap(4)
	fh := &fsutil.FileHandle{LineTimestamp: newTime(7)}

	// Before push
	if h.Len() != 0 {
		t.Errorf("expected Len()=0 before push, got %d", h.Len())
	}
	if h.Peek() != nil {
		t.Error("expected Peek()=nil on empty heap")
	}
	if h.Pop() != nil {
		t.Error("expected Pop()=nil on empty heap")
	}

	h.Push(fh)

	if h.Len() != 1 {
		t.Errorf("expected Len()=1 after push, got %d", h.Len())
	}
	if h.Peek() != fh {
		t.Error("expected Peek() to return pushed element")
	}
	got := h.Pop()
	if got != fh {
		t.Error("expected Pop() to return pushed element")
	}
	if h.Len() != 0 {
		t.Errorf("expected Len()=0 after pop, got %d", h.Len())
	}
	if h.Peek() != nil {
		t.Error("expected Peek()=nil after pop")
	}
	if h.Pop() != nil {
		t.Error("expected Pop()=nil after pop")
	}
}

func TestMinHeap_LargeHeap(t *testing.T) {
	const n = 100
	h := NewMinHeap(n)

	// Generate pseudo-random timestamps using a simple LCG
	seed := 12345
	for i := 0; i < n; i++ {
		seed = (seed*1103515245 + 12345) & 0x7FFFFFFF
		ts := newTime(seed % 1000000)
		h.Push(&fsutil.FileHandle{LineTimestamp: ts})
	}

	if h.Len() != n {
		t.Fatalf("expected Len()=%d, got %d", n, h.Len())
	}

	prev := logtime.ZeroTimestamp
	for i := 0; i < n; i++ {
		got := h.Pop()
		if got == nil {
			t.Fatalf("unexpected nil at index %d", i)
		}
		if got.LineTimestamp < prev {
			t.Fatalf("out of order at index %d: %v < %v", i, got.LineTimestamp, prev)
		}
		prev = got.LineTimestamp
	}

	if h.Len() != 0 {
		t.Errorf("expected empty heap, got Len()=%d", h.Len())
	}
}

func newTime(nanos int) logtime.Timestamp {
	return logtime.NewTimestamp(1970, 1, 1, 0, 0, 0, nanos, 0, 0, 0)
}

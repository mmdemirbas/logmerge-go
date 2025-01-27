package main

// MinHeap is a specialized priority queue for FileHandle
type MinHeap struct {
	items []*FileHandle
}

// NewMinHeap returns an empty MinHeap with optional capacity preallocation
func NewMinHeap(capacity int) *MinHeap {
	return &MinHeap{
		items: make([]*FileHandle, 0, capacity),
	}
}

// Push inserts a FileHandle into the heap
func (h *MinHeap) Push(file *FileHandle) {
	h.items = append(h.items, file)
	idx := len(h.items) - 1

	for idx > 0 {
		parent := (idx - 1) / 2
		if h.items[parent].Timestamp < h.items[idx].Timestamp {
			break
		}
		h.items[idx], h.items[parent] = h.items[parent], h.items[idx]
		idx = parent
	}
}

// Pop removes and returns the smallest Timestamp FileHandle
func (h *MinHeap) Pop() *FileHandle {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	// Swap top and last, then shrink slice
	h.items[0], h.items[n-1] = h.items[n-1], h.items[0]
	item := h.items[n-1]
	h.items = h.items[:n-1]

	// Restore heap property
	n--
	if n > 0 {
		idx := 0
		for {
			left := 2*idx + 1
			right := left + 1
			smallest := idx

			// Check left child
			if left < n && h.items[smallest].Timestamp >= h.items[left].Timestamp {
				smallest = left
			}
			// Check right child
			if right < n && h.items[smallest].Timestamp >= h.items[right].Timestamp {
				smallest = right
			}
			if smallest == idx {
				break
			}
			h.items[idx], h.items[smallest] = h.items[smallest], h.items[idx]
			idx = smallest
		}
	}
	return item
}

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

// Len returns the number of items in the heap
func (h *MinHeap) Len() int {
	return len(h.items)
}

// Push inserts a FileHandle into the heap
func (h *MinHeap) Push(file *FileHandle) {
	h.items = append(h.items, file)
	h.siftUp(len(h.items) - 1)
}

// Pop removes and returns the smallest Timestamp FileHandle
func (h *MinHeap) Pop() *FileHandle {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	// Swap top and last, then shrink slice
	h.swap(0, n-1)
	item := h.items[n-1]
	h.items = h.items[:n-1]

	// Restore heap property
	if len(h.items) > 0 {
		h.siftDown(0)
	}
	return item
}

// Top returns the top element without removing it
func (h *MinHeap) Top() *FileHandle {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// siftUp moves the item at idx up until heap order is restored
func (h *MinHeap) siftUp(idx int) {
	for idx > 0 {
		parent := (idx - 1) / 2
		if h.less(parent, idx) {
			break
		}
		h.swap(idx, parent)
		idx = parent
	}
}

// siftDown moves the item at idx down until heap order is restored
func (h *MinHeap) siftDown(idx int) {
	n := len(h.items)
	for {
		left := 2*idx + 1
		right := left + 1
		smallest := idx

		// Check left child
		if left < n && !h.less(smallest, left) {
			smallest = left
		}
		// Check right child
		if right < n && !h.less(smallest, right) {
			smallest = right
		}
		if smallest == idx {
			break
		}
		h.swap(idx, smallest)
		idx = smallest
	}
}

// less reports whether items[i] < items[j]
func (h *MinHeap) less(i, j int) bool {
	return h.items[i].Timestamp < h.items[j].Timestamp
}

// swap exchanges items at i and j
func (h *MinHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

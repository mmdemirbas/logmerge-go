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

		parentItem := h.items[parent]
		childItem := h.items[idx]

		if parentItem.Timestamp <= childItem.Timestamp {
			break
		}

		h.items[idx] = parentItem
		h.items[parent] = childItem
		idx = parent
	}
}

// Pop removes and returns the smallest Timestamp FileHandle
func (h *MinHeap) Pop() *FileHandle {
	n := len(h.items)
	if n == 0 {
		return nil
	}

	item := h.items[0]

	n--
	h.items[0] = h.items[n]
	h.items = h.items[:n]

	// Restore heap property
	if n > 0 {
		idx := 0
		for {
			left := 2*idx + 1
			right := left + 1

			smallest := idx
			smallestItem := h.items[smallest]
			smallestTimestamp := smallestItem.Timestamp

			if left < n {
				leftItem := h.items[left]
				if leftItem.Timestamp < smallestTimestamp {
					smallest = left
					smallestItem = leftItem
					smallestTimestamp = leftItem.Timestamp
				}
			}
			if right < n {
				rightItem := h.items[right]
				if rightItem.Timestamp < smallestTimestamp {
					smallest = right
					smallestItem = rightItem
					smallestTimestamp = rightItem.Timestamp
				}
			}

			if smallest == idx {
				break
			}

			h.items[smallest] = h.items[idx]
			h.items[idx] = smallestItem
			idx = smallest
		}
	}
	return item
}

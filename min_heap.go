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
			smallest := idx
			smallestItem := h.items[smallest]
			smallestTimestamp := smallestItem.Timestamp

			x := 2*idx + 1
			if x < n {
				leftItem := h.items[x]
				if leftItem.Timestamp < smallestTimestamp {
					smallest = x
					smallestItem = leftItem
					smallestTimestamp = leftItem.Timestamp
				}

				x++
				if x < n {
					rightItem := h.items[x]
					if rightItem.Timestamp < smallestTimestamp {
						smallest = x
						smallestItem = rightItem
						smallestTimestamp = rightItem.Timestamp
					}
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

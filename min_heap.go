package main

// MinHeap is a specialized priority queue for FileHandle
type MinHeap struct {
	items []*FileHandle
}

func NewMinHeap(capacity int) *MinHeap {
	return &MinHeap{
		items: make([]*FileHandle, 0, capacity),
	}
}

func (h *MinHeap) Len() int {
	return len(h.items)
}

func (h *MinHeap) Peek() *FileHandle {
	if len(h.items) == 0 {
		return nil
	}
	return h.items[0]
}

// Push inserts a FileHandle into the heap
func (h *MinHeap) Push(file *FileHandle) {
	h.items = append(h.items, file) // grow slice by 1

	index := len(h.items) - 1 // start at the end
	for index > 0 {
		parent := (index - 1) >> 1
		if file.LineTimestamp >= h.items[parent].LineTimestamp {
			// new item is in correct place (not smaller than parent)
			break
		}

		h.items[index] = h.items[parent] // shift parent down
		index = parent                   // move up one level
	}

	h.items[index] = file
}

// Pop removes and returns the smallest Timestamp FileHandle
func (h *MinHeap) Pop() *FileHandle {
	n := len(h.items)
	if n == 0 {
		return nil // empty heap
	}

	root := h.items[0] // smallest item is at root
	if n == 1 {
		h.items = h.items[:0] // clear slice
		return root
	}

	n--                   // length decreased by 1
	last := h.items[n]    // last item which will be re-located
	h.items[n] = nil      // Prevent memory leak by clearing the pointer
	h.items = h.items[:n] // shorten slice by 1

	index := 0               // start at the root
	firstLeafIndex := n >> 1 // index of first leaf node

	for index < firstLeafIndex { // loop until the leaf level
		left := (index << 1) + 1
		right := left + 1

		smallestChild, indexOfSmallestChild := h.items[left], left
		if right < n && h.items[right].LineTimestamp < smallestChild.LineTimestamp {
			smallestChild, indexOfSmallestChild = h.items[right], right
		}

		if last.LineTimestamp <= smallestChild.LineTimestamp {
			break
		}

		h.items[index] = smallestChild
		index = indexOfSmallestChild
	}

	h.items[index] = last
	return root
}

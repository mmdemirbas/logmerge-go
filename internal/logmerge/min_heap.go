package logmerge

type heapEntry struct {
	timestamp Timestamp
	file      *FileHandle
}

// MinHeap is a specialized priority queue for FileHandle
type MinHeap struct {
	entries []heapEntry
}

func NewMinHeap(capacity int) *MinHeap {
	return &MinHeap{
		entries: make([]heapEntry, 0, capacity),
	}
}

func (h *MinHeap) Len() int {
	return len(h.entries)
}

func (h *MinHeap) Peek() *FileHandle {
	if len(h.entries) == 0 {
		return nil
	}
	return h.entries[0].file
}

// Push inserts a FileHandle into the heap
func (h *MinHeap) Push(file *FileHandle) {
	ts := file.LineTimestamp
	h.entries = append(h.entries, heapEntry{timestamp: ts, file: file})

	index := len(h.entries) - 1
	for index > 0 {
		parent := (index - 1) >> 1
		if ts >= h.entries[parent].timestamp {
			// new item is in correct place (not smaller than parent)
			break
		}

		h.entries[index] = h.entries[parent] // shift parent down
		index = parent                       // move up one level
	}

	h.entries[index] = heapEntry{timestamp: ts, file: file}
}

// Pop removes and returns the smallest Timestamp FileHandle
func (h *MinHeap) Pop() *FileHandle {
	n := len(h.entries)
	if n == 0 {
		return nil // empty heap
	}

	root := h.entries[0].file // smallest item is at root
	if n == 1 {
		h.entries = h.entries[:0] // clear slice
		return root
	}

	n--                        // length decreased by 1
	last := h.entries[n]       // last item which will be re-located
	h.entries[n] = heapEntry{} // Prevent memory leak by clearing the pointer
	h.entries = h.entries[:n]  // shorten slice by 1
	h.entries = h.entries[:n]

	index := 0               // start at the root
	firstLeafIndex := n >> 1 // index of first leaf node

	for index < firstLeafIndex { // loop until the leaf level
		left := (index << 1) + 1
		right := left + 1

		smallestChild, indexOfSmallestChild := h.entries[left], left
		if right < n && h.entries[right].timestamp < smallestChild.timestamp {
			smallestChild, indexOfSmallestChild = h.entries[right], right
		}

		if last.timestamp <= smallestChild.timestamp {
			break
		}

		h.entries[index] = smallestChild
		index = indexOfSmallestChild
	}

	h.entries[index] = last
	return root
}

package main

// MinHeap implements a priority queue for LinePrefix
type MinHeap []*LinePrefix

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Timestamp.Before(h[j].Timestamp) }
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*LinePrefix))
}
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h *MinHeap) Peek() *LinePrefix {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}

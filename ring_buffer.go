package main

import "io"

// RingBuffer is a circular buffer that can be used to store a fixed number of bytes.
type RingBuffer struct {
	buf        []byte // buffer
	cap        int    // capacity
	readIndex  int    // start position (inclusive)
	writeIndex int    // end position (exclusive)
}

// NewRingBuffer creates a new RingBuffer which is capable of storing n bytes.
func NewRingBuffer(n int) *RingBuffer {
	if n <= 0 {
		return nil
	}
	bufSize := n + 1 // +1 to differentiate between empty and full buffer
	return &RingBuffer{
		buf:        make([]byte, bufSize),
		cap:        bufSize,
		readIndex:  0,
		writeIndex: 0,
	}
}

// String returns the content of the buffer as a string.
func (r *RingBuffer) String() string {
	if r.IsEmpty() {
		return ""
	}
	if r.writeIndex > r.readIndex {
		return string(r.buf[r.readIndex:r.writeIndex])
	}
	return string(r.buf[r.readIndex:]) + string(r.buf[0:r.writeIndex])
}

// IsEmpty returns true if the buffer is empty.
func (r *RingBuffer) IsEmpty() bool {
	return r.readIndex == r.writeIndex
}

// IsFull returns true if the buffer is full.
func (r *RingBuffer) IsFull() bool {
	return (r.writeIndex+1)%r.cap == r.readIndex
}

// Len returns the number of bytes in the buffer.
func (r *RingBuffer) Len() int {
	return (r.writeIndex - r.readIndex + r.cap) % r.cap
}

// Peek returns the byte at the given index starting from the read position without advancing the read/write positions.
func (r *RingBuffer) Peek(index int) byte {
	return r.buf[(r.readIndex+index)%r.cap]
}

// Skip advances the read position by one byte.
func (r *RingBuffer) Skip(count int) {
	r.readIndex = (r.readIndex + count) % r.cap
}

// Read returns the next byte to be read from the buffer and advances the read position by one byte.
func (r *RingBuffer) Read() byte {
	b := r.Peek(0)
	r.Skip(1)
	return b
}

// Write appends a byte to the buffer and advances the write position by one byte.
func (r *RingBuffer) Write(b byte) {
	r.buf[r.writeIndex] = b
	r.writeIndex = (r.writeIndex + 1) % r.cap
}

// Fill reads data from the reader into the buffer until the buffer is full or the reader returns an error.
func (r *RingBuffer) Fill(reader io.Reader) (count int, err error) {
	singlePart := true
	var end int
	if r.writeIndex < r.readIndex {
		end = r.readIndex - 1
	} else if r.readIndex == 0 {
		end = r.cap - 1
	} else {
		end = r.cap
		singlePart = false
	}

	if end == r.writeIndex {
		return 0, nil // buffer is full
	}

	n, err := reader.Read(r.buf[r.writeIndex:end])
	r.writeIndex = (r.writeIndex + n) % r.cap
	if singlePart || n == 0 || err != nil {
		return n, err // cannot read more data or buffer is full now
	}

	end = r.readIndex - 1
	nn, err := reader.Read(r.buf[0:end])
	r.writeIndex = (r.writeIndex + nn) % r.cap
	return n + nn, err
}

func (r *RingBuffer) WriteLinePartial(writer io.Writer, count *int, crFound *bool, lfFound *bool) (bool, error) {
	if r.IsEmpty() {
		return false, nil // empty buffer
	}

	var searchUntil int
	if r.writeIndex < r.readIndex {
		searchUntil = r.cap
	} else {
		searchUntil = r.writeIndex
	}

	i := r.readIndex

	for ; !*lfFound && i < searchUntil; i++ {
		if r.buf[i] == '\n' {
			*lfFound = true
		} else {
			if *crFound {
				// Single \r without \n
				//break
				*crFound = false
			}
			if r.buf[i] == '\r' {
				*crFound = true
			}
		}
	}

	n, err := writer.Write(r.buf[r.readIndex:i])
	r.readIndex = (r.readIndex + n) % r.cap
	*count += n

	// Second part
	if err == nil && i == r.cap {
		i := 0
		for ; !*lfFound && i < r.writeIndex; i++ {
			if r.buf[i] == '\n' {
				*lfFound = true
			} else {
				if *crFound {
					// Single \r without \n
					//break
					*crFound = false
				}
				if r.buf[i] == '\r' {
					*crFound = true
				}
			}
		}

		n, err = writer.Write(r.buf[0:i])
		r.readIndex = (r.readIndex + n) % r.cap
		*count += n
	}

	return true, err
}

package main

import (
	"io"
)

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
	newReadIndex := (r.readIndex + count) % r.cap
	if newReadIndex == r.writeIndex {
		// Reset buffer if empty for better memory usage
		r.readIndex = 0
		r.writeIndex = 0
	} else {
		r.readIndex = newReadIndex
	}
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
	if r.IsFull() {
		return 0, nil
	}

	var firstPartEnd int
	if r.writeIndex >= r.readIndex {
		if r.readIndex == 0 {
			firstPartEnd = r.cap - 1 // Leave last slot empty
		} else {
			firstPartEnd = r.cap
		}
	} else {
		firstPartEnd = r.readIndex - 1
	}

	n, err := reader.Read(r.buf[r.writeIndex:firstPartEnd])
	count = n
	r.writeIndex = (r.writeIndex + n) % r.cap

	if err == nil && n > 0 &&
		r.writeIndex == 0 && r.readIndex > 1 {
		n, err = reader.Read(r.buf[0 : r.readIndex-1])
		count += n
		r.writeIndex = n
	}

	return count, err
}

// EOLType represents the type of end-of-line character(s) found.
type EOLType int

const (
	None EOLType = iota
	CR
	LF
	CRLF
)

var newline = []byte{'\n'}

// PeekNextLineSlice returns the slice including the first end-of-line character found in the buffer,
// and the type of end-of-line character(s) found. If it couldn't find an end-of-line character
// until the end of the buffer, it returns the slice until the end of the buffer and
// sets the latestCharWasCR flag if the last character was CR.
func (r *RingBuffer) PeekNextLineSlice(latestCharWasCR *bool) ([]byte, EOLType) {
	readIndex := r.readIndex
	writeIndex := r.writeIndex
	buf := r.buf

	if readIndex == writeIndex {
		return nil, None // Buffer is empty
	}

	if *latestCharWasCR {
		if buf[readIndex] == '\n' {
			return newline, CRLF
		} else {
			return nil, CR
		}
	}

	var searchUntil int
	if readIndex < writeIndex {
		searchUntil = writeIndex
	} else {
		searchUntil = r.cap
	}

	i := readIndex
	for ; i < searchUntil; i++ {
		b := buf[i]
		if b == '\r' || b == '\n' {
			break
		}
	}

	if i == searchUntil {
		return buf[readIndex:searchUntil], None
	}
	if buf[i] == '\r' {
		if i+1 == searchUntil {
			*latestCharWasCR = true
			return buf[readIndex:searchUntil], None // EOL could be CR or CRLF
		}
		if buf[i+1] == '\n' {
			return buf[readIndex : i+2], CRLF
		}
		return buf[readIndex : i+1], CR
	}
	return buf[readIndex : i+1], LF
}

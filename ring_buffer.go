package main

import (
	bytes2 "bytes"
	"fmt"
	"io"
	"os"
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
	NIL EOLType = iota
	CR
	LF
	CRLF
)

func (r *RingBuffer) WriteLinePartial(writer io.Writer, count *int, latestCharWasCR *bool) (eolType EOLType, err error) {
	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(os.Stderr, "WriteLinePartial: Recovered from panic: %v\n", r)
			os.Exit(1)
		}
	}()

	if enableDebugLogging {
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "-- WriteLinePartial --\n")
		fmt.Fprintf(os.Stderr, "latestCharWasCR: %v\n", *latestCharWasCR)
		fmt.Fprintf(os.Stderr, "r.readIndex    : %v\n", r.readIndex)
		fmt.Fprintf(os.Stderr, "r.writeIndex   : %v\n", r.writeIndex)
	}

	var writeUntil int

	if *latestCharWasCR {
		if r.buf[r.readIndex] == '\n' {
			writeUntil = r.readIndex + 1
			eolType = CRLF
		} else {
			writeUntil = r.readIndex
			eolType = CR
		}
	} else {
		var searchUntil int
		if r.writeIndex < r.readIndex {
			searchUntil = r.cap
		} else {
			searchUntil = r.writeIndex
		}

		lfIndex := bytes2.IndexByte(r.buf[r.readIndex:searchUntil], '\n')
		if enableDebugLogging {
			fmt.Fprintf(os.Stderr, "searchUntil    : %d\n", searchUntil)
			fmt.Fprintf(os.Stderr, "lfIndex        : %d\n", lfIndex)
		}

		if lfIndex != -1 {
			// LF found in the first part
			crIndex := bytes2.IndexByte(r.buf[r.readIndex:r.readIndex+lfIndex+1], '\r')
			if enableDebugLogging {
				fmt.Fprintf(os.Stderr, "crIndex        : %d\n", crIndex)
			}

			if crIndex == -1 {
				// CR not found before LF, use LF as EOL
				writeUntil = r.readIndex + lfIndex + 1
				eolType = LF
			} else if crIndex+1 == lfIndex {
				// CR found just before LF, use CRLF as EOL
				writeUntil = r.readIndex + lfIndex + 1
				eolType = CRLF
			} else {
				// CR found before LF, use CR as EOL
				writeUntil = r.readIndex + crIndex + 1
				eolType = CR
			}
		} else {
			// LF not found in the first part
			crIndex := bytes2.IndexByte(r.buf[r.readIndex:searchUntil], '\r')
			if enableDebugLogging {
				fmt.Fprintf(os.Stderr, "crIndex        : %d\n", crIndex)
			}

			if crIndex == -1 {
				// CR not found in the first part either, continue searching CR and LF in the second part
				writeUntil = searchUntil
			} else if crIndex+1 == r.cap {
				// CR found just before the end of the first part, just check next char to decide CR vs CRLF as EOL
				writeUntil = r.readIndex + crIndex + 1
				*latestCharWasCR = true
			} else {
				// CR found before the end of the first part, use CR as EOL
				writeUntil = r.readIndex + crIndex + 1
				eolType = CR
			}
		}
	}
	if enableDebugLogging {
		fmt.Fprintf(os.Stderr, "writeUntil     : %d\n", writeUntil)
		fmt.Fprintf(os.Stderr, "latestCharWasCR: %v\n", *latestCharWasCR)
		fmt.Fprintf(os.Stderr, "eolType        : %v\n", eolType)
	}
	if r.readIndex < writeUntil {
		var n int
		n, err = writer.Write(r.buf[r.readIndex:writeUntil])
		r.readIndex = (r.readIndex + n) % r.cap
		*count += n
	}
	return eolType, err
}

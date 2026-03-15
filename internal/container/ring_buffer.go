package container

import (
	bytes2 "bytes"
	"io"
)

// indexCRorLF finds the first '\r' or '\n' in buf, returning its index or -1.
// Optimized to avoid bytes.IndexAny's per-call ASCII bitset construction.
// Searches for '\n' first (SIMD-accelerated, stops early since '\n' is common),
// then only scans the short prefix before it for '\r'.
func indexCRorLF(buf []byte) int {
	lf := bytes2.IndexByte(buf, '\n')
	// Only scan for '\r' in the portion before the '\n' (or the full buffer if no '\n')
	searchLen := lf
	if lf == -1 {
		searchLen = len(buf)
	}
	cr := bytes2.IndexByte(buf[:searchLen], '\r')
	if cr != -1 {
		return cr
	}
	return lf
}

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

	// If buffer is empty, reset read/write index to reduce chance of split buffer into two parts.
	// Buffer split could cause bad performance for other operations.
	// Below operations are optimized using branchless code to avoid slow conditional branches.
	//r.readIndex = (newReadIndex == r.writeIndex) ? 0 : newReadIndex
	//r.writeIndex = (newReadIndex == r.writeIndex) ? 0 : r.writeIndex
	diff := newReadIndex - r.writeIndex
	nonEmpty := (diff >> 31) ^ ((-diff) >> 31)
	r.readIndex = nonEmpty & newReadIndex
	r.writeIndex = nonEmpty & r.writeIndex
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
func (r *RingBuffer) Fill(reader io.Reader) (int, error) {
	if r.IsFull() {
		return 0, nil
	}

	// Realignment: If the buffer is nearly empty or data is split,
	// move existing data to the start to maximize contiguous space.
	if r.readIndex > 0 && (r.IsEmpty() || r.writeIndex < r.readIndex) {
		n := r.Len()
		if n > 0 {
			if r.writeIndex > r.readIndex {
				// Contiguous: [--R===W--]
				copy(r.buf, r.buf[r.readIndex:r.writeIndex])
			} else {
				// Wrapped: [==W---R==]
				tailLen := r.cap - r.readIndex
				copy(r.buf, r.buf[r.readIndex:r.cap])
				copy(r.buf[tailLen:], r.buf[:r.writeIndex])
			}
		}
		r.readIndex = 0
		r.writeIndex = n
	}

	readPartSplit := (r.writeIndex - r.readIndex) >> 31 // W--R-- or --W--R or --W--R--
	readAtZero := (r.readIndex - 1) >> 31               // R--W--
	writePartSplit := ^readPartSplit & ^readAtZero      // --R--W-- or --R--W

	endIndex := readPartSplit&(r.readIndex-1) | readAtZero&(r.cap-1) | writePartSplit&(r.cap)
	n, err := reader.Read(r.buf[r.writeIndex:endIndex])
	count := n

	// Read second part if first part already filled and there is a second part.
	if writePartSplit == -1 && ((r.writeIndex + n) == r.cap) && r.readIndex > 1 && err == nil {
		n, err = reader.Read(r.buf[0 : r.readIndex-1])
		count += n
	}

	r.writeIndex = (r.writeIndex + count) % r.cap
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

// SkipNextLineSlice advances past the next line in the buffer, returning bytes skipped and EOL type.
func (r *RingBuffer) SkipNextLineSlice(latestCharWasCR *bool) (int, EOLType) {
	readIndex := r.readIndex
	writeIndex := r.writeIndex
	buf := r.buf

	if readIndex == writeIndex {
		return 0, None // Buffer is empty
	}

	if *latestCharWasCR {
		*latestCharWasCR = false
		if buf[readIndex] == '\n' {
			r.Skip(1)
			return 1, CRLF
		}
		return 0, CR
	}

	// (readIndex < writeIndex) ? writeIndex : cap (we are sure that readIndex != writeIndex)
	searchUntil := ((readIndex-writeIndex)>>31)&writeIndex | ((writeIndex-readIndex)>>31)&r.cap

	idx := indexCRorLF(buf[readIndex:searchUntil])
	if idx == -1 {
		r.Skip(searchUntil - readIndex)
		return searchUntil - readIndex, None
	}

	i := readIndex + idx
	if buf[i] == '\r' {
		if i+1 == searchUntil {
			*latestCharWasCR = true
			r.Skip(searchUntil - readIndex)
			return searchUntil - readIndex, None // EOL could be CR or CRLF
		}
		if buf[i+1] == '\n' {
			r.Skip(i + 2 - readIndex)
			return i + 2 - readIndex, CRLF
		}
		r.Skip(i + 1 - readIndex)
		return i + 1 - readIndex, CR
	}
	r.Skip(i + 1 - readIndex)
	return i + 1 - readIndex, LF
}

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
		*latestCharWasCR = false
		if buf[readIndex] == '\n' {
			return newline, CRLF
		} else {
			return nil, CR
		}
	}

	// (readIndex < writeIndex) ? writeIndex : cap (we are sure that readIndex != writeIndex)
	searchUntil := ((readIndex-writeIndex)>>31)&writeIndex | ((writeIndex-readIndex)>>31)&r.cap

	idx := indexCRorLF(buf[readIndex:searchUntil])
	if idx == -1 {
		return buf[readIndex:searchUntil], None
	}

	i := readIndex + idx
	if buf[i] == '\r' {
		*latestCharWasCR = i+1 == searchUntil
		if *latestCharWasCR {
			return buf[readIndex:searchUntil], None // EOL could be CR or CRLF
		}
		// if buf[i+1] == '\n' { return buf[readIndex : i+2], CRLF } else { return buf[readIndex : i+1], CR }
		nextIsLF := ((int(buf[i+1]) ^ int('\n')) - 1) >> 31
		return buf[readIndex:(i + 1 - nextIsLF)], EOLType(nextIsLF&0x2 + 1)
	}
	return buf[readIndex : i+1], LF
}

// PeekSlice copies up to cap(buffer) bytes from the buffer into a contiguous slice.
func (r *RingBuffer) PeekSlice(buffer []byte) []byte {
	readIndex := r.readIndex
	capacity := r.cap

	outLen := min(cap(buffer), r.Len())
	tailLen := readIndex + outLen - capacity
	if tailLen <= 0 {
		return r.buf[readIndex : readIndex+outLen]
	}

	buffer = buffer[:outLen]
	copy(buffer, r.buf[readIndex:])
	copy(buffer[(outLen-tailLen):], r.buf[:tailLen])
	return buffer
}

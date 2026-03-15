package container_test

import (
	"bytes"
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/container"
)

var (
	sinkSlice []byte
	sinkByte  byte
	sinkInt   int
	sinkEOL   EOLType
)

// --- PeekSlice benchmarks ---

func BenchmarkPeekSlice_Contiguous(b *testing.B) {
	rb := NewRingBuffer(4096)
	data := []byte("2026-03-13 01:21:22.000000000 [INFO] Simulation log line 174182528200\n")
	rb.Fill(bytes.NewReader(data))
	var peekBuf [250]byte
	var result []byte
	for i := 0; i < b.N; i++ {
		result = rb.PeekSlice(peekBuf[:])
	}
	sinkSlice = result
}

func BenchmarkPeekSlice_Wrapped(b *testing.B) {
	// Create a ring buffer that wraps around
	rb := NewRingBuffer(128)
	// Fill to near end of buffer
	filler := make([]byte, 100)
	rb.Fill(bytes.NewReader(filler))
	rb.Skip(90) // advance read position near end
	// Write new data that wraps
	data := []byte("2026-03-13 01:21:22.000000000 [INFO] Simulation log line 174182528200\n")
	rb.Fill(bytes.NewReader(data))
	var peekBuf [250]byte
	var result []byte
	for i := 0; i < b.N; i++ {
		result = rb.PeekSlice(peekBuf[:])
	}
	sinkSlice = result
}

// --- Skip benchmarks ---

func BenchmarkSkip_Small(b *testing.B) {
	rb := NewRingBuffer(4096)
	data := make([]byte, 4000)
	rb.Fill(bytes.NewReader(data))
	for i := 0; i < b.N; i++ {
		rb.Skip(1)
		if rb.IsEmpty() {
			rb.Fill(bytes.NewReader(data))
		}
	}
}

func BenchmarkSkip_LineLength(b *testing.B) {
	rb := NewRingBuffer(4096)
	data := make([]byte, 4000)
	rb.Fill(bytes.NewReader(data))
	for i := 0; i < b.N; i++ {
		rb.Skip(60)
		if rb.Len() < 60 {
			rb.Fill(bytes.NewReader(data))
		}
	}
}

// --- PeekNextLineSlice benchmarks ---

func BenchmarkPeekNextLineSlice_ShortLine(b *testing.B) {
	rb := NewRingBuffer(4096)
	line := []byte("2026-03-13 01:21:22.000 [INFO] short msg\n")
	// Fill with multiple lines
	var buf bytes.Buffer
	for buf.Len() < 3000 {
		buf.Write(line)
	}
	rb.Fill(&buf)
	var result []byte
	var eol EOLType
	latestCharWasCR := false
	for i := 0; i < b.N; i++ {
		result, eol = rb.PeekNextLineSlice(&latestCharWasCR)
		if result != nil {
			rb.Skip(len(result))
		}
		if rb.Len() < 100 {
			buf.Reset()
			for buf.Len() < 3000 {
				buf.Write(line)
			}
			rb.Fill(&buf)
		}
	}
	sinkSlice, sinkEOL = result, eol
}

func BenchmarkPeekNextLineSlice_LongLine(b *testing.B) {
	rb := NewRingBuffer(8192)
	line := make([]byte, 500)
	for j := range line {
		line[j] = 'x'
	}
	line[499] = '\n'
	var buf bytes.Buffer
	for buf.Len() < 7000 {
		buf.Write(line)
	}
	rb.Fill(&buf)
	var result []byte
	var eol EOLType
	latestCharWasCR := false
	for i := 0; i < b.N; i++ {
		result, eol = rb.PeekNextLineSlice(&latestCharWasCR)
		if result != nil {
			rb.Skip(len(result))
		}
		if rb.Len() < 600 {
			buf.Reset()
			for buf.Len() < 7000 {
				buf.Write(line)
			}
			rb.Fill(&buf)
		}
	}
	sinkSlice, sinkEOL = result, eol
}

// --- Fill benchmarks ---

func BenchmarkFill_1KB(b *testing.B) {
	rb := NewRingBuffer(4096)
	data := make([]byte, 1024)
	b.SetBytes(1024)
	for i := 0; i < b.N; i++ {
		rb.Skip(rb.Len()) // drain
		rb.Fill(bytes.NewReader(data))
	}
}

func BenchmarkFill_1MB(b *testing.B) {
	rb := NewRingBuffer(1024 * 1024)
	data := make([]byte, 1024*1024)
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		rb.Skip(rb.Len())
		rb.Fill(bytes.NewReader(data))
	}
}

// --- Peek benchmarks ---

func BenchmarkPeek_Sequential(b *testing.B) {
	rb := NewRingBuffer(4096)
	data := make([]byte, 4000)
	for j := range data {
		data[j] = byte(j)
	}
	rb.Fill(bytes.NewReader(data))
	var result byte
	for i := 0; i < b.N; i++ {
		result = rb.Peek(i % rb.Len())
	}
	sinkByte = result
}

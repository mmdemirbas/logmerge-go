package container_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/container"
	"github.com/mmdemirbas/logmerge/internal/testutil"
)

func TestRingBuffer(t *testing.T) {
	t.Run("NewRingBuffer", func(t *testing.T) {
		t.Run("-1-cap", func(t *testing.T) {
			r := NewRingBuffer(-1)
			testutil.AssertEquals(t, nil, r)
		})
		t.Run("0-cap", func(t *testing.T) {
			r := NewRingBuffer(0)
			testutil.AssertEquals(t, nil, r)
		})
		t.Run("1-cap", func(t *testing.T) {
			r := NewRingBuffer(1)
			testutil.AssertNotEquals(t, nil, r)
		})
	})

	t.Run("IsEmpty-IsFull-Len", func(t *testing.T) {
		t.Run("new buffer", func(t *testing.T) {
			r := NewRingBuffer(3)
			testutil.AssertEquals(t, "", r.String())
			testutil.AssertEquals(t, true, r.IsEmpty())
			testutil.AssertEquals(t, false, r.IsFull())
			testutil.AssertEquals(t, 0, r.Len())
		})

		t.Run("half-full buffer", func(t *testing.T) {
			r := NewRingBuffer(3)
			r.Write('a')
			testutil.AssertEquals(t, "a", r.String())
			testutil.AssertEquals(t, false, r.IsEmpty())
			testutil.AssertEquals(t, false, r.IsFull())
			testutil.AssertEquals(t, 1, r.Len())
		})

		t.Run("full buffer", func(t *testing.T) {
			r := NewRingBuffer(3)
			r.Write('a')
			r.Write('b')
			r.Write('c')
			testutil.AssertEquals(t, "abc", r.String())
			testutil.AssertEquals(t, false, r.IsEmpty())
			testutil.AssertEquals(t, true, r.IsFull())
			testutil.AssertEquals(t, 3, r.Len())
		})

		t.Run("empty buffer after read", func(t *testing.T) {
			r := NewRingBuffer(3)
			r.Write('a')
			r.Read()
			testutil.AssertEquals(t, "", r.String())
			testutil.AssertEquals(t, true, r.IsEmpty())
			testutil.AssertEquals(t, false, r.IsFull())
			testutil.AssertEquals(t, 0, r.Len())
		})

		t.Run("half-full buffer after read", func(t *testing.T) {
			r := NewRingBuffer(3)
			r.Write('a')
			r.Write('b')
			r.Read()
			r.Write('c')
			testutil.AssertEquals(t, "bc", r.String())
			testutil.AssertEquals(t, false, r.IsEmpty())
			testutil.AssertEquals(t, false, r.IsFull())
			testutil.AssertEquals(t, 2, r.Len())
		})

		t.Run("full buffer after read", func(t *testing.T) {
			r := NewRingBuffer(3)
			r.Write('a')
			r.Write('b')
			r.Write('c')
			r.Read()
			r.Write('d')
			testutil.AssertEquals(t, "bcd", r.String())
			testutil.AssertEquals(t, false, r.IsEmpty())
			testutil.AssertEquals(t, true, r.IsFull())
			testutil.AssertEquals(t, 3, r.Len())
		})
	})

	t.Run("Peek-Skip", func(t *testing.T) {
		r := NewRingBuffer(3)
		r.Write('a')
		r.Write('b')
		testutil.AssertEquals(t, "ab", r.String())

		testutil.AssertEquals(t, byte('a'), r.Peek(0))
		testutil.AssertEquals(t, byte('b'), r.Peek(1))
		testutil.AssertEquals(t, "ab", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 2, r.Len())

		r.Write('c')
		testutil.AssertEquals(t, "abc", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, true, r.IsFull())
		testutil.AssertEquals(t, 3, r.Len())

		testutil.AssertEquals(t, byte('a'), r.Peek(0))
		testutil.AssertEquals(t, byte('b'), r.Peek(1))
		testutil.AssertEquals(t, byte('c'), r.Peek(2))
		testutil.AssertEquals(t, "abc", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, true, r.IsFull())
		testutil.AssertEquals(t, 3, r.Len())

		r.Skip(1)
		testutil.AssertEquals(t, "bc", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 2, r.Len())

		testutil.AssertEquals(t, byte('b'), r.Peek(0))
		testutil.AssertEquals(t, byte('c'), r.Peek(1))
		testutil.AssertEquals(t, "bc", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 2, r.Len())

		r.Skip(1)
		testutil.AssertEquals(t, "c", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 1, r.Len())

		testutil.AssertEquals(t, byte('c'), r.Peek(0))
		testutil.AssertEquals(t, "c", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 1, r.Len())

		r.Skip(1)
		testutil.AssertEquals(t, "", r.String())
		testutil.AssertEquals(t, true, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 0, r.Len())

		r.Write('d')
		testutil.AssertEquals(t, "d", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 1, r.Len())

		testutil.AssertEquals(t, byte('d'), r.Peek(0))
		testutil.AssertEquals(t, "d", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 1, r.Len())

		r.Skip(1)
		testutil.AssertEquals(t, "", r.String())
		testutil.AssertEquals(t, true, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 0, r.Len())
	})

	t.Run("Read-Write", func(t *testing.T) {
		r := NewRingBuffer(3)
		r.Write('a')
		r.Write('b')
		r.Write('c')
		testutil.AssertEquals(t, "abc", r.String())

		testutil.AssertEquals(t, byte('a'), r.Read())
		testutil.AssertEquals(t, "bc", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 2, r.Len())

		testutil.AssertEquals(t, byte('b'), r.Read())
		testutil.AssertEquals(t, "c", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 1, r.Len())

		testutil.AssertEquals(t, byte('c'), r.Read())
		testutil.AssertEquals(t, "", r.String())
		testutil.AssertEquals(t, true, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 0, r.Len())

		r.Write('d')
		testutil.AssertEquals(t, "d", r.String())
		testutil.AssertEquals(t, false, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 1, r.Len())

		testutil.AssertEquals(t, byte('d'), r.Read())
		testutil.AssertEquals(t, "", r.String())
		testutil.AssertEquals(t, true, r.IsEmpty())
		testutil.AssertEquals(t, false, r.IsFull())
		testutil.AssertEquals(t, 0, r.Len())
	})

	t.Run("Fill", func(t *testing.T) {
		t.Run("new buffer", func(t *testing.T) {
			t.Run("empty reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				n, err := r.Fill(strings.NewReader(""))
				testutil.AssertEquals(t, "", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, io.EOF, err)
			})
			t.Run("shorter reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				n, err := r.Fill(strings.NewReader("ab"))
				testutil.AssertEquals(t, "ab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("equal reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				n, err := r.Fill(strings.NewReader("abc"))
				testutil.AssertEquals(t, "abc", r.String())
				testutil.AssertEquals(t, 3, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("longer reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				n, err := r.Fill(strings.NewReader("abcdef"))
				testutil.AssertEquals(t, "abc", r.String())
				testutil.AssertEquals(t, 3, n)
				testutil.AssertEquals(t, nil, err)
			})
		})
		t.Run("half-full buffer", func(t *testing.T) {
			t.Run("empty reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				n, err := r.Fill(strings.NewReader(""))
				testutil.AssertEquals(t, "x", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, io.EOF, err)
			})
			t.Run("shorter reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				n, err := r.Fill(strings.NewReader("ab"))
				testutil.AssertEquals(t, "xab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("equal reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				n, err := r.Fill(strings.NewReader("abc"))
				testutil.AssertEquals(t, "xab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("longer reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				n, err := r.Fill(strings.NewReader("abcdef"))
				testutil.AssertEquals(t, "xab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
		})
		t.Run("full buffer", func(t *testing.T) {
			t.Run("empty reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				n, err := r.Fill(strings.NewReader(""))
				testutil.AssertEquals(t, "xyz", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("shorter reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				n, err := r.Fill(strings.NewReader("ab"))
				testutil.AssertEquals(t, "xyz", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("equal reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				n, err := r.Fill(strings.NewReader("abc"))
				testutil.AssertEquals(t, "xyz", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("longer reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				n, err := r.Fill(strings.NewReader("abcdef"))
				testutil.AssertEquals(t, "xyz", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
		})
		t.Run("full buffer after read", func(t *testing.T) {
			t.Run("empty reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				n, err := r.Fill(strings.NewReader(""))
				testutil.AssertEquals(t, "yzt", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("shorter reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				n, err := r.Fill(strings.NewReader("ab"))
				testutil.AssertEquals(t, "yzt", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("equal reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				n, err := r.Fill(strings.NewReader("abc"))
				testutil.AssertEquals(t, "yzt", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("longer reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				n, err := r.Fill(strings.NewReader("abcdef"))
				testutil.AssertEquals(t, "yzt", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, nil, err)
			})
		})
		t.Run("half-full buffer after read", func(t *testing.T) {
			t.Run("empty reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader(""))
				testutil.AssertEquals(t, "t", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, io.EOF, err)
			})
			t.Run("shorter reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader("ab"))
				testutil.AssertEquals(t, "tab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("equal reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader("abc"))
				testutil.AssertEquals(t, "tab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("longer reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader("abcdef"))
				testutil.AssertEquals(t, "tab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
		})
		t.Run("empty buffer after read", func(t *testing.T) {
			t.Run("empty reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader(""))
				testutil.AssertEquals(t, "", r.String())
				testutil.AssertEquals(t, 0, n)
				testutil.AssertEquals(t, io.EOF, err)
			})
			t.Run("shorter reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader("ab"))
				testutil.AssertEquals(t, "ab", r.String())
				testutil.AssertEquals(t, 2, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("equal reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader("abc"))
				testutil.AssertEquals(t, "abc", r.String())
				testutil.AssertEquals(t, 3, n)
				testutil.AssertEquals(t, nil, err)
			})
			t.Run("longer reader", func(t *testing.T) {
				r := NewRingBuffer(3)
				r.Write('x')
				r.Write('y')
				r.Write('z')
				r.Read()
				r.Write('t')
				r.Read()
				r.Read()
				r.Read()
				n, err := r.Fill(strings.NewReader("abcdef"))
				testutil.AssertEquals(t, "abc", r.String())
				testutil.AssertEquals(t, 3, n)
				testutil.AssertEquals(t, nil, err)
			})
		})
	})

	t.Run("EOL Parsing", func(t *testing.T) {
		t.Run("Empty Buffer", func(t *testing.T) {
			r := NewRingBuffer(5)
			latestCR := false
			slice, eol := r.PeekNextLineSlice(&latestCR)
			testutil.AssertEquals(t, []byte(nil), slice)
			testutil.AssertEquals(t, None, eol)

			n, eolSkip := r.SkipNextLineSlice(&latestCR)
			testutil.AssertEquals(t, 0, n)
			testutil.AssertEquals(t, None, eolSkip)
		})

		t.Run("No EOL", func(t *testing.T) {
			r := NewRingBuffer(5)
			r.Write('a')
			r.Write('b')
			latestCR := false
			slice, eol := r.PeekNextLineSlice(&latestCR)
			testutil.AssertEquals(t, []byte("ab"), slice)
			testutil.AssertEquals(t, None, eol)

			n, eolSkip := r.SkipNextLineSlice(&latestCR)
			testutil.AssertEquals(t, 2, n)
			testutil.AssertEquals(t, None, eolSkip)
			testutil.AssertEquals(t, 0, r.Len())
		})

		t.Run("LF only", func(t *testing.T) {
			r := NewRingBuffer(5)
			r.Write('a')
			r.Write('\n')
			r.Write('b')
			latestCR := false
			slice, eol := r.PeekNextLineSlice(&latestCR)
			testutil.AssertEquals(t, []byte("a\n"), slice)
			testutil.AssertEquals(t, LF, eol)

			n, eolSkip := r.SkipNextLineSlice(&latestCR)
			testutil.AssertEquals(t, 2, n)
			testutil.AssertEquals(t, LF, eolSkip)
			testutil.AssertEquals(t, 1, r.Len())
		})

		t.Run("CR only", func(t *testing.T) {
			r := NewRingBuffer(5)
			r.Write('a')
			r.Write('\r')
			r.Write('b')
			latestCR := false
			slice, eol := r.PeekNextLineSlice(&latestCR)
			testutil.AssertEquals(t, []byte("a\r"), slice)
			testutil.AssertEquals(t, CR, eol)

			n, eolSkip := r.SkipNextLineSlice(&latestCR)
			testutil.AssertEquals(t, 2, n)
			testutil.AssertEquals(t, CR, eolSkip)
			testutil.AssertEquals(t, 1, r.Len())
		})

		t.Run("CRLF", func(t *testing.T) {
			r := NewRingBuffer(5)
			r.Write('a')
			r.Write('\r')
			r.Write('\n')
			latestCR := false
			slice, eol := r.PeekNextLineSlice(&latestCR)
			testutil.AssertEquals(t, []byte("a\r\n"), slice)
			testutil.AssertEquals(t, CRLF, eol)
		})

		t.Run("Split CRLF", func(t *testing.T) {
			r := NewRingBuffer(5)
			latestCR := true
			r.Write('\n')
			slice, eol := r.PeekNextLineSlice(&latestCR)
			testutil.AssertEquals(t, []byte("\n"), slice)
			testutil.AssertEquals(t, CRLF, eol)
			testutil.AssertEquals(t, false, latestCR)
		})
	})

	t.Run("PeekSlice", func(t *testing.T) {
		r := NewRingBuffer(5)
		r.Write('a')
		r.Write('b')
		r.Write('c')

		buf := make([]byte, 5)
		res := r.PeekSlice(buf)
		testutil.AssertEquals(t, []byte("abc"), res)
		testutil.AssertEquals(t, 3, r.Len())
	})
}

// errReader returns data and an error simultaneously on the first Read,
// simulating a reader that partially succeeds before encountering an error.
type errReader struct {
	data string
	err  error
	read bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.read {
		return 0, r.err
	}
	r.read = true
	n := copy(p, r.data)
	return n, r.err
}

func TestFill_ReaderError(t *testing.T) {
	customErr := errors.New("read failure")
	reader := &errReader{data: "ab", err: customErr}

	r := NewRingBuffer(10)
	n, err := r.Fill(reader)

	testutil.AssertEquals(t, customErr, err)
	// Data from the read should be kept despite the error
	testutil.AssertEquals(t, "ab", r.String())
	testutil.AssertEquals(t, 2, n)
}

func TestFill_WrappedBufferRealignment(t *testing.T) {
	// Create a wrapped buffer state where writeIndex < readIndex, then fill.
	// Buffer capacity 6 (internal cap 7).
	r := NewRingBuffer(6)
	// Write 6 bytes to fill: [a b c d e f _], readIndex=0, writeIndex=6
	r.Write('a')
	r.Write('b')
	r.Write('c')
	r.Write('d')
	r.Write('e')
	r.Write('f')
	// Read 4: [_ _ _ _ e f _], readIndex=4, writeIndex=6
	r.Read()
	r.Read()
	r.Read()
	r.Read()
	testutil.AssertEquals(t, "ef", r.String())
	// Write 1 to wrap: [g _ _ _ e f _] -> internal: writeIndex wraps to 0
	// Actually: buf[6]='g' => writeIndex=(6+1)%7=0 => [g _ _ _ e f _], readIndex=4, writeIndex=0
	// Wait, that's not right. Let me re-check: cap=7, buf indices 0-6.
	// After reads: readIndex=4, writeIndex=6. Write 'g': buf[6]='g', writeIndex=0.
	// So buffer is wrapped: [_ _ _ _ e f g] with readIndex=4, writeIndex=0.
	// That's wrong, writeIndex=0 means empty... no, IsFull checks (w+1)%cap==r.
	// Actually writeIndex=0, readIndex=4. Len=(0-4+7)%7=3. String="efg".
	r.Write('g')
	testutil.AssertEquals(t, "efg", r.String())
	testutil.AssertEquals(t, 3, r.Len())

	// Now we have a wrapped state: readIndex=4, writeIndex=0 (writeIndex < readIndex).
	// Fill should realign data to position 0, then append new data.
	// After realignment: [e f g _ _ _ _], readIndex=0, writeIndex=3
	// Reader provides "xy", should append: [e f g x y _ _]
	n, err := r.Fill(strings.NewReader("xy"))

	testutil.AssertEquals(t, nil, err)
	testutil.AssertEquals(t, 2, n)
	testutil.AssertEquals(t, "efgxy", r.String())
	testutil.AssertEquals(t, 5, r.Len())
}

func TestFill_WrappedBufferRealignment_WithHeadData(t *testing.T) {
	// Reproduce a wrapped buffer where writeIndex > 0 (head data exists).
	// This exercises the realignment path where the tail copy could
	// overwrite head data if done incorrectly.
	r := NewRingBuffer(6) // cap=7, indices 0-6
	// Write 6 bytes: [a b c d e f _], readIndex=0, writeIndex=6
	for _, b := range []byte("abcdef") {
		r.Write(b)
	}
	// Read 5: [_ _ _ _ _ f _], readIndex=5, writeIndex=6
	for range 5 {
		r.Read()
	}
	testutil.AssertEquals(t, "f", r.String())
	// Write 2 more to wrap: buf[6]='g' (writeIndex→0), buf[0]='h' (writeIndex→1)
	// State: [h _ _ _ _ f g], readIndex=5, writeIndex=1, data="fgh"
	r.Write('g')
	r.Write('h')
	testutil.AssertEquals(t, "fgh", r.String())
	testutil.AssertEquals(t, 3, r.Len())

	// Fill triggers realignment. The tail is buf[5:7]="fg", head is buf[0:1]="h".
	// Correct result after realignment: [f g h _ _ _ _]
	// Bug: if tail copy overwrites head, we'd get [f g f _ _ _ _] (corrupted!)
	n, err := r.Fill(strings.NewReader("XY"))

	testutil.AssertEquals(t, nil, err)
	testutil.AssertEquals(t, 2, n)
	testutil.AssertEquals(t, "fghXY", r.String()) // Would be "fgfXY" if buggy
	testutil.AssertEquals(t, 5, r.Len())
}

func TestSkipNextLineSlice_CRAtBufferEnd(t *testing.T) {
	// Create a buffer where CR is the last byte before the buffer's contiguous end.
	// Use capacity 5 (internal cap 6). Write "abc\r" to positions 0-3.
	r := NewRingBuffer(5)
	r.Write('a')
	r.Write('b')
	r.Write('c')
	r.Write('\r')

	latestCR := false

	// First call: should return everything up to and including CR.
	// Since CR is at the end of contiguous data, we don't know if it's CR or CRLF yet.
	// So eol should be None and latestCharWasCR should be set to true.
	n, eol := r.SkipNextLineSlice(&latestCR)
	testutil.AssertEquals(t, 4, n)
	testutil.AssertEquals(t, None, eol)
	testutil.AssertEquals(t, true, latestCR)

	// Now add more data that is NOT LF
	r.Write('x')

	// Next call: latestCR is true, next byte is 'x' (not LF), so EOL is CR
	n2, eol2 := r.SkipNextLineSlice(&latestCR)
	testutil.AssertEquals(t, 0, n2)
	testutil.AssertEquals(t, CR, eol2)
	testutil.AssertEquals(t, false, latestCR)
}

func TestSkipNextLineSlice_CRAtEOF(t *testing.T) {
	// Buffer containing "hello\r" with no more data.
	r := NewRingBuffer(10)
	for _, b := range []byte("hello\r") {
		r.Write(b)
	}

	latestCR := false

	// Skip should consume all 6 bytes, set latestCharWasCR=true, return None
	n, eol := r.SkipNextLineSlice(&latestCR)
	testutil.AssertEquals(t, 6, n)
	testutil.AssertEquals(t, None, eol)
	testutil.AssertEquals(t, true, latestCR)
	testutil.AssertEquals(t, true, r.IsEmpty())

	// Buffer is now empty. With latestCR=true but nothing in buffer,
	// the CR should be treated as a standalone CR line ending.
	n2, eol2 := r.SkipNextLineSlice(&latestCR)
	testutil.AssertEquals(t, 0, n2)
	testutil.AssertEquals(t, CR, eol2)
	testutil.AssertEquals(t, false, latestCR)
}

func TestPeekSlice_WrappedBuffer(t *testing.T) {
	// Create a wrapped buffer: write data, read some, write more to wrap around.
	r := NewRingBuffer(5) // internal cap 6
	// Fill: [a b c d e _], readIndex=0, writeIndex=5
	r.Write('a')
	r.Write('b')
	r.Write('c')
	r.Write('d')
	r.Write('e')

	// Read 3: [_ _ _ d e _], readIndex=3, writeIndex=5
	r.Read()
	r.Read()
	r.Read()
	testutil.AssertEquals(t, "de", r.String())

	// Write 2 more to wrap: [g _ _ d e f], readIndex=3, writeIndex=1
	r.Write('f')
	r.Write('g')
	testutil.AssertEquals(t, "defg", r.String())
	testutil.AssertEquals(t, 4, r.Len())

	// PeekSlice should return all data including wrapped portion
	buf := make([]byte, 6)
	res := r.PeekSlice(buf)
	testutil.AssertEquals(t, []byte("defg"), res)
	testutil.AssertEquals(t, 4, r.Len()) // PeekSlice should not consume data
}

// partialReader returns fewer bytes than the buffer space available.
type partialReader struct {
	data    string
	offset  int
	maxRead int // max bytes to return per Read call
}

func (r *partialReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	end := r.offset + r.maxRead
	if end > len(r.data) {
		end = len(r.data)
	}
	if end-r.offset > len(p) {
		end = r.offset + len(p)
	}
	n := copy(p, r.data[r.offset:end])
	r.offset += n
	return n, nil
}

func TestFill_PartialRead(t *testing.T) {
	tests := []struct {
		name        string
		bufCap      int
		readerData  string
		maxPerRead  int
		wantContent string
		wantN       int
	}{
		{
			name:        "reader returns 1 byte at a time",
			bufCap:      10,
			readerData:  "hello",
			maxPerRead:  1,
			wantContent: "hello",
			wantN:       5,
		},
		{
			name:        "reader returns 2 bytes at a time into large buffer",
			bufCap:      10,
			readerData:  "abcdef",
			maxPerRead:  2,
			wantContent: "abcdef",
			wantN:       6,
		},
		{
			name:        "reader returns fewer than buffer space but more than one byte",
			bufCap:      8,
			readerData:  "xyz",
			maxPerRead:  2,
			wantContent: "xyz",
			wantN:       3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRingBuffer(tt.bufCap)
			reader := &partialReader{data: tt.readerData, maxRead: tt.maxPerRead}

			totalN := 0
			for {
				n, err := r.Fill(reader)
				totalN += n
				if err != nil || r.IsFull() {
					break
				}
			}

			testutil.AssertEquals(t, tt.wantContent, r.String())
			testutil.AssertEquals(t, tt.wantN, totalN)
		})
	}
}

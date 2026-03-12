package main

type BufferedWriter struct {
	File   *WritableFile
	Buffer []byte
}

func NewBufferedWriter(file *WritableFile, bufferSize int) *BufferedWriter {
	buffer := make([]byte, bufferSize)
	buffer = buffer[:0] // reset the buffer
	return &BufferedWriter{
		File:   file,
		Buffer: buffer,
	}
}

// Write writes data to buffer first, then to file if buffer is full avoiding memory allocations
func (w *BufferedWriter) Write(data []byte) (int, error) {
	bufLen := len(w.Buffer)
	dataLen := len(data)
	capacity := cap(w.Buffer)

	// Append data to buffer if it has capacity
	if bufLen+dataLen <= capacity {
		w.Buffer = append(w.Buffer, data...)
		return dataLen, nil
	}

	// Write buffer to file if it has data
	if bufLen == 0 {
		n, err := w.File.Write(data)
		return n, err
	}

	// Write buffer to file
	{
		_, err := w.File.Write(w.Buffer)
		if err != nil {
			return 0, err
		}
	}

	// Reset buffer
	w.Buffer = w.Buffer[:0]

	// Write data to buffer if it has capacity
	if dataLen < capacity {
		w.Buffer = append(w.Buffer, data...)
		return dataLen, nil
	}

	// Write data to file directly if it doesn't fit in buffer
	return w.File.Write(data)
}

func (w *BufferedWriter) Flush() error {
	if len(w.Buffer) == 0 {
		return nil
	}

	_, err := w.File.Write(w.Buffer)
	w.Buffer = w.Buffer[:0] // reset the buffer
	return err
}

func (w *BufferedWriter) Close() error {
	err := w.Flush()
	if err != nil {
		return err
	}
	return w.File.Close()
}

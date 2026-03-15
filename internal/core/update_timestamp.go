package core

import (
	"fmt"
	"time"

	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
)

// UpdateTimestamp reads the next line's prefix from file's buffer and parses
// a timestamp, setting file.LineTimestamp and file.LineTimestampParsed.
// When computeStripPositions is true, it also computes the prefix/trailing
// delimiter boundaries for timestamp stripping.
func UpdateTimestamp(c *logtime.ParseTimestampConfig, file *fsutil.FileHandle, computeStripPositions bool) error {
	mt := file.Metrics // cache nil check

	bufLen := file.Buffer.Len()
	if bufLen < c.TimestampSearchEndIndex {
		var startTime time.Time
		if mt != nil {
			startTime = mt.Start("FillBuffer")
		}
		err := file.FillBuffer()
		if err != nil {
			file.LineTimestampParsed = false
			file.LineTimestamp = logtime.ZeroTimestamp
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
		if mt != nil {
			mt.Stop(startTime)
		}

		if bufLen == 0 && file.Buffer.IsEmpty() {
			file.LineTimestampParsed = false
			file.LineTimestamp = logtime.ZeroTimestamp
			return nil
		}
	}

	// Use PeekSlice to get a contiguous view of the buffer data.
	// Stack-allocated array avoids heap allocation; 250 bytes covers all
	// real-world timestamp formats (timestamps are at the start of lines).
	var startTime time.Time
	if mt != nil {
		startTime = mt.Start("BufferAsSlice")
	}
	var peekBuf [250]byte
	buf := file.Buffer.PeekSlice(peekBuf[:])
	if mt != nil {
		mt.Stop(startTime)
	}

	var timestamp logtime.Timestamp
	if computeStripPositions {
		var tsStart, tsEnd int
		timestamp, tsStart, tsEnd = logtime.ParseTimestampForStrip(c, buf)
		file.LineTimestampStart = tsStart
		file.LineTimestampEnd = tsEnd
	} else {
		var tsEnd int
		timestamp, tsEnd = logtime.ParseTimestampWithEnd(c, buf)
		file.LineTimestampEnd = tsEnd
	}
	file.LineTimestampParsed = true
	file.LineTimestamp = timestamp
	return nil
}

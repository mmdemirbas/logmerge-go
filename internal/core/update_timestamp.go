package core

import (
	"fmt"

	"github.com/mmdemirbas/logmerge/internal/fsutil"
	"github.com/mmdemirbas/logmerge/internal/logtime"
)

// UpdateTimestamp reads the next line's prefix from file's buffer and parses
// a timestamp, setting file.LineTimestamp and file.LineTimestampParsed.
func UpdateTimestamp(c *logtime.ParseTimestampConfig, file *fsutil.FileHandle) error {
	bufLen := file.Buffer.Len()
	if bufLen < c.TimestampSearchEndIndex {
		startTime := file.Metrics.Start("FillBuffer")
		err := file.FillBuffer()
		if err != nil {
			file.LineTimestampParsed = false
			file.LineTimestamp = logtime.ZeroTimestamp
			return fmt.Errorf("failed to fill buffer: %v", err)
		}
		file.Metrics.Stop(startTime)

		if bufLen == 0 && file.Buffer.IsEmpty() {
			file.LineTimestampParsed = false
			file.LineTimestamp = logtime.ZeroTimestamp
			return nil
		}
	}

	startTime := file.Metrics.Start("BufferAsSlice")
	var latestCharWasCR bool
	buf, _ := file.Buffer.PeekNextLineSlice(&latestCharWasCR)
	file.Metrics.Stop(startTime)

	timestamp := logtime.ParseTimestamp(c, buf)
	file.LineTimestampParsed = true
	file.LineTimestamp = timestamp
	return nil
}

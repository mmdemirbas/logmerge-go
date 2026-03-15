package metrics

// MergeMetrics tracks byte and line counts during the merge process.
type MergeMetrics struct {

	// Byte count stats

	BytesRead                      int64
	BytesReadAndSkipped            int64
	BytesNotRead                   int64
	BytesWrittenForTimestamps      int64
	BytesWrittenForAliasPerLine    int64
	BytesWrittenForAliasPerBlock   int64
	BytesWrittenForRawData         int64
	BytesWrittenForMissingNewlines int64

	// Line count stats

	LinesRead              int64
	LinesReadAndSkipped    int64
	LinesWithTimestamps    int64
	LinesWithoutTimestamps int64
	LineLengths            *BucketMetric
	SkippedLineCounts      *BucketMetric
	SuccessiveLineCounts   *BucketMetric
	BlockLineCounts        *BucketMetric
}

// Merge aggregates counters from another MergeMetrics into this one.
func (m *MergeMetrics) Merge(other *MergeMetrics) {
	m.BytesRead += other.BytesRead
	m.BytesReadAndSkipped += other.BytesReadAndSkipped
	m.BytesNotRead += other.BytesNotRead
	m.BytesWrittenForTimestamps += other.BytesWrittenForTimestamps
	m.BytesWrittenForAliasPerLine += other.BytesWrittenForAliasPerLine
	m.BytesWrittenForAliasPerBlock += other.BytesWrittenForAliasPerBlock
	m.BytesWrittenForRawData += other.BytesWrittenForRawData
	m.BytesWrittenForMissingNewlines += other.BytesWrittenForMissingNewlines
	m.LinesRead += other.LinesRead
	m.LinesReadAndSkipped += other.LinesReadAndSkipped
	m.LinesWithTimestamps += other.LinesWithTimestamps
	m.LinesWithoutTimestamps += other.LinesWithoutTimestamps
	m.LineLengths.Merge(other.LineLengths)
	m.SkippedLineCounts.Merge(other.SkippedLineCounts)
	m.SuccessiveLineCounts.Merge(other.SuccessiveLineCounts)
	m.BlockLineCounts.Merge(other.BlockLineCounts)
}

// NewMergeMetrics creates a MergeMetrics with initialized bucket histograms.
func NewMergeMetrics() *MergeMetrics {
	return &MergeMetrics{
		LineLengths:          NewBucketMetric("LineLengths", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000),
		SkippedLineCounts:    NewBucketMetric("SkippedLineCounts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
		SuccessiveLineCounts: NewBucketMetric("SuccessiveLineCounts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
		BlockLineCounts:      NewBucketMetric("BlockLineCounts", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90, 100),
	}
}

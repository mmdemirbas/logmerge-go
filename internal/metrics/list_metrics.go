package metrics

// ListFilesMetrics tracks statistics from the file discovery phase.
type ListFilesMetrics struct {
	DirsScanned  int64
	FilesScanned int64
	FilesMatched int64
	MatchedFiles []string
}

// NewListFilesMetrics returns a zero-valued ListFilesMetrics ready for use.
func NewListFilesMetrics() *ListFilesMetrics {
	return &ListFilesMetrics{
		DirsScanned:  0,
		FilesScanned: 0,
		FilesMatched: 0,
		MatchedFiles: make([]string, 0),
	}
}

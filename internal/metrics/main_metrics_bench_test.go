package metrics_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/metrics"
)

func BenchmarkUpdateBucketCount_SmallValue(b *testing.B) {
	bm := NewBucketMetric("test", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 50, 100)
	for i := 0; i < b.N; i++ {
		bm.UpdateBucketCount(5)
	}
}

func BenchmarkUpdateBucketCount_LargeValue(b *testing.B) {
	bm := NewBucketMetric("test", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 50, 100)
	for i := 0; i < b.N; i++ {
		bm.UpdateBucketCount(75)
	}
}

func BenchmarkUpdateBucketCount_LineLengths(b *testing.B) {
	// Use the actual LineLengths bucket config from NewMergeMetrics
	bm := NewBucketMetric("LineLengths",
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 40, 50, 60, 70, 80, 90, 100,
		150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900, 1000,
		2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000,
		20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)
	for i := 0; i < b.N; i++ {
		bm.UpdateBucketCount(60) // typical log line length
	}
}

func BenchmarkMetricsTree_StartStop(b *testing.B) {
	m := NewMetricsTree()
	m.Enabled = true
	for i := 0; i < b.N; i++ {
		start := m.Start("FillBuffer")
		m.Stop(start)
	}
}

func BenchmarkMetricsTree_StartStop_Disabled(b *testing.B) {
	m := NewMetricsTree()
	m.Enabled = false
	for i := 0; i < b.N; i++ {
		start := m.Start("FillBuffer")
		m.Stop(start)
	}
}

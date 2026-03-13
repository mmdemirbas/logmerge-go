package logmerge_test

import (
	"testing"

	. "github.com/mmdemirbas/logmerge/internal/logmerge"
)

func TestMetricsTree_StartStop(t *testing.T) {
	tree := NewMetricsTree()
	tree.Enabled = true

	start := tree.Start("operation1")
	elapsed := tree.Stop(start)

	if elapsed < 0 {
		t.Errorf("expected non-negative elapsed, got %d", elapsed)
	}
}

func TestMetricsTree_StartStop_Disabled(t *testing.T) {
	tree := NewMetricsTree()
	// Enabled defaults to false

	start := tree.Start("operation1")
	elapsed := tree.Stop(start)

	if elapsed != 0 {
		t.Errorf("expected 0 elapsed when disabled, got %d", elapsed)
	}
}

func TestMetricsTree_NestedContexts(t *testing.T) {
	tree := NewMetricsTree()
	tree.Enabled = true

	outer := tree.Start("outer")
	inner := tree.Start("inner")
	tree.Stop(inner)
	tree.Stop(outer)

	// Verify the tree has the expected structure
	root := tree.Root
	if len(root.Children) != 1 {
		t.Fatalf("expected 1 child of root, got %d", len(root.Children))
	}
	outerNode := root.Children[0]
	if outerNode.Metric.Name != "outer" {
		t.Errorf("expected child name 'outer', got %q", outerNode.Metric.Name)
	}
	if len(outerNode.Children) != 1 {
		t.Fatalf("expected 1 child of outer, got %d", len(outerNode.Children))
	}
	if outerNode.Children[0].Metric.Name != "inner" {
		t.Errorf("expected nested child name 'inner', got %q", outerNode.Children[0].Metric.Name)
	}
}

func TestMetricsTree_RepeatedCallsMerge(t *testing.T) {
	tree := NewMetricsTree()
	tree.Enabled = true

	for i := 0; i < 5; i++ {
		s := tree.Start("repeated")
		tree.Stop(s)
	}

	if len(tree.Root.Children) != 1 {
		t.Fatalf("expected 1 child (reused), got %d", len(tree.Root.Children))
	}
	if tree.Root.Children[0].Metric.CallCount != 5 {
		t.Errorf("expected CallCount=5, got %d", tree.Root.Children[0].Metric.CallCount)
	}
}

func TestMetricsTree_Merge(t *testing.T) {
	tree1 := NewMetricsTree()
	tree1.Enabled = true
	s := tree1.Start("op")
	tree1.Stop(s)

	tree2 := NewMetricsTree()
	tree2.Enabled = true
	s = tree2.Start("op")
	tree2.Stop(s)
	s = tree2.Start("op")
	tree2.Stop(s)

	tree1.Merge(tree2)

	opNode := tree1.Root.ChildrenByName["op"]
	if opNode == nil {
		t.Fatal("expected 'op' node after merge")
	}
	if opNode.Metric.CallCount != 3 {
		t.Errorf("expected merged CallCount=3, got %d", opNode.Metric.CallCount)
	}
}

func TestMetricsTree_MergeNil(t *testing.T) {
	tree := NewMetricsTree()
	// Should not panic
	tree.Merge(nil)
}

func TestBucketMetric_UpdateBucketCount(t *testing.T) {
	b := NewBucketMetric("test", 0, 10, 20, 50, 100)

	b.UpdateBucketCount(5)
	b.UpdateBucketCount(15)
	b.UpdateBucketCount(0)
	b.UpdateBucketCount(100)
	b.UpdateBucketCount(75)

	// min=0, max=100, count=5, sum=195
	// Bucket 0: count 1 (value 0)
	// Bucket 10: count 1 (value 5)
	// Bucket 20: count 1 (value 15)
	// Bucket 50: count 0
	// Bucket 100: count 2 (values 75, 100)
}

func TestBucketMetric_Merge(t *testing.T) {
	b1 := NewBucketMetric("test", 0, 10, 20)
	b2 := NewBucketMetric("test", 0, 10, 20)

	b1.UpdateBucketCount(5)
	b2.UpdateBucketCount(15)

	b1.Merge(b2)

	// After merge, both buckets should have their respective counts
}

func TestMergeMetrics_Merge(t *testing.T) {
	m1 := NewMergeMetrics()
	m2 := NewMergeMetrics()

	m1.BytesRead = 100
	m1.LinesRead = 10
	m1.LinesWithTimestamps = 8
	m1.LinesWithoutTimestamps = 2

	m2.BytesRead = 200
	m2.LinesRead = 20
	m2.LinesWithTimestamps = 15
	m2.LinesWithoutTimestamps = 5

	m1.Merge(m2)

	assertEquals(t, int64(300), m1.BytesRead)
	assertEquals(t, int64(30), m1.LinesRead)
	assertEquals(t, int64(23), m1.LinesWithTimestamps)
	assertEquals(t, int64(7), m1.LinesWithoutTimestamps)
}

func TestMergeMetrics_MergeAllFields(t *testing.T) {
	m1 := NewMergeMetrics()
	m2 := NewMergeMetrics()

	m2.BytesReadAndSkipped = 50
	m2.BytesNotRead = 30
	m2.BytesWrittenForTimestamps = 100
	m2.BytesWrittenForAliasPerLine = 200
	m2.BytesWrittenForAliasPerBlock = 300
	m2.BytesWrittenForRawData = 400
	m2.BytesWrittenForMissingNewlines = 5
	m2.LinesReadAndSkipped = 3

	m1.Merge(m2)

	assertEquals(t, int64(50), m1.BytesReadAndSkipped)
	assertEquals(t, int64(30), m1.BytesNotRead)
	assertEquals(t, int64(100), m1.BytesWrittenForTimestamps)
	assertEquals(t, int64(200), m1.BytesWrittenForAliasPerLine)
	assertEquals(t, int64(300), m1.BytesWrittenForAliasPerBlock)
	assertEquals(t, int64(400), m1.BytesWrittenForRawData)
	assertEquals(t, int64(5), m1.BytesWrittenForMissingNewlines)
	assertEquals(t, int64(3), m1.LinesReadAndSkipped)
}

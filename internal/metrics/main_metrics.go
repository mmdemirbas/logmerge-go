package metrics

import (
	"fmt"
	"io"
	"math"
	"runtime"
	"sort"
	"strings"
	"time"
)

// TODO: Consider sampling metrics (e.g. measure per 1000 lines instead of every single line)
// TODO: Consider batching metrics (e.g. accumulate data locally per 1000 lines, then merge to the global metrics)

type MainMetrics struct {
	ListFilesMetrics *ListFilesMetrics
	MergeMetrics     *MergeMetrics
	Tree             *MetricsTree
}

type MetricsTree struct {
	Enabled bool
	Root    *MetricsTreeNode
	Current *MetricsTreeNode

	// Aggregated metrics

	HeapTotal *CallMetric
}

type MetricsTreeNode struct {
	Metric         *CallMetric
	Parent         *MetricsTreeNode
	Children       []*MetricsTreeNode
	ChildrenByName map[string]*MetricsTreeNode
}

type CallMetric struct {
	Name        string
	CallCount   int64
	Duration    int64
	MetricsTree *MetricsTree
}

type BucketMetric struct {
	name   string
	levels []int
	values []int64
	min    int64
	max    int64
	sum    int64
	count  int64
}

// NewMetrics returns a MainMetrics with zero-valued sub-metrics.
func NewMetrics() *MainMetrics {
	return &MainMetrics{
		ListFilesMetrics: NewListFilesMetrics(),
		MergeMetrics:     NewMergeMetrics(),
	}
}

// NewMetricsTree creates a MetricsTree with an initialized root node.
func NewMetricsTree() *MetricsTree {
	tree := &MetricsTree{}
	root := NewMetricsTreeNode(tree, nil, "MetricsTree")

	tree.Root = root
	tree.Current = root

	tree.HeapTotal = NewCallMetric("HeapTotal", tree)

	return tree
}

func NewMetricsTreeNode(m *MetricsTree, parent *MetricsTreeNode, name string) *MetricsTreeNode {
	return &MetricsTreeNode{
		Metric: NewCallMetric(name, m),
		Parent: parent,
	}
}

func NewCallMetric(name string, metricsTree *MetricsTree) *CallMetric {
	return &CallMetric{
		Name:        name,
		MetricsTree: metricsTree,
	}
}

// NewBucketMetric creates a histogram-style metric with the given bucket boundaries.
func NewBucketMetric(name string, levels ...int) *BucketMetric {
	return &BucketMetric{
		name:   name,
		levels: levels,
		values: make([]int64, len(levels)),
		min:    1<<63 - 1,
		max:    0,
		sum:    0,
		count:  0,
	}
}

// Start enters a named timing context and returns the start time. Call Stop to record duration.
// Nil-safe: returns zero time if m is nil or disabled.
func (m *MetricsTree) Start(name string) time.Time {
	if m == nil || !m.Enabled {
		return time.Time{}
	}

	// enter context
	parent := m.Current
	children := parent.ChildrenByName

	if children == nil {
		children = make(map[string]*MetricsTreeNode)
		parent.ChildrenByName = children
	}

	child, ok := children[name]
	if !ok {
		child = NewMetricsTreeNode(m, parent, name)
		children[name] = child
		parent.Children = append(parent.Children, child)
	}

	m.Current = child

	return time.Now()
}

// Stop records the elapsed duration since startNanos and exits the current timing context.
// Nil-safe: returns 0 if m is nil or disabled.
func (m *MetricsTree) Stop(startNanos time.Time) (elapsed int64) {
	if m == nil || !m.Enabled {
		return 0
	}

	elapsed = time.Since(startNanos).Nanoseconds()

	// exit context
	m.Current.Metric.CallCount++
	m.Current.Metric.Duration += elapsed
	m.Current = m.Current.Parent

	return elapsed
}

func (c *CallMetric) Stop(startNanos time.Time) {
	c.Duration += c.MetricsTree.Stop(startNanos)
	c.CallCount++
}

// Merge combines another MetricsTree's nodes and durations into this one.
// Nil-safe: returns immediately if either receiver or other is nil.
func (m *MetricsTree) Merge(other *MetricsTree) {
	if m == nil || other == nil || other.Root == nil {
		return
	}
	m.Enabled = m.Enabled || other.Enabled
	m.Root.merge(other.Root)
}

func (n *MetricsTreeNode) merge(other *MetricsTreeNode) {
	n.Metric.merge(other.Metric)
	if n.ChildrenByName == nil {
		n.ChildrenByName = make(map[string]*MetricsTreeNode)
	}
	for name, otherChild := range other.ChildrenByName {
		if child, exists := n.ChildrenByName[name]; exists {
			child.merge(otherChild)
		} else {
			newChild := NewMetricsTreeNode(n.Metric.MetricsTree, n, name)
			n.ChildrenByName[name] = newChild
			n.Children = append(n.Children, newChild)
			newChild.merge(otherChild)
		}
	}
}

func (c *CallMetric) merge(other *CallMetric) {
	if other == nil {
		return
	}
	c.CallCount += other.CallCount
	c.Duration += other.Duration
}

// UpdateBucketCount increments the bucket that n falls into and updates min/max/sum.
// Nil-safe: returns immediately if b is nil.
func (b *BucketMetric) UpdateBucketCount(n int) {
	if b == nil {
		return
	}
	// Binary search for the first level >= n
	lo, hi := 0, len(b.levels)
	for lo < hi {
		mid := (lo + hi) >> 1
		if b.levels[mid] < n {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo < len(b.levels) {
		b.values[lo]++
	}
	b.min = min(b.min, int64(n))
	b.max = max(b.max, int64(n))
	b.sum += int64(n)
	b.count++
}

// Merge aggregates another BucketMetric's counts into this one.
// Nil-safe: returns immediately if either receiver or other is nil.
func (b *BucketMetric) Merge(other *BucketMetric) {
	if b == nil || other == nil {
		return
	}
	for i := range b.values {
		b.values[i] += other.values[i]
	}
	b.min = min(b.min, other.min)
	b.max = max(b.max, other.max)
	b.sum += other.sum
	b.count += other.count
}

// PrintMetrics writes a full diagnostic report (byte counts, line counts, memory stats,
// timing tree) to the configured log file.
//
//goland:noinspection GoUnhandledErrorResult
func (m *MainMetrics) PrintMetrics(logFile io.Writer, configYAML string, startTime time.Time, elapsedTime time.Duration, err error) {
	inputBytes := m.MergeMetrics.BytesRead + m.MergeMetrics.BytesNotRead
	bytesReadAndProcessed := m.MergeMetrics.BytesRead - m.MergeMetrics.BytesReadAndSkipped
	linesReadAndProcessed := m.MergeMetrics.LinesRead - m.MergeMetrics.LinesReadAndSkipped
	writtenBytesOverhead := m.MergeMetrics.BytesWrittenForTimestamps + m.MergeMetrics.BytesWrittenForAliasPerLine + m.MergeMetrics.BytesWrittenForAliasPerBlock + m.MergeMetrics.BytesWrittenForMissingNewlines
	outputBytes := m.MergeMetrics.BytesWrittenForRawData + writtenBytesOverhead

	ms := runtime.MemStats{}
	runtime.ReadMemStats(&ms)

	elapsedNanoseconds := elapsedTime.Nanoseconds()
	m.Tree.Root.Metric.Duration = elapsedNanoseconds
	m.Tree.Root.Metric.CallCount = 1

	fmt.Fprintf(logFile, "===== SUMMARY ====================================================================================================================================================================\n")
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "Start time               : %s\n", startTime.Format(time.RFC3339Nano))
	fmt.Fprintf(logFile, "Error                    : %v\n", err)
	fmt.Fprintf(logFile, "Total main duration      : %v\n", elapsedTime)
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "===== CONFIGURATION ==============================================================================================================================================================\n")
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "%s\n", configYAML)
	fmt.Fprintf(logFile, "\n")
	printStatisticsSection(logFile, m, inputBytes, bytesReadAndProcessed, linesReadAndProcessed, writtenBytesOverhead, outputBytes, elapsedNanoseconds)
	fmt.Fprintf(logFile, "===== TIMING SUMMARY =============================================================================================================================================================\n")
	fmt.Fprintf(logFile, "\n")
	if m.Tree.HeapTotal != nil {
		m.Tree.HeapTotal.printCallMetric(logFile, bytesSpeed(m.MergeMetrics.BytesRead, elapsedNanoseconds))
	}
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "===== TIMING BREAKDOWN ===========================================================================================================================================================\n")
	fmt.Fprintf(logFile, "\n")
	m.Tree.Root.printTree(logFile, 0)
	fmt.Fprintf(logFile, "\n")
	printRuntimeMetricsSection(logFile, ms)
	fmt.Fprintf(logFile, "===== DEBUG METRICS ==============================================================================================================================================================\n")
	m.MergeMetrics.LineLengths.printBuckets(logFile)
	m.MergeMetrics.SkippedLineCounts.printBuckets(logFile)
	m.MergeMetrics.SuccessiveLineCounts.printBuckets(logFile)
	m.MergeMetrics.BlockLineCounts.printBuckets(logFile)
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "===== FILE LIST ==================================================================================================================================================================\n")
	fmt.Fprintf(logFile, "File list (%d files):\n", len(m.ListFilesMetrics.MatchedFiles))
	sort.Strings(m.ListFilesMetrics.MatchedFiles)
	for i, file := range m.ListFilesMetrics.MatchedFiles {
		fmt.Fprintf(logFile, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(logFile, "==================================================================================================================================================================================\n")
}

func printStatisticsSection(logFile io.Writer, m *MainMetrics, inputBytes, bytesReadAndProcessed, linesReadAndProcessed, writtenBytesOverhead, outputBytes, elapsedNanoseconds int64) {
	fmt.Fprintf(logFile, "===== STATISTICS =================================================================================================================================================================\n")
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "File count stats\n")
	fmt.Fprintf(logFile, "  dirs scanned           : %8s ~ %15d\n", "", m.ListFilesMetrics.DirsScanned)
	fmt.Fprintf(logFile, "  files scanned          : %8s ~ %15d\n", percent(m.ListFilesMetrics.FilesScanned, m.ListFilesMetrics.FilesScanned), m.ListFilesMetrics.FilesScanned)
	fmt.Fprintf(logFile, "  files matched          : %8s ~ %15d\n", percent(m.ListFilesMetrics.FilesMatched, m.ListFilesMetrics.FilesScanned), m.ListFilesMetrics.FilesMatched)
	fmt.Fprintf(logFile, "Byte count stats\n")
	fmt.Fprintf(logFile, "  input bytes            : %8s ~ %15d = %10s ≈ %s\n", percent(inputBytes, inputBytes), inputBytes, bytes(inputBytes), bytesSpeed(inputBytes, elapsedNanoseconds))
	fmt.Fprintf(logFile, "    read                 : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.BytesRead, inputBytes), m.MergeMetrics.BytesRead, bytes(m.MergeMetrics.BytesRead), bytesSpeed(m.MergeMetrics.BytesRead, elapsedNanoseconds))
	fmt.Fprintf(logFile, "      read and skipped   : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.BytesReadAndSkipped, inputBytes), m.MergeMetrics.BytesReadAndSkipped, bytes(m.MergeMetrics.BytesReadAndSkipped), bytesSpeed(m.MergeMetrics.BytesReadAndSkipped, elapsedNanoseconds))
	fmt.Fprintf(logFile, "      read and processed : %8s ~ %15d = %10s ≈ %s\n", percent(bytesReadAndProcessed, inputBytes), bytesReadAndProcessed, bytes(bytesReadAndProcessed), bytesSpeed(bytesReadAndProcessed, elapsedNanoseconds))
	fmt.Fprintf(logFile, "    not read             : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.BytesNotRead, inputBytes), m.MergeMetrics.BytesNotRead, bytes(m.MergeMetrics.BytesNotRead), bytesSpeed(m.MergeMetrics.BytesNotRead, elapsedNanoseconds))
	fmt.Fprintf(logFile, "  output bytes           : %8s ~ %15d = %10s ≈ %s\n", percent(outputBytes, outputBytes), outputBytes, bytes(outputBytes), bytesSpeed(outputBytes, elapsedNanoseconds))
	fmt.Fprintf(logFile, "    raw data             : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForRawData, outputBytes), m.MergeMetrics.BytesWrittenForRawData, bytes(m.MergeMetrics.BytesWrittenForRawData))
	fmt.Fprintf(logFile, "    overhead             : %8s ~ %15v = %10s\n", percent(writtenBytesOverhead, outputBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(logFile, "      source name @block : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForAliasPerBlock, outputBytes), m.MergeMetrics.BytesWrittenForAliasPerBlock, bytes(m.MergeMetrics.BytesWrittenForAliasPerBlock))
	fmt.Fprintf(logFile, "      source name @line  : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForAliasPerLine, outputBytes), m.MergeMetrics.BytesWrittenForAliasPerLine, bytes(m.MergeMetrics.BytesWrittenForAliasPerLine))
	fmt.Fprintf(logFile, "      timestamps  @line  : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForTimestamps, outputBytes), m.MergeMetrics.BytesWrittenForTimestamps, bytes(m.MergeMetrics.BytesWrittenForTimestamps))
	fmt.Fprintf(logFile, "      missing newlines   : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForMissingNewlines, outputBytes), m.MergeMetrics.BytesWrittenForMissingNewlines, bytes(m.MergeMetrics.BytesWrittenForMissingNewlines))
	fmt.Fprintf(logFile, "Line count stats\n")
	fmt.Fprintf(logFile, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.LinesRead, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesRead, count(m.MergeMetrics.LinesRead), countSpeed(m.MergeMetrics.LinesRead, elapsedNanoseconds))
	fmt.Fprintf(logFile, "    with timestamp       : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.LinesWithTimestamps, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesWithTimestamps, count(m.MergeMetrics.LinesWithTimestamps))
	fmt.Fprintf(logFile, "    without timestamp    : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.LinesWithoutTimestamps, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesWithoutTimestamps, count(m.MergeMetrics.LinesWithoutTimestamps))
	fmt.Fprintf(logFile, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.LinesRead, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesRead, count(m.MergeMetrics.LinesRead), countSpeed(m.MergeMetrics.LinesRead, elapsedNanoseconds))
	fmt.Fprintf(logFile, "    skipped              : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.LinesReadAndSkipped, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesReadAndSkipped, count(m.MergeMetrics.LinesReadAndSkipped))
	fmt.Fprintf(logFile, "    processed            : %8s ~ %15v = %10s\n", percent(linesReadAndProcessed, m.MergeMetrics.LinesRead), linesReadAndProcessed, count(linesReadAndProcessed))
	fmt.Fprintf(logFile, "\n")
}

func printRuntimeMetricsSection(logFile io.Writer, ms runtime.MemStats) {
	fmt.Fprintf(logFile, "===== RUNTIME METRICS ============================================================================================================================================================\n")
	fmt.Fprintf(logFile, "NumCPU                               : %12v\n", runtime.NumCPU())
	fmt.Fprintf(logFile, "NumGoroutine                         : %12v\n", runtime.NumGoroutine())
	fmt.Fprintf(logFile, "NumCgoCall                           : %12v\n", runtime.NumCgoCall())
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "MemStats                             : %+v\n", ms)
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "Allocated heap objects               : %12v = %10s\n", ms.Alloc, bytes(int64(ms.Alloc)))
	fmt.Fprintf(logFile, "Allocated heap objects (cumulative)  : %12v = %10s\n", ms.TotalAlloc, bytes(int64(ms.TotalAlloc)))
	fmt.Fprintf(logFile, "Memory obtained from the OS          : %12v = %10s\n", ms.Sys, bytes(int64(ms.Sys)))
	fmt.Fprintf(logFile, "Number of pointer lookups            : %12v = %10s\n", ms.Lookups, count(int64(ms.Lookups)))
	fmt.Fprintf(logFile, "Number of mallocs                    : %12v = %10s\n", ms.Mallocs, count(int64(ms.Mallocs)))
	fmt.Fprintf(logFile, "Number of frees                      : %12v = %10s\n", ms.Frees, count(int64(ms.Frees)))
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "Allocated heap objects               : %12v = %10s\n", ms.HeapAlloc, bytes(int64(ms.HeapAlloc)))
	fmt.Fprintf(logFile, "Allocated heap objects (cumulative)  : %12v = %10s\n", ms.HeapSys, bytes(int64(ms.HeapSys)))
	fmt.Fprintf(logFile, "Heap idle memory                     : %12v = %10s\n", ms.HeapIdle, bytes(int64(ms.HeapIdle)))
	fmt.Fprintf(logFile, "Heap in-use memory                   : %12v = %10s\n", ms.HeapInuse, bytes(int64(ms.HeapInuse)))
	fmt.Fprintf(logFile, "Heap released memory                 : %12v = %10s\n", ms.HeapReleased, bytes(int64(ms.HeapReleased)))
	fmt.Fprintf(logFile, "Heap objects waiting to be freed     : %12v = %10s\n", ms.HeapObjects, count(int64(ms.HeapObjects)))
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "Stack memory in use                  : %12v = %10s\n", ms.StackInuse, bytes(int64(ms.StackInuse)))
	fmt.Fprintf(logFile, "Stack memory obtained from the OS    : %12v = %10s\n", ms.StackSys, bytes(int64(ms.StackSys)))
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "Allocated mspan structures           : %12v = %10s\n", ms.MSpanInuse, bytes(int64(ms.MSpanInuse)))
	fmt.Fprintf(logFile, "mspan memory obtained from the OS    : %12v = %10s\n", ms.MSpanSys, bytes(int64(ms.MSpanSys)))
	fmt.Fprintf(logFile, "Allocated mcache structures          : %12v = %10s\n", ms.MCacheInuse, bytes(int64(ms.MCacheInuse)))
	fmt.Fprintf(logFile, "mcache memory obtained from the OS   : %12v = %10s\n", ms.MCacheSys, bytes(int64(ms.MCacheSys)))
	fmt.Fprintf(logFile, "Allocated buckhash tables            : %12v = %10s\n", ms.BuckHashSys, bytes(int64(ms.BuckHashSys)))
	fmt.Fprintf(logFile, "Allocated GC metadata                : %12v = %10s\n", ms.GCSys, bytes(int64(ms.GCSys)))
	fmt.Fprintf(logFile, "Allocated other system allocations   : %12v = %10s\n", ms.OtherSys, bytes(int64(ms.OtherSys)))
	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "Last GC finish time                  :   %s\n", strings.Replace(time.Unix(0, int64(ms.LastGC)).Format(time.RFC3339Nano), "T", "   ", 1))
	fmt.Fprintf(logFile, "Target heap size of the next GC cycle: %12v = %10s\n", ms.NextGC, bytes(int64(ms.NextGC)))
	fmt.Fprintf(logFile, "GC pause duration                    : %12v = %10s\n", ms.PauseTotalNs, duration(int64(ms.PauseTotalNs)))
	fmt.Fprintf(logFile, "Number of completed GC cycles        : %12v = %10s\n", ms.NumGC, count(int64(ms.NumGC)))
	fmt.Fprintf(logFile, "Number of forced GC cycles by app    : %12v = %10s\n", ms.NumForcedGC, count(int64(ms.NumForcedGC)))
	fmt.Fprintf(logFile, "GCCPUFraction                        : %12.2f / %10s\n", ms.GCCPUFraction*1_000_000, "1_000_000")
	fmt.Fprintf(logFile, "\n")
}

func (t *MetricsTreeNode) printTree(logFile io.Writer, depth int) {
	metric := t.Metric
	metricsTree := metric.MetricsTree
	metricsTree.printDurationLog(logFile, depth, metric.Name, metric.CallCount, metric.Duration, "")

	if t.Children != nil {
		childTotal := int64(0)
		for _, child := range t.Children {
			child.printTree(logFile, depth+1)
			childTotal += child.Metric.Duration
		}

		rest := metric.Duration - childTotal
		if rest > 0 {
			metricsTree.printDurationLog(logFile, depth+1, "..rest of "+metric.Name, metric.CallCount, rest, "")
		}
	}
}

func (m *MetricsTree) printDurationLog(logFile io.Writer, depth int, name string, n int64, nanoseconds int64, extra string) {
	padLen := 35
	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(
		logFile,
		"%-*s%-*s : %6.2f %% ~ %15v ≈ %12v avg of %12v times = %12v %s\n",
		depth*2,
		"",
		padLen-depth*2,
		name,
		divideSafe(nanoseconds, m.Root.Metric.Duration)*100,
		duration(nanoseconds),
		durationFloat(divideSafe(nanoseconds, n)),
		n,
		count(n),
		extra,
	)
}

func (c *CallMetric) printCallMetric(logFile io.Writer, extra string) {
	c.MetricsTree.printDurationLog(logFile, 0, c.Name, c.CallCount, c.Duration, extra)
}

//goland:noinspection GoUnhandledErrorResult
func (b *BucketMetric) printBuckets(logFile io.Writer) {
	if b == nil {
		return
	}
	minValue := b.min
	maxValue := b.max
	total := b.count
	avgValue := divideSafe(b.sum, total)
	avgValueRounded := math.Round(avgValue*1e3) / 1e3

	fmt.Fprintf(logFile, "\n")
	fmt.Fprintf(logFile, "%s\n", b.name)
	fmt.Fprintf(logFile, "  summary\n")
	fmt.Fprintf(logFile, "    min          : %8s ~ %15v = %10s\n", "", minValue, count(minValue))
	fmt.Fprintf(logFile, "    avg          : %8s ~ %15v ~ %10s\n", "", avgValueRounded, count(int64(math.Round(avgValue))))
	fmt.Fprintf(logFile, "    max          : %8s ~ %15v = %10s\n", "", maxValue, count(maxValue))
	fmt.Fprintf(logFile, "    count        : %8s ~ %15v = %10s\n", "", total, count(total))
	fmt.Fprintf(logFile, "  buckets\n")

	var cumulative int64

	for i, level := range b.levels {
		value := b.values[i]
		cumulative += value
		fmt.Fprintf(logFile, "    ≤ %-6d     : %8s ~ %15d ≈ %15d (cumulative) ~ %10s (cumulative)\n", level, percent(value, total), value, cumulative, percent(cumulative, total))
	}

	lastLevel := b.levels[len(b.levels)-1]
	remaining := total - cumulative
	cumulative += remaining
	fmt.Fprintf(logFile, "    > %-6d     : %8s ~ %15d ≈ %15d (cumulative) ~ %10s (cumulative)\n", lastLevel, percent(remaining, total), remaining, cumulative, percent(total, total))
}

func duration(d int64) string {
	return durationFloat(float64(d))
}

func durationFloat(d float64) string {
	if d < 1000 {
		return fmt.Sprintf("%.3fns", d)
	}

	if d < 1000*1000 {
		return fmt.Sprintf("%.3fµs", d/1000)
	}

	if d < 1000*1000*1000 {
		return fmt.Sprintf("%.3fms", d/(1000*1000))
	}

	return fmt.Sprintf("%v", time.Duration(d).Round(time.Millisecond))
}

func bytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%7d B ", bytes)
	}
	if bytes < 1024*1024 {
		return fmt.Sprintf("%7.2f KB", float64(bytes)/1024)
	}
	if bytes < 1024*1024*1024 {
		return fmt.Sprintf("%7.2f MB", float64(bytes)/(1024*1024))
	}
	return fmt.Sprintf("%7.2f GB", float64(bytes)/(1024*1024*1024))
}

func count(value int64) string {
	if value < 1000 {
		return fmt.Sprintf("%7d", value)
	}
	if value < 1000*1000 {
		return fmt.Sprintf("%7.2f K", float64(value)/1000)
	}
	if value < 1000*1000*1000 {
		return fmt.Sprintf("%7.2f M", float64(value)/(1000*1000))
	}
	return fmt.Sprintf("%7.2f G", float64(value)/(1000*1000*1000))
}

func percent(value, total int64) string {
	return fmt.Sprintf("%6.2f %%", divideSafe(value, total)*100)
}

func bytesSpeed(value, duration int64) string {
	return fmt.Sprintf("%s / s", bytes(int64(speed(value, duration))))
}

func countSpeed(value, duration int64) string {
	return fmt.Sprintf("%s / s", count(int64(speed(value, duration))))
}

func speed(value, duration int64) float64 {
	return divideSafe(value, duration) * 1e9
}

func divideSafe(value int64, total int64) float64 {
	if total == 0 {
		return 0
	} else {
		return float64(value) / float64(total)
	}
}

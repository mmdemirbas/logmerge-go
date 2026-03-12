package logmerge

import (
	"fmt"
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

func NewMetrics() *MainMetrics {
	return &MainMetrics{
		ListFilesMetrics: NewListFilesMetrics(),
		MergeMetrics:     NewMergeMetrics(),
	}
}

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

func (m *MetricsTree) Start(name string) time.Time {
	if !m.Enabled {
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

func (m *MetricsTree) Stop(startNanos time.Time) int64 {
	if !m.Enabled {
		return 0
	}

	elapsed := time.Since(startNanos).Nanoseconds()

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

func (m *MetricsTree) Merge(other *MetricsTree) {
	if other == nil || other.Root == nil {
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

func (b *BucketMetric) UpdateBucketCount(n int) {
	for i, level := range b.levels {
		if n <= level {
			b.values[i]++
			break
		}
	}
	b.min = min(b.min, int64(n))
	b.max = max(b.max, int64(n))
	b.sum += int64(n)
	b.count++
}

func (b *BucketMetric) Merge(other *BucketMetric) {
	for i := range b.values {
		b.values[i] += other.values[i]
	}
	b.min = min(b.min, other.min)
	b.max = max(b.max, other.max)
	b.sum += other.sum
	b.count += other.count
}

//goland:noinspection GoUnhandledErrorResult
func (m *MainMetrics) PrintMetrics(c *MainConfig, startTime time.Time, elapsedTime time.Duration, err error) {
	inputBytes := m.MergeMetrics.BytesRead + m.MergeMetrics.BytesNotRead
	bytesReadAndProcessed := m.MergeMetrics.BytesRead - m.MergeMetrics.BytesReadAndSkipped
	linesReadAndProcessed := m.MergeMetrics.LinesRead - m.MergeMetrics.LinesReadAndSkipped

	writtenBytesOverhead := m.MergeMetrics.BytesWrittenForTimestamps + m.MergeMetrics.BytesWrittenForAliasPerLine + m.MergeMetrics.BytesWrittenForAliasPerBlock + m.MergeMetrics.BytesWrittenForMissingNewlines
	outputBytes := m.MergeMetrics.BytesWrittenForRawData + writtenBytesOverhead

	MemStats := runtime.MemStats{}
	runtime.ReadMemStats(&MemStats)

	elapsedNanoseconds := elapsedTime.Nanoseconds()
	m.Tree.Root.Metric.Duration = elapsedNanoseconds
	m.Tree.Root.Metric.CallCount = 1

	logFile := c.LogFile

	fmt.Fprintf(c.LogFile, "===== SUMMARY ====================================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "Start time               : %s\n", startTime.Format(time.RFC3339Nano))
	fmt.Fprintf(c.LogFile, "Error                    : %v\n", err)
	fmt.Fprintf(c.LogFile, "Total main duration      : %v\n", elapsedTime)
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== CONFIGURATION ==============================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "\n")
	yaml, err := c.ToYAML()
	if err != nil {
		fmt.Fprintf(c.LogFile, "Failed to convert configuration to YAML: %v\n", err)
	} else {
		fmt.Fprintf(c.LogFile, "%s\n", yaml)
	}
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== STATISTICS =================================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "File count stats\n")
	fmt.Fprintf(c.LogFile, "  dirs scanned           : %8s ~ %15d\n", "", m.ListFilesMetrics.DirsScanned)
	fmt.Fprintf(c.LogFile, "  files scanned          : %8s ~ %15d\n", percent(m.ListFilesMetrics.FilesScanned, m.ListFilesMetrics.FilesScanned), m.ListFilesMetrics.FilesScanned)
	fmt.Fprintf(c.LogFile, "  files matched          : %8s ~ %15d\n", percent(m.ListFilesMetrics.FilesMatched, m.ListFilesMetrics.FilesScanned), m.ListFilesMetrics.FilesMatched)
	fmt.Fprintf(c.LogFile, "Byte count stats\n")
	fmt.Fprintf(c.LogFile, "  input bytes            : %8s ~ %15d = %10s ≈ %s\n", percent(inputBytes, inputBytes), inputBytes, bytes(inputBytes), bytesSpeed(inputBytes, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "    read                 : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.BytesRead, inputBytes), m.MergeMetrics.BytesRead, bytes(m.MergeMetrics.BytesRead), bytesSpeed(m.MergeMetrics.BytesRead, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "      read and skipped   : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.BytesReadAndSkipped, inputBytes), m.MergeMetrics.BytesReadAndSkipped, bytes(m.MergeMetrics.BytesReadAndSkipped), bytesSpeed(m.MergeMetrics.BytesReadAndSkipped, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "      read and processed : %8s ~ %15d = %10s ≈ %s\n", percent(bytesReadAndProcessed, inputBytes), bytesReadAndProcessed, bytes(bytesReadAndProcessed), bytesSpeed(bytesReadAndProcessed, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "    not read             : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.BytesNotRead, inputBytes), m.MergeMetrics.BytesNotRead, bytes(m.MergeMetrics.BytesNotRead), bytesSpeed(m.MergeMetrics.BytesNotRead, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "  output bytes           : %8s ~ %15d = %10s ≈ %s\n", percent(outputBytes, outputBytes), outputBytes, bytes(outputBytes), bytesSpeed(outputBytes, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "    raw data             : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForRawData, outputBytes), m.MergeMetrics.BytesWrittenForRawData, bytes(m.MergeMetrics.BytesWrittenForRawData))
	fmt.Fprintf(c.LogFile, "    overhead             : %8s ~ %15v = %10s\n", percent(writtenBytesOverhead, outputBytes), writtenBytesOverhead, bytes(writtenBytesOverhead))
	fmt.Fprintf(c.LogFile, "      source name @block : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForAliasPerBlock, outputBytes), m.MergeMetrics.BytesWrittenForAliasPerBlock, bytes(m.MergeMetrics.BytesWrittenForAliasPerBlock))
	fmt.Fprintf(c.LogFile, "      source name @line  : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForAliasPerLine, outputBytes), m.MergeMetrics.BytesWrittenForAliasPerLine, bytes(m.MergeMetrics.BytesWrittenForAliasPerLine))
	fmt.Fprintf(c.LogFile, "      timestamps  @line  : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForTimestamps, outputBytes), m.MergeMetrics.BytesWrittenForTimestamps, bytes(m.MergeMetrics.BytesWrittenForTimestamps))
	fmt.Fprintf(c.LogFile, "      missing newlines   : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.BytesWrittenForMissingNewlines, outputBytes), m.MergeMetrics.BytesWrittenForMissingNewlines, bytes(m.MergeMetrics.BytesWrittenForMissingNewlines))
	fmt.Fprintf(c.LogFile, "Line count stats\n")
	fmt.Fprintf(c.LogFile, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.LinesRead, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesRead, count(m.MergeMetrics.LinesRead), countSpeed(m.MergeMetrics.LinesRead, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "    with timestamp       : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.LinesWithTimestamps, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesWithTimestamps, count(m.MergeMetrics.LinesWithTimestamps))
	fmt.Fprintf(c.LogFile, "    without timestamp    : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.LinesWithoutTimestamps, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesWithoutTimestamps, count(m.MergeMetrics.LinesWithoutTimestamps))
	fmt.Fprintf(c.LogFile, "  lines read             : %8s ~ %15d = %10s ≈ %s\n", percent(m.MergeMetrics.LinesRead, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesRead, count(m.MergeMetrics.LinesRead), countSpeed(m.MergeMetrics.LinesRead, elapsedNanoseconds))
	fmt.Fprintf(c.LogFile, "    skipped              : %8s ~ %15v = %10s\n", percent(m.MergeMetrics.LinesReadAndSkipped, m.MergeMetrics.LinesRead), m.MergeMetrics.LinesReadAndSkipped, count(m.MergeMetrics.LinesReadAndSkipped))
	fmt.Fprintf(c.LogFile, "    processed            : %8s ~ %15v = %10s\n", percent(linesReadAndProcessed, m.MergeMetrics.LinesRead), linesReadAndProcessed, count(linesReadAndProcessed))
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== TIMING SUMMARY =============================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "\n")
	if m.Tree.HeapTotal != nil {
		m.Tree.HeapTotal.printCallMetric(logFile, bytesSpeed(m.MergeMetrics.BytesRead, elapsedNanoseconds))
	}
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== TIMING BREAKDOWN ===========================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "\n")
	m.Tree.Root.printTree(logFile, 0)
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== RUNTIME METRICS ============================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "NumCPU                               : %12v\n", runtime.NumCPU())
	fmt.Fprintf(c.LogFile, "NumGoroutine                         : %12v\n", runtime.NumGoroutine())
	fmt.Fprintf(c.LogFile, "NumCgoCall                           : %12v\n", runtime.NumCgoCall())
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "MemStats                             : %+v\n", MemStats)
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "Allocated heap objects               : %12v = %10s\n", MemStats.Alloc, bytes(int64(MemStats.Alloc)))
	fmt.Fprintf(c.LogFile, "Allocated heap objects (cumulative)  : %12v = %10s\n", MemStats.TotalAlloc, bytes(int64(MemStats.TotalAlloc)))
	fmt.Fprintf(c.LogFile, "Memory obtained from the OS          : %12v = %10s\n", MemStats.Sys, bytes(int64(MemStats.Sys)))
	fmt.Fprintf(c.LogFile, "Number of pointer lookups            : %12v = %10s\n", MemStats.Lookups, count(int64(MemStats.Lookups)))
	fmt.Fprintf(c.LogFile, "Number of mallocs                    : %12v = %10s\n", MemStats.Mallocs, count(int64(MemStats.Mallocs)))
	fmt.Fprintf(c.LogFile, "Number of frees                      : %12v = %10s\n", MemStats.Frees, count(int64(MemStats.Frees)))
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "Allocated heap objects               : %12v = %10s\n", MemStats.HeapAlloc, bytes(int64(MemStats.HeapAlloc)))
	fmt.Fprintf(c.LogFile, "Allocated heap objects (cumulative)  : %12v = %10s\n", MemStats.HeapSys, bytes(int64(MemStats.HeapSys)))
	fmt.Fprintf(c.LogFile, "Heap idle memory                     : %12v = %10s\n", MemStats.HeapIdle, bytes(int64(MemStats.HeapIdle)))
	fmt.Fprintf(c.LogFile, "Heap in-use memory                   : %12v = %10s\n", MemStats.HeapInuse, bytes(int64(MemStats.HeapInuse)))
	fmt.Fprintf(c.LogFile, "Heap released memory                 : %12v = %10s\n", MemStats.HeapReleased, bytes(int64(MemStats.HeapReleased)))
	fmt.Fprintf(c.LogFile, "Heap objects waiting to be freed     : %12v = %10s\n", MemStats.HeapObjects, count(int64(MemStats.HeapObjects)))
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "Stack memory in use                  : %12v = %10s\n", MemStats.StackInuse, bytes(int64(MemStats.StackInuse)))
	fmt.Fprintf(c.LogFile, "Stack memory obtained from the OS    : %12v = %10s\n", MemStats.StackSys, bytes(int64(MemStats.StackSys)))
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "Allocated mspan structures           : %12v = %10s\n", MemStats.MSpanInuse, bytes(int64(MemStats.MSpanInuse)))
	fmt.Fprintf(c.LogFile, "mspan memory obtained from the OS    : %12v = %10s\n", MemStats.MSpanSys, bytes(int64(MemStats.MSpanSys)))
	fmt.Fprintf(c.LogFile, "Allocated mcache structures          : %12v = %10s\n", MemStats.MCacheInuse, bytes(int64(MemStats.MCacheInuse)))
	fmt.Fprintf(c.LogFile, "mcache memory obtained from the OS   : %12v = %10s\n", MemStats.MCacheSys, bytes(int64(MemStats.MCacheSys)))
	fmt.Fprintf(c.LogFile, "Allocated buckhash tables            : %12v = %10s\n", MemStats.BuckHashSys, bytes(int64(MemStats.BuckHashSys)))
	fmt.Fprintf(c.LogFile, "Allocated GC metadata                : %12v = %10s\n", MemStats.GCSys, bytes(int64(MemStats.GCSys)))
	fmt.Fprintf(c.LogFile, "Allocated other system allocations   : %12v = %10s\n", MemStats.OtherSys, bytes(int64(MemStats.OtherSys)))
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "Last GC finish time                  :   %s\n", strings.Replace(time.Unix(0, int64(MemStats.LastGC)).Format(time.RFC3339Nano), "T", "   ", 1))
	fmt.Fprintf(c.LogFile, "Target heap size of the next GC cycle: %12v = %10s\n", MemStats.NextGC, bytes(int64(MemStats.NextGC)))
	fmt.Fprintf(c.LogFile, "GC pause duration                    : %12v = %10s\n", MemStats.PauseTotalNs, duration(int64(MemStats.PauseTotalNs)))
	fmt.Fprintf(c.LogFile, "Number of completed GC cycles        : %12v = %10s\n", MemStats.NumGC, count(int64(MemStats.NumGC)))
	fmt.Fprintf(c.LogFile, "Number of forced GC cycles by app    : %12v = %10s\n", MemStats.NumForcedGC, count(int64(MemStats.NumForcedGC)))
	fmt.Fprintf(c.LogFile, "GCCPUFraction                        : %12.2f / %10s\n", MemStats.GCCPUFraction*1_000_000, "1_000_000")
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== DEBUG METRICS ==============================================================================================================================================================\n")
	m.MergeMetrics.LineLengths.printBuckets(logFile)
	m.MergeMetrics.SkippedLineCounts.printBuckets(logFile)
	m.MergeMetrics.SuccessiveLineCounts.printBuckets(logFile)
	m.MergeMetrics.BlockLineCounts.printBuckets(logFile)
	fmt.Fprintf(c.LogFile, "\n")
	fmt.Fprintf(c.LogFile, "===== FILE LIST ==================================================================================================================================================================\n")
	fmt.Fprintf(c.LogFile, "File list (%d files):\n", len(m.ListFilesMetrics.MatchedFiles))
	sort.Strings(m.ListFilesMetrics.MatchedFiles)
	for i, file := range m.ListFilesMetrics.MatchedFiles {
		fmt.Fprintf(c.LogFile, "%5d %s\n", i+1, file)
	}
	fmt.Fprintf(c.LogFile, "==================================================================================================================================================================================\n")
}

func (t *MetricsTreeNode) printTree(logFile *WritableFile, depth int) {
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

func (m *MetricsTree) printDurationLog(logFile *WritableFile, depth int, name string, n int64, nanoseconds int64, extra string) {
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

func (c *CallMetric) printCallMetric(logFile *WritableFile, extra string) {
	c.MetricsTree.printDurationLog(logFile, 0, c.Name, c.CallCount, c.Duration, extra)
}

//goland:noinspection GoUnhandledErrorResult
func (b *BucketMetric) printBuckets(logFile *WritableFile) {
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

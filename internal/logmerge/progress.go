package logmerge

import (
	"fmt"
	"os"
	"time"
)

type PrintProgressConfig struct {
	PrintProgressEnabled bool `yaml:"PrintProgressEnabled"`
	InitialDelayMillis   int  `yaml:"InitialDelayMillis"`
	PeriodMillis         int  `yaml:"PeriodMillis"`
}

func PrintProgressPeriodically(c *PrintProgressConfig, files []*FileHandle, programStartTime time.Time) {
	if !c.PrintProgressEnabled {
		return
	}

	// Print progress only if it takes some time
	time.Sleep(time.Duration(c.InitialDelayMillis) * time.Millisecond)
	ticker := time.NewTicker(time.Duration(c.PeriodMillis) * time.Millisecond)

	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(os.Stderr, "\n")

	for range ticker.C {
		PrintProgress(c, files, programStartTime)
	}
}

func PrintProgress(c *PrintProgressConfig, files []*FileHandle, programStartTime time.Time) {
	if !c.PrintProgressEnabled {
		return
	}

	var completedSize int64
	completedCount := 0

	var totalSize int64
	totalCount := len(files)

	for _, file := range files {
		if file.Done {
			completedSize += file.Size
			completedCount++
		} else {
			completedSize += file.BytesRead
		}
		totalSize += file.Size
	}

	totalSize = max(totalSize, 1)
	totalCount = max(totalCount, 1)

	elapsedTime := time.Since(programStartTime)

	//goland:noinspection GoUnhandledErrorResult
	fmt.Fprintf(os.Stderr, "Progress: %6.2f %% of data (%12s / %12s) - %6.2f %% of files (%5d / %5d) - Elapsed: %s\r",
		float64(completedSize)/(float64(totalSize)/100), bytes(completedSize), bytes(totalSize),
		float64(completedCount)/(float64(totalCount)/100), int64(completedCount), int64(totalCount),
		elapsedTime.Round(time.Millisecond).String(),
	)
}

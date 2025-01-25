package main

import (
	"fmt"
	"os"
	"runtime/pprof"
	"time"
)

func main() {
	programStartTime := time.Now() // measure program duration even if metrics disabled

	defer func() {
		if r := recover(); r != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "main: Recovered from panic: %v\n", r)
		}
		Stdout.Close()
		Stderr.Close()
	}()

	// Enable profiling only if configured
	if EnableProfiling {
		// Start CPU profiling
		cpuFile, err := os.Create("out/cpu.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "could not create CPU profile: %v\n", err)
		} else {
			defer cpuFile.Close()
			if err := pprof.StartCPUProfile(cpuFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "could not start CPU profile: %v\n", err)
			} else {
				defer pprof.StopCPUProfile()
			}
		}
	}

	var err error
	//goland:noinspection GoUnhandledErrorResult,
	if len(os.Args) != 2 {
		fmt.Fprintf(Stderr, "logmerge\n")
		fmt.Fprintf(Stderr, "  Merge multiple log files into a single file while preserving the chronological order of log lines.\n")
		fmt.Fprintf(Stderr, "\n")
		fmt.Fprintf(Stderr, "Usage:\n")
		fmt.Fprintf(Stderr, "  %s <confFile>\n", os.Args[0])
		fmt.Fprintf(Stderr, "\n")
		fmt.Fprintf(Stderr, "  <confFile>   Path to the configuration file in YAML format.\n")
		fmt.Fprintf(Stderr, "\n")
		os.Exit(1)
	} else {
		err = loadConfigFromYaml(os.Args[1])
		if err == nil {
			err = ProcessFiles(InputPath, programStartTime)
		}
	}

	// TODO: Catch interrupt signal during merge process and do the post-work anyway

	if EnableProfiling {
		// Capture memory profile
		memFile, err := os.Create("out/mem.prof")
		if err != nil {
			//goland:noinspection GoUnhandledErrorResult
			fmt.Fprintf(Stderr, "could not create memory profile: %v\n", err)
		} else {
			defer memFile.Close()
			if err := pprof.WriteHeapProfile(memFile); err != nil {
				//goland:noinspection GoUnhandledErrorResult
				fmt.Fprintf(Stderr, "could not write memory profile: %v\n", err)
			}
		}
	}

	elapsedTime := time.Since(programStartTime)
	PrintMetrics(programStartTime, elapsedTime, err)

	if err != nil {
		//goland:noinspection GoUnhandledErrorResult
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
}

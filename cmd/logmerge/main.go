package main

import (
	"fmt"
	"os"

	"github.com/mmdemirbas/logmerge/internal/cli"
)

func main() {
	os.Exit(run())
}

func run() int {
	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		return 1
	}
	return 0
}

package main

import (
	"fmt"
	"os"

	"github.com/mmdemirbas/logmerge/internal/cli"
)

func main() {
	if err := cli.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		os.Exit(1)
	}
}

package main

import (
	"fmt"
	"os"

	"github.com/mmdemirbas/logmerge/internal/logmerge"
)

func main() {
	if err := logmerge.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		os.Exit(1)
	}
}

// Command orchigram provides the TUI, CLI, daemon, and installer entrypoint.
package main

import (
	"fmt"
	"os"

	"github.com/alexrett/orchigram/internal/cli"
)

func main() {
	root := cli.NewRoot()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

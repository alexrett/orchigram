// Command orchigram-release packages reproducible release-only artifacts.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/alexrett/orchigram/internal/releasepack"
)

func main() {
	if len(os.Args) < 2 || os.Args[1] != "plugins" {
		fmt.Fprintln(os.Stderr, "usage: orchigram-release plugins --output DIR --version VERSION --commit COMMIT --date RFC3339")
		os.Exit(2)
	}
	flags := flag.NewFlagSet("plugins", flag.ExitOnError)
	output := flags.String("output", "dist/plugin-bundles", "output directory")
	version := flags.String("version", "", "semantic release version")
	commit := flags.String("commit", "", "source commit")
	date := flags.String("date", "", "reproducible RFC3339 build date")
	_ = flags.Parse(os.Args[2:])
	paths, err := releasepack.Build(context.Background(), releasepack.Options{OutputDir: *output, Version: *version, Commit: *commit, Date: *date})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, path := range paths {
		fmt.Println(path)
	}
}

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
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "plugins":
		err = runPlugins(os.Args[2:])
	case "normalize-sbom":
		err = runNormalizeSBOM(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runPlugins(arguments []string) error {
	flags := flag.NewFlagSet("plugins", flag.ExitOnError)
	output := flags.String("output", "dist/plugin-bundles", "output directory")
	version := flags.String("version", "", "semantic release version")
	commit := flags.String("commit", "", "source commit")
	date := flags.String("date", "", "reproducible RFC3339 build date")
	_ = flags.Parse(arguments)
	paths, err := releasepack.Build(context.Background(), releasepack.Options{OutputDir: *output, Version: *version, Commit: *commit, Date: *date})
	if err != nil {
		return err
	}
	for _, path := range paths {
		fmt.Println(path)
	}
	return nil
}

func runNormalizeSBOM(arguments []string) error {
	flags := flag.NewFlagSet("normalize-sbom", flag.ExitOnError)
	file := flags.String("file", "", "SPDX JSON document to normalize")
	artifact := flags.String("artifact", "", "cataloged release artifact")
	date := flags.String("date", "", "reproducible RFC3339 creation time")
	_ = flags.Parse(arguments)
	return releasepack.NormalizeSPDXFile(*file, *artifact, *date)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: orchigram-release <plugins|normalize-sbom> [flags]")
}

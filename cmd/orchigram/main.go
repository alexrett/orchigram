// Command orchigram provides the TUI, CLI, daemon, and installer entrypoint.
package main

import (
	"fmt"
	"os"

	"github.com/alexrett/orchigram/internal/version"
	"github.com/spf13/cobra"
)

func main() {
	root := newRootCommand()
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "orchigram",
		Short:         "Declarative agent workflows from the terminal",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return cmd.Help()
		},
	}
	root.Version = version.String()
	root.SetVersionTemplate("{{.Version}}\n")
	for _, spec := range []struct{ use, short string }{
		{"server", "Run the Orchigram daemon"},
		{"apply -f FILE", "Apply resources"},
		{"get KIND [NAME]", "List or get resources"},
		{"describe KIND NAME", "Describe a resource"},
		{"delete KIND NAME", "Delete a resource"},
		{"flow", "Validate and inspect flows"},
		{"run", "Start, watch, approve, reject, or cancel runs"},
		{"trigger", "Inspect and control triggers"},
		{"plugin", "Install and inspect plugins"},
		{"context", "Manage local and SSH contexts"},
		{"install", "Install the local server service"},
	} {
		root.AddCommand(&cobra.Command{Use: spec.use, Short: spec.short, RunE: func(cmd *cobra.Command, _ []string) error { return cmd.Help() }})
	}
	return root
}

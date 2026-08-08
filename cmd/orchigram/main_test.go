package main

import (
	"testing"

	"github.com/alexrett/orchigram/internal/cli"
)

func TestCanonicalCommandsExist(t *testing.T) {
	t.Parallel()
	root := cli.NewRoot()
	for _, name := range []string{"server", "apply", "get", "describe", "delete", "flow", "run", "trigger", "plugin", "context", "install"} {
		if _, _, err := root.Find([]string{name}); err != nil {
			t.Errorf("command %q missing: %v", name, err)
		}
	}
	if _, _, err := root.Find([]string{"plugin", "pack"}); err != nil {
		t.Errorf("command plugin pack missing: %v", err)
	}
}

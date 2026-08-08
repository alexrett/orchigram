package main

import (
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/cli"
)

func TestCanonicalCommandsExist(t *testing.T) {
	t.Parallel()
	root := cli.NewRoot()
	for _, name := range []string{"server", "apply", "get", "watch", "export", "describe", "delete", "flow", "run", "trigger", "plugin", "system", "context", "install"} {
		if _, _, err := root.Find([]string{name}); err != nil {
			t.Errorf("command %q missing: %v", name, err)
		}
	}
	if _, _, err := root.Find([]string{"plugin", "pack"}); err != nil {
		t.Errorf("command plugin pack missing: %v", err)
	}
	if _, _, err := root.Find([]string{"system", "health"}); err != nil {
		t.Errorf("command system health missing: %v", err)
	}
	for _, path := range [][]string{{"flow", "graph"}, {"run", "list"}, {"run", "describe"}, {"run", "reconcile"}, {"trigger", "receipts"}, {"plugin", "describe"}} {
		if _, _, err := root.Find(path); err != nil {
			t.Errorf("command %q missing: %v", strings.Join(path, " "), err)
		}
	}
}

package main

import "testing"

func TestCanonicalCommandsExist(t *testing.T) {
	t.Parallel()
	root := newRootCommand()
	for _, name := range []string{"server", "apply", "get", "describe", "delete", "flow", "run", "trigger", "plugin", "context", "install"} {
		if _, _, err := root.Find([]string{name}); err != nil {
			t.Errorf("command %q missing: %v", name, err)
		}
	}
}

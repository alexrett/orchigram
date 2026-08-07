package contextcfg

import (
	"testing"
)

func TestRejectsAmbiguousTransport(t *testing.T) {
	t.Parallel()
	f := File{Current: "bad", Contexts: map[string]Context{"bad": {
		Socket: "/tmp/a.sock",
		SSH:    &SSHContext{Destination: "server", Socket: "/run/a.sock"},
	}}}
	if err := f.Validate(); err == nil {
		t.Fatal("expected ambiguous transport to fail")
	}
}

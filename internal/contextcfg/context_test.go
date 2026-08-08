package contextcfg

import (
	"os"
	"path/filepath"
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

func TestRejectsSSHOptionInjectionAndRelativeSockets(t *testing.T) {
	t.Parallel()
	for name, selected := range map[string]Context{
		"option":   {SSH: &SSHContext{Destination: "-oProxyCommand=bad", Socket: "/run/orchigram.sock"}},
		"space":    {SSH: &SSHContext{Destination: "operator@host extra", Socket: "/run/orchigram.sock"}},
		"relative": {SSH: &SSHContext{Destination: "operator@host", Socket: "run/orchigram.sock"}},
		"local":    {Socket: "run/orchigram.sock"},
	} {
		t.Run(name, func(t *testing.T) {
			file := File{Current: name, Contexts: map[string]Context{name: selected}}
			if err := file.Validate(); err == nil {
				t.Fatal("expected invalid context")
			}
		})
	}
}

func TestSaveAndUseContextWithPrivatePermissions(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config", "contexts.yaml")
	file := File{Current: "local", Contexts: map[string]Context{
		"local":  {Socket: "/run/orchigram/orchigram.sock"},
		"remote": {SSH: &SSHContext{Destination: "operator@example", Socket: "/run/orchigram/orchigram.sock"}},
	}}
	if err := Save(path, file); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil || loaded.Current != "local" || loaded.Contexts["remote"].SSH == nil {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%v", info.Mode().Perm())
	}
}

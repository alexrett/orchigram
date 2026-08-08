package pluginmanager

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/pluginpack"
	"github.com/alexrett/orchigram/internal/store"
)

func TestEchoPluginBuildsOutsideModuleAndRunsThroughRealHost(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(root, "examples", "plugins", "echo")
	external := filepath.Join(t.TempDir(), "community-echo")
	if err := os.MkdirAll(filepath.Join(external, "bin"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"go.mod", "main.go"} {
		data, readErr := os.ReadFile(filepath.Join(source, name)) //nolint:gosec // Fixed repository fixture.
		if readErr != nil {
			t.Fatal(readErr)
		}
		if strings.Contains(string(data), "/internal/") {
			t.Fatalf("example %s imports an internal package", name)
		}
		if writeErr := os.WriteFile(filepath.Join(external, name), data, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	runGo(t, external, "mod", "edit", "-replace=github.com/alexrett/orchigram="+root)
	runGo(t, external, "mod", "tidy")
	binaryPath := filepath.Join(external, "bin", "echo")
	runGo(t, external, "build", "-o", binaryPath, ".")
	manifest := "apiVersion: orchigram.dev/plugin/v1alpha1\nname: echo\nversion: 0.1.0\nprotocol: {minimum: 1, maximum: 1}\ncapabilities: [task.echo.echo]\nplatforms:\n  - os: " + runtime.GOOS + "\n    arch: " + runtime.GOARCH + "\n    path: bin/echo\n"
	manifestPath := filepath.Join(external, "plugin.yaml")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	bundlePath := filepath.Join(external, "dist", "echo-0.1.0.tar.gz")
	if _, err := pluginpack.Pack(manifestPath, bundlePath, false); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(bundlePath) //nolint:gosec // Test-owned output.
	if err != nil {
		t.Fatal(err)
	}
	stateRoot := t.TempDir()
	state, err := store.Open(filepath.Join(stateRoot, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	manager := New(state, stateRoot)
	defer manager.Close()
	if _, err := manager.Install(context.Background(), bundle); err != nil {
		t.Fatal(err)
	}
	if err := manager.Enable(context.Background(), "echo", "0.1.0"); err != nil {
		t.Fatal(err)
	}
	if err := manager.Doctor(context.Background(), "echo", "0.1.0"); err != nil {
		t.Fatal(err)
	}
	output, err := manager.Execute(context.Background(), "run-echo", flow.PlanNode{ID: "echo", Uses: "echo.echo", Timeout: "10s", With: map[string]any{"prefix": "received: ", "emitProgress": true}}, json.RawMessage(`{"message":"hello"}`), nil, "run-echo/echo/1")
	if err != nil {
		t.Fatal(err)
	}
	if string(output) != `{"message":"received: hello"}` {
		t.Fatalf("echo output=%s", output)
	}
}

func runGo(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.CommandContext(context.Background(), "go", args...) //nolint:gosec // Fixed Go tool with direct test-owned argv.
	command.Dir = directory
	command.Env = append(os.Environ(), "GOWORK=off", "GOCACHE="+filepath.Join(t.TempDir(), "go-cache"))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("go %v: %v\n%s", args, err, output)
	}
}

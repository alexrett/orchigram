package install

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/firstparty"
)

func TestFilesystemInstallContainsHardenedNetworkClosedService(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	plugins := t.TempDir()
	for _, plugin := range firstparty.All() {
		if err := os.WriteFile(filepath.Join(plugins, plugin.Command), []byte("fixture "+plugin.Name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	result, err := Run(context.Background(), Options{Root: root, PluginDir: plugins, Start: false})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{result.Binary, result.Unit, rootPath(root, "/etc/orchigram/config.yaml")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("missing %s: %v", path, err)
		}
	}
	unitData, err := os.ReadFile(result.Unit) //nolint:gosec // Test reads its isolated installation root.
	if err != nil {
		t.Fatal(err)
	}
	unitText := string(unitData)
	for _, directive := range []string{"User=orchigram", "Environment=HOME=/var/lib/orchigram", "NoNewPrivileges=yes", "ProtectSystem=strict", "ProtectProc=invisible", "SystemCallFilter=@system-service", "CapabilityBoundingSet=", "RuntimeDirectory=orchigram", "MemoryHigh=512M", "MemoryMax=768M", "CPUQuota=200%", "TasksMax=256", "OOMPolicy=stop"} {
		if !strings.Contains(unitText, directive) {
			t.Fatalf("unit is missing %s", directive)
		}
	}
	configData, err := os.ReadFile(rootPath(root, "/etc/orchigram/config.yaml")) //nolint:gosec // Test reads its isolated installation root.
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(configData), "listen:") {
		t.Fatalf("default config opened a listener: %s", configData)
	}
	for _, plugin := range firstparty.All() {
		info, err := os.Stat(rootPath(root, "/usr/local/lib/orchigram/plugins/"+plugin.Command))
		if err != nil {
			t.Fatalf("plugin %s: %v", plugin.Name, err)
		}
		if info.Mode().Perm() != 0o755 {
			t.Fatalf("plugin %s mode=%v", plugin.Name, info.Mode().Perm())
		}
	}
}

func TestCopyFileRejectsOversizeWithoutReplacingTarget(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	source := filepath.Join(directory, "source")
	target := filepath.Join(directory, "target")
	if err := os.WriteFile(source, []byte("123456"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("original"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyFileWithLimit(source, target, 0o755, 5); err == nil {
		t.Fatal("expected oversize source to fail")
	}
	data, err := os.ReadFile(target) //nolint:gosec // Test reads its isolated target.
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "original" {
		t.Fatalf("target was replaced: %q", data)
	}
}

func TestInstallSnapshotRestoresReplacedAndNewFiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o750); err != nil {
		t.Fatal(err)
	}
	existing := filepath.Join(root, "existing")
	created := filepath.Join(root, "created")
	if err := os.WriteFile(existing, []byte("previous"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(existing, 0o640); err != nil { //nolint:gosec // The fixture verifies restoration of this exact non-secret mode.
		t.Fatal(err)
	}
	snapshot, err := captureInstallSnapshot(state, []string{existing, created})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(existing, []byte("broken-upgrade"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(existing, 0o755); err != nil { //nolint:gosec // The fixture models a replaced executable.
		t.Fatal(err)
	}
	if err := os.WriteFile(created, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.restore(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(existing) //nolint:gosec // Test reads its isolated fixture.
	if err != nil || string(data) != "previous" {
		t.Fatalf("restored existing=%q err=%v", data, err)
	}
	if info, err := os.Stat(existing); err != nil {
		t.Fatal(err)
	} else if info.Mode().Perm() != 0o640 {
		t.Fatalf("restored mode=%v", info.Mode().Perm())
	}
	if _, err := os.Stat(created); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new partial target survived rollback: %v", err)
	}
}

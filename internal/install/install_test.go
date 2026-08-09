package install

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/backup"
	"github.com/alexrett/orchigram/internal/firstparty"
	_ "modernc.org/sqlite"
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

func TestUpgradeStateSnapshotRestoresDatabasesAndCarriesOperatorData(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(filepath.Join(state, "plugins", "exec", "old"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		writeDatabaseValue(t, filepath.Join(state, name), "before")
	}
	oldPlugin := filepath.Join(state, "plugins", "exec", "old", "plugin")
	if err := os.WriteFile(oldPlugin, []byte("old plugin"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(oldPlugin, 0o500); err != nil { //nolint:gosec // Fixture models an immutable executable plugin.
		t.Fatal(err)
	}
	result, err := backup.Create(context.Background(), state, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		writeDatabaseValue(t, filepath.Join(state, name), "after")
	}
	if err := os.RemoveAll(filepath.Join(state, "plugins")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(state, "plugins", "exec", "new"), 0o750); err != nil {
		t.Fatal(err)
	}
	newPlugin := filepath.Join(state, "plugins", "exec", "new", "plugin")
	if err := os.WriteFile(newPlugin, []byte("new plugin"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(newPlugin, 0o500); err != nil { //nolint:gosec // Fixture models an immutable executable plugin.
		t.Fatal(err)
	}
	workspace := filepath.Join(state, "workspaces", "run-current", "result.txt")
	if err := os.MkdirAll(filepath.Dir(workspace), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(workspace, []byte("keep current workspace"), 0o600); err != nil {
		t.Fatal(err)
	}

	snapshot := &upgradeStateSnapshot{archive: result.Path, stateDir: state, chownTree: func(string) error { return nil }}
	failed, err := snapshot.restore(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := readDatabaseValue(t, filepath.Join(state, "orchigram.sqlite")); got != "before" {
		t.Fatalf("restored product database value=%q", got)
	}
	if got := readDatabaseValue(t, filepath.Join(state, "workflows.sqlite")); got != "before" {
		t.Fatalf("restored workflow database value=%q", got)
	}
	if data, err := os.ReadFile(oldPlugin); err != nil || string(data) != "old plugin" { //nolint:gosec // Test reads its isolated fixture.
		t.Fatalf("restored plugin=%q err=%v", data, err)
	}
	if data, err := os.ReadFile(workspace); err != nil || string(data) != "keep current workspace" { //nolint:gosec // Test reads its isolated fixture.
		t.Fatalf("carried workspace=%q err=%v", data, err)
	}
	if got := readDatabaseValue(t, filepath.Join(failed, "orchigram.sqlite")); got != "after" {
		t.Fatalf("preserved failed database value=%q", got)
	}
	if _, err := os.Stat(result.Path); err != nil {
		t.Fatalf("pre-upgrade backup was not carried forward: %v", err)
	}
}

func TestWithinRejectsSiblingPrefix(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "state")
	if !within(root, filepath.Join(root, "backups", "valid.tar.gz")) {
		t.Fatal("valid descendant rejected")
	}
	if within(root, root+"-other/backup.tar.gz") {
		t.Fatal("sibling prefix accepted")
	}
}

func writeDatabaseValue(t *testing.T, path, value string) {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(context.Background(), `CREATE TABLE IF NOT EXISTS marker (value TEXT NOT NULL); DELETE FROM marker; INSERT INTO marker(value) VALUES (?)`, value); err != nil {
		t.Fatal(err)
	}
}

func readDatabaseValue(t *testing.T, path string) string {
	t.Helper()
	database, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	var value string
	if err := database.QueryRowContext(context.Background(), `SELECT value FROM marker`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

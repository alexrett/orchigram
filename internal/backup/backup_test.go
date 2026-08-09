package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateAndRestoreSQLiteAndPlugins(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(filepath.Join(state, "plugins", "exec", "0.1.0"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		createTestDatabase(t, filepath.Join(state, name))
	}
	pluginPath := filepath.Join(state, "plugins", "exec", "0.1.0", "plugin")
	if err := os.WriteFile(pluginPath, []byte("immutable-plugin"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(pluginPath, 0o500); err != nil { //nolint:gosec // Fixture models an immutable executable plugin.
		t.Fatal(err)
	}
	result, err := Create(context.Background(), state, "")
	if err != nil {
		t.Fatal(err)
	}
	archive, err := os.ReadFile(result.Path) //nolint:gosec // Test reads the returned private archive.
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(archive)
	if hex.EncodeToString(digest[:]) != result.SHA256 {
		t.Fatal("backup digest mismatch")
	}
	restored := filepath.Join(root, "restored")
	if err := Restore(context.Background(), result.Path, restored); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(restored, "plugins", "exec", "0.1.0", "plugin")) //nolint:gosec // Test reads a fixed restored path.
	if err != nil || string(data) != "immutable-plugin" {
		t.Fatalf("plugin=%q err=%v", data, err)
	}
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		if err := verifySQLite(context.Background(), filepath.Join(restored, name)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRestoreRejectsTraversal(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	archivePath := filepath.Join(root, "bad.tar.gz")
	file, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // Test-owned path under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	if err := tarWriter.WriteHeader(&tar.Header{Name: "../escaped", Typeflag: tar.TypeReg, Mode: 0o600}); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), archivePath, filepath.Join(root, "restored")); err == nil {
		t.Fatal("expected traversal archive to fail")
	}
	if _, err := os.Stat(filepath.Join(root, "escaped")); !os.IsNotExist(err) {
		t.Fatalf("traversal target exists: %v", err)
	}
}

func TestRestoreRejectsManifestMismatch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		createTestDatabase(t, filepath.Join(root, name))
	}
	archivePath := filepath.Join(root, "mismatch.tar.gz")
	file, err := os.OpenFile(archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600) //nolint:gosec // Test-owned path under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		if err := addFile(tarWriter, filepath.Join(root, name), name, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := addBytes(tarWriter, "plugins/exec/0.1.0/plugin", []byte("unexpected"), 0o500); err != nil {
		t.Fatal(err)
	}
	metadata, err := json.Marshal(manifest{Format: formatVersion, Files: []string{"orchigram.sqlite", "workflows.sqlite"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := addBytes(tarWriter, "backup.json", metadata, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gzipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := Restore(context.Background(), archivePath, filepath.Join(root, "restored")); err == nil {
		t.Fatal("expected manifest mismatch to fail")
	}
}

func TestCreateSnapshotsWorkflowBeforePossiblyAheadProductState(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.MkdirAll(state, 0o750); err != nil {
		t.Fatal(err)
	}
	workflowPath := filepath.Join(state, "workflows.sqlite")
	productPath := filepath.Join(state, "orchigram.sqlite")
	createValueDatabase(t, workflowPath, "scheduled")
	createValueDatabase(t, productPath, "pending")
	result, err := create(context.Background(), state, "", func() error {
		if err := updateValueDatabase(productPath, "completed"); err != nil {
			return err
		}
		return updateValueDatabase(workflowPath, "completed")
	})
	if err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(root, "restored")
	if err := Restore(context.Background(), result.Path, restored); err != nil {
		t.Fatal(err)
	}
	if value := readValueDatabase(t, filepath.Join(restored, "workflows.sqlite")); value != "scheduled" {
		t.Fatalf("workflow snapshot=%q", value)
	}
	if value := readValueDatabase(t, filepath.Join(restored, "orchigram.sqlite")); value != "completed" {
		t.Fatalf("product snapshot=%q", value)
	}
}

func createTestDatabase(t *testing.T, path string) {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(context.Background(), "CREATE TABLE evidence(value TEXT); INSERT INTO evidence(value) VALUES ('ok')"); err != nil {
		t.Fatal(err)
	}
}

func createValueDatabase(t *testing.T, path, value string) {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(context.Background(), "CREATE TABLE evidence(value TEXT); INSERT INTO evidence(value) VALUES (?)", value); err != nil {
		t.Fatal(err)
	}
}

func updateValueDatabase(path, value string) error {
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()
	_, err = database.ExecContext(context.Background(), "UPDATE evidence SET value=?", value)
	return err
}

func readValueDatabase(t *testing.T, path string) string {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = database.Close() }()
	var value string
	if err := database.QueryRowContext(context.Background(), "SELECT value FROM evidence").Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

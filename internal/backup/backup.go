// Package backup creates and restores bounded, path-safe single-node snapshots.
package backup

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	_ "modernc.org/sqlite" // Register the pure-Go SQLite database/sql driver.
)

const (
	formatVersion    = 1
	maxArchiveSize   = int64(2 << 30)
	maxExtractedSize = int64(4 << 30)
	maxArchivedFile  = int64(256 << 20)
	maxArchiveFiles  = 10000
)

// Result identifies one completed backup archive.
type Result struct {
	Path   string
	SHA256 string
}

type manifest struct {
	Format        int      `json:"format"`
	CreatedAt     string   `json:"createdAt"`
	SnapshotOrder []string `json:"snapshotOrder,omitempty"`
	Files         []string `json:"files"`
}

// Create snapshots both SQLite databases and immutable plugin installations.
// Destination must remain under stateDir so the hardened service cannot be
// used as an arbitrary filesystem writer.
func Create(ctx context.Context, stateDir, destination string) (Result, error) {
	return create(ctx, stateDir, destination, nil)
}

func create(ctx context.Context, stateDir, destination string, afterWorkflowSnapshot func() error) (Result, error) {
	stateDir = filepath.Clean(stateDir)
	if !filepath.IsAbs(stateDir) {
		return Result{}, errors.New("state directory must be absolute")
	}
	if destination == "" {
		destination = filepath.Join(stateDir, "backups")
	}
	destination = filepath.Clean(destination)
	if !filepath.IsAbs(destination) || !within(stateDir, destination) {
		return Result{}, errors.New("backup destination must be inside the state directory")
	}
	finalPath := destination
	if !strings.HasSuffix(strings.ToLower(destination), ".tar.gz") {
		finalPath = filepath.Join(destination, "orchigram-backup-"+time.Now().UTC().Format("20060102T150405Z")+"-"+uuid.NewString()[:8]+".tar.gz")
	}
	if _, err := os.Stat(finalPath); err == nil {
		return Result{}, fmt.Errorf("backup destination already exists: %s", finalPath)
	} else if !errors.Is(err, os.ErrNotExist) {
		return Result{}, err
	}
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o750); err != nil {
		return Result{}, err
	}
	working, err := os.MkdirTemp(stateDir, ".backup-snapshot-")
	if err != nil {
		return Result{}, err
	}
	defer func() { _ = os.RemoveAll(working) }()
	// Snapshot durable history first and product state second. A concurrent
	// activity can therefore make product state ahead of history, which the
	// idempotent attempt replay contract reconciles. The inverse ordering could
	// restore history ahead of its authoritative product evidence.
	for index, name := range []string{"workflows.sqlite", "orchigram.sqlite"} {
		if err := snapshotSQLite(ctx, filepath.Join(stateDir, name), filepath.Join(working, name)); err != nil {
			return Result{}, fmt.Errorf("snapshot %s: %w", name, err)
		}
		if index == 0 && afterWorkflowSnapshot != nil {
			if err := afterWorkflowSnapshot(); err != nil {
				return Result{}, err
			}
		}
	}
	temporary, err := os.CreateTemp(filepath.Dir(finalPath), ".backup-*.tar.gz")
	if err != nil {
		return Result{}, err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return Result{}, err
	}
	archiveHash := sha256.New()
	writer := io.MultiWriter(temporary, archiveHash)
	gzipWriter, err := gzip.NewWriterLevel(writer, gzip.BestSpeed)
	if err != nil {
		_ = temporary.Close()
		return Result{}, err
	}
	tarWriter := tar.NewWriter(gzipWriter)
	files := []string{"orchigram.sqlite", "workflows.sqlite"}
	for _, name := range files {
		if err := addFile(tarWriter, filepath.Join(working, name), name, 0o600); err != nil {
			_ = tarWriter.Close()
			_ = gzipWriter.Close()
			_ = temporary.Close()
			return Result{}, err
		}
	}
	pluginFiles, err := addTree(tarWriter, filepath.Join(stateDir, "plugins"), "plugins")
	if err != nil {
		_ = tarWriter.Close()
		_ = gzipWriter.Close()
		_ = temporary.Close()
		return Result{}, err
	}
	files = append(files, pluginFiles...)
	metadata, err := json.Marshal(manifest{Format: formatVersion, CreatedAt: time.Now().UTC().Format(time.RFC3339), SnapshotOrder: []string{"workflows.sqlite", "orchigram.sqlite"}, Files: files})
	if err != nil {
		return Result{}, err
	}
	if err := addBytes(tarWriter, "backup.json", metadata, 0o600); err != nil {
		return Result{}, err
	}
	if err := tarWriter.Close(); err != nil {
		return Result{}, err
	}
	if err := gzipWriter.Close(); err != nil {
		return Result{}, err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return Result{}, err
	}
	if err := temporary.Close(); err != nil {
		return Result{}, err
	}
	if err := os.Rename(temporaryPath, finalPath); err != nil {
		return Result{}, err
	}
	return Result{Path: finalPath, SHA256: hex.EncodeToString(archiveHash.Sum(nil))}, nil
}

// Restore extracts a verified backup into a destination that does not exist.
// It writes a sibling temporary directory and renames only after both SQLite
// databases pass integrity checks.
func Restore(ctx context.Context, archivePath, destination string) error {
	archivePath, destination = filepath.Clean(archivePath), filepath.Clean(destination)
	if !filepath.IsAbs(archivePath) || !filepath.IsAbs(destination) {
		return errors.New("archive and restore destination must be absolute")
	}
	if _, err := os.Stat(destination); err == nil {
		return errors.New("restore destination must not exist")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	input, err := os.Open(archivePath) //nolint:gosec // Operator-selected local archive, validated entry-by-entry.
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if info.Size() <= 0 || info.Size() > maxArchiveSize {
		return errors.New("backup archive size is invalid")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o750); err != nil {
		return err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(destination), ".restore-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	gzipReader, err := gzip.NewReader(input)
	if err != nil {
		return err
	}
	defer func() { _ = gzipReader.Close() }()
	tarReader := tar.NewReader(gzipReader)
	seen := map[string]bool{}
	count := 0
	var extractedSize int64
	for {
		header, nextErr := tarReader.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return nextErr
		}
		count++
		if count > maxArchiveFiles || header.Size < 0 || header.Size > maxArchivedFile {
			return errors.New("backup archive limits exceeded")
		}
		extractedSize += header.Size
		if extractedSize > maxExtractedSize {
			return errors.New("backup extracted size exceeds limit")
		}
		name, err := safeArchiveName(header.Name)
		if err != nil {
			return err
		}
		if header.Typeflag != tar.TypeReg || seen[name] {
			return fmt.Errorf("unsupported or duplicate backup entry %q", name)
		}
		seen[name] = true
		target := filepath.Join(temporary, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
			return err
		}
		mode := fs.FileMode(0o600)
		if header.Mode&0o100 != 0 {
			mode = 0o500
		}
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode) //nolint:gosec // Mode is bounded and destination is a private temporary tree.
		if err != nil {
			return err
		}
		written, copyErr := io.Copy(output, io.LimitReader(tarReader, header.Size+1))
		closeErr := output.Close()
		if copyErr != nil || closeErr != nil || written != header.Size {
			return errors.Join(copyErr, closeErr, errors.New("backup entry size mismatch"))
		}
	}
	for _, required := range []string{"backup.json", "orchigram.sqlite", "workflows.sqlite"} {
		if !seen[required] {
			return fmt.Errorf("backup is missing %s", required)
		}
	}
	metadata, err := os.ReadFile(filepath.Join(temporary, "backup.json")) //nolint:gosec // Path is fixed inside the private restore tree.
	if err != nil {
		return err
	}
	var decoded manifest
	if err := json.Unmarshal(metadata, &decoded); err != nil || decoded.Format != formatVersion {
		return errors.New("unsupported backup manifest")
	}
	manifestFiles := make(map[string]bool, len(decoded.Files))
	for _, entry := range decoded.Files {
		name, err := safeArchiveName(entry)
		if err != nil || name == "backup.json" || manifestFiles[name] || !seen[name] {
			return errors.New("backup manifest does not match archive contents")
		}
		manifestFiles[name] = true
	}
	if len(manifestFiles)+1 != len(seen) {
		return errors.New("backup manifest does not match archive contents")
	}
	for _, name := range []string{"orchigram.sqlite", "workflows.sqlite"} {
		if err := verifySQLite(ctx, filepath.Join(temporary, name)); err != nil {
			return fmt.Errorf("verify restored %s: %w", name, err)
		}
	}
	return os.Rename(temporary, destination)
}

func snapshotSQLite(ctx context.Context, source, target string) error {
	if _, err := os.Stat(source); err != nil {
		return err
	}
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(source)+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	database.SetMaxOpenConns(1)
	defer func() { _ = database.Close() }()
	if _, err := database.ExecContext(ctx, "VACUUM INTO ?", target); err != nil {
		return err
	}
	return verifySQLite(ctx, target)
}

func verifySQLite(ctx context.Context, path string) error {
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		return err
	}
	defer func() { _ = database.Close() }()
	var result string
	if err := database.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return fmt.Errorf("integrity_check: %s", result)
	}
	return nil
}

func addTree(writer *tar.Writer, root, prefix string) ([]string, error) {
	if _, err := os.Stat(root); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	files := []string{}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() > maxArchivedFile {
			return fmt.Errorf("unsupported plugin backup file %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		name := filepath.ToSlash(filepath.Join(prefix, relative))
		if err := addFile(writer, path, name, info.Mode().Perm()&0o750); err != nil {
			return err
		}
		files = append(files, name)
		return nil
	})
	return files, err
}

func addFile(writer *tar.Writer, path, name string, mode fs.FileMode) error {
	input, err := os.Open(filepath.Clean(path)) //nolint:gosec // Paths originate from fixed state roots and WalkDir.
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	info, err := input.Stat()
	if err != nil {
		return err
	}
	if info.Size() > maxArchivedFile {
		return fmt.Errorf("backup file %s exceeds limit", name)
	}
	header := &tar.Header{Name: name, Mode: int64(mode), Size: info.Size(), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	written, err := io.Copy(writer, io.LimitReader(input, info.Size()+1))
	if err != nil || written != info.Size() {
		return errors.Join(err, errors.New("backup source changed while reading"))
	}
	return nil
}

func addBytes(writer *tar.Writer, name string, data []byte, mode fs.FileMode) error {
	header := &tar.Header{Name: name, Mode: int64(mode), Size: int64(len(data)), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg}
	if err := writer.WriteHeader(header); err != nil {
		return err
	}
	_, err := writer.Write(data)
	return err
}

func safeArchiveName(name string) (string, error) {
	name = filepath.ToSlash(filepath.Clean(name))
	if name == "." || name == "" || strings.HasPrefix(name, "/") || name == ".." || strings.HasPrefix(name, "../") {
		return "", fmt.Errorf("unsafe backup entry %q", name)
	}
	if name != "backup.json" && name != "orchigram.sqlite" && name != "workflows.sqlite" && !strings.HasPrefix(name, "plugins/") {
		return "", fmt.Errorf("unexpected backup entry %q", name)
	}
	return name, nil
}

func within(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

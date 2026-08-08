// Package pluginpack builds community plugin bundles from local manifests.
package pluginpack

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/alexrett/orchigram/internal/pluginbundle"
	"gopkg.in/yaml.v3"
)

const (
	maxManifest = 1 << 20
	maxBinary   = 96 << 20
)

// Result identifies the canonical bundle written by Pack.
type Result struct {
	Path   string
	SHA256 string
}

// Pack reads platform binaries relative to manifestPath and atomically creates outputPath.
func Pack(manifestPath, outputPath string, force bool) (Result, error) {
	if strings.TrimSpace(manifestPath) == "" || strings.TrimSpace(outputPath) == "" {
		return Result{}, errors.New("manifest and output paths are required")
	}
	manifestData, err := readRegular(manifestPath, maxManifest)
	if err != nil {
		return Result{}, fmt.Errorf("read plugin manifest: %w", err)
	}
	var manifest pluginbundle.Manifest
	decoder := yaml.NewDecoder(bytes.NewReader(manifestData))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return Result{}, fmt.Errorf("decode plugin manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Result{}, errors.New("plugin manifest must contain exactly one document")
	}
	base, err := filepath.Abs(filepath.Dir(manifestPath))
	if err != nil {
		return Result{}, err
	}
	binaries := make(map[string][]byte, len(manifest.Platforms))
	seenTargets := map[string]bool{}
	for index := range manifest.Platforms {
		platform := &manifest.Platforms[index]
		cleaned, cleanErr := cleanRelative(platform.Path)
		if cleanErr != nil {
			return Result{}, cleanErr
		}
		if cleaned != platform.Path {
			return Result{}, fmt.Errorf("platform path %q must be normalized", platform.Path)
		}
		if seenTargets[platform.Path] {
			return Result{}, fmt.Errorf("duplicate platform target %q", platform.Path)
		}
		seenTargets[platform.Path] = true
		binary, readErr := secureReadRegularAt(base, platform.Path, maxBinary, nil)
		if readErr != nil {
			return Result{}, fmt.Errorf("read platform binary %q: %w", platform.Path, readErr)
		}
		digest := sha256.Sum256(binary)
		digestText := hex.EncodeToString(digest[:])
		if platform.SHA256 != "" && !strings.EqualFold(platform.SHA256, digestText) {
			return Result{}, fmt.Errorf("platform binary %q digest mismatch", platform.Path)
		}
		platform.SHA256 = digestText
		binaries[platform.Path] = binary
	}
	if err := manifest.Validate(); err != nil {
		return Result{}, err
	}
	bundle, err := pluginbundle.Build(manifest, binaries)
	if err != nil {
		return Result{}, err
	}
	absoluteOutput, err := filepath.Abs(outputPath)
	if err != nil {
		return Result{}, err
	}
	if err := writeAtomic(absoluteOutput, bundle, force); err != nil {
		return Result{}, err
	}
	digest := sha256.Sum256(bundle)
	return Result{Path: absoluteOutput, SHA256: hex.EncodeToString(digest[:])}, nil
}

func readRegular(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("input is not a regular file")
	}
	if info.Size() < 0 || info.Size() > limit {
		return nil, fmt.Errorf("input exceeds %d-byte limit", limit)
	}
	file, err := os.Open(path) //nolint:gosec // The local operator supplies the source path.
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("input exceeds %d-byte limit", limit)
	}
	return data, nil
}

func cleanRelative(value string) (string, error) {
	if filepath.IsAbs(value) || strings.Contains(value, `\`) {
		return "", fmt.Errorf("unsafe platform path %q", value)
	}
	cleaned := filepath.ToSlash(filepath.Clean(value))
	if cleaned == "." || cleaned == "" || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("unsafe platform path %q", value)
	}
	return cleaned, nil
}

func writeAtomic(destination string, data []byte, force bool) error {
	directory := filepath.Dir(destination)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, ".orchigram-plugin-*.tmp")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o640); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if force {
		return os.Rename(temporaryPath, destination)
	}
	if err := os.Link(temporaryPath, destination); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("output %q already exists; use --force to replace it", destination)
		}
		return err
	}
	return nil
}

// Package pluginbundle validates and installs immutable plugin archives.
package pluginbundle

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"gopkg.in/yaml.v3"
)

const (
	// APIVersion identifies the stable v0.1 bundle manifest shape.
	APIVersion = "orchigram.dev/plugin/v1alpha1"
	maxBundle  = 128 << 20
	maxFile    = 96 << 20
)

var pluginName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// ProtocolRange declares compatible business protocol versions.
type ProtocolRange struct {
	Minimum uint32 `yaml:"minimum" json:"minimum"`
	Maximum uint32 `yaml:"maximum" json:"maximum"`
}

// Platform selects one binary and pins its content digest.
type Platform struct {
	OS     string `yaml:"os" json:"os"`
	Arch   string `yaml:"arch" json:"arch"`
	Path   string `yaml:"path" json:"path"`
	SHA256 string `yaml:"sha256" json:"sha256"`
}

// Manifest is stored at plugin.yaml in every bundle.
type Manifest struct {
	APIVersion   string        `yaml:"apiVersion" json:"apiVersion"`
	Name         string        `yaml:"name" json:"name"`
	Version      string        `yaml:"version" json:"version"`
	Protocol     ProtocolRange `yaml:"protocol" json:"protocol"`
	Capabilities []string      `yaml:"capabilities" json:"capabilities"`
	Platforms    []Platform    `yaml:"platforms" json:"platforms"`
}

// Installed is the verified local projection returned by Install.
type Installed struct {
	Manifest   Manifest
	Digest     string
	Directory  string
	Executable string
}

// Parse validates the archive and returns the current platform payload.
func Parse(bundle []byte) (Manifest, []byte, string, error) {
	if len(bundle) == 0 || len(bundle) > maxBundle {
		return Manifest{}, nil, "", errors.New("plugin bundle must be between 1 byte and 128 MiB")
	}
	digestBytes := sha256.Sum256(bundle)
	digest := hex.EncodeToString(digestBytes[:])
	gzipReader, err := gzip.NewReader(bytes.NewReader(bundle))
	if err != nil {
		return Manifest{}, nil, "", fmt.Errorf("open plugin bundle: %w", err)
	}
	defer func() { _ = gzipReader.Close() }()
	archive := tar.NewReader(gzipReader)
	files := map[string][]byte{}
	for {
		header, nextErr := archive.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return Manifest{}, nil, "", fmt.Errorf("read plugin bundle: %w", nextErr)
		}
		if header.Typeflag == tar.TypeDir {
			continue
		}
		if header.Typeflag != tar.TypeReg {
			return Manifest{}, nil, "", fmt.Errorf("bundle entry %q is not a regular file", header.Name)
		}
		name, cleanErr := cleanArchivePath(header.Name)
		if cleanErr != nil {
			return Manifest{}, nil, "", cleanErr
		}
		if header.Size < 0 || header.Size > maxFile {
			return Manifest{}, nil, "", fmt.Errorf("bundle entry %q exceeds size limit", name)
		}
		if _, duplicate := files[name]; duplicate {
			return Manifest{}, nil, "", fmt.Errorf("duplicate bundle entry %q", name)
		}
		data, readErr := io.ReadAll(io.LimitReader(archive, header.Size+1))
		if readErr != nil || int64(len(data)) != header.Size {
			return Manifest{}, nil, "", fmt.Errorf("read bundle entry %q: %w", name, readErr)
		}
		files[name] = data
	}
	manifestData, exists := files["plugin.yaml"]
	if !exists {
		return Manifest{}, nil, "", errors.New("plugin.yaml is required")
	}
	var manifest Manifest
	decoder := yaml.NewDecoder(bytes.NewReader(manifestData))
	decoder.KnownFields(true)
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, nil, "", fmt.Errorf("decode plugin.yaml: %w", err)
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, nil, "", err
	}
	platform, err := manifest.CurrentPlatform()
	if err != nil {
		return Manifest{}, nil, "", err
	}
	binary, exists := files[platform.Path]
	if !exists {
		return Manifest{}, nil, "", fmt.Errorf("platform binary %q is missing", platform.Path)
	}
	binaryDigest := sha256.Sum256(binary)
	if !strings.EqualFold(platform.SHA256, hex.EncodeToString(binaryDigest[:])) {
		return Manifest{}, nil, "", fmt.Errorf("platform binary %q digest mismatch", platform.Path)
	}
	return manifest, binary, digest, nil
}

// Validate checks names, semantic version, protocol range, and platform uniqueness.
func (m Manifest) Validate() error {
	if m.APIVersion != APIVersion {
		return fmt.Errorf("unsupported plugin apiVersion %q", m.APIVersion)
	}
	if !pluginName.MatchString(m.Name) {
		return fmt.Errorf("invalid plugin name %q", m.Name)
	}
	if _, err := semver.StrictNewVersion(m.Version); err != nil {
		return fmt.Errorf("plugin version must be semantic: %w", err)
	}
	if m.Protocol.Minimum == 0 || m.Protocol.Maximum < m.Protocol.Minimum {
		return errors.New("plugin protocol range is invalid")
	}
	if len(m.Capabilities) == 0 {
		return errors.New("plugin capabilities must not be empty")
	}
	seen := map[string]bool{}
	for _, platform := range m.Platforms {
		key := platform.OS + "/" + platform.Arch
		if seen[key] {
			return fmt.Errorf("duplicate plugin platform %s", key)
		}
		seen[key] = true
		if _, err := cleanArchivePath(platform.Path); err != nil {
			return err
		}
		decoded, err := hex.DecodeString(platform.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("platform %s has invalid sha256", key)
		}
	}
	return nil
}

// CurrentPlatform selects the binary for this daemon host.
func (m Manifest) CurrentPlatform() (Platform, error) {
	for _, platform := range m.Platforms {
		if platform.OS == runtime.GOOS && platform.Arch == runtime.GOARCH {
			return platform, nil
		}
	}
	return Platform{}, fmt.Errorf("plugin %s %s has no binary for %s/%s", m.Name, m.Version, runtime.GOOS, runtime.GOARCH)
}

// Install atomically writes an immutable verified version directory.
func Install(root string, bundle []byte) (Installed, error) {
	manifest, binary, digest, err := Parse(bundle)
	if err != nil {
		return Installed{}, err
	}
	finalDirectory := filepath.Join(root, manifest.Name, manifest.Version)
	if existing, statErr := os.Stat(finalDirectory); statErr == nil && existing.IsDir() {
		digestData, readErr := os.ReadFile(filepath.Join(finalDirectory, "bundle.sha256")) //nolint:gosec // Path is derived from validated manifest components.
		if readErr == nil && strings.TrimSpace(string(digestData)) == digest {
			return Installed{Manifest: manifest, Digest: digest, Directory: finalDirectory, Executable: filepath.Join(finalDirectory, "plugin")}, nil
		}
		return Installed{}, fmt.Errorf("plugin %s version %s is already installed with a different digest", manifest.Name, manifest.Version)
	}
	if err := os.MkdirAll(filepath.Dir(finalDirectory), 0o750); err != nil {
		return Installed{}, err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(finalDirectory), ".install-")
	if err != nil {
		return Installed{}, err
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	manifestData, err := yaml.Marshal(manifest)
	if err != nil {
		return Installed{}, err
	}
	if err := os.WriteFile(filepath.Join(temporary, "plugin.yaml"), manifestData, 0o600); err != nil {
		return Installed{}, err
	}
	if err := os.WriteFile(filepath.Join(temporary, "bundle.sha256"), []byte(digest+"\n"), 0o600); err != nil {
		return Installed{}, err
	}
	if err := os.WriteFile(filepath.Join(temporary, "plugin"), binary, 0o500); err != nil { //nolint:gosec // The verified payload must be owner-executable and not writable.
		return Installed{}, err
	}
	if err := os.Rename(temporary, finalDirectory); err != nil {
		return Installed{}, err
	}
	return Installed{Manifest: manifest, Digest: digest, Directory: finalDirectory, Executable: filepath.Join(finalDirectory, "plugin")}, nil
}

// Build creates a deterministic tar.gz bundle for release and tests.
func Build(manifest Manifest, binaries map[string][]byte) ([]byte, error) {
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	manifestData, err := yaml.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	files := map[string][]byte{"plugin.yaml": manifestData}
	for _, platform := range manifest.Platforms {
		binary, exists := binaries[platform.Path]
		if !exists {
			return nil, fmt.Errorf("binary %q is missing", platform.Path)
		}
		digest := sha256.Sum256(binary)
		if !strings.EqualFold(platform.SHA256, hex.EncodeToString(digest[:])) {
			return nil, fmt.Errorf("binary %q digest mismatch", platform.Path)
		}
		files[platform.Path] = binary
	}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var output bytes.Buffer
	// BestSpeed keeps release packaging deterministic while making large static
	// Go plugin binaries practical in race and cross-platform conformance suites.
	gzipWriter, err := gzip.NewWriterLevel(&output, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	gzipWriter.ModTime = unixEpoch
	gzipWriter.OS = 255
	tarWriter := tar.NewWriter(gzipWriter)
	for _, name := range names {
		mode := int64(0o640)
		if name != "plugin.yaml" {
			mode = 0o550
		}
		data := files[name]
		header := &tar.Header{Name: name, Mode: mode, Size: int64(len(data)), ModTime: unixEpoch, Typeflag: tar.TypeReg}
		if err := tarWriter.WriteHeader(header); err != nil {
			return nil, err
		}
		if _, err := tarWriter.Write(data); err != nil {
			return nil, err
		}
	}
	if err := tarWriter.Close(); err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

var unixEpoch = time.Unix(0, 0).UTC()

func cleanArchivePath(value string) (string, error) {
	cleaned := path.Clean(strings.ReplaceAll(value, "\\", "/"))
	if cleaned == "." || cleaned == "" || strings.HasPrefix(cleaned, "/") || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("unsafe bundle path %q", value)
	}
	return cleaned, nil
}

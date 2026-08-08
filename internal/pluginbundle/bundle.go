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

var (
	pluginName     = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	targetName     = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,31}$`)
	capabilityName = regexp.MustCompile(`^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+$`)
)

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
	Manifest       Manifest
	Digest         string
	Directory      string
	Executable     string
	FinalDirectory string
	Published      bool
}

// Parse validates the archive and returns the current platform payload.
func Parse(bundle []byte) (Manifest, []byte, string, error) {
	return ParseForPlatform(bundle, runtime.GOOS, runtime.GOARCH)
}

// ParseForPlatform validates an archive and returns its selected target payload.
// Release tooling uses this to verify bundles before publishing them.
func ParseForPlatform(bundle []byte, targetOS, targetArch string) (Manifest, []byte, string, error) {
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
	platform, err := manifest.Platform(targetOS, targetArch)
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
	if len(m.Platforms) == 0 {
		return errors.New("plugin platforms must not be empty")
	}
	seenCapabilities := map[string]bool{}
	for _, capability := range m.Capabilities {
		if !capabilityName.MatchString(capability) || seenCapabilities[capability] {
			return fmt.Errorf("invalid or duplicate plugin capability %q", capability)
		}
		namespace := strings.SplitN(capability, ".", 2)[0]
		if namespace != "task" && namespace != "trigger" && namespace != "agent" {
			return fmt.Errorf("unsupported plugin capability namespace %q", namespace)
		}
		if namespace == "task" && !strings.HasPrefix(capability, "task."+m.Name+".") {
			return fmt.Errorf("task capability %q must be rooted at plugin name %q", capability, m.Name)
		}
		seenCapabilities[capability] = true
	}
	seen := map[string]bool{}
	seenPaths := map[string]bool{}
	for _, platform := range m.Platforms {
		key := platform.OS + "/" + platform.Arch
		if !targetName.MatchString(platform.OS) || !targetName.MatchString(platform.Arch) {
			return fmt.Errorf("invalid plugin platform %q", key)
		}
		if seen[key] {
			return fmt.Errorf("duplicate plugin platform %s", key)
		}
		seen[key] = true
		cleaned, err := cleanArchivePath(platform.Path)
		if err != nil {
			return err
		}
		if cleaned != platform.Path {
			return fmt.Errorf("platform %s path must be normalized", key)
		}
		if seenPaths[platform.Path] {
			return fmt.Errorf("duplicate platform target %q", platform.Path)
		}
		seenPaths[platform.Path] = true
		decoded, err := hex.DecodeString(platform.SHA256)
		if err != nil || len(decoded) != sha256.Size {
			return fmt.Errorf("platform %s has invalid sha256", key)
		}
	}
	return nil
}

// CurrentPlatform selects the binary for this daemon host.
func (m Manifest) CurrentPlatform() (Platform, error) {
	return m.Platform(runtime.GOOS, runtime.GOARCH)
}

// Platform selects the declared binary for a target.
func (m Manifest) Platform(targetOS, targetArch string) (Platform, error) {
	for _, platform := range m.Platforms {
		if platform.OS == targetOS && platform.Arch == targetArch {
			return platform, nil
		}
	}
	return Platform{}, fmt.Errorf("plugin %s %s has no binary for %s/%s", m.Name, m.Version, targetOS, targetArch)
}

// Install atomically writes an immutable verified version directory.
func Install(root string, bundle []byte) (Installed, error) {
	staged, err := Stage(root, bundle)
	if err != nil {
		return Installed{}, err
	}
	defer func() { _ = os.RemoveAll(staged.Directory) }()
	return Publish(staged)
}

// Stage writes a verified bundle to a private temporary directory. Callers may
// launch the staged executable for negotiation without publishing the version.
func Stage(root string, bundle []byte) (Installed, error) {
	manifest, binary, digest, err := Parse(bundle)
	if err != nil {
		return Installed{}, err
	}
	finalDirectory := filepath.Join(root, manifest.Name, manifest.Version)
	if err := os.MkdirAll(filepath.Dir(finalDirectory), 0o750); err != nil {
		return Installed{}, err
	}
	temporary, err := os.MkdirTemp(filepath.Dir(finalDirectory), ".install-")
	if err != nil {
		return Installed{}, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			_ = os.RemoveAll(temporary)
		}
	}()
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
	succeeded = true
	return Installed{Manifest: manifest, Digest: digest, Directory: temporary, Executable: filepath.Join(temporary, "plugin"), FinalDirectory: finalDirectory}, nil
}

// Publish atomically moves a negotiated staged installation into its immutable
// version directory. Publishing the same digest is idempotent.
func Publish(staged Installed) (Installed, error) {
	if staged.FinalDirectory == "" || staged.Directory == "" {
		return Installed{}, errors.New("staged plugin installation is incomplete")
	}
	if existing, statErr := os.Stat(staged.FinalDirectory); statErr == nil && existing.IsDir() {
		digestData, readErr := os.ReadFile(filepath.Join(staged.FinalDirectory, "bundle.sha256")) //nolint:gosec // Path is derived from validated manifest components.
		if readErr == nil && strings.TrimSpace(string(digestData)) == staged.Digest {
			staged.Directory = staged.FinalDirectory
			staged.Executable = filepath.Join(staged.FinalDirectory, "plugin")
			return staged, nil
		}
		return Installed{}, fmt.Errorf("plugin %s version %s is already installed with a different digest", staged.Manifest.Name, staged.Manifest.Version)
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return Installed{}, statErr
	}
	if err := os.Rename(staged.Directory, staged.FinalDirectory); err != nil {
		return Installed{}, err
	}
	staged.Directory = staged.FinalDirectory
	staged.Executable = filepath.Join(staged.FinalDirectory, "plugin")
	staged.Published = true
	return staged, nil
}

// Build creates a deterministic tar.gz bundle for release and tests.
func Build(manifest Manifest, binaries map[string][]byte) ([]byte, error) {
	manifest = normalizeManifest(manifest)
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
	gzipWriter.Header = gzip.Header{ModTime: unixEpoch, OS: 255}
	tarWriter := tar.NewWriter(gzipWriter)
	for _, name := range names {
		mode := int64(0o640)
		if name != "plugin.yaml" {
			mode = 0o550
		}
		data := files[name]
		header := &tar.Header{
			Name: name, Mode: mode, Size: int64(len(data)), ModTime: unixEpoch,
			Typeflag: tar.TypeReg, Uid: 0, Gid: 0, Uname: "", Gname: "", Format: tar.FormatUSTAR,
		}
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
	if output.Len() > maxBundle {
		return nil, errors.New("plugin bundle exceeds 128 MiB")
	}
	return output.Bytes(), nil
}

var unixEpoch = time.Unix(0, 0).UTC()

func normalizeManifest(manifest Manifest) Manifest {
	manifest.Capabilities = append([]string(nil), manifest.Capabilities...)
	sort.Strings(manifest.Capabilities)
	manifest.Platforms = append([]Platform(nil), manifest.Platforms...)
	sort.Slice(manifest.Platforms, func(i, j int) bool {
		left, right := manifest.Platforms[i], manifest.Platforms[j]
		if left.OS != right.OS {
			return left.OS < right.OS
		}
		if left.Arch != right.Arch {
			return left.Arch < right.Arch
		}
		return left.Path < right.Path
	})
	return manifest
}

func cleanArchivePath(value string) (string, error) {
	cleaned := path.Clean(strings.ReplaceAll(value, "\\", "/"))
	if cleaned == "." || cleaned == "" || strings.HasPrefix(cleaned, "/") || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return "", fmt.Errorf("unsafe bundle path %q", value)
	}
	return cleaned, nil
}

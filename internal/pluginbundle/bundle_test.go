package pluginbundle

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestBuildParseAndImmutableInstall(t *testing.T) {
	t.Parallel()
	binary := []byte("verified plugin executable")
	digest := sha256.Sum256(binary)
	manifest := Manifest{
		APIVersion: APIVersion, Name: "conformance", Version: "0.1.0",
		Protocol: ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.conformance"},
		Platforms: []Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	first, err := Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(second) {
		t.Fatal("bundle build is not deterministic")
	}
	parsed, payload, bundleDigest, err := Parse(first)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Name != manifest.Name || string(payload) != string(binary) || len(bundleDigest) != 64 {
		t.Fatalf("unexpected parse result: %+v %q %q", parsed, payload, bundleDigest)
	}
	root := t.TempDir()
	installed, err := Install(root, first)
	if err != nil {
		t.Fatal(err)
	}
	mode, err := os.Stat(installed.Executable)
	if err != nil {
		t.Fatal(err)
	}
	if mode.Mode().Perm() != 0o500 {
		t.Fatalf("executable mode is %o", mode.Mode().Perm())
	}
	if _, err := Install(root, first); err != nil {
		t.Fatalf("same digest must be idempotent: %v", err)
	}
	if err := os.WriteFile(filepath.Join(installed.Directory, "bundle.sha256"), []byte("different\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Install(root, first); err == nil {
		t.Fatal("version directory accepted a different installed digest")
	}
}

func TestManifestRejectsTraversalAndIncompatiblePlatform(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("x"))
	manifest := Manifest{
		APIVersion: APIVersion, Name: "unsafe", Version: "0.1.0",
		Protocol: ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.test"},
		Platforms: []Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "../plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	if err := manifest.Validate(); err == nil {
		t.Fatal("unsafe platform path was accepted")
	}
	manifest.Platforms[0].Path = "bin/plugin"
	manifest.Platforms[0].OS = "unsupported"
	if _, err := manifest.CurrentPlatform(); err == nil {
		t.Fatal("missing current platform was accepted")
	}
}

func TestParseForForeignPlatform(t *testing.T) {
	t.Parallel()
	binary := []byte("foreign executable")
	digest := sha256.Sum256(binary)
	manifest := Manifest{
		APIVersion: APIVersion, Name: "foreign", Version: "1.2.3",
		Protocol: ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.foreign"},
		Platforms: []Platform{{OS: "plan9", Arch: "amd64", Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	bundle, err := Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	parsed, payload, _, err := ParseForPlatform(bundle, "plan9", "amd64")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Name != manifest.Name || !bytes.Equal(payload, binary) {
		t.Fatalf("unexpected parsed bundle: %#v %q", parsed, payload)
	}
}

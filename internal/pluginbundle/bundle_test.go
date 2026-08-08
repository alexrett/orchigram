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
		Protocol: ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.conformance.run"},
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
	shuffled := manifest
	shuffled.Capabilities = []string{"task.conformance.zeta", "task.conformance.run"}
	manifest.Capabilities = []string{"task.conformance.run", "task.conformance.zeta"}
	first, err = Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	second, err = Build(shuffled, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("manifest capability ordering changed bundle bytes")
	}
	parsed, payload, bundleDigest, err := Parse(first)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Name != manifest.Name || string(payload) != string(binary) || len(bundleDigest) != 64 {
		t.Fatalf("unexpected parse result: %+v %q %q", parsed, payload, bundleDigest)
	}
	root := t.TempDir()
	staged, err := Stage(root, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(staged.FinalDirectory); !os.IsNotExist(err) {
		t.Fatalf("staged plugin was published before negotiation: %v", err)
	}
	if _, err := os.Stat(staged.Executable); err != nil {
		t.Fatalf("staged executable is unavailable: %v", err)
	}
	installed, err := Publish(staged)
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
		Protocol: ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.unsafe.run"},
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
		Protocol: ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.foreign.run"},
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

func TestManifestRejectsUnsupportedAndUnroutableCapabilities(t *testing.T) {
	t.Parallel()
	digest := sha256.Sum256([]byte("x"))
	base := Manifest{
		APIVersion: APIVersion, Name: "echo", Version: "1.0.0",
		Protocol:  ProtocolRange{Minimum: 1, Maximum: 1},
		Platforms: []Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	for _, capability := range []string{"storage.echo.read", "task.other.run"} {
		manifest := base
		manifest.Capabilities = []string{capability}
		if err := manifest.Validate(); err == nil {
			t.Fatalf("capability %q was accepted", capability)
		}
	}
}

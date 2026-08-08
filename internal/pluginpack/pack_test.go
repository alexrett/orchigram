package pluginpack

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/pluginbundle"
)

func TestPackCalculatesDigestsAndIsRepeatable(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	binary := []byte("community echo binary")
	writeFixture(t, root, binary, "")
	firstPath := filepath.Join(root, "dist", "first.tar.gz")
	first, err := Pack(filepath.Join(root, "plugin.yaml"), firstPath, false)
	if err != nil {
		t.Fatal(err)
	}
	secondPath := filepath.Join(root, "dist", "second.tar.gz")
	second, err := Pack(filepath.Join(root, "plugin.yaml"), secondPath, false)
	if err != nil {
		t.Fatal(err)
	}
	firstBytes, _ := os.ReadFile(first.Path)
	secondBytes, _ := os.ReadFile(second.Path)
	if !bytes.Equal(firstBytes, secondBytes) || first.SHA256 != second.SHA256 {
		t.Fatal("packed bundles are not byte-for-byte repeatable")
	}
	manifest, payload, _, err := pluginbundle.ParseForPlatform(firstBytes, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	if manifest.Platforms[0].SHA256 != hex.EncodeToString(digest[:]) || !bytes.Equal(payload, binary) {
		t.Fatalf("manifest=%+v payload=%q", manifest, payload)
	}
}

func TestPackRefusesOverwriteUnlessForced(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	writeFixture(t, root, []byte("first"), "")
	output := filepath.Join(root, "bundle.tar.gz")
	if _, err := Pack(filepath.Join(root, "plugin.yaml"), output, false); err != nil {
		t.Fatal(err)
	}
	if _, err := Pack(filepath.Join(root, "plugin.yaml"), output, false); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("overwrite error=%v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "echo"), []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Pack(filepath.Join(root, "plugin.yaml"), output, true); err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(output) //nolint:gosec // Test reads its own output under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	_, payload, _, err := pluginbundle.ParseForPlatform(bundle, runtime.GOOS, runtime.GOARCH)
	if err != nil || string(payload) != "second" {
		t.Fatalf("forced payload=%q err=%v", payload, err)
	}
}

func TestPackParsesEveryDeclaredPlatform(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o750); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"first": "one", "second": "two"} {
		if err := os.WriteFile(filepath.Join(root, "bin", name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := "apiVersion: orchigram.dev/plugin/v1alpha1\nname: echo\nversion: 0.1.0\nprotocol: {minimum: 1, maximum: 1}\ncapabilities: [task.echo.echo]\nplatforms:\n  - {os: plan9, arch: amd64, path: bin/first}\n  - {os: linux, arch: arm64, path: bin/second}\n"
	manifestPath := filepath.Join(root, "plugin.yaml")
	if err := os.WriteFile(manifestPath, []byte(manifest), 0o600); err != nil {
		t.Fatal(err)
	}
	result, err := Pack(manifestPath, filepath.Join(root, "bundle.tar.gz"), false)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := os.ReadFile(result.Path)
	if err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ os, arch, payload string }{{"plan9", "amd64", "one"}, {"linux", "arm64", "two"}} {
		_, payload, _, err := pluginbundle.ParseForPlatform(bundle, target.os, target.arch)
		if err != nil || string(payload) != target.payload {
			t.Fatalf("%s/%s payload=%q err=%v", target.os, target.arch, payload, err)
		}
	}
}

func TestPackRejectsUnsafePathsAndDigestMismatch(t *testing.T) {
	t.Parallel()
	for name, platformPath := range map[string]string{"traversal": "../echo", "absolute": "/tmp/echo"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			manifest := manifestFixture(platformPath, "")
			if err := os.WriteFile(filepath.Join(root, "plugin.yaml"), []byte(manifest), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Pack(filepath.Join(root, "plugin.yaml"), filepath.Join(root, "out.tar.gz"), false); err == nil {
				t.Fatal("unsafe path was accepted")
			}
		})
	}
	root := t.TempDir()
	writeFixture(t, root, []byte("binary"), strings.Repeat("0", 64))
	if _, err := Pack(filepath.Join(root, "plugin.yaml"), filepath.Join(root, "out.tar.gz"), false); err == nil {
		t.Fatal("mismatched supplied digest was accepted")
	}
	unknown := t.TempDir()
	writeFixture(t, unknown, []byte("binary"), "")
	file := filepath.Join(unknown, "plugin.yaml")
	data, err := os.ReadFile(file) //nolint:gosec // Test reads its own manifest under t.TempDir.
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, append(data, []byte("unsupportedField: true\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Pack(file, filepath.Join(unknown, "out.tar.gz"), false); err == nil {
		t.Fatal("unsupported manifest field was accepted")
	}
}

func writeFixture(t *testing.T, root string, binary []byte, digest string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "echo"), binary, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "plugin.yaml"), []byte(manifestFixture("bin/echo", digest)), 0o600); err != nil {
		t.Fatal(err)
	}
}

func manifestFixture(path, digest string) string {
	shaLine := ""
	if digest != "" {
		shaLine = "\n    sha256: " + digest
	}
	return "apiVersion: orchigram.dev/plugin/v1alpha1\nname: echo\nversion: 0.1.0\nprotocol:\n  minimum: 1\n  maximum: 1\ncapabilities:\n  - task.echo.echo\nplatforms:\n  - os: " + runtime.GOOS + "\n    arch: " + runtime.GOARCH + "\n    path: " + path + shaLine + "\n"
}

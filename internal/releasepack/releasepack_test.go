package releasepack

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginbundle"
)

func TestBuildProducesReproducibleVerifiedBundle(t *testing.T) {
	plugin, ok := firstparty.Find("exec")
	if !ok {
		t.Fatal("exec plugin missing from catalog")
	}
	options := Options{
		OutputDir: filepath.Join(t.TempDir(), "first"), Version: "0.1.0-test.1", Commit: "0123456789abcdef", Date: "2026-08-08T00:00:00Z",
		Targets: []Target{{OS: runtime.GOOS, Arch: runtime.GOARCH}}, Plugins: []firstparty.Plugin{plugin},
	}
	first, err := Build(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	options.OutputDir = filepath.Join(t.TempDir(), "second")
	second, err := Build(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	firstData, err := os.ReadFile(first[0]) //nolint:gosec // Test reads its own output.
	if err != nil {
		t.Fatal(err)
	}
	secondData, err := os.ReadFile(second[0]) //nolint:gosec // Test reads its own output.
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstData, secondData) {
		t.Fatal("release bundle is not reproducible")
	}
	manifest, _, _, err := pluginbundle.Parse(firstData)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Version != options.Version || manifest.Name != plugin.Name {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
}

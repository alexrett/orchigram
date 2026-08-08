package install

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/firstparty"
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
	for _, directive := range []string{"User=orchigram", "Environment=HOME=/var/lib/orchigram", "NoNewPrivileges=yes", "ProtectSystem=strict", "ProtectProc=invisible", "SystemCallFilter=@system-service", "CapabilityBoundingSet=", "RuntimeDirectory=orchigram"} {
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

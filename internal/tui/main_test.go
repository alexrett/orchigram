package tui

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"gopkg.in/yaml.v3"
)

func TestMain(m *testing.M) {
	if os.Getenv(pluginsdk.Handshake.MagicCookieKey) == pluginsdk.Handshake.MagicCookieValue {
		executable, err := os.Executable()
		if err != nil {
			os.Exit(2)
		}
		manifestData, err := os.ReadFile(filepath.Join(filepath.Dir(executable), "plugin.yaml")) //nolint:gosec // The daemon's private staged manifest defines this test plugin process.
		if err != nil {
			os.Exit(2)
		}
		var manifest pluginbundle.Manifest
		if yaml.Unmarshal(manifestData, &manifest) != nil {
			os.Exit(2)
		}
		catalog, ok := firstparty.Find(manifest.Name)
		if !ok || manifest.Name != "exec" {
			os.Exit(2)
		}
		pluginsdk.Serve(pluginsdk.Config{
			Metadata: pluginsdk.Metadata{Name: manifest.Name, Version: manifest.Version, Capabilities: catalog.Capabilities, Actions: catalog.Actions},
			Task:     &pluginruntime.Exec{Runner: process.NewRunner()},
		})
		return
	}
	os.Exit(m.Run())
}

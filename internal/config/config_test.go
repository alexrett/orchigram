package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadRejectsUnknownFields(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("socketPath: /tmp/test.sock\nunknown: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "field unknown not found") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestDevelopmentConfigIsValid(t *testing.T) {
	t.Parallel()
	if err := Development(t.TempDir()).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestOperationsBoundsAreStrict(t *testing.T) {
	t.Parallel()
	for name, mutate := range map[string]func(*Config){
		"zero active Runs":         func(cfg *Config) { cfg.Operations.MaxActiveRuns = 0 },
		"zero activities":          func(cfg *Config) { cfg.Operations.MaxConcurrentActivities = 0 },
		"agents exceed activities": func(cfg *Config) { cfg.Operations.MaxAgentProcesses = cfg.Operations.MaxConcurrentActivities + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			cfg := Default()
			mutate(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("expected invalid operations bounds")
			}
		})
	}
}

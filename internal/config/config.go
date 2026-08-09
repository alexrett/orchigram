// Package config loads and validates the daemon's static system configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultStateDir is the production daemon state root.
	DefaultStateDir = "/var/lib/orchigram"
	// DefaultRuntimeDir is the systemd-managed runtime root.
	DefaultRuntimeDir = "/run/orchigram"
	// DefaultConfigDir is the system configuration root.
	DefaultConfigDir = "/etc/orchigram"
	// DefaultSocketPath is the default local control-plane endpoint.
	DefaultSocketPath = "/run/orchigram/orchigram.sock"
)

// Config is the strict daemon configuration loaded from /etc/orchigram.
type Config struct {
	StateDir   string           `yaml:"stateDir"`
	RuntimeDir string           `yaml:"runtimeDir"`
	SocketPath string           `yaml:"socketPath"`
	HTTP       HTTPConfig       `yaml:"http"`
	Logging    LogConfig        `yaml:"logging"`
	Operations OperationsConfig `yaml:"operations"`
}

// OperationsConfig bounds the single-node scheduler and external processes.
type OperationsConfig struct {
	MaxActiveRuns           int `yaml:"maxActiveRuns"`
	MaxConcurrentActivities int `yaml:"maxConcurrentActivities"`
	MaxAgentProcesses       int `yaml:"maxAgentProcesses"`
}

// HTTPConfig controls the optional webhook listener.
type HTTPConfig struct {
	Listen string `yaml:"listen,omitempty"`
}

// LogConfig selects the daemon slog level and formatter.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns the network-closed production defaults.
func Default() Config {
	return Config{
		StateDir:   DefaultStateDir,
		RuntimeDir: DefaultRuntimeDir,
		SocketPath: DefaultSocketPath,
		Logging:    LogConfig{Level: "info", Format: "json"},
		Operations: OperationsConfig{MaxActiveRuns: 8, MaxConcurrentActivities: 4, MaxAgentProcesses: 2},
	}
}

// Load reads a strict YAML configuration. A missing default file is valid.
func Load(path string) (Config, error) {
	cfg := Default()
	if path == "" {
		path = filepath.Join(DefaultConfigDir, "config.yaml")
	}
	b, err := os.ReadFile(path) //nolint:gosec // The operator explicitly selects the configuration file.
	if errors.Is(err, os.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate checks paths and enumerated values before any service starts.
func (c Config) Validate() error {
	if c.StateDir == "" || c.RuntimeDir == "" || c.SocketPath == "" {
		return errors.New("stateDir, runtimeDir, and socketPath are required")
	}
	if !filepath.IsAbs(c.StateDir) || !filepath.IsAbs(c.RuntimeDir) || !filepath.IsAbs(c.SocketPath) {
		return errors.New("stateDir, runtimeDir, and socketPath must be absolute")
	}
	if c.Logging.Level != "debug" && c.Logging.Level != "info" && c.Logging.Level != "warn" && c.Logging.Level != "error" {
		return fmt.Errorf("unsupported logging.level %q", c.Logging.Level)
	}
	if c.Logging.Format != "json" && c.Logging.Format != "text" {
		return fmt.Errorf("unsupported logging.format %q", c.Logging.Format)
	}
	if c.Operations.MaxActiveRuns < 1 || c.Operations.MaxActiveRuns > 1024 {
		return errors.New("operations.maxActiveRuns must be between 1 and 1024")
	}
	if c.Operations.MaxConcurrentActivities < 1 || c.Operations.MaxConcurrentActivities > 1024 {
		return errors.New("operations.maxConcurrentActivities must be between 1 and 1024")
	}
	if c.Operations.MaxAgentProcesses < 1 || c.Operations.MaxAgentProcesses > c.Operations.MaxConcurrentActivities {
		return errors.New("operations.maxAgentProcesses must be between 1 and operations.maxConcurrentActivities")
	}
	return nil
}

// Development returns an isolated local configuration rooted at root.
func Development(root string) Config {
	runtimeDir := filepath.Join(root, "run")
	return Config{
		StateDir:   root,
		RuntimeDir: runtimeDir,
		SocketPath: filepath.Join(runtimeDir, "orchigram.sock"),
		Logging:    LogConfig{Level: "debug", Format: "text"},
		Operations: OperationsConfig{MaxActiveRuns: 8, MaxConcurrentActivities: 4, MaxAgentProcesses: 2},
	}
}

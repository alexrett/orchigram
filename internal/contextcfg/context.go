// Package contextcfg loads local routes to Orchigram daemon contexts.
package contextcfg

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// File is the complete local context routing file.
type File struct {
	Current  string             `yaml:"current-context"`
	Contexts map[string]Context `yaml:"contexts"`
}

// Context selects exactly one local or SSH transport.
type Context struct {
	Socket string      `yaml:"socket,omitempty"`
	SSH    *SSHContext `yaml:"ssh,omitempty"`
}

// SSHContext describes an OpenSSH StreamLocal forwarding target.
type SSHContext struct {
	Destination string `yaml:"destination"`
	Socket      string `yaml:"socket"`
	Identity    string `yaml:"identity,omitempty"`
}

// Path resolves contexts.yaml using XDG conventions.
func Path() (string, error) {
	if root := os.Getenv("XDG_CONFIG_HOME"); root != "" {
		return filepath.Join(root, "orchigram", "contexts.yaml"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".config", "orchigram", "contexts.yaml"), nil
}

// Load strictly decodes a context file or returns the local default.
func Load(path string) (File, error) {
	if path == "" {
		var err error
		path, err = Path()
		if err != nil {
			return File{}, err
		}
	}
	b, err := os.ReadFile(path) //nolint:gosec // The operator explicitly selects the context file.
	if errors.Is(err, os.ErrNotExist) {
		return File{Current: "local", Contexts: map[string]Context{"local": {Socket: "/run/orchigram/orchigram.sock"}}}, nil
	}
	if err != nil {
		return File{}, fmt.Errorf("read contexts: %w", err)
	}
	var result File
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&result); err != nil {
		return File{}, fmt.Errorf("decode contexts: %w", err)
	}
	if err := result.Validate(); err != nil {
		return File{}, err
	}
	return result, nil
}

// Validate ensures the current context exists and transports are unambiguous.
func (f File) Validate() error {
	if f.Current == "" {
		return errors.New("current-context is required")
	}
	if _, ok := f.Contexts[f.Current]; !ok {
		return fmt.Errorf("current context %q is not defined", f.Current)
	}
	for name, context := range f.Contexts {
		if (context.Socket == "") == (context.SSH == nil) {
			return fmt.Errorf("context %q must configure exactly one of socket or ssh", name)
		}
		if context.Socket != "" && !filepath.IsAbs(context.Socket) {
			return fmt.Errorf("context %q socket must be absolute", name)
		}
		if context.SSH != nil && (context.SSH.Destination == "" || context.SSH.Socket == "") {
			return fmt.Errorf("context %q ssh.destination and ssh.socket are required", name)
		}
		if context.SSH != nil {
			if strings.HasPrefix(context.SSH.Destination, "-") || len(strings.Fields(context.SSH.Destination)) != 1 {
				return fmt.Errorf("context %q ssh.destination is invalid", name)
			}
			if !filepath.IsAbs(context.SSH.Socket) || strings.ContainsAny(context.SSH.Socket, ":\r\n") {
				return fmt.Errorf("context %q ssh.socket must be an absolute path without ':'", name)
			}
			if strings.ContainsAny(context.SSH.Identity, "\r\n") {
				return fmt.Errorf("context %q ssh.identity is invalid", name)
			}
		}
	}
	return nil
}

// Save atomically writes contexts with user-only permissions.
func Save(path string, file File) error {
	if err := file.Validate(); err != nil {
		return err
	}
	if path == "" {
		var err error
		path, err = Path()
		if err != nil {
			return err
		}
	}
	encoded, err := yaml.Marshal(file)
	if err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create context directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".contexts-*.yaml")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return os.Chmod(path, 0o600)
}

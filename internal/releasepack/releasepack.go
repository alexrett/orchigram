// Package releasepack creates reproducible first-party plugin release bundles.
package releasepack

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginbundle"
)

// Target identifies one supported release platform.
type Target struct {
	OS   string
	Arch string
}

// Options controls deterministic release packaging.
type Options struct {
	OutputDir string
	Version   string
	Commit    string
	Date      string
	Targets   []Target
	Plugins   []firstparty.Plugin
}

// DefaultTargets are the supported v0.1 client and server platforms.
var DefaultTargets = []Target{{OS: "darwin", Arch: "amd64"}, {OS: "darwin", Arch: "arm64"}, {OS: "linux", Arch: "amd64"}, {OS: "linux", Arch: "arm64"}}

// Build cross-compiles and verifies one immutable bundle per plugin and target.
func Build(ctx context.Context, options Options) ([]string, error) {
	if options.OutputDir == "" || options.Version == "" || options.Commit == "" || options.Date == "" {
		return nil, errors.New("output, version, commit and date are required")
	}
	if len(options.Targets) == 0 {
		options.Targets = append([]Target(nil), DefaultTargets...)
	}
	if len(options.Plugins) == 0 {
		options.Plugins = firstparty.All()
	}
	if err := os.MkdirAll(options.OutputDir, 0o750); err != nil {
		return nil, err
	}
	root, err := projectRoot()
	if err != nil {
		return nil, err
	}
	work, err := os.MkdirTemp("", "orchigram-release-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(work) }()

	buildEpoch := "0"
	if parsed, parseErr := time.Parse(time.RFC3339, options.Date); parseErr == nil {
		buildEpoch = strconv.FormatInt(parsed.Unix(), 10)
	}
	var paths []string
	for _, plugin := range options.Plugins {
		if plugin.Name == "" || plugin.Command == "" || len(plugin.Capabilities) == 0 {
			return nil, errors.New("release catalog contains an incomplete plugin")
		}
		for _, target := range options.Targets {
			if target.OS == "" || target.Arch == "" {
				return nil, errors.New("release target must include os and arch")
			}
			binaryPath := filepath.Join(work, plugin.Command+"_"+target.OS+"_"+target.Arch)
			if err := buildPlugin(ctx, root, plugin, target, binaryPath, options, buildEpoch); err != nil {
				return nil, err
			}
			binary, err := os.ReadFile(binaryPath) //nolint:gosec // Path is created in a private release temporary directory.
			if err != nil {
				return nil, err
			}
			digest := sha256.Sum256(binary)
			payloadPath := "bin/" + plugin.Command
			manifest := pluginbundle.Manifest{
				APIVersion:   pluginbundle.APIVersion,
				Name:         plugin.Name,
				Version:      options.Version,
				Protocol:     pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1},
				Capabilities: append([]string(nil), plugin.Capabilities...),
				Platforms:    []pluginbundle.Platform{{OS: target.OS, Arch: target.Arch, Path: payloadPath, SHA256: hex.EncodeToString(digest[:])}},
			}
			bundle, err := pluginbundle.Build(manifest, map[string][]byte{payloadPath: binary})
			if err != nil {
				return nil, fmt.Errorf("bundle %s for %s/%s: %w", plugin.Name, target.OS, target.Arch, err)
			}
			if _, verifiedBinary, _, err := pluginbundle.ParseForPlatform(bundle, target.OS, target.Arch); err != nil || len(verifiedBinary) != len(binary) {
				return nil, fmt.Errorf("verify bundle %s for %s/%s: %w", plugin.Name, target.OS, target.Arch, err)
			}
			name := fmt.Sprintf("%s_%s_%s_%s.tar.gz", plugin.Command, options.Version, target.OS, target.Arch)
			outputPath := filepath.Join(options.OutputDir, name)
			if err := writeAtomic(outputPath, bundle); err != nil {
				return nil, err
			}
			paths = append(paths, outputPath)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

func buildPlugin(ctx context.Context, root string, plugin firstparty.Plugin, target Target, outputPath string, options Options, buildEpoch string) error {
	ldflags := strings.Join([]string{
		"-s", "-w", "-buildid=",
		"-X", "github.com/alexrett/orchigram/internal/version.Version=" + options.Version,
		"-X", "github.com/alexrett/orchigram/internal/version.Commit=" + options.Commit,
		"-X", "github.com/alexrett/orchigram/internal/version.Date=" + options.Date,
	}, " ")
	command := exec.CommandContext(ctx, "go", "build", "-trimpath", "-buildvcs=false", "-ldflags", ldflags, "-o", outputPath, "./cmd/"+plugin.Command) //nolint:gosec // Command and target come from the compiled first-party catalog.
	command.Dir = root
	command.Env = append(os.Environ(), "CGO_ENABLED=0", "GOOS="+target.OS, "GOARCH="+target.Arch, "SOURCE_DATE_EPOCH="+buildEpoch)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build %s for %s/%s: %w: %s", plugin.Name, target.OS, target.Arch, err, strings.TrimSpace(string(output)))
	}
	return nil
}

func projectRoot() (string, error) {
	directory, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, statErr := os.Stat(filepath.Join(directory, "go.mod")); statErr == nil {
			return directory, nil
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			return "", errors.New("cannot locate module root")
		}
		directory = parent
	}
}

func writeAtomic(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".bundle-")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o644); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
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
	return os.Rename(temporaryPath, path)
}

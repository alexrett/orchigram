// Package install provisions the single-node Linux service and its bundled plugins.
package install

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/version"
	"google.golang.org/protobuf/types/known/emptypb"
)

const unit = `[Unit]
Description=Orchigram declarative agent workflow daemon
Documentation=https://github.com/alexrett/orchigram
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=orchigram
Group=orchigram
Environment=HOME=/var/lib/orchigram
ExecStart=/usr/local/bin/orchigram server --config /etc/orchigram/config.yaml
Restart=on-failure
RestartSec=2s
MemoryHigh=512M
MemoryMax=768M
CPUQuota=200%
TasksMax=256
OOMPolicy=stop
RuntimeDirectory=orchigram
RuntimeDirectoryMode=0750
StateDirectory=orchigram
StateDirectoryMode=0750
LogsDirectory=orchigram
LogsDirectoryMode=0750
UMask=0077
NoNewPrivileges=yes
PrivateTmp=yes
PrivateDevices=yes
PrivateUsers=yes
ProtectSystem=strict
ProtectHome=yes
ProtectClock=yes
ProtectHostname=yes
ProtectProc=invisible
ProcSubset=pid
ProtectKernelTunables=yes
ProtectKernelModules=yes
ProtectKernelLogs=yes
ProtectControlGroups=yes
RestrictNamespaces=yes
RestrictRealtime=yes
RestrictSUIDSGID=yes
LockPersonality=yes
MemoryDenyWriteExecute=yes
RemoveIPC=yes
KeyringMode=private
SystemCallArchitectures=native
SystemCallFilter=@system-service
SystemCallErrorNumber=EPERM
CapabilityBoundingSet=
AmbientCapabilities=
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
ReadWritePaths=/var/lib/orchigram /run/orchigram /var/log/orchigram

[Install]
WantedBy=multi-user.target
`

const defaultConfig = `stateDir: /var/lib/orchigram
runtimeDir: /run/orchigram
socketPath: /run/orchigram/orchigram.sock
http: {}
logging:
  level: info
  format: json
operations:
  maxActiveRuns: 8
  maxConcurrentActivities: 4
  maxAgentProcesses: 2
`

// Options select sources and make filesystem-only tests possible under Root.
type Options struct {
	Root      string
	PluginDir string
	Start     bool
}

// Result reports installed paths and non-fatal executable discovery.
type Result struct {
	Binary   string
	Unit     string
	Socket   string
	Warnings []string
}

type installFile struct {
	target  string
	backup  string
	existed bool
	mode    os.FileMode
}

type installSnapshot struct {
	directory string
	files     []installFile
}

// Run installs files, creates the service identity, starts systemd, and uploads bundled plugins.
func Run(ctx context.Context, options Options) (Result, error) {
	root := options.Root
	if root == "" {
		root = string(filepath.Separator)
	}
	if !filepath.IsAbs(root) {
		return Result{}, errors.New("install root must be absolute")
	}
	realSystem := filepath.Clean(root) == string(filepath.Separator)
	if realSystem && runtime.GOOS != "linux" {
		return Result{}, errors.New("system installation is supported on Linux only")
	}
	if realSystem && os.Geteuid() != 0 {
		return Result{}, errors.New("orchigram install must run as root")
	}
	source, err := os.Executable()
	if err != nil {
		return Result{}, err
	}
	if options.PluginDir == "" {
		options.PluginDir = filepath.Dir(source)
	}
	paths := struct {
		binary, pluginDir, configDir, config, state, runtime, logs, unit string
	}{
		binary: rootPath(root, "/usr/local/bin/orchigram"), pluginDir: rootPath(root, "/usr/local/lib/orchigram/plugins"),
		configDir: rootPath(root, config.DefaultConfigDir), config: rootPath(root, "/etc/orchigram/config.yaml"),
		state: rootPath(root, config.DefaultStateDir), runtime: rootPath(root, config.DefaultRuntimeDir), logs: rootPath(root, "/var/log/orchigram"),
		unit: rootPath(root, "/etc/systemd/system/orchigram.service"),
	}
	if realSystem {
		if err := ensureServiceIdentity(ctx); err != nil {
			return Result{}, err
		}
	}
	sources := []string{source}
	for _, plugin := range firstparty.All() {
		sources = append(sources, filepath.Join(options.PluginDir, plugin.Command))
	}
	for _, sourcePath := range sources {
		if err := validateInstallSource(sourcePath, 256<<20); err != nil {
			return Result{}, err
		}
	}
	for _, directory := range []string{filepath.Dir(paths.binary), paths.pluginDir, paths.configDir, paths.state, filepath.Join(paths.state, "workspaces"), filepath.Join(paths.state, "artifacts"), paths.runtime, paths.logs, filepath.Dir(paths.unit)} {
		if err := os.MkdirAll(directory, 0o750); err != nil {
			return Result{}, fmt.Errorf("create %s: %w", directory, err)
		}
	}
	targets := []string{paths.binary, paths.unit, paths.config}
	for _, plugin := range firstparty.All() {
		targets = append(targets, filepath.Join(paths.pluginDir, plugin.Command))
	}
	snapshot, err := captureInstallSnapshot(paths.state, targets)
	if err != nil {
		return Result{}, err
	}
	committed := false
	defer func() {
		if committed {
			snapshot.discard()
		}
	}()
	rollbackFiles := func(cause error) error {
		return errors.Join(cause, snapshot.restore())
	}
	if err := copyFile(source, paths.binary, 0o755); err != nil {
		return Result{}, rollbackFiles(err)
	}
	pluginTargets := map[string]string{}
	for _, plugin := range firstparty.All() {
		filename := plugin.Command
		sourcePath := filepath.Join(options.PluginDir, filename)
		targetPath := filepath.Join(paths.pluginDir, filename)
		if err := copyFile(sourcePath, targetPath, 0o755); err != nil {
			return Result{}, rollbackFiles(fmt.Errorf("install bundled plugin %s: %w", plugin.Name, err))
		}
		pluginTargets[plugin.Name] = targetPath
	}
	if _, err := os.Stat(paths.config); errors.Is(err, os.ErrNotExist) {
		if err := writeAtomic(paths.config, []byte(defaultConfig), 0o640); err != nil {
			return Result{}, rollbackFiles(err)
		}
	} else if err != nil {
		return Result{}, rollbackFiles(err)
	}
	if err := writeAtomic(paths.unit, []byte(unit), 0o644); err != nil {
		return Result{}, rollbackFiles(err)
	}
	if realSystem {
		if err := chownServicePaths(
			paths.state,
			filepath.Join(paths.state, "workspaces"),
			filepath.Join(paths.state, "artifacts"),
			paths.runtime,
			paths.logs,
		); err != nil {
			return Result{}, rollbackFiles(err)
		}
		if err := chownServiceConfig(paths.configDir, paths.config); err != nil {
			return Result{}, rollbackFiles(err)
		}
	}
	result := Result{Binary: paths.binary, Unit: paths.unit, Socket: paths.runtime + "/orchigram.sock"}
	for _, executable := range []string{"git", "codex", "claude"} {
		if _, err := exec.LookPath(executable); err != nil {
			result.Warnings = append(result.Warnings, executable+" is not installed or not on PATH")
		}
	}
	if realSystem && options.Start {
		previousActivations, activationErr := activePluginVersions(ctx, config.DefaultSocketPath)
		if activationErr != nil && snapshot.existed(paths.binary) {
			return Result{}, rollbackFiles(fmt.Errorf("inspect existing plugin activations before upgrade: %w", activationErr))
		}
		rollbackRunningService := func(cause error) error {
			fileErr := snapshot.restore()
			reloadErr := runCommand(ctx, "systemctl", "daemon-reload")
			restartErr := runCommand(ctx, "systemctl", "restart", "orchigram.service")
			activationRestoreErr := restorePluginVersions(ctx, config.DefaultSocketPath, previousActivations)
			return errors.Join(cause, fileErr, reloadErr, restartErr, activationRestoreErr)
		}
		if err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
			return Result{}, rollbackRunningService(err)
		}
		if err := runCommand(ctx, "systemctl", "enable", "orchigram.service"); err != nil {
			return Result{}, rollbackRunningService(err)
		}
		// An install is also the supported single-node upgrade path. Restarting
		// ensures the just-copied daemon and unit are actually running; durable
		// approvals, timers, and retries reconcile from SQLite after startup.
		if err := runCommand(ctx, "systemctl", "restart", "orchigram.service"); err != nil {
			return Result{}, rollbackRunningService(err)
		}
		if err := bootstrapPlugins(ctx, config.DefaultSocketPath, pluginTargets); err != nil {
			return Result{}, rollbackRunningService(err)
		}
	}
	committed = true
	return result, nil
}

func validateInstallSource(path string, maximumSize int64) error {
	info, err := os.Stat(filepath.Clean(path))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() < 1 || info.Size() > maximumSize {
		return fmt.Errorf("install source %s is not a bounded regular file", filepath.Base(path))
	}
	return nil
}

func captureInstallSnapshot(stateDir string, targets []string) (*installSnapshot, error) {
	directory, err := os.MkdirTemp(stateDir, ".install-rollback-*")
	if err != nil {
		return nil, err
	}
	result := &installSnapshot{directory: directory, files: make([]installFile, 0, len(targets))}
	for index, target := range targets {
		entry := installFile{target: target, backup: filepath.Join(directory, strconv.Itoa(index))}
		info, statErr := os.Stat(target)
		if errors.Is(statErr, os.ErrNotExist) {
			result.files = append(result.files, entry)
			continue
		}
		if statErr != nil {
			result.discard()
			return nil, statErr
		}
		if !info.Mode().IsRegular() {
			result.discard()
			return nil, fmt.Errorf("install target %s is not a regular file", target)
		}
		entry.existed, entry.mode = true, info.Mode().Perm()
		if err := copyFile(target, entry.backup, entry.mode); err != nil {
			result.discard()
			return nil, err
		}
		result.files = append(result.files, entry)
	}
	return result, nil
}

func (s *installSnapshot) existed(target string) bool {
	for _, file := range s.files {
		if file.target == target {
			return file.existed
		}
	}
	return false
}

func (s *installSnapshot) restore() error {
	if s == nil || s.directory == "" {
		return nil
	}
	var result error
	for _, file := range s.files {
		if file.existed {
			result = errors.Join(result, copyFile(file.backup, file.target, file.mode))
			continue
		}
		if err := os.Remove(file.target); err != nil && !errors.Is(err, os.ErrNotExist) {
			result = errors.Join(result, err)
		}
	}
	return errors.Join(result, os.RemoveAll(s.directory))
}

func (s *installSnapshot) discard() {
	if s != nil && s.directory != "" {
		_ = os.RemoveAll(s.directory)
	}
}

func bootstrapPlugins(ctx context.Context, socket string, binaries map[string]string) error {
	connection, err := dialHealthy(ctx, socket, 30*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	type installedPlugin struct{ name, version string }
	installed := make([]installedPlugin, 0, len(binaries))
	for _, catalogPlugin := range firstparty.All() {
		name, binaryPath := catalogPlugin.Name, binaries[catalogPlugin.Name]
		binary, err := os.ReadFile(binaryPath) //nolint:gosec // Installer reads its selected first-party plugin directory.
		if err != nil {
			return err
		}
		digest := sha256.Sum256(binary)
		plugin, exists := firstparty.Find(name)
		if !exists {
			return fmt.Errorf("unknown bundled plugin %q", name)
		}
		manifest := pluginbundle.Manifest{
			APIVersion: pluginbundle.APIVersion, Name: name, Version: version.Semver(),
			Protocol: pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: plugin.Capabilities,
			Platforms: []pluginbundle.Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
		}
		bundle, err := pluginbundle.Build(manifest, map[string][]byte{"bin/plugin": binary})
		if err != nil {
			return err
		}
		stream, err := connection.Plugins.Install(ctx)
		if err != nil {
			return err
		}
		const chunkSize = 1 << 20
		for offset := 0; offset < len(bundle); offset += chunkSize {
			end := min(offset+chunkSize, len(bundle))
			if err := stream.Send(&controlv1alpha1.PluginUploadRequest{BundleChunk: bundle[offset:end], Final: end == len(bundle)}); err != nil {
				return err
			}
		}
		if _, err := stream.CloseAndRecv(); err != nil {
			return err
		}
		installed = append(installed, installedPlugin{name: name, version: version.Semver()})
	}
	for _, plugin := range installed {
		if _, err := connection.Plugins.Enable(ctx, &controlv1alpha1.PluginRequest{Name: plugin.name, Version: plugin.version}); err != nil {
			return err
		}
	}
	return nil
}

func dialHealthy(ctx context.Context, socket string, maximumWait time.Duration) (*clientpkg.Client, error) {
	deadline := time.Now().Add(maximumWait)
	for {
		client, err := clientpkg.DialUnix(ctx, socket)
		if err == nil {
			healthContext, cancel := context.WithTimeout(ctx, 5*time.Second)
			health, healthErr := client.System.Health(healthContext, &emptypb.Empty{})
			cancel()
			if healthErr == nil && health.GetReady() {
				return client, nil
			}
			_ = client.Close()
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if time.Now().After(deadline) {
			return nil, errors.New("installed Orchigram service did not become healthy")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func activePluginVersions(ctx context.Context, socket string) (map[string]string, error) {
	connection, err := dialHealthy(ctx, socket, 5*time.Second)
	if err != nil {
		return nil, err
	}
	defer func() { _ = connection.Close() }()
	response, err := connection.Plugins.List(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	result := map[string]string{}
	for _, plugin := range response.GetPlugins() {
		if plugin.GetState() == "active" {
			result[plugin.GetName()] = plugin.GetVersion()
		}
	}
	return result, nil
}

func restorePluginVersions(ctx context.Context, socket string, expected map[string]string) error {
	if expected == nil {
		return nil
	}
	connection, err := dialHealthy(ctx, socket, 30*time.Second)
	if err != nil {
		return err
	}
	defer func() { _ = connection.Close() }()
	response, err := connection.Plugins.List(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	for _, plugin := range response.GetPlugins() {
		if plugin.GetState() != "active" {
			continue
		}
		if version, exists := expected[plugin.GetName()]; exists && version == plugin.GetVersion() {
			continue
		}
		if _, err := connection.Plugins.Disable(ctx, &controlv1alpha1.PluginRequest{Name: plugin.GetName(), Version: plugin.GetVersion()}); err != nil {
			return err
		}
	}
	names := make([]string, 0, len(expected))
	for name := range expected {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if _, err := connection.Plugins.Enable(ctx, &controlv1alpha1.PluginRequest{Name: name, Version: expected[name]}); err != nil {
			return err
		}
	}
	return nil
}

func ensureServiceIdentity(ctx context.Context) error {
	if err := runCommand(ctx, "getent", "group", "orchigram"); err != nil {
		if err := runCommand(ctx, "groupadd", "--system", "orchigram"); err != nil {
			return err
		}
	}
	if err := runCommand(ctx, "id", "-u", "orchigram"); err != nil {
		return runCommand(ctx, "useradd", "--system", "--gid", "orchigram", "--home-dir", config.DefaultStateDir, "--shell", "/usr/sbin/nologin", "orchigram")
	}
	return nil
}

func chownServicePaths(paths ...string) error {
	account, err := user.Lookup("orchigram")
	if err != nil {
		return err
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := os.Chown(path, uid, gid); err != nil {
			return err
		}
	}
	return nil
}

func chownServiceConfig(paths ...string) error {
	account, err := user.Lookup("orchigram")
	if err != nil {
		return err
	}
	gid, err := strconv.Atoi(account.Gid)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := os.Chown(path, 0, gid); err != nil {
			return err
		}
	}
	return nil
}

func runCommand(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...) //nolint:gosec // Installer invokes fixed administrative argv without a shell.
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func rootPath(root, absolute string) string {
	return filepath.Join(root, strings.TrimPrefix(filepath.Clean(absolute), string(filepath.Separator)))
}

func copyFile(source, target string, mode os.FileMode) error {
	return copyFileWithLimit(source, target, mode, 256<<20)
}

func copyFileWithLimit(source, target string, mode os.FileMode, maximumSize int64) error {
	input, err := os.Open(filepath.Clean(source)) //nolint:gosec // Sources are the running binary or an operator-selected plugin directory.
	if err != nil {
		return err
	}
	defer func() { _ = input.Close() }()
	temporary, err := os.CreateTemp(filepath.Dir(target), ".install-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	written, err := io.Copy(temporary, io.LimitReader(input, maximumSize+1))
	if err != nil {
		_ = temporary.Close()
		return err
	}
	if written > maximumSize {
		_ = temporary.Close()
		return fmt.Errorf("install source %s exceeds %d bytes", filepath.Base(source), maximumSize)
	}
	if err := temporary.Chmod(mode); err != nil {
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
	return os.Rename(temporaryPath, target)
}

func writeAtomic(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".write-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(mode); err != nil {
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

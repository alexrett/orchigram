package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMain(m *testing.M) {
	if os.Getenv(pluginprotocol.Handshake.MagicCookieKey) == pluginprotocol.Handshake.MagicCookieValue {
		pluginprotocol.Serve(pluginprotocol.Servers{
			Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: "exec", Version: "0.1.0", Capabilities: []string{"task.exec.run"}}},
			Task:    &pluginruntime.Exec{Runner: process.NewRunner()},
		})
		return
	}
	os.Exit(m.Run())
}

func TestInstalledPluginExecutesThroughDurableDaemon(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-plugin-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	stop := serveTestDaemon(t, cfg)
	defer stop()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()

	bundle := daemonConformanceBundle(t)
	stream, err := client.Plugins.Install(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const chunkSize = 1 << 20
	for offset := 0; offset < len(bundle); offset += chunkSize {
		end := min(offset+chunkSize, len(bundle))
		if err := stream.Send(&controlv1alpha1.PluginUploadRequest{BundleChunk: bundle[offset:end], Final: end == len(bundle)}); err != nil {
			t.Fatal(err)
		}
	}
	installed, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if installed.GetName() != "exec" || installed.GetVersion() != "0.1.0" {
		t.Fatalf("installed plugin: %+v", installed)
	}
	if _, err := client.Plugins.Enable(context.Background(), &controlv1alpha1.PluginRequest{Name: "exec", Version: "0.1.0"}); err != nil {
		t.Fatal(err)
	}
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: plugin-execution}
spec:
  nodes:
    - id: execute
      uses: exec.run
      timeout: 10s
      with:
        argv: [/bin/echo, durable-plugin-flow]
`)
	applied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.GetDiagnostics()) != 0 {
		t.Fatalf("apply diagnostics: %+v", applied.GetDiagnostics())
	}
	run, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "plugin-execution", InputJson: []byte(`{}`), IdempotencyKey: "plugin-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, run.GetUid(), 0, "run.succeeded")
	artifact, err := os.ReadFile(filepath.Join(cfg.StateDir, "artifacts", run.GetUid(), "execute", "attempt-1", "raw.log")) //nolint:gosec // Test-owned daemon state path.
	if err != nil || string(artifact) != "durable-plugin-flow\n" {
		t.Fatalf("artifact=%q err=%v", artifact, err)
	}
}

func TestRunApprovalSurvivesDaemonRestart(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))

	stopFirst := serveTestDaemon(t, cfg)
	first := dialReadyClient(t, cfg.SocketPath)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata:
  name: restart-approval
spec:
  policies:
    timeout: 1m
    maxParallel: 1
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 30s}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	apply, err := first.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil {
		t.Fatal(err)
	}
	if len(apply.GetDiagnostics()) != 0 {
		t.Fatalf("apply diagnostics: %+v", apply.GetDiagnostics())
	}
	started, err := first.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{
		Flow:           "restart-approval",
		InputJson:      []byte(`{"scope":"restart-gate"}`),
		IdempotencyKey: "restart-gate-occurrence",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, first, started.GetUid(), 0, "approval.waiting")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	stopFirst()

	stopSecond := serveTestDaemon(t, cfg)
	defer stopSecond()
	second := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = second.Close() }()
	duplicate, err := second.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{
		Flow:           "restart-approval",
		InputJson:      []byte(`{"scope":"different-payload"}`),
		IdempotencyKey: "restart-gate-occurrence",
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.GetUid() != started.GetUid() {
		t.Fatalf("duplicate occurrence created run %s instead of %s", duplicate.GetUid(), started.GetUid())
	}
	if _, err := second.Runs.Approve(context.Background(), &controlv1alpha1.ApprovalRequest{
		RunUid: started.GetUid(), NodeId: "approval", Reason: "restart acceptance gate",
	}); err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, second, started.GetUid(), 0, "run.succeeded")
}

func TestShutdownIsBoundedWithActiveWatch(t *testing.T) {
	t.Parallel()
	root, err := os.MkdirTemp("/tmp", "orchigram-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	ctx, cancel := context.WithCancel(context.Background())
	d, err := Open(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx) }()
	client, err := clientpkg.DialUnix(ctx, cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	stream, err := client.Resources.Watch(context.Background(), &controlv1alpha1.WatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	receiving := make(chan error, 1)
	go func() { _, receiveErr := stream.Recv(); receiving <- receiveErr }()
	time.Sleep(100 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon shutdown exceeded bounded grace period")
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("shutdown took %s", elapsed)
	}
	select {
	case <-receiving:
	case <-time.After(time.Second):
		t.Fatal("watch was not interrupted")
	}
}

func TestPrepareSocketRefusesRegularFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchigram.sock")
	if err := os.WriteFile(path, []byte("do not replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareSocket(path); err == nil {
		t.Fatal("expected refusal")
	}
	data, err := os.ReadFile(path) //nolint:gosec // This test owns the temporary path.
	if err != nil || string(data) != "do not replace" {
		t.Fatalf("file changed: %q err=%v", data, err)
	}
}

func serveTestDaemon(t *testing.T, cfg config.Config) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	instance, err := Open(ctx, cfg, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(ctx) }()
	return func() {
		cancel()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}
}

func dialReadyClient(t *testing.T, socketPath string) *clientpkg.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := clientpkg.DialUnix(ctx, socketPath)
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, healthErr := client.System.Health(ctx, &emptypb.Empty{})
		if healthErr == nil {
			return client
		}
		if ctx.Err() != nil {
			_ = client.Close()
			t.Fatalf("daemon was not ready: %v", healthErr)
		}
		if status.Code(healthErr) != codes.Unavailable {
			_ = client.Close()
			t.Fatal(healthErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForRunEvent(t *testing.T, client *clientpkg.Client, runUID string, after uint64, eventType string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: runUID, AfterSequence: after})
	if err != nil {
		t.Fatal(err)
	}
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			t.Fatalf("wait for %s: %v", eventType, receiveErr)
		}
		if event.GetType() == eventType {
			return
		}
	}
}

func daemonConformanceBundle(t *testing.T) []byte {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable) //nolint:gosec // The test intentionally bundles its own executable.
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	manifest := pluginbundle.Manifest{
		APIVersion: pluginbundle.APIVersion, Name: "exec", Version: "0.1.0",
		Protocol: pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task.exec.run"},
		Platforms: []pluginbundle.Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	bundle, err := pluginbundle.Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

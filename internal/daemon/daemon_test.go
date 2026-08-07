package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

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

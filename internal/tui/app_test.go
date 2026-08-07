package tui

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/daemon"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestOperatorApprovesWaitingRunThroughTUI(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()

	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: tui-approval}
spec:
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 30s}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	response, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetDiagnostics()) != 0 {
		t.Fatalf("apply diagnostics: %+v", response.GetDiagnostics())
	}
	run, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "tui-approval", InputJson: []byte(`{}`), IdempotencyKey: "tui-approval-gate"})
	if err != nil {
		t.Fatal(err)
	}
	waitForTUIRunEvent(t, client, run.GetUid(), "approval.waiting")

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(120, 40)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	time.Sleep(100 * time.Millisecond)
	for range 4 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'a', tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForTUIRunEvent(t, client, run.GetUid(), "run.succeeded")
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	stopTUI()
	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not stop")
	}
}

func postTUIEvent(t *testing.T, screen tcell.SimulationScreen, event tcell.Event) {
	t.Helper()
	if err := screen.PostEvent(event); err != nil {
		t.Fatal(err)
	}
}

func waitForTUIHealth(t *testing.T, client *clientpkg.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if _, err := client.System.Health(ctx, &emptypb.Empty{}); err == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("daemon was not ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForTUIRunEvent(t *testing.T, client *clientpkg.Client, runUID, eventType string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: runUID})
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

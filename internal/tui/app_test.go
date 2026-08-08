package tui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestTUILiveNavigationReflectsResourceAddAndDelete(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-live-")
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

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(80, 24)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	waitForScreenText(t, application, screen, "Contexts", true)

	flow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: live-visible}
spec: {nodes: [{id: done, uses: core.noop}]}
`)
	applied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flow})
	if err != nil {
		t.Fatal(err)
	}
	waitForScreenText(t, application, screen, "live-visible", true)
	for range 3 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "done", true)
	updatedFlow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: live-visible}
spec: {nodes: [{id: changed-live, uses: core.noop}]}
`)
	updated, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: updatedFlow, ExpectedResourceVersion: applied.GetResource().GetResourceVersion()})
	if err != nil {
		t.Fatal(err)
	}
	waitForScreenText(t, application, screen, "changed-live", true)
	if _, err := client.Resources.Delete(context.Background(), &controlv1alpha1.DeleteRequest{Key: updated.GetResource().GetKey(), ExpectedResourceVersion: updated.GetResource().GetResourceVersion()}); err != nil {
		t.Fatal(err)
	}
	waitForScreenText(t, application, screen, "live-visible", false)

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
	// Contexts/current, Flows/flow, Triggers, Repositories, AgentProfiles,
	// PluginInstallations, and the Runs heading precede the run item.
	for range 9 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	for _, view := range []struct {
		key   rune
		title string
	}{{key: 'e', title: "Events "}, {key: 't', title: "Attempts "}, {key: 'f', title: "Artifacts "}, {key: 'l', title: "Logs "}} {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, view.key, tcell.ModNone))
		waitForScreenText(t, application, screen, view.title, true)
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone))
	}
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
	deadline := time.Now().Add(time.Second)
	for {
		err := screen.PostEvent(event)
		if err == nil {
			return
		}
		if !errors.Is(err, tcell.ErrEventQFull) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForScreenText(t *testing.T, application *tview.Application, screen tcell.SimulationScreen, expected string, present bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		content := snapshotScreenText(application, screen)
		if strings.Contains(content, expected) == present {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("screen presence of %q did not become %t:\n%s", expected, present, content)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func snapshotScreenText(application *tview.Application, screen tcell.SimulationScreen) string {
	var content strings.Builder
	application.QueueUpdate(func() {
		cells, width, _ := screen.GetContents()
		for index, cell := range cells {
			content.Write(cell.Bytes)
			if width > 0 && (index+1)%width == 0 {
				content.WriteByte('\n')
			}
		}
	})
	return content.String()
}

func waitForTUIHealth(t *testing.T, client *clientpkg.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		health, err := client.System.Health(ctx, &emptypb.Empty{})
		if err == nil && health.GetReady() {
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

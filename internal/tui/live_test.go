package tui

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/daemon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLiveModelDeduplicatesResourceAndRunReplay(t *testing.T) {
	t.Parallel()
	model := newLiveModel()
	document := liveResource("Flow", "default", "alpha", 7)
	model.replaceResources([]*controlv1alpha1.ResourceDocument{document}, 7)
	if model.applyResourceEvent(&controlv1alpha1.ResourceEvent{Revision: 7, Type: "MODIFIED", Resource: liveResource("Flow", "default", "alpha", 8)}) {
		t.Fatal("duplicate resource revision changed the model")
	}
	if !model.applyResourceEvent(&controlv1alpha1.ResourceEvent{Revision: 8, Type: "MODIFIED", Resource: liveResource("Flow", "default", "alpha", 8)}) {
		t.Fatal("new resource revision was ignored")
	}
	model.replaceRuns([]*controlv1alpha1.RunSummary{{Uid: "run-1", Phase: "pending", UpdatedAt: timestamppb.Now()}})
	first := &controlv1alpha1.RunEvent{RunUid: "run-1", Sequence: 1, Type: "approval.waiting", OccurredAt: timestamppb.Now()}
	if !model.applyRunEvent(first) || model.applyRunEvent(first) {
		t.Fatal("run sequence was not applied exactly once")
	}
	model.applyRunEvent(&controlv1alpha1.RunEvent{RunUid: "run-1", Sequence: 2, Type: "run.succeeded", OccurredAt: timestamppb.Now()})
	snapshot := model.snapshot()
	if snapshot.Revision != 8 || snapshot.Resources[resourceLiveKey(document.GetKey())].GetResourceVersion() != 8 {
		t.Fatalf("resource projection is stale: %+v", snapshot)
	}
	if snapshot.Runs["run-1"].GetPhase() != "succeeded" || len(snapshot.RunEvents["run-1"]) != 2 {
		t.Fatalf("run projection is stale: %+v", snapshot.Runs["run-1"])
	}
	if terminalRunEvent("run.progress") {
		t.Fatal("an unknown run event must not terminate a watch")
	}
}

func TestResourceSnapshotRetriesRevisionConflictWithBoundedBackoff(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	resource := liveResource("Flow", "default", "stable", 12)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resources, revision, err := retryResourceSnapshot(ctx, func(context.Context) ([]*controlv1alpha1.ResourceDocument, uint64, error) {
		if calls.Add(1) < 3 {
			return nil, 0, status.Error(codes.Aborted, "snapshot changed")
		}
		return []*controlv1alpha1.ResourceDocument{resource}, 12, nil
	})
	if err != nil || revision != 12 || len(resources) != 1 || calls.Load() != 3 {
		t.Fatalf("calls=%d revision=%d resources=%d err=%v", calls.Load(), revision, len(resources), err)
	}
}

func TestLiveControllerProjectsUDSChangesWithoutRestart(t *testing.T) {
	testRoot, err := os.MkdirTemp("/tmp", "orchigram-live-tui-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	cfg := config.Development(filepath.Join(testRoot, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	t.Cleanup(func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	})

	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)
	controller := newLiveController(client)
	controller.runPollInterval = 20 * time.Millisecond
	controller.statusInterval = 50 * time.Millisecond
	if _, err := controller.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	liveContext, stopLive := context.WithCancel(context.Background())
	defer stopLive()
	updates := newSnapshotObserver()
	controller.run(liveContext, updates.observe)

	flow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: live-approval}
spec:
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 30s}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	applied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flow})
	if err != nil {
		t.Fatal(err)
	}
	resourceKey := resourceLiveKey(applied.GetResource().GetKey())
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool { return snapshot.Resources[resourceKey] != nil })
	installation, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: []byte(`apiVersion: orchigram.dev/v1alpha1
kind: PluginInstallation
metadata: {name: missing-live-plugin}
spec:
  plugin: missing-live
  version: 1.0.0
  digest: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  enabled: false
`)})
	if err != nil {
		t.Fatal(err)
	}
	installationKey := resourceLiveKey(installation.GetResource().GetKey())
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool {
		document := snapshot.Resources[installationKey]
		return document != nil && document.GetResourceVersion() > installation.GetResource().GetResourceVersion() && resourceStatusPhase(document.GetJson()) == "Error"
	})

	run, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "live-approval", InputJson: []byte(`{}`), IdempotencyKey: "live-controller-test"})
	if err != nil {
		t.Fatal(err)
	}
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool {
		projected := snapshot.Runs[run.GetUid()]
		return projected != nil && projected.GetPhase() == "waiting" && hasRunEvent(snapshot.RunEvents[run.GetUid()], "approval.waiting")
	})
	if _, err := client.Runs.Approve(context.Background(), &controlv1alpha1.ApprovalRequest{RunUid: run.GetUid(), NodeId: "approval", Reason: "live model test"}); err != nil {
		t.Fatal(err)
	}
	terminal := waitSnapshot(t, updates, func(snapshot liveSnapshot) bool {
		projected := snapshot.Runs[run.GetUid()]
		return projected != nil && projected.GetPhase() == "succeeded" && hasRunEvent(snapshot.RunEvents[run.GetUid()], "run.succeeded")
	})
	assertStrictRunSequences(t, terminal.RunEvents[run.GetUid()])

	current := terminal.Resources[resourceKey]
	if current == nil {
		t.Fatal("live Flow disappeared before delete")
	}
	if _, err := client.Resources.Delete(context.Background(), &controlv1alpha1.DeleteRequest{Key: current.GetKey(), ExpectedResourceVersion: current.GetResourceVersion()}); err != nil {
		t.Fatal(err)
	}
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool { return snapshot.Resources[resourceKey] == nil })
}

func TestLiveControllerResumesAfterDaemonSocketRestart(t *testing.T) {
	testRoot, err := os.MkdirTemp("/tmp", "orchigram-live-reconnect-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	cfg := config.Development(filepath.Join(testRoot, "state"))
	firstContext, stopFirst := context.WithCancel(context.Background())
	first, err := daemon.Open(firstContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- first.Serve(firstContext) }()

	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)
	controller := newLiveController(client)
	controller.runPollInterval = 20 * time.Millisecond
	controller.statusInterval = 30 * time.Millisecond
	if _, err := controller.bootstrap(context.Background()); err != nil {
		t.Fatal(err)
	}
	liveContext, stopLive := context.WithCancel(context.Background())
	defer stopLive()
	updates := newSnapshotObserver()
	controller.run(liveContext, updates.observe)

	firstFlow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: before-restart}
spec: {nodes: [{id: done, uses: core.noop}]}
`)
	firstApplied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: firstFlow})
	if err != nil {
		t.Fatal(err)
	}
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool {
		return snapshot.Resources[resourceLiveKey(firstApplied.GetResource().GetKey())] != nil
	})

	stopFirst()
	select {
	case serveErr := <-firstDone:
		if serveErr != nil {
			t.Fatal(serveErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first daemon did not stop")
	}
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool {
		return snapshot.Connections["resources"] == "reconnecting" || snapshot.Connections["runs"] == "reconnecting" || snapshot.Connections["status"] == "reconnecting"
	})

	secondContext, stopSecond := context.WithCancel(context.Background())
	second, err := daemon.Open(secondContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- second.Serve(secondContext) }()
	defer func() {
		stopSecond()
		select {
		case serveErr := <-secondDone:
			if serveErr != nil {
				t.Errorf("serve restarted daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("restarted daemon did not stop")
		}
	}()
	waitForTUIHealth(t, client)
	secondFlow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: after-restart}
spec: {nodes: [{id: done, uses: core.noop}]}
`)
	secondApplied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: secondFlow})
	if err != nil {
		t.Fatal(err)
	}
	waitSnapshot(t, updates, func(snapshot liveSnapshot) bool {
		return snapshot.Resources[resourceLiveKey(secondApplied.GetResource().GetKey())] != nil && snapshot.Connections["resources"] == "connected"
	})
}

type snapshotObserver struct {
	mu       sync.RWMutex
	latest   liveSnapshot
	notified chan struct{}
}

func newSnapshotObserver() *snapshotObserver {
	return &snapshotObserver{notified: make(chan struct{}, 1)}
}

func (o *snapshotObserver) observe(snapshot liveSnapshot) {
	o.mu.Lock()
	o.latest = snapshot
	o.mu.Unlock()
	select {
	case o.notified <- struct{}{}:
	default:
	}
}

func (o *snapshotObserver) snapshot() liveSnapshot {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.latest
}

func waitSnapshot(t *testing.T, observer *snapshotObserver, predicate func(liveSnapshot) bool) liveSnapshot {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		snapshot := observer.snapshot()
		if predicate(snapshot) {
			return snapshot
		}
		select {
		case <-observer.notified:
		case <-deadline.C:
			t.Fatal("timed out waiting for live TUI projection")
		}
	}
}

func hasRunEvent(events []*controlv1alpha1.RunEvent, eventType string) bool {
	for _, event := range events {
		if event.GetType() == eventType {
			return true
		}
	}
	return false
}

func assertStrictRunSequences(t *testing.T, events []*controlv1alpha1.RunEvent) {
	t.Helper()
	var previous uint64
	for _, event := range events {
		if event.GetSequence() <= previous {
			t.Fatalf("run event sequences are not strictly increasing: %d after %d", event.GetSequence(), previous)
		}
		previous = event.GetSequence()
	}
}

func liveResource(kind, namespace, name string, version uint64) *controlv1alpha1.ResourceDocument {
	return &controlv1alpha1.ResourceDocument{Key: &controlv1alpha1.ResourceKey{Kind: kind, Namespace: namespace, Name: name}, ResourceVersion: version, Json: []byte(`{}`)}
}

func resourceStatusPhase(raw []byte) string {
	var projection struct {
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	}
	_ = json.Unmarshal(raw, &projection)
	return projection.Status.Phase
}

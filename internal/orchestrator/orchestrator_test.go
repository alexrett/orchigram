package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

type fakeEngine struct {
	mu      sync.Mutex
	plans   []string
	unique  map[string]string
	signals []engine.EventSignal
}

func (f *fakeEngine) Start(_ context.Context, runUID string, plan flow.ExecutionPlan, _ json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.plans = append(f.plans, plan.PlanHash)
	if f.unique == nil {
		f.unique = map[string]string{}
	}
	if existing := f.unique[runUID]; existing != "" && existing != plan.PlanHash {
		return errors.New("run was restarted with a different plan")
	}
	f.unique[runUID] = plan.PlanHash
	return nil
}
func (f *fakeEngine) Signal(context.Context, string, string, engine.ApprovalSignal) error { return nil }

func (f *fakeEngine) SignalEvent(_ context.Context, _ string, _ string, signal engine.EventSignal) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.signals = append(f.signals, signal)
	return nil
}
func (f *fakeEngine) Cancel(context.Context, string, string) error        { return nil }
func (f *fakeEngine) Reconcile(context.Context) error                     { return nil }
func (f *fakeEngine) Describe(context.Context, string) (store.Run, error) { return store.Run{}, nil }
func (f *fakeEngine) Close() error                                        { return nil }

type healthEngine struct {
	*fakeEngine
	healthMu     sync.Mutex
	reconcileErr error
}

func (e *healthEngine) Reconcile(context.Context) error {
	e.healthMu.Lock()
	defer e.healthMu.Unlock()
	return e.reconcileErr
}

func (e *healthEngine) setReconcileError(err error) {
	e.healthMu.Lock()
	e.reconcileErr = err
	e.healthMu.Unlock()
}

func TestRuntimeHealthRetainsFailuresUntilSuccessfulRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	applyFlow(ctx, t, state, `
    - {id: effect, uses: core.noop}
`, "", 0)
	durable := &healthEngine{fakeEngine: &fakeEngine{}}
	control := New(state, flow.NewCompiler(nil), durable)
	control.claimStaleAfter = 10 * time.Millisecond
	var injectOutboxFailure atomic.Bool
	control.SetFaultHook(func(boundary Boundary) error {
		if boundary == BoundaryAfterRun && injectOutboxFailure.CompareAndSwap(true, false) {
			return errors.New("private-outbox-payload-must-not-escape")
		}
		return nil
	})
	control.Start(ctx)
	waitForHealthDiagnostics(t, control, func(codes map[string]string) bool { return len(codes) == 0 })

	durable.setReconcileError(errors.New("credential-like-value-must-not-escape"))
	waitForHealthDiagnostics(t, control, func(codes map[string]string) bool { return codes["engine"] == "reconcile_failed" })
	assertHealthDoesNotContain(t, control, "credential-like-value-must-not-escape")
	durable.setReconcileError(nil)
	waitForHealthDiagnostics(t, control, func(codes map[string]string) bool { return codes["engine"] == "" })

	injectOutboxFailure.Store(true)
	if _, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{}`), "health-recovery"); err != nil {
		t.Fatal(err)
	}
	waitForHealthDiagnostics(t, control, func(codes map[string]string) bool { return codes["outbox"] == "reconcile_failed" })
	assertHealthDoesNotContain(t, control, "private-outbox-payload-must-not-escape")
	waitForHealthDiagnostics(t, control, func(codes map[string]string) bool { return codes["outbox"] == "" })
}

func waitForHealthDiagnostics(t *testing.T, control *Orchestrator, predicate func(map[string]string) bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		codes := map[string]string{}
		for _, diagnostic := range control.HealthDiagnostics() {
			codes[diagnostic.Path] = diagnostic.Code
		}
		if predicate(codes) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health diagnostics did not converge: %+v", control.HealthDiagnostics())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertHealthDoesNotContain(t *testing.T, control *Orchestrator, forbidden string) {
	t.Helper()
	for _, diagnostic := range control.HealthDiagnostics() {
		if strings.Contains(diagnostic.Path+diagnostic.Code+diagnostic.Message, forbidden) {
			t.Fatalf("health diagnostic leaked dependency text: %+v", diagnostic)
		}
	}
}

func TestCrashBoundariesRecoverOnePinnedRun(t *testing.T) {
	t.Parallel()
	for _, boundary := range []Boundary{BoundaryAfterRun, BoundaryAfterEngine} {
		boundary := boundary
		t.Run(string(boundary), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = state.Close() }()
			original := applyFlow(ctx, t, state, `
    - {id: begin, uses: core.noop}
    - {id: approval, uses: core.approval}
`, `
    - {from: begin, to: approval}
`, 0)
			fake := &fakeEngine{}
			control := New(state, flow.NewCompiler(nil), fake)
			control.claimStaleAfter = 10 * time.Millisecond
			receipt, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{}`), "same-occurrence")
			if err != nil {
				t.Fatal(err)
			}
			injected := errors.New("simulated process loss")
			control.SetFaultHook(func(candidate Boundary) error {
				if candidate == boundary {
					return injected
				}
				return nil
			})
			if err := control.ReconcileOne(ctx); !errors.Is(err, injected) {
				t.Fatalf("first reconcile: %v", err)
			}
			run, err := state.GetRun(ctx, receipt.RunUID)
			if err != nil {
				t.Fatal(err)
			}
			originalPlan, diagnostics := flow.NewCompiler(nil).Compile(mustFlow(t, original.JSON))
			if len(diagnostics) != 0 || run.PlanHash != originalPlan.PlanHash {
				t.Fatalf("run plan=%s original=%s diagnostics=%+v", run.PlanHash, originalPlan.PlanHash, diagnostics)
			}
			applyFlow(ctx, t, state, `
    - {id: replacement, uses: core.noop}
`, "", original.Metadata.ResourceVersion)
			time.Sleep(20 * time.Millisecond)
			control.SetFaultHook(nil)
			if err := control.ReconcileOne(ctx); err != nil {
				t.Fatal(err)
			}
			if len(fake.unique) != 1 || fake.unique[receipt.RunUID] != originalPlan.PlanHash {
				t.Fatalf("engine starts=%+v unique=%+v", fake.plans, fake.unique)
			}
			duplicate, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{"changed":true}`), "same-occurrence")
			if err != nil {
				t.Fatal(err)
			}
			if duplicate.RunUID != receipt.RunUID {
				t.Fatalf("duplicate run uid %s != %s", duplicate.RunUID, receipt.RunUID)
			}
			if _, err := state.ClaimStart(ctx, time.Hour); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("unexpected second command: %v", err)
			}
		})
	}
}

func TestAcceptedPlanSurvivesFlowMutationBeforeFirstDispatch(t *testing.T) {
	t.Parallel()
	for _, mutation := range []string{"update", "delete"} {
		mutation := mutation
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = state.Close() }()

			original := applyFlow(ctx, t, state, `
    - {id: accepted, uses: core.noop}
`, "", 0)
			expectedPlan, diagnostics := flow.NewCompiler(nil).Compile(mustFlow(t, original.JSON))
			if len(diagnostics) != 0 {
				t.Fatalf("compile original: %+v", diagnostics)
			}
			fake := &fakeEngine{}
			control := New(state, flow.NewCompiler(nil), fake)
			receipt, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{"accepted":true}`), "mutate-before-dispatch-"+mutation)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := state.GetPlan(ctx, expectedPlan.PlanHash); err != nil {
				t.Fatalf("accepted plan was not stored before acknowledgement: %v", err)
			}

			switch mutation {
			case "update":
				applyFlow(ctx, t, state, `
    - {id: replacement, uses: core.noop}
`, "", original.Metadata.ResourceVersion)
			case "delete":
				if err := state.Delete(ctx, "Flow", original.Metadata.Namespace, original.Metadata.Name, original.Metadata.ResourceVersion, "delete-before-dispatch"); err != nil {
					t.Fatal(err)
				}
			}

			if err := control.ReconcileOne(ctx); err != nil {
				t.Fatalf("dispatch accepted plan after Flow %s: %v", mutation, err)
			}
			run, err := state.GetRun(ctx, receipt.RunUID)
			if err != nil {
				t.Fatal(err)
			}
			if run.PlanHash != expectedPlan.PlanHash || fake.unique[receipt.RunUID] != expectedPlan.PlanHash {
				t.Fatalf("run=%+v engine=%+v expected plan=%s", run, fake.unique, expectedPlan.PlanHash)
			}
		})
	}
}

func TestProviderReplayAdvancesCursorWithoutRecompilingDeletedFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	acceptedFlow := applyFlow(ctx, t, state, `
    - {id: accepted, uses: core.noop}
`, "", 0)
	triggerDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: provider-replay}
spec:
  flow: demo
  provider: {plugin: fixture}
`))
	if err != nil {
		t.Fatal(err)
	}
	storedTrigger, err := state.Apply(ctx, triggerDocument, store.ApplyOptions{RequestID: "provider-replay-trigger"})
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := resource.DecodeTrigger(storedTrigger.JSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	control := New(state, flow.NewCompiler(nil), &fakeEngine{})
	first, err := control.AcceptProviderTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "provider-event-1", "demo", resource.DefaultNamespace, json.RawMessage(`{"event":1}`), "cursor-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Delete(ctx, "Flow", acceptedFlow.Metadata.Namespace, acceptedFlow.Metadata.Name, acceptedFlow.Metadata.ResourceVersion, "delete-after-provider-accept"); err != nil {
		t.Fatal(err)
	}
	replay, err := control.AcceptProviderTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "provider-event-1", "demo", resource.DefaultNamespace, json.RawMessage(`{"event":1}`), "cursor-2")
	if err != nil {
		t.Fatalf("replay after Flow deletion: %v", err)
	}
	if !replay.Existing || replay.UID != first.UID || replay.RunUID != first.RunUID {
		t.Fatalf("first=%+v replay=%+v", first, replay)
	}
	cursor, eventID, err := state.ProviderCursor(ctx, trigger.Metadata.UID)
	if err != nil || cursor != "cursor-2" || eventID != "provider-event-1" {
		t.Fatalf("cursor=%q event=%q err=%v", cursor, eventID, err)
	}
}

func TestActiveRunAdmissionQueuesAcceptedWork(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	applyFlow(ctx, t, state, `
    - {id: hold, uses: core.noop}
`, "", 0)
	durable := &fakeEngine{}
	control := New(state, flow.NewCompiler(nil), durable, Options{MaxActiveRuns: 1})
	control.capacityRetryAfter = 0
	first, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{}`), "capacity-first")
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReconcileOne(ctx); err != nil {
		t.Fatal(err)
	}
	second, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{}`), "capacity-second")
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReconcileOne(ctx); !errors.Is(err, errRunCapacity) {
		t.Fatalf("second admission=%v", err)
	}
	if _, err := state.GetRun(ctx, second.RunUID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("queued accepted Run was admitted: %v", err)
	}
	diagnostics := control.HealthDiagnostics()
	foundCapacity := false
	for _, diagnostic := range diagnostics {
		foundCapacity = foundCapacity || diagnostic.Path == "capacity/runs" && diagnostic.Code == "saturated"
	}
	if !foundCapacity {
		t.Fatalf("capacity diagnostics=%+v", diagnostics)
	}
	if err := state.AppendRunEvent(ctx, first.RunUID, "", "run.succeeded", "succeeded", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := control.ReconcileOne(ctx); err != nil {
		t.Fatal(err)
	}
	if run, err := state.GetRun(ctx, second.RunUID); err != nil || run.Phase != "pending" {
		t.Fatalf("admitted queued Run=%+v err=%v", run, err)
	}
}

func TestProviderSignalOutboxRedeliversSameStableEventAfterCrashBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	applyFlow(ctx, t, state, `
    - {id: review, uses: core.event, timeout: 1h}
`, "", 0)
	fake := &fakeEngine{}
	control := New(state, flow.NewCompiler(nil), fake)
	startReceipt, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{}`), "signal-run")
	if err != nil {
		t.Fatal(err)
	}
	if err := control.ReconcileOne(ctx); err != nil {
		t.Fatal(err)
	}
	triggerDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: reviews}
spec:
  flow: demo
  provider: {plugin: fixture}
  delivery: {mode: signal, node: review}
`))
	if err != nil {
		t.Fatal(err)
	}
	storedTrigger, err := state.Apply(ctx, triggerDocument, store.ApplyOptions{RequestID: "signal-trigger"})
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := resource.DecodeTrigger(storedTrigger.JSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	payload := json.RawMessage(`{"review":{"state":"changes_requested"}}`)
	if _, err := control.AcceptProviderSignal(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "review-event-1", "demo", "default", startReceipt.RunUID, "review", payload, "1"); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("crash after workflow signal delivery")
	control.SetFaultHook(func(boundary Boundary) error {
		if boundary == BoundaryAfterEngine {
			return injected
		}
		return nil
	})
	if err := control.ReconcileOne(ctx); !errors.Is(err, injected) {
		t.Fatalf("first signal reconcile=%v", err)
	}
	control.SetFaultHook(nil)
	control.claimStaleAfter = 0
	time.Sleep(time.Millisecond)
	if err := control.ReconcileOne(ctx); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	signals := append([]engine.EventSignal(nil), fake.signals...)
	fake.mu.Unlock()
	if len(signals) != 2 || signals[0].ProviderEventID != "review-event-1" || signals[1].ProviderEventID != "review-event-1" || string(signals[0].Payload) != string(payload) || string(signals[1].Payload) != string(payload) {
		t.Fatalf("redelivered signals=%+v", signals)
	}
	if _, err := state.ClaimSignal(ctx, time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("completed signal remained claimable: %v", err)
	}
}

func TestCancelBeforeStartSuppressesOutboxAndReconcilesTerminalRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	durable, err := engine.Open(ctx, filepath.Join(root, "workflows.sqlite"), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = durable.Close() }()
	applyFlow(ctx, t, state, `
    - {id: effect, uses: core.noop}
`, "", 0)
	control := New(state, flow.NewCompiler(nil), durable)
	receipt, err := control.StartManual(ctx, "demo", "default", json.RawMessage(`{}`), "cancel-before-start")
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("stop after ensuring run")
	control.SetFaultHook(func(boundary Boundary) error {
		if boundary == BoundaryAfterRun {
			return injected
		}
		return nil
	})
	if err := control.ReconcileOne(ctx); !errors.Is(err, injected) {
		t.Fatalf("initial outbox processing: %v", err)
	}
	if err := state.RequestRunCancellation(ctx, receipt.RunUID, "cancel before workflow start"); err != nil {
		t.Fatal(err)
	}
	if err := durable.Cancel(ctx, receipt.RunUID, "cancel before workflow start"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancel missing workflow: %v", err)
	}
	pending, err := state.UndeliveredRunCancellations(ctx, 100)
	if err != nil || len(pending) != 1 {
		t.Fatalf("cancellation was acknowledged while start remained possible: pending=%+v err=%v", pending, err)
	}

	control.SetFaultHook(nil)
	control.claimStaleAfter = 0
	time.Sleep(time.Millisecond)
	if err := control.ReconcileOne(ctx); err != nil {
		t.Fatalf("cancelled outbox processing: %v", err)
	}
	if err := durable.Reconcile(ctx); err != nil {
		t.Fatalf("cancellation reconciliation: %v", err)
	}
	pending, err = state.UndeliveredRunCancellations(ctx, 100)
	if err != nil || len(pending) != 0 {
		t.Fatalf("cancellation remained pending after start suppression: pending=%+v err=%v", pending, err)
	}
	run, err := state.GetRun(ctx, receipt.RunUID)
	if err != nil || run.Phase != "cancelled" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	events, err := state.RunEventsAfter(ctx, receipt.RunUID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == "node.started" || event.Type == "node.completed" {
			t.Fatalf("cancelled run executed node event %q", event.Type)
		}
	}
	if _, err := state.ClaimStart(ctx, time.Hour); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("durable start remained claimable: %v", err)
	}
}

func applyFlow(ctx context.Context, t *testing.T, state *store.Store, nodes, edges string, expected uint64) resource.Document {
	t.Helper()
	doc, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: demo}
spec:
  nodes:` + nodes + `  edges:` + edges))
	if err != nil {
		t.Fatal(err)
	}
	result, err := state.Apply(ctx, doc, store.ApplyOptions{ExpectedResourceVersion: expected})
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func mustFlow(t *testing.T, data []byte) resource.Flow {
	t.Helper()
	value, err := resource.DecodeFlow(data)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

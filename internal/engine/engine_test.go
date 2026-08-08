package engine

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"github.com/cschleiden/go-workflows/core"
)

func TestApprovalSignalCompletesWithoutPendingTimer(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := flow.ExecutionPlan{
		APIVersion: "orchigram.dev/v1alpha1", FlowUID: "flow-1", FlowGeneration: 1,
		InterpreterVersion: flow.InterpreterVersion, Timeout: "1m0s", MaxParallel: 1, PlanHash: "test-plan",
		Nodes: []flow.PlanNode{
			{ID: "first", Name: "first", Uses: "core.noop", Timeout: "1m0s", RetryBackoff: "1ms"},
			{ID: "approval", Name: "approval", Uses: "core.approval", Timeout: "10s", RetryBackoff: "1ms"},
			{ID: "last", Name: "last", Uses: "core.noop", Timeout: "1m0s", RetryBackoff: "1ms"},
		},
		Edges: []flow.PlanEdge{{From: "approval", To: "last", Condition: "result.approved"}, {From: "first", To: "approval"}},
	}
	payload := store.StartPayload{RunUID: "run-approval", ReceiptUID: "receipt-1", FlowName: "flow", Namespace: "default", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "waiting")
	if err := state.DecideApproval(ctx, payload.RunUID, "approval", "approved", "test", "ok"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Signal(ctx, payload.RunUID, "approval", ApprovalSignal{State: "approved", Reason: "ok"}); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
	instance, err := adapter.findInstance(ctx, payload.RunUID)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		frameworkState, stateErr := adapter.client.GetWorkflowInstanceState(ctx, instance)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if frameworkState == core.WorkflowInstanceStateFinished {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("framework state remained %v", frameworkState)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestActivityHeartbeatsPreventConcurrentRedispatch(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	executor := &slowExecutor{delay: 3 * time.Second}
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := flow.ExecutionPlan{FlowUID: "flow-heartbeat", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "heartbeat-plan", Nodes: []flow.PlanNode{{ID: "slow", Uses: "slow.execute", Timeout: "10s", RetryBackoff: "10ms"}}}
	payload := store.StartPayload{RunUID: "run-heartbeat", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
	executor.mu.Lock()
	calls, maximum := executor.calls, executor.maximum
	executor.mu.Unlock()
	if calls != 1 || maximum != 1 {
		t.Fatalf("long activity calls=%d maximum concurrency=%d", calls, maximum)
	}
}

func TestPhysicalRetriesAreDurableAndReuseExternalIdempotencyKey(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	executor := &flakyAttemptExecutor{}
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := flow.ExecutionPlan{
		FlowUID: "flow-retry-evidence", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "retry-evidence-plan",
		Nodes: []flow.PlanNode{{ID: "effect", Uses: "fixture.execute", Timeout: "5s", RetryLimit: 1, RetryBackoff: "10ms"}},
	}
	payload := store.StartPayload{RunUID: "run-retry-evidence", Input: json.RawMessage(`{"request":true}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")

	attempts, err := state.ListNodeAttempts(ctx, payload.RunUID, "effect", 10)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if attempts[0].Attempt != 1 || attempts[0].Phase != "failed" || attempts[1].Attempt != 2 || attempts[1].Phase != "succeeded" {
		t.Fatalf("attempt phases=%+v", attempts)
	}
	if attempts[0].IdempotencyKey != attempts[1].IdempotencyKey {
		t.Fatalf("idempotency key changed across physical retries: %+v", attempts)
	}
	executor.mu.Lock()
	identities, keys := append([]TaskIdentity(nil), executor.identities...), append([]string(nil), executor.keys...)
	executor.mu.Unlock()
	if len(identities) != 2 || identities[0].Attempt != 1 || identities[1].Attempt != 2 || identities[0].FrameworkAttempt != 1 || identities[1].FrameworkAttempt != 2 || identities[0].LogicalIteration != 0 || identities[1].LogicalIteration != 0 {
		t.Fatalf("executor identities=%+v", identities)
	}
	if len(keys) != 2 || keys[0] != keys[1] || keys[0] == "" {
		t.Fatalf("executor idempotency keys=%+v", keys)
	}
}

func TestCancellationRemainsTerminalAfterLateActivityCompletion(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	executor := &blockingExecutor{started: make(chan struct{}), release: make(chan struct{})}
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := flow.ExecutionPlan{FlowUID: "flow-cancel-race", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "cancel-race-plan", Nodes: []flow.PlanNode{{ID: "slow", Uses: "slow.execute", Timeout: "10s", RetryBackoff: "10ms"}}}
	payload := store.StartPayload{RunUID: "run-cancel-race", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	select {
	case <-executor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("activity did not start")
	}
	if err := state.AppendRunEvent(ctx, payload.RunUID, "", "run.cancelled", "cancelled", 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Cancel(ctx, payload.RunUID, "test cancellation"); err != nil {
		t.Fatal(err)
	}
	close(executor.release)
	time.Sleep(300 * time.Millisecond)
	run, err := state.GetRun(ctx, payload.RunUID)
	if err != nil || run.Phase != "cancelled" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	events, err := state.RunEventsAfter(ctx, payload.RunUID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if event.Type == "node.completed" || event.Type == "run.succeeded" || event.Type == "run.failed" || event.Type == "approval.waiting" {
			t.Fatalf("late event %q appended after cancellation", event.Type)
		}
	}
}

func TestCancellationDeliveryReconcilesAfterEngineRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	workflowPath := filepath.Join(root, "workflows.sqlite")
	first, err := Open(ctx, workflowPath, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan := flow.ExecutionPlan{
		FlowUID: "flow-cancel-restart", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "cancel-restart-plan",
		Nodes: []flow.PlanNode{{ID: "approval", Uses: "core.approval", Timeout: "30s", RetryBackoff: "10ms"}},
	}
	payload := store.StartPayload{RunUID: "run-cancel-restart", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := first.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "waiting")
	if err := state.RequestRunCancellation(ctx, payload.RunUID, "crash boundary"); err != nil {
		t.Fatal(err)
	}
	// Simulate process loss after the state transaction commits but before
	// Adapter.Cancel is called. The replacement adapter owns delivery.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	recorder := &cancellationRecorder{}
	second, err := Open(ctx, workflowPath, state, recorder)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	if err := second.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if recorder.runUID != payload.RunUID || recorder.reason != "crash boundary" {
		t.Fatalf("provider cancellation=%q reason=%q", recorder.runUID, recorder.reason)
	}
	pending, err := state.UndeliveredRunCancellations(ctx, 100)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending cancellations=%+v err=%v", pending, err)
	}
	run, err := state.GetRun(ctx, payload.RunUID)
	if err != nil || run.Phase != "cancelled" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
}

func TestStartSuppressesRunCancelledBeforeWorkflowCreation(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := flow.ExecutionPlan{
		FlowUID: "flow-cancel-before-start", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "cancel-before-start-plan",
		Nodes: []flow.PlanNode{{ID: "effect", Uses: "core.noop", Timeout: "30s", RetryBackoff: "10ms"}},
	}
	payload := store.StartPayload{RunUID: "run-cancel-before-start", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := state.RequestRunCancellation(ctx, payload.RunUID, "cancel before start"); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.findInstance(ctx, payload.RunUID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("cancelled run created a workflow instance: %v", err)
	}
}

type cancellationRecorder struct {
	runUID string
	reason string
}

func (*cancellationRecorder) Execute(context.Context, string, flow.PlanNode, json.RawMessage, map[string]any, string) (json.RawMessage, error) {
	return json.RawMessage(`{}`), nil
}

func (r *cancellationRecorder) CancelRun(_ context.Context, runUID, reason string) error {
	r.runUID = runUID
	r.reason = reason
	return nil
}

type slowExecutor struct {
	mu              sync.Mutex
	active, maximum int
	calls           int
	delay           time.Duration
}

type flakyAttemptExecutor struct {
	mu         sync.Mutex
	identities []TaskIdentity
	keys       []string
}

func (e *flakyAttemptExecutor) Execute(ctx context.Context, _ string, _ flow.PlanNode, _ json.RawMessage, _ map[string]any, idempotencyKey string) (json.RawMessage, error) {
	identity := TaskIdentityFromContext(ctx)
	e.mu.Lock()
	e.identities = append(e.identities, identity)
	e.keys = append(e.keys, idempotencyKey)
	e.mu.Unlock()
	if identity.Attempt == 1 {
		return nil, errors.New("transient fixture failure")
	}
	return json.RawMessage(`{"ok":true}`), nil
}

type blockingExecutor struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (e *blockingExecutor) Execute(context.Context, string, flow.PlanNode, json.RawMessage, map[string]any, string) (json.RawMessage, error) {
	e.once.Do(func() { close(e.started) })
	<-e.release
	return json.RawMessage(`{"late":true}`), nil
}

func (e *slowExecutor) Execute(context.Context, string, flow.PlanNode, json.RawMessage, map[string]any, string) (json.RawMessage, error) {
	e.mu.Lock()
	e.calls++
	e.active++
	if e.active > e.maximum {
		e.maximum = e.active
	}
	e.mu.Unlock()
	time.Sleep(e.delay)
	e.mu.Lock()
	e.active--
	e.mu.Unlock()
	return json.RawMessage(`{"ok":true}`), nil
}

func TestFiniteCycleExecutesDeclaredIterationsAndExits(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: finite-cycle, uid: flow-cycle, generation: 1}
spec:
  nodes:
    - id: repeat
      uses: core.noop
      loop: {maxIterations: 3}
    - {id: finish, uses: core.noop}
  edges:
    - {from: repeat, to: repeat}
    - {from: repeat, to: finish}
`))
	if err != nil {
		t.Fatal(err)
	}
	plan, diagnostics := flow.NewCompiler(nil).Compile(flowResource)
	if len(diagnostics) != 0 {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
	payload := store.StartPayload{RunUID: "run-cycle", ReceiptUID: "receipt-cycle", FlowName: "finite-cycle", Namespace: "default", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
	events, err := state.RunEventsAfter(ctx, payload.RunUID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, event := range events {
		if event.NodeID == "repeat" && event.Type == "node.completed" {
			completed++
		}
	}
	if completed != 3 {
		t.Fatalf("repeat completed %d times, events=%+v", completed, events)
	}
}

func waitForPhase(ctx context.Context, t *testing.T, state *store.Store, runUID, phase string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		run, err := state.GetRun(ctx, runUID)
		if err == nil && run.Phase == phase {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not reach %s: run=%+v err=%v", phase, run, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

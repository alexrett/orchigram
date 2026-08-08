package engine

import (
	"context"
	"encoding/json"
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

type slowExecutor struct {
	mu              sync.Mutex
	active, maximum int
	calls           int
	delay           time.Duration
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

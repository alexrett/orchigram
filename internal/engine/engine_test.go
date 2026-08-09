package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
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

func TestRemoveFinishedRunPrunesOnlyFrameworkHistory(t *testing.T) {
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
		APIVersion: "orchigram.dev/v1alpha1", FlowUID: "flow-retention", FlowGeneration: 1,
		InterpreterVersion: flow.InterpreterVersion, Timeout: "1m0s", MaxParallel: 1, PlanHash: "plan-retention",
		Nodes: []flow.PlanNode{{ID: "done", Name: "done", Uses: "core.noop", Timeout: "1m0s", RetryBackoff: "1ms"}},
	}
	payload := store.StartPayload{RunUID: "run-retention", ReceiptUID: "receipt-retention", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
	if err := adapter.RemoveFinishedRun(ctx, payload.RunUID); err != nil {
		t.Fatal(err)
	}
	if _, err := adapter.findInstance(ctx, payload.RunUID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("workflow history survived: %v", err)
	}
	if _, err := state.GetRun(ctx, payload.RunUID); err != nil {
		t.Fatalf("product evidence was removed by framework prune: %v", err)
	}
}

func TestExternalEventsResumeSameRunAndDeduplicateRedeliveryAcrossLoopIterations(t *testing.T) {
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
		APIVersion: resource.APIVersion, FlowUID: "flow-review-loop", FlowGeneration: 1,
		InterpreterVersion: flow.InterpreterVersion, Timeout: "1m0s", MaxParallel: 1, PlanHash: "review-loop-plan",
		Nodes: []flow.PlanNode{
			{ID: "review", Name: "review", Uses: "core.event", Timeout: "20s", RetryBackoff: "1ms", LoopMaxIterations: 3},
			{ID: "rework", Name: "rework", Uses: "core.noop", With: map[string]any{"result": map[string]any{"updated": true}}, Timeout: "20s", RetryBackoff: "1ms", LoopMaxIterations: 3},
			{ID: "ready", Name: "ready", Uses: "core.noop", Timeout: "20s", RetryBackoff: "1ms"},
		},
		Edges: []flow.PlanEdge{
			{From: "review", To: "rework", Condition: `result.review.state == "changes_requested"`},
			{From: "rework", To: "review"},
			{From: "review", To: "ready", Condition: `result.review.state == "approved"`},
		},
		Components: [][]string{{"review", "rework"}, {"ready"}},
	}
	payload := store.StartPayload{RunUID: "run-review-loop", ReceiptUID: "receipt-review-loop", FlowName: "review-loop", Namespace: "default", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	changes := EventSignal{ProviderEventID: "review-event-1", Payload: json.RawMessage(`{"review":{"state":"changes_requested"}}`)}
	approved := EventSignal{ProviderEventID: "review-event-2", Payload: json.RawMessage(`{"review":{"state":"approved"}}`)}
	waitForPhase(ctx, t, state, payload.RunUID, "waiting")
	if err := adapter.SignalEvent(ctx, payload.RunUID, "review", changes); err != nil {
		t.Fatal(err)
	}
	waitForRunEventCount(ctx, t, state, payload.RunUID, "event.waiting", 2)
	if err := adapter.SignalEvent(ctx, payload.RunUID, "review", changes); err != nil {
		t.Fatal(err)
	}
	waitForRunEventCount(ctx, t, state, payload.RunUID, "event.duplicate", 1)
	if err := adapter.SignalEvent(ctx, payload.RunUID, "review", approved); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
	events, err := state.RunEventsAfter(ctx, payload.RunUID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	received, reworks := 0, 0
	for _, event := range events {
		if event.Type == "event.received" {
			received++
		}
		if event.Type == "node.completed" && event.NodeID == "rework" {
			reworks++
		}
	}
	if received != 2 || reworks != 1 {
		t.Fatalf("event.received=%d rework completions=%d events=%+v", received, reworks, events)
	}
}

func TestExternalEventDeliveredBeforeWaitChannelIsConsumed(t *testing.T) {
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
		FlowUID: "flow-event-early", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "event-early-plan",
		Nodes: []flow.PlanNode{{ID: "review", Uses: "core.event", Timeout: "30s", RetryBackoff: "10ms"}},
	}
	payload := store.StartPayload{RunUID: "run-event-early", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	if err := adapter.SignalEvent(ctx, payload.RunUID, "review", EventSignal{ProviderEventID: "early-review", Payload: json.RawMessage(`{"review":{"state":"approved"}}`)}); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
}

func TestExternalEventWaitSurvivesEngineRestart(t *testing.T) {
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
		FlowUID: "flow-event-restart", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "event-restart-plan",
		Nodes: []flow.PlanNode{{ID: "review", Uses: "core.event", Timeout: "30s", RetryBackoff: "10ms"}},
	}
	payload := store.StartPayload{RunUID: "run-event-restart", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := first.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "waiting")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(ctx, workflowPath, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	if err := second.SignalEvent(ctx, payload.RunUID, "review", EventSignal{ProviderEventID: "review-after-restart", Payload: json.RawMessage(`{"review":{"state":"approved"}}`)}); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
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

func TestMaxParallelBoundsForkJoinDeterministically(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprintf("limit-%d", limit), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			root := t.TempDir()
			state, err := store.Open(filepath.Join(root, "state.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = state.Close() }()
			executor := &parallelGateExecutor{started: make(chan string, 3), release: make(chan struct{})}
			adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = adapter.Close() }()
			plan := forkJoinPlan(limit)
			payload := store.StartPayload{RunUID: fmt.Sprintf("run-parallel-%d", limit), Input: json.RawMessage(`{}`)}
			if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
				t.Fatal(err)
			}
			if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
				t.Fatal(err)
			}
			firstWave := make([]string, 0, limit)
			for len(firstWave) < limit {
				select {
				case nodeID := <-executor.started:
					firstWave = append(firstWave, nodeID)
				case <-time.After(5 * time.Second):
					t.Fatalf("only %d fork nodes started", len(firstWave))
				}
			}
			select {
			case extra := <-executor.started:
				t.Fatalf("node %s exceeded maxParallel=%d before release", extra, limit)
			case <-time.After(250 * time.Millisecond):
			}
			sort.Strings(firstWave)
			wantFirst := []string{"alpha"}
			if limit == 2 {
				wantFirst = []string{"alpha", "beta"}
			}
			if !reflect.DeepEqual(firstWave, wantFirst) {
				t.Fatalf("first admitted wave=%v want=%v", firstWave, wantFirst)
			}
			close(executor.release)
			waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
			executor.mu.Lock()
			maximum := executor.maximum
			calls := append([]string(nil), executor.calls...)
			executor.mu.Unlock()
			if maximum != limit {
				t.Fatalf("observed concurrency=%d want=%d calls=%v", maximum, limit, calls)
			}
			sort.Strings(calls)
			if !reflect.DeepEqual(calls, []string{"alpha", "beta", "gamma"}) {
				t.Fatalf("fork calls=%v", calls)
			}
			attempts, err := state.ListNodeAttempts(ctx, payload.RunUID, "", 20)
			if err != nil {
				t.Fatal(err)
			}
			var joinStarted time.Time
			branchCompleted := map[string]time.Time{}
			for _, attempt := range attempts {
				if attempt.Attempt != 1 || attempt.FrameworkAttempt != 1 {
					t.Fatalf("duplicate attempt identity: %+v", attempt)
				}
				if attempt.NodeID == "join" {
					joinStarted = attempt.StartedAt
				}
				if attempt.NodeID == "alpha" || attempt.NodeID == "beta" || attempt.NodeID == "gamma" {
					branchCompleted[attempt.NodeID] = attempt.CompletedAt
				}
			}
			if joinStarted.IsZero() || len(branchCompleted) != 3 {
				t.Fatalf("fork/join attempts=%+v", attempts)
			}
			for nodeID, completedAt := range branchCompleted {
				if completedAt.IsZero() || joinStarted.Before(completedAt) {
					t.Fatalf("join started at %v before %s completed at %v", joinStarted, nodeID, completedAt)
				}
			}
		})
	}
}

func TestConditionalFanInWaitsForSkippedPredecessor(t *testing.T) {
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
		FlowUID: "flow-conditional-join", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "conditional-join", MaxParallel: 2,
		Nodes: []flow.PlanNode{
			{ID: "source", Uses: "core.noop", Timeout: "5s", RetryBackoff: "10ms", With: map[string]any{"result": map[string]any{"ok": true}}},
			{ID: "left", Uses: "core.noop", Timeout: "5s", RetryBackoff: "10ms"},
			{ID: "right", Uses: "fixture.must-not-run", Timeout: "5s", RetryBackoff: "10ms"},
			{ID: "join", Uses: "core.noop", Timeout: "5s", RetryBackoff: "10ms"},
		},
		Edges: []flow.PlanEdge{
			{From: "source", To: "left", Condition: "result.ok == true"},
			{From: "source", To: "right", Condition: "result.ok == false"},
			{From: "left", To: "join"}, {From: "right", To: "join"},
		},
	}
	payload := store.StartPayload{RunUID: "run-conditional-join", Input: json.RawMessage(`{}`)}
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
	rightSkipped, joinStarted := uint64(0), uint64(0)
	for _, event := range events {
		if event.NodeID == "right" && event.Type == "node.skipped" {
			rightSkipped = event.Sequence
		}
		if event.NodeID == "join" && event.Type == "node.started" {
			joinStarted = event.Sequence
		}
	}
	if rightSkipped == 0 || joinStarted == 0 || joinStarted <= rightSkipped {
		t.Fatalf("conditional fan-in events=%+v", events)
	}
	if _, err := state.NodeAttempt(ctx, payload.RunUID, "right", 0, 1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("inactive branch created an attempt: %v", err)
	}
}

func TestParallelBranchFailureStopsAdmissionAndCancelsActiveWork(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	executor := &parallelFailureExecutor{bothStarted: make(chan struct{}), cancelActive: make(chan struct{})}
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := forkJoinPlan(2)
	plan.PlanHash = "parallel-failure"
	payload := store.StartPayload{RunUID: "run-parallel-failure", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "failed")
	executor.mu.Lock()
	calls := append([]string(nil), executor.calls...)
	maximum := executor.maximum
	executor.mu.Unlock()
	sort.Strings(calls)
	if maximum != 2 || !reflect.DeepEqual(calls, []string{"alpha", "beta"}) {
		t.Fatalf("parallel failure calls=%v maximum=%d", calls, maximum)
	}
	attempts, err := state.ListNodeAttempts(ctx, payload.RunUID, "", 20)
	if err != nil {
		t.Fatal(err)
	}
	byNode := map[string]store.NodeAttempt{}
	for _, attempt := range attempts {
		byNode[attempt.NodeID] = attempt
	}
	if byNode["alpha"].Phase != "failed" || byNode["beta"].Phase != "failed" || byNode["beta"].ExitOutcome != "cancelled" {
		t.Fatalf("parallel failure attempts=%+v", attempts)
	}
	if _, exists := byNode["gamma"]; exists {
		t.Fatalf("pending gamma was admitted: %+v", byNode["gamma"])
	}
	if _, exists := byNode["join"]; exists {
		t.Fatalf("join ran after branch failure: %+v", byNode["join"])
	}
	events, err := state.RunEventsAfter(ctx, payload.RunUID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	terminal := 0
	for _, event := range events {
		if event.Type == "run.failed" {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("run.failed events=%d events=%+v", terminal, events)
	}
}

func TestParallelForkResumesAfterWorkerRestartWithoutDuplicateCompletion(t *testing.T) {
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), executable, "-test.run=^TestParallelRestartCrashHelper$", "-test.v") //nolint:gosec // Re-executes the fixed current test binary.
	command.Env = append(os.Environ(), "ORCHIGRAM_ENGINE_CRASH_HELPER="+root)
	var helperOutput bytes.Buffer
	command.Stdout = &helperOutput
	command.Stderr = &helperOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(root, "ready")
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, readErr := os.ReadFile(readyPath) //nolint:gosec // Test-owned crash synchronization file.
		if readErr == nil && string(data) == "alpha,beta" {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			_ = command.Wait()
			t.Fatalf("crash helper did not reach the parallel boundary: %v: %s", readErr, helperOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("crash helper exited successfully instead of being killed")
	}
	time.Sleep(2200 * time.Millisecond)
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	payload := store.StartPayload{RunUID: "run-parallel-restart", Input: json.RawMessage(`{}`)}
	run, err := state.GetRun(context.Background(), payload.RunUID)
	if err != nil || run.Phase != "running" {
		t.Fatalf("run after worker crash=%+v err=%v helper=%s", run, err, helperOutput.String())
	}
	secondContext, stopSecondContext := context.WithCancel(context.Background())
	defer stopSecondContext()
	secondExecutor := &recordingExecutor{}
	second, err := Open(secondContext, filepath.Join(root, "workflows.sqlite"), state, secondExecutor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close() }()
	waitForPhase(secondContext, t, state, payload.RunUID, "succeeded")
	attempts, err := state.ListNodeAttempts(secondContext, payload.RunUID, "", 30)
	if err != nil {
		t.Fatal(err)
	}
	byNode := map[string][]store.NodeAttempt{}
	for _, attempt := range attempts {
		byNode[attempt.NodeID] = append(byNode[attempt.NodeID], attempt)
	}
	for _, nodeID := range []string{"alpha", "beta"} {
		nodeAttempts := byNode[nodeID]
		if len(nodeAttempts) != 2 || nodeAttempts[0].ExitOutcome != "delivery-lost" || nodeAttempts[1].Phase != "succeeded" {
			t.Fatalf("%s restart attempts=%+v", nodeID, nodeAttempts)
		}
	}
	secondExecutor.mu.Lock()
	secondCalls := append([]string(nil), secondExecutor.calls...)
	secondExecutor.mu.Unlock()
	sort.Strings(secondCalls)
	if !reflect.DeepEqual(secondCalls, []string{"alpha", "beta", "gamma"}) {
		t.Fatalf("restart executor calls=%v", secondCalls)
	}
	events, err := state.RunEventsAfter(secondContext, payload.RunUID, 0, 200)
	if err != nil {
		t.Fatal(err)
	}
	terminal := 0
	for _, event := range events {
		if event.Type == "run.succeeded" {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("run.succeeded events=%d events=%+v", terminal, events)
	}
}

func TestParallelRetryKeepsItsSlotAndAttemptIdentity(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	executor := &parallelRetryExecutor{}
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = adapter.Close() }()
	plan := forkJoinPlan(2)
	plan.PlanHash = "parallel-retry"
	for index := range plan.Nodes {
		if plan.Nodes[index].ID == "alpha" {
			plan.Nodes[index].RetryLimit = 1
		}
	}
	payload := store.StartPayload{RunUID: "run-parallel-retry", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	waitForPhase(ctx, t, state, payload.RunUID, "succeeded")
	executor.mu.Lock()
	maximum := executor.maximum
	calls := append([]string(nil), executor.calls...)
	executor.mu.Unlock()
	if maximum > 2 {
		t.Fatalf("retry exceeded maxParallel: maximum=%d calls=%v", maximum, calls)
	}
	sort.Strings(calls)
	if !reflect.DeepEqual(calls, []string{"alpha/1", "alpha/2", "beta/1", "gamma/1"}) {
		t.Fatalf("parallel retry calls=%v", calls)
	}
	attempts, err := state.ListNodeAttempts(ctx, payload.RunUID, "alpha", 10)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("alpha attempts=%+v err=%v", attempts, err)
	}
	if attempts[0].FrameworkAttempt != 1 || attempts[0].Phase != "failed" || attempts[1].FrameworkAttempt != 2 || attempts[1].Phase != "succeeded" || attempts[0].IdempotencyKey != attempts[1].IdempotencyKey {
		t.Fatalf("alpha retry identity=%+v", attempts)
	}
}

func TestParallelRestartCrashHelper(t *testing.T) {
	root := os.Getenv("ORCHIGRAM_ENGINE_CRASH_HELPER")
	if root == "" {
		t.Skip("subprocess helper")
	}
	ctx := context.Background()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	executor := &crashBoundaryExecutor{root: root, started: map[string]bool{}}
	adapter, err := Open(ctx, filepath.Join(root, "workflows.sqlite"), state, executor)
	if err != nil {
		t.Fatal(err)
	}
	plan := forkJoinPlan(2)
	plan.PlanHash = "parallel-restart"
	payload := store.StartPayload{RunUID: "run-parallel-restart", Input: json.RawMessage(`{}`)}
	if _, err := state.EnsureRun(ctx, payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := adapter.Start(ctx, payload.RunUID, plan, payload.Input); err != nil {
		t.Fatal(err)
	}
	select {}
}

func forkJoinPlan(maxParallel int) flow.ExecutionPlan {
	node := func(id, uses string) flow.PlanNode {
		return flow.PlanNode{ID: id, Name: id, Uses: uses, Timeout: "5s", RetryBackoff: "10ms"}
	}
	return flow.ExecutionPlan{
		FlowUID: "flow-fork-join", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion,
		PlanHash: fmt.Sprintf("fork-join-%d", maxParallel), MaxParallel: maxParallel,
		Nodes: []flow.PlanNode{node("source", "core.noop"), node("alpha", "fixture.block"), node("beta", "fixture.block"), node("gamma", "fixture.block"), node("join", "core.noop")},
		Edges: []flow.PlanEdge{
			{From: "source", To: "alpha"}, {From: "source", To: "beta"}, {From: "source", To: "gamma"},
			{From: "alpha", To: "join"}, {From: "beta", To: "join"}, {From: "gamma", To: "join"},
		},
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

type parallelGateExecutor struct {
	mu              sync.Mutex
	started         chan string
	release         chan struct{}
	active, maximum int
	calls           []string
}

type parallelFailureExecutor struct {
	mu              sync.Mutex
	active, maximum int
	calls           []string
	bothStarted     chan struct{}
	cancelActive    chan struct{}
	startOnce       sync.Once
	cancelOnce      sync.Once
}

type crashBoundaryExecutor struct {
	mu      sync.Mutex
	root    string
	started map[string]bool
}

func (e *crashBoundaryExecutor) Execute(_ context.Context, _ string, node flow.PlanNode, _ json.RawMessage, _ map[string]any, _ string) (json.RawMessage, error) {
	e.mu.Lock()
	e.started[node.ID] = true
	if e.started["alpha"] && e.started["beta"] {
		_ = os.WriteFile(filepath.Join(e.root, "ready"), []byte("alpha,beta"), 0o600) //nolint:gosec // Test-owned crash synchronization file.
	}
	e.mu.Unlock()
	select {}
}

type recordingExecutor struct {
	mu    sync.Mutex
	calls []string
}

type parallelRetryExecutor struct {
	mu              sync.Mutex
	active, maximum int
	calls           []string
}

func (e *parallelRetryExecutor) Execute(ctx context.Context, _ string, node flow.PlanNode, _ json.RawMessage, _ map[string]any, _ string) (json.RawMessage, error) {
	identity := TaskIdentityFromContext(ctx)
	e.mu.Lock()
	e.active++
	if e.active > e.maximum {
		e.maximum = e.active
	}
	e.calls = append(e.calls, fmt.Sprintf("%s/%d", node.ID, identity.Attempt))
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.active--
		e.mu.Unlock()
	}()
	if node.ID == "alpha" && identity.Attempt == 1 {
		return nil, errors.New("transient alpha failure")
	}
	select {
	case <-time.After(75 * time.Millisecond):
		return json.RawMessage(`{"ok":true}`), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *recordingExecutor) Execute(_ context.Context, _ string, node flow.PlanNode, _ json.RawMessage, _ map[string]any, _ string) (json.RawMessage, error) {
	e.mu.Lock()
	e.calls = append(e.calls, node.ID)
	e.mu.Unlock()
	return json.RawMessage(`{"ok":true}`), nil
}

func (e *parallelFailureExecutor) Execute(ctx context.Context, _ string, node flow.PlanNode, _ json.RawMessage, _ map[string]any, _ string) (json.RawMessage, error) {
	e.mu.Lock()
	e.active++
	if e.active > e.maximum {
		e.maximum = e.active
	}
	e.calls = append(e.calls, node.ID)
	if e.active == 2 {
		e.startOnce.Do(func() { close(e.bothStarted) })
	}
	e.mu.Unlock()
	select {
	case <-e.bothStarted:
	case <-ctx.Done():
	}
	if node.ID == "alpha" {
		e.mu.Lock()
		e.active--
		e.mu.Unlock()
		return nil, errors.New("alpha fixture failed")
	}
	select {
	case <-e.cancelActive:
	case <-ctx.Done():
	}
	e.mu.Lock()
	e.active--
	e.mu.Unlock()
	return nil, context.Canceled
}

func (e *parallelFailureExecutor) CancelRun(_ context.Context, _ string, _ string) error {
	e.cancelOnce.Do(func() { close(e.cancelActive) })
	return nil
}

func (e *parallelGateExecutor) Execute(ctx context.Context, _ string, node flow.PlanNode, _ json.RawMessage, _ map[string]any, _ string) (json.RawMessage, error) {
	e.mu.Lock()
	e.active++
	if e.active > e.maximum {
		e.maximum = e.active
	}
	e.calls = append(e.calls, node.ID)
	e.mu.Unlock()
	e.started <- node.ID
	select {
	case <-e.release:
	case <-ctx.Done():
		e.mu.Lock()
		e.active--
		e.mu.Unlock()
		return nil, ctx.Err()
	}
	e.mu.Lock()
	e.active--
	e.mu.Unlock()
	return json.RawMessage(`{"ok":true}`), nil
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

func waitForRunEventCount(ctx context.Context, t *testing.T, state *store.Store, runUID, eventType string, count int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		events, err := state.RunEventsAfter(ctx, runUID, 0, 1000)
		matched := 0
		for _, event := range events {
			if event.Type == eventType {
				matched++
			}
		}
		if err == nil && matched >= count {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run event %s count=%d want=%d err=%v events=%+v", eventType, matched, count, err, events)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

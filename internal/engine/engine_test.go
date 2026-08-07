package engine

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
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

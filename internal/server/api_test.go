package server

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/store"
)

type missingWorkflowEngine struct{}

func (missingWorkflowEngine) Start(context.Context, string, flow.ExecutionPlan, json.RawMessage) error {
	return nil
}
func (missingWorkflowEngine) Signal(context.Context, string, string, engine.ApprovalSignal) error {
	return nil
}
func (missingWorkflowEngine) Cancel(context.Context, string, string) error { return store.ErrNotFound }
func (missingWorkflowEngine) Reconcile(context.Context) error              { return nil }
func (missingWorkflowEngine) Describe(context.Context, string) (store.Run, error) {
	return store.Run{}, store.ErrNotFound
}
func (missingWorkflowEngine) Close() error { return nil }

func TestCancelDoesNotAcknowledgeMissingWorkflowWhileStartIsDurable(t *testing.T) {
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	receipt, err := state.AcceptTrigger(ctx, "manual:default:demo", "cancel-api-race", "demo", "default", json.RawMessage(`{}`), true)
	if err != nil {
		t.Fatal(err)
	}
	command, err := state.ClaimStart(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	plan := flow.ExecutionPlan{FlowUID: "flow-cancel-api", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "plan-cancel-api"}
	if _, err := state.EnsureRun(ctx, command.Payload, plan); err != nil {
		t.Fatal(err)
	}
	api := NewAPI(state, flow.NewCompiler(nil), nil, missingWorkflowEngine{}, nil, nil, t.TempDir())
	if _, err := api.Cancel(ctx, &controlv1alpha1.CancelRunRequest{RunUid: receipt.RunUID, Reason: "cancel before start"}); err != nil {
		t.Fatal(err)
	}
	pending, err := state.UndeliveredRunCancellations(ctx, 100)
	if err != nil || len(pending) != 1 || pending[0].RunUID != receipt.RunUID {
		t.Fatalf("pending cancellations=%+v err=%v", pending, err)
	}
	if run, err := state.GetRun(ctx, receipt.RunUID); err != nil || run.Phase != "cancelled" {
		t.Fatalf("run=%+v err=%v", run, err)
	}
	if delivered, err := state.MarkRunCancellationDeliveredIfStartImpossible(ctx, receipt.RunUID); err != nil {
		t.Fatal(err)
	} else if delivered {
		t.Fatal("inflight start allowed cancellation acknowledgement")
	} else if pending, err = state.UndeliveredRunCancellations(ctx, 100); err != nil || len(pending) != 1 {
		t.Fatalf("conditional acknowledgement ignored inflight start: pending=%+v err=%v", pending, err)
	}
}

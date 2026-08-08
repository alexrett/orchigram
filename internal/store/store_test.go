package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestTerminalRunTransitionsAreImmutable(t *testing.T) {
	t.Parallel()
	for _, terminal := range []string{"succeeded", "failed", "rejected", "cancelled"} {
		t.Run(terminal, func(t *testing.T) {
			ctx := context.Background()
			s := openTestStore(t)
			plan := flow.ExecutionPlan{FlowUID: "flow-" + terminal, FlowGeneration: 1, PlanHash: "plan-" + terminal, InterpreterVersion: flow.InterpreterVersion}
			runUID := "run-" + terminal
			if _, err := s.EnsureRun(ctx, StartPayload{RunUID: runUID, Input: json.RawMessage(`{}`)}, plan); err != nil {
				t.Fatal(err)
			}
			if err := s.AppendRunEvent(ctx, runUID, "", "run."+terminal, terminal, 0, nil); err != nil {
				t.Fatal(err)
			}
			before, err := s.RunEventsAfter(ctx, runUID, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			for _, late := range []struct{ event, phase string }{{"node.completed", "running"}, {"run.failed", "failed"}, {"approval.waiting", "waiting"}, {"run.succeeded", "succeeded"}} {
				if err := s.AppendRunEvent(ctx, runUID, "node", late.event, late.phase, 1, nil); err != nil {
					t.Fatal(err)
				}
			}
			run, err := s.GetRun(ctx, runUID)
			if err != nil || run.Phase != terminal {
				t.Fatalf("run=%+v err=%v", run, err)
			}
			after, err := s.RunEventsAfter(ctx, runUID, 0, 100)
			if err != nil || len(after) != len(before) {
				t.Fatalf("late events were appended: before=%d after=%d err=%v", len(before), len(after), err)
			}
		})
	}
}

func TestRunCancellationIntentAndDeliveryAreDurableAndIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	plan := flow.ExecutionPlan{FlowUID: "flow-cancel", FlowGeneration: 1, PlanHash: "plan-cancel", InterpreterVersion: flow.InterpreterVersion}
	if _, err := s.EnsureRun(ctx, StartPayload{RunUID: "run-cancel", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestRunCancellation(ctx, "run-cancel", "first reason"); err != nil {
		t.Fatal(err)
	}
	if err := s.RequestRunCancellation(ctx, "run-cancel", "later reason"); err != nil {
		t.Fatal(err)
	}
	pending, err := s.UndeliveredRunCancellations(ctx, 100)
	if err != nil || len(pending) != 1 || pending[0].Reason != "first reason" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	events, err := s.RunEventsAfter(ctx, "run-cancel", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	cancellations := 0
	for _, event := range events {
		if event.Type == "run.cancelled" {
			cancellations++
		}
	}
	if cancellations != 1 {
		t.Fatalf("cancellation events=%d", cancellations)
	}
	if err := s.MarkRunCancellationDelivered(ctx, "run-cancel"); err != nil {
		t.Fatal(err)
	}
	pending, err = s.UndeliveredRunCancellations(ctx, 100)
	if err != nil || len(pending) != 0 {
		t.Fatalf("delivered cancellation remained pending: %+v err=%v", pending, err)
	}
}

func testFlowDocument(t *testing.T) resource.Document {
	t.Helper()
	doc, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: demo}
spec:
  nodes:
    - {id: begin, uses: core.noop}
`))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestApplyCASGenerationAndEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	created, err := s.Apply(ctx, testFlowDocument(t), ApplyOptions{RequestID: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Metadata.ResourceVersion != 1 || created.Metadata.Generation != 1 || created.Metadata.UID == "" {
		t.Fatalf("metadata: %+v", created.Metadata)
	}
	_, err = s.Apply(ctx, testFlowDocument(t), ApplyOptions{ExpectedResourceVersion: 99})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Current != 1 {
		t.Fatalf("expected conflict, got %v", err)
	}
	updatedDoc := testFlowDocument(t)
	updatedDoc.Metadata.ResourceVersion = 1
	updated, err := s.Apply(ctx, updatedDoc, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Metadata.Generation != 1 {
		t.Fatalf("no-op apply incremented generation: %d", updated.Metadata.Generation)
	}
	events, err := s.EventsAfter(ctx, "Flow", "default", 0, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
}

func TestTriggerReceiptAndOutboxAreDeduplicated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	first, err := s.AcceptTrigger(ctx, "manual", "key-1", "demo", "default", json.RawMessage(`{"x":1}`), true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcceptTrigger(ctx, "manual", "key-1", "demo", "default", json.RawMessage(`{"x":2}`), true)
	if err != nil {
		t.Fatal(err)
	}
	if first.UID != second.UID || first.RunUID != second.RunUID {
		t.Fatalf("duplicate created new identity: %+v %+v", first, second)
	}
	command, err := s.ClaimStart(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if command.Payload.RunUID != first.RunUID || command.Attempts != 1 {
		t.Fatalf("command: %+v", command)
	}
	if _, err := s.ClaimStart(ctx, time.Hour); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second command: %v", err)
	}
}

func TestTriggerApplyPersistsActivationAndScopesProviderCursorToGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	firstActivation := time.Date(2026, 8, 8, 10, 0, 0, 123, time.UTC)
	s.now = func() time.Time { return firstActivation }
	document, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: ready-issues}
spec:
  flow: issue-to-pr
  provider: {plugin: github, config: {repository: first}}
`))
	if err != nil {
		t.Fatal(err)
	}
	created, err := s.Apply(ctx, document, ApplyOptions{RequestID: "create-trigger"})
	if err != nil {
		t.Fatal(err)
	}
	state, err := s.TriggerState(ctx, created.Metadata.UID)
	if err != nil || state.Generation != 1 || !state.CursorAt.Equal(firstActivation) {
		t.Fatalf("initial trigger state=%+v err=%v", state, err)
	}
	if _, err := s.AcceptProviderTrigger(ctx, created.Metadata.UID, created.Metadata.Generation, "event-10", "issue-to-pr", "default", json.RawMessage(`{"issue":10}`), "10"); err != nil {
		t.Fatal(err)
	}

	secondActivation := firstActivation.Add(time.Hour)
	s.now = func() time.Time { return secondActivation }
	updatedDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata:
  name: ready-issues
  resourceVersion: 1
spec:
  flow: issue-to-pr
  provider: {plugin: github, config: {repository: second}}
`))
	if err != nil {
		t.Fatal(err)
	}
	updated, err := s.Apply(ctx, updatedDocument, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	state, err = s.TriggerState(ctx, created.Metadata.UID)
	if err != nil || updated.Metadata.Generation != 2 || state.Generation != 2 || !state.CursorAt.Equal(secondActivation) {
		t.Fatalf("updated trigger=%+v state=%+v err=%v", updated.Metadata, state, err)
	}
	cursor, eventID, err := s.ProviderCursor(ctx, created.Metadata.UID)
	if err != nil || cursor != "" || eventID != "" {
		t.Fatalf("generation retained provider cursor=%q event=%q err=%v", cursor, eventID, err)
	}
	if _, err := s.EnsureTriggerState(ctx, created.Metadata.UID, created.Metadata.Generation, true, secondActivation.Add(time.Minute)); err == nil {
		t.Fatal("superseded controller watch rolled Trigger state backward")
	} else {
		var stale *StaleTriggerGenerationError
		if !errors.As(err, &stale) || stale.Expected != 1 || stale.Current != 2 {
			t.Fatalf("stale controller state error=%v", err)
		}
	}

	if _, err := s.AcceptProviderTrigger(ctx, created.Metadata.UID, created.Metadata.Generation, "stale-event", "issue-to-pr", "default", json.RawMessage(`{"issue":19}`), "19"); err == nil {
		t.Fatal("superseded provider watch mutated the new Trigger generation")
	} else {
		var stale *StaleTriggerGenerationError
		if !errors.As(err, &stale) || stale.Expected != 1 || stale.Current != 2 {
			t.Fatalf("stale provider error=%v", err)
		}
	}
	if _, err := s.AcceptProviderTrigger(ctx, created.Metadata.UID, updated.Metadata.Generation, "event-20", "issue-to-pr", "default", json.RawMessage(`{"issue":20}`), "20"); err != nil {
		t.Fatal(err)
	}
	noOp, err := s.Apply(ctx, updated, ApplyOptions{})
	if err != nil || noOp.Metadata.Generation != 2 {
		t.Fatalf("no-op trigger apply=%+v err=%v", noOp.Metadata, err)
	}
	cursor, eventID, err = s.ProviderCursor(ctx, created.Metadata.UID)
	if err != nil || cursor != "20" || eventID != "event-20" {
		t.Fatalf("no-op apply reset provider cursor=%q event=%q err=%v", cursor, eventID, err)
	}
}

func TestPluginVersionsActivateAndRollbackWithoutOverwrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	for _, version := range []string{"0.1.0", "0.2.0"} {
		if err := s.PutPlugin(ctx, PluginRecord{Name: "exec", Version: version, Digest: "digest-" + version, ManifestJSON: json.RawMessage(`{"name":"exec"}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutPlugin(ctx, PluginRecord{Name: "exec", Version: "0.1.0", Digest: "different", ManifestJSON: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("immutable plugin version was overwritten")
	}
	if err := s.ActivatePlugin(ctx, "exec", "0.2.0"); err != nil {
		t.Fatal(err)
	}
	active, err := s.Plugin(ctx, "exec", "")
	if err != nil || active.Version != "0.2.0" || !active.Active {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if err := s.ActivatePlugin(ctx, "exec", "0.1.0"); err != nil {
		t.Fatal(err)
	}
	rolledBack, err := s.Plugin(ctx, "exec", "")
	if err != nil || rolledBack.Version != "0.1.0" {
		t.Fatalf("rollback=%+v err=%v", rolledBack, err)
	}
}

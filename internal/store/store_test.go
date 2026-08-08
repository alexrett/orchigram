package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
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

func TestOpenRejectsAStateDatabaseFromANewerSchema(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "future.sqlite")
	state, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.ExecContext(context.Background(), `INSERT INTO schema_migrations(version,applied_at) VALUES(999,'2030-01-01T00:00:00Z')`); err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = Open(path)
	if err == nil || !strings.Contains(err.Error(), "database schema version 999 is newer than supported version") {
		t.Fatalf("future schema error=%v", err)
	}
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

func TestNodeAttemptsPreservePhysicalRetriesAndEvidence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	plan := flow.ExecutionPlan{FlowUID: "flow-attempts", FlowGeneration: 1, PlanHash: "plan-attempts", InterpreterVersion: flow.InterpreterVersion}
	if _, err := s.EnsureRun(ctx, StartPayload{RunUID: "run-attempts", Input: json.RawMessage(`{"request":true}`)}, plan); err != nil {
		t.Fatal(err)
	}

	const stableKey = "run/run-attempts/node/effect/iteration/0/operation/execute"
	first, created, err := s.BeginNodeAttempt(ctx, "run-attempts", "effect", 0, 1, stableKey, json.RawMessage(`{"request":true}`))
	if err != nil || !created || first.Attempt != 1 {
		t.Fatalf("first attempt=%+v created=%v err=%v", first, created, err)
	}
	eventTime := time.Now()
	if err := s.AppendPluginEvent(ctx, "run-attempts", "effect", 0, 1, 1, "log", json.RawMessage(`{"message":"first"}`), eventTime); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendPluginEvent(ctx, "run-attempts", "effect", 0, 1, 1, "log", json.RawMessage(`{"message":"first"}`), eventTime); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendPluginEvent(ctx, "run-attempts", "effect", 0, 1, 1, "log", json.RawMessage(`{"message":"conflict"}`), eventTime); err == nil {
		t.Fatal("conflicting duplicate plugin event was accepted")
	}
	if _, err := s.CompleteNodeAttempt(ctx, "run-attempts", "effect", 0, 1, "failed", nil, "transient", "plugin-failed"); err != nil {
		t.Fatal(err)
	}
	replayed, created, err := s.BeginNodeAttempt(ctx, "run-attempts", "effect", 0, 1, stableKey, json.RawMessage(`{"ignored":true}`))
	if err != nil || created || string(replayed.Input) != `{"request":true}` || replayed.Phase != "failed" {
		t.Fatalf("replayed attempt=%+v created=%v err=%v", replayed, created, err)
	}
	second, created, err := s.BeginNodeAttempt(ctx, "run-attempts", "effect", 0, 2, stableKey, json.RawMessage(`{"request":true}`))
	if err != nil || !created || second.IdempotencyKey != stableKey {
		t.Fatalf("second attempt=%+v created=%v err=%v", second, created, err)
	}
	if _, err := s.CompleteNodeAttempt(ctx, "run-attempts", "effect", 0, 2, "succeeded", json.RawMessage(`{"ok":true}`), "", "exited-0"); err != nil {
		t.Fatal(err)
	}

	attempts, err := s.ListNodeAttempts(ctx, "run-attempts", "effect", 10)
	if err != nil || len(attempts) != 2 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	if attempts[0].FrameworkAttempt != 1 || attempts[0].Phase != "failed" || attempts[0].ExitOutcome != "plugin-failed" || attempts[1].FrameworkAttempt != 2 || attempts[1].Phase != "succeeded" || attempts[1].ExitOutcome != "exited-0" {
		t.Fatalf("attempt outcomes=%+v", attempts)
	}
	if attempts[0].IdempotencyKey != attempts[1].IdempotencyKey {
		t.Fatalf("external idempotency key changed across retries: %+v", attempts)
	}

	artifact, err := s.PutArtifact(ctx, ArtifactRecord{
		RunUID: "run-attempts", NodeID: "effect", LogicalIteration: 0, Attempt: 2,
		Name: "raw.log", MediaType: "text/plain", RelativePath: "artifacts/run-attempts/effect/iteration-0/attempt-2/raw.log",
		SizeBytes: 7, SHA256: "0000000000000000000000000000000000000000000000000000000000000000",
	})
	if err != nil || artifact.UID == "" {
		t.Fatalf("artifact=%+v err=%v", artifact, err)
	}
	if _, err := s.PutArtifact(ctx, ArtifactRecord{
		RunUID: "run-attempts", NodeID: "effect", LogicalIteration: 0, Attempt: 2,
		Name: "raw.log", MediaType: "text/plain", RelativePath: "artifacts/run-attempts/effect/iteration-0/attempt-2/raw.log",
		SizeBytes: 8, SHA256: "1111111111111111111111111111111111111111111111111111111111111111",
	}); err == nil {
		t.Fatal("completed artifact metadata was overwritten")
	}
	artifacts, err := s.ListArtifacts(ctx, "run-attempts", 10)
	if err != nil || len(artifacts) != 1 || artifacts[0].RelativePath == "" {
		t.Fatalf("artifacts=%+v err=%v", artifacts, err)
	}

	events, err := s.RunEventsAfter(ctx, "run-attempts", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	started, pluginLogs := 0, 0
	for _, event := range events {
		switch event.Type {
		case "node.started":
			started++
		case "plugin.log":
			pluginLogs++
		}
	}
	if started != 2 || pluginLogs != 1 {
		t.Fatalf("event evidence started=%d plugin.log=%d events=%+v", started, pluginLogs, events)
	}
}

func TestAmbiguousFrameworkRedeliveryAllocatesNewPhysicalAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	plan := flow.ExecutionPlan{FlowUID: "flow-redelivery", FlowGeneration: 1, PlanHash: "plan-redelivery", InterpreterVersion: flow.InterpreterVersion}
	if _, err := s.EnsureRun(ctx, StartPayload{RunUID: "run-redelivery", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	first, created, err := s.BeginNodeAttempt(ctx, "run-redelivery", "effect", 0, 1, "stable-key", json.RawMessage(`{}`))
	if err != nil || !created || first.Attempt != 1 {
		t.Fatalf("first=%+v created=%v err=%v", first, created, err)
	}
	second, created, err := s.BeginNodeAttempt(ctx, "run-redelivery", "effect", 0, 1, "stable-key", json.RawMessage(`{}`))
	if err != nil || !created || second.Attempt != 2 || second.FrameworkAttempt != 1 {
		t.Fatalf("redelivery=%+v created=%v err=%v", second, created, err)
	}
	lost, err := s.NodeAttempt(ctx, "run-redelivery", "effect", 0, 1)
	if err != nil || lost.Phase != "failed" || lost.ExitOutcome != "delivery-lost" || lost.CompletedAt.IsZero() {
		t.Fatalf("lost delivery=%+v err=%v", lost, err)
	}
	third, created, err := s.BeginNodeAttempt(ctx, "run-redelivery", "effect", 0, 1, "stable-key", json.RawMessage(`{}`))
	if err != nil || !created || third.Attempt != 3 || third.FrameworkAttempt != 1 {
		t.Fatalf("second redelivery=%+v created=%v err=%v", third, created, err)
	}
	secondLost, err := s.NodeAttempt(ctx, "run-redelivery", "effect", 0, 2)
	if err != nil || secondLost.ExitOutcome != "delivery-lost" {
		t.Fatalf("second lost delivery=%+v err=%v", secondLost, err)
	}
	if _, err := s.CompleteNodeAttempt(ctx, "run-redelivery", "effect", 0, 3, "succeeded", json.RawMessage(`{"ok":true}`), "", "exited"); err != nil {
		t.Fatal(err)
	}
	replayed, created, err := s.BeginNodeAttempt(ctx, "run-redelivery", "effect", 0, 1, "stable-key", json.RawMessage(`{}`))
	if err != nil || created || replayed.Attempt != 3 || replayed.Phase != "succeeded" {
		t.Fatalf("completed redelivery replay=%+v created=%v err=%v", replayed, created, err)
	}
	if first.IdempotencyKey != second.IdempotencyKey || second.IdempotencyKey != third.IdempotencyKey {
		t.Fatalf("redelivery idempotency changed: first=%+v second=%+v third=%+v", first, second, third)
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
	first, err := s.AcceptTrigger(ctx, "manual", 0, "key-1", "demo", "default", json.RawMessage(`{"x":1}`), true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcceptTrigger(ctx, "manual", 0, "key-1", "demo", "default", json.RawMessage(`{"x":2}`), true)
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

func TestAcceptedPlanAndOutboxAreCommittedTogether(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	flowDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: demo}
spec:
  nodes: [{id: accepted, uses: core.noop}]
`))
	if err != nil {
		t.Fatal(err)
	}
	storedFlow, err := s.Apply(ctx, flowDocument, ApplyOptions{RequestID: "accepted-plan-flow"})
	if err != nil {
		t.Fatal(err)
	}
	plan := flow.ExecutionPlan{
		APIVersion: resource.APIVersion, FlowUID: storedFlow.Metadata.UID, FlowGeneration: storedFlow.Metadata.Generation,
		InterpreterVersion: flow.InterpreterVersion, Timeout: "1h0m0s", MaxParallel: 1,
		Nodes:    []flow.PlanNode{{ID: "accepted", Name: "accepted", Uses: "core.noop", Timeout: "1h0m0s", RetryBackoff: "1s"}},
		PlanHash: "accepted-plan-hash",
	}
	receipt, err := s.AcceptTriggerWithPlan(ctx, "manual", 0, "accepted-occurrence", "demo", "default", json.RawMessage(`{"x":1}`), true, plan)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := s.GetPlan(ctx, plan.PlanHash)
	if err != nil || stored.FlowUID != plan.FlowUID {
		t.Fatalf("stored plan=%+v err=%v", stored, err)
	}
	command, err := s.ClaimStart(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if command.Payload.RunUID != receipt.RunUID || command.Payload.PlanHash != plan.PlanHash {
		t.Fatalf("command=%+v receipt=%+v", command, receipt)
	}
}

func TestAcceptanceRejectsAFlowChangedAfterCompilation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	storedFlow := applyAcceptanceFlow(t, s, "stale-flow", "first")
	plan := acceptancePlan(storedFlow, "stale-plan")
	updatedDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: stale-flow}
spec:
  nodes: [{id: done, uses: core.noop, with: {result: second}}]
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, updatedDocument, ApplyOptions{ExpectedResourceVersion: storedFlow.Metadata.ResourceVersion, RequestID: "change-flow"}); err != nil {
		t.Fatal(err)
	}
	_, err = s.AcceptTriggerWithPlan(ctx, "manual:default:stale-flow", 0, "stale-occurrence", "stale-flow", resource.DefaultNamespace, json.RawMessage(`{}`), true, plan)
	var stale *StaleFlowPlanError
	if !errors.As(err, &stale) {
		t.Fatalf("stale plan acceptance error=%v", err)
	}
	if _, err := s.ReceiptByOccurrence(ctx, "manual:default:stale-flow", "stale-occurrence"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale plan wrote a receipt: %v", err)
	}
	if _, err := s.GetPlan(ctx, plan.PlanHash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale plan was persisted: %v", err)
	}
	if _, err := s.ClaimStart(ctx, time.Hour); !errors.Is(err, ErrNotFound) {
		t.Fatalf("stale plan wrote an outbox command: %v", err)
	}

	deletedStore := openTestStore(t)
	deletedFlow := applyAcceptanceFlow(t, deletedStore, "deleted-flow", "first")
	deletedPlan := acceptancePlan(deletedFlow, "deleted-plan")
	if err := deletedStore.Delete(ctx, "Flow", resource.DefaultNamespace, "deleted-flow", deletedFlow.Metadata.ResourceVersion, "delete-flow"); err != nil {
		t.Fatal(err)
	}
	if _, err := deletedStore.AcceptTriggerWithPlan(ctx, "manual:default:deleted-flow", 0, "deleted-occurrence", "deleted-flow", resource.DefaultNamespace, json.RawMessage(`{}`), true, deletedPlan); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted Flow acceptance error=%v", err)
	}
	if _, err := deletedStore.ReceiptByOccurrence(ctx, "manual:default:deleted-flow", "deleted-occurrence"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted Flow wrote a receipt: %v", err)
	}
}

func TestAcceptanceAndFlowMutationHaveOneTransactionOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	storedFlow := applyAcceptanceFlow(t, s, "linearized-flow", "first")
	trigger := applyStoreTriggerForFlow(t, s, "linearized-trigger", "linearized-flow")
	plan := acceptancePlan(storedFlow, "linearized-plan")
	validated, releaseAcceptance := make(chan struct{}), make(chan struct{})
	acceptDone := make(chan error, 1)
	go func() {
		_, err := s.acceptTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "linearized-occurrence", "linearized-flow", resource.DefaultNamespace, json.RawMessage(`{}`), true, "", "", func() {
			close(validated)
			<-releaseAcceptance
		}, &plan)
		acceptDone <- err
	}()
	<-validated

	updatedDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: linearized-flow}
spec:
  nodes: [{id: done, uses: core.noop, with: {result: second}}]
`))
	if err != nil {
		t.Fatal(err)
	}
	waitCount := s.db.Stats().WaitCount
	mutationDone := make(chan error, 1)
	go func() {
		_, applyErr := s.Apply(ctx, updatedDocument, ApplyOptions{ExpectedResourceVersion: storedFlow.Metadata.ResourceVersion, RequestID: "linearized-update"})
		mutationDone <- applyErr
	}()
	waitForDatabaseWaiter(t, s, waitCount, mutationDone)
	close(releaseAcceptance)
	if err := <-acceptDone; err != nil {
		t.Fatalf("acceptance-first receipt: %v", err)
	}
	if err := <-mutationDone; err != nil {
		t.Fatalf("acceptance-first Flow mutation: %v", err)
	}
	receipt, err := s.ReceiptByOccurrence(ctx, trigger.Metadata.UID, "linearized-occurrence")
	if err != nil || receipt.RunUID == "" {
		t.Fatalf("accepted receipt=%+v err=%v", receipt, err)
	}
}

func TestResourceTriggerAcceptancePinsGenerationAndFlowReference(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	storedFlow := applyAcceptanceFlow(t, s, "trigger-target", "first")
	otherFlow := applyAcceptanceFlow(t, s, "other-target", "other")
	trigger := applyStoreTriggerForFlow(t, s, "strict-trigger", "trigger-target")
	plan := acceptancePlan(otherFlow, "wrong-flow-plan")
	_, err := s.AcceptTriggerWithPlan(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "wrong-flow", "other-target", resource.DefaultNamespace, json.RawMessage(`{}`), true, plan)
	var changed *TriggerReferenceChangedError
	if !errors.As(err, &changed) {
		t.Fatalf("changed Trigger reference error=%v", err)
	}
	updatedTriggerDocument, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: strict-trigger}
spec:
  flow: trigger-target
  provider: {plugin: github, config: {repository: changed}}
`))
	if err != nil {
		t.Fatal(err)
	}
	updatedTrigger, err := s.Apply(ctx, updatedTriggerDocument, ApplyOptions{ExpectedResourceVersion: trigger.Metadata.ResourceVersion, RequestID: "update-trigger"})
	if err != nil {
		t.Fatal(err)
	}
	plan = acceptancePlan(storedFlow, "stale-trigger-plan")
	_, err = s.AcceptTriggerWithPlan(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "stale-trigger", "trigger-target", resource.DefaultNamespace, json.RawMessage(`{}`), true, plan)
	var staleTrigger *StaleTriggerGenerationError
	if !errors.As(err, &staleTrigger) {
		t.Fatalf("stale Trigger generation error=%v", err)
	}
	trigger = updatedTrigger
	if err := s.SetTriggerEnabled(ctx, trigger.Metadata.UID, false); err != nil {
		t.Fatal(err)
	}
	plan = acceptancePlan(storedFlow, "disabled-plan")
	if _, err := s.AcceptTriggerWithPlan(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "disabled", "trigger-target", resource.DefaultNamespace, json.RawMessage(`{}`), true, plan); !errors.Is(err, ErrTriggerDisabled) {
		t.Fatalf("disabled Trigger acceptance error=%v", err)
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

func TestProviderAcceptanceRequiresExistingEnabledTrigger(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	trigger := applyStoreTrigger(t, s, "provider-acceptance")
	if _, err := s.AcceptProviderTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "event-1", "target", "default", json.RawMessage(`{"issue":1}`), "1"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetTriggerEnabled(ctx, trigger.Metadata.UID, false); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptProviderTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "event-2", "target", "default", json.RawMessage(`{"issue":2}`), "2"); !errors.Is(err, ErrTriggerDisabled) {
		t.Fatalf("disabled Trigger acceptance error=%v", err)
	}
	if _, err := s.AcceptProviderTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, "event-1", "target", "default", json.RawMessage(`{"issue":1}`), "2"); !errors.Is(err, ErrTriggerDisabled) {
		t.Fatalf("disabled Trigger duplicate error=%v", err)
	}
	cursor, eventID, err := s.ProviderCursor(ctx, trigger.Metadata.UID)
	if err != nil || cursor != "1" || eventID != "event-1" {
		t.Fatalf("disabled Trigger advanced cursor=%q event=%q err=%v", cursor, eventID, err)
	}
	receipts, err := s.TriggerReceipts(ctx, trigger.Metadata.UID, 10)
	if err != nil || len(receipts) != 1 || receipts[0].OccurrenceID != "event-1" {
		t.Fatalf("disabled Trigger persisted receipts=%+v err=%v", receipts, err)
	}

	deleted := applyStoreTrigger(t, s, "provider-deletion")
	if _, err := s.AcceptProviderTrigger(ctx, deleted.Metadata.UID, deleted.Metadata.Generation, "event-before-delete", "target", "default", json.RawMessage(`{"issue":3}`), "3"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "Trigger", deleted.Metadata.Namespace, deleted.Metadata.Name, deleted.Metadata.ResourceVersion, "delete-trigger"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AcceptProviderTrigger(ctx, deleted.Metadata.UID, deleted.Metadata.Generation, "event-after-delete", "target", "default", json.RawMessage(`{"issue":4}`), "4"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted Trigger acceptance error=%v", err)
	}
	if _, err := s.TriggerState(ctx, deleted.Metadata.UID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted Trigger retained state: %v", err)
	}
	if cursor, eventID, err := s.ProviderCursor(ctx, deleted.Metadata.UID); err != nil || cursor != "" || eventID != "" {
		t.Fatalf("deleted Trigger retained provider cursor=%q event=%q err=%v", cursor, eventID, err)
	}
	receipts, err = s.TriggerReceipts(ctx, deleted.Metadata.UID, 10)
	if err != nil || len(receipts) != 1 || receipts[0].OccurrenceID != "event-before-delete" {
		t.Fatalf("deleted Trigger persisted receipts=%+v err=%v", receipts, err)
	}
}

func TestProviderAcceptanceLinearizesWithTriggerMutations(t *testing.T) {
	t.Parallel()
	for _, mutation := range []string{"disable", "delete"} {
		mutation := mutation
		t.Run(mutation+"/acceptance-first", func(t *testing.T) {
			s := openTestStore(t)
			trigger := applyStoreTrigger(t, s, "race-acceptance-first-"+mutation)
			validated, releaseAcceptance := make(chan struct{}), make(chan struct{})
			acceptDone := make(chan error, 1)
			go func() {
				_, err := s.acceptProviderTrigger(context.Background(), trigger.Metadata.UID, trigger.Metadata.Generation, "race-event", "target", "default", json.RawMessage(`{"issue":1}`), "1", func() {
					close(validated)
					<-releaseAcceptance
				})
				acceptDone <- err
			}()
			<-validated

			waitCount := s.db.Stats().WaitCount
			mutationDone := make(chan error, 1)
			go func() { mutationDone <- mutateTriggerForTest(s, trigger, mutation, nil) }()
			waitForDatabaseWaiter(t, s, waitCount, mutationDone)
			close(releaseAcceptance)
			if err := <-acceptDone; err != nil {
				t.Fatalf("acceptance-first event: %v", err)
			}
			if err := <-mutationDone; err != nil {
				t.Fatalf("acceptance-first %s: %v", mutation, err)
			}
			assertProviderRaceState(t, s, trigger.Metadata.UID, mutation, true)
		})

		t.Run(mutation+"/mutation-first", func(t *testing.T) {
			s := openTestStore(t)
			trigger := applyStoreTrigger(t, s, "race-mutation-first-"+mutation)
			mutated, releaseMutation := make(chan struct{}), make(chan struct{})
			mutationDone := make(chan error, 1)
			go func() {
				mutationDone <- mutateTriggerForTest(s, trigger, mutation, func() {
					close(mutated)
					<-releaseMutation
				})
			}()
			<-mutated

			_, err := s.AcceptProviderTrigger(context.Background(), trigger.Metadata.UID, trigger.Metadata.Generation, "race-event", "target", "default", json.RawMessage(`{"issue":1}`), "1")
			if mutation == "disable" && !errors.Is(err, ErrTriggerDisabled) {
				t.Fatalf("mutation-first disabled Trigger acceptance error=%v", err)
			}
			if mutation == "delete" && !errors.Is(err, ErrNotFound) {
				t.Fatalf("mutation-first deleted Trigger acceptance error=%v", err)
			}
			close(releaseMutation)
			if err := <-mutationDone; err != nil {
				t.Fatalf("mutation-first %s: %v", mutation, err)
			}
			assertProviderRaceState(t, s, trigger.Metadata.UID, mutation, false)
		})
	}
}

func mutateTriggerForTest(s *Store, trigger resource.Document, mutation string, afterCommit func()) error {
	if mutation == "disable" {
		return s.setTriggerEnabled(context.Background(), trigger.Metadata.UID, false, afterCommit)
	}
	return s.delete(context.Background(), "Trigger", trigger.Metadata.Namespace, trigger.Metadata.Name, trigger.Metadata.ResourceVersion, "race-delete", afterCommit)
}

func waitForDatabaseWaiter(t *testing.T, s *Store, previous int64, operationDone <-chan error) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if s.db.Stats().WaitCount > previous {
			return
		}
		select {
		case err := <-operationDone:
			t.Fatalf("Trigger mutation crossed the uncommitted provider acceptance: %v", err)
		case <-deadline.C:
			t.Fatal("Trigger mutation did not contend on the provider acceptance transaction")
		case <-ticker.C:
		}
	}
}

func assertProviderRaceState(t *testing.T, s *Store, triggerUID, mutation string, accepted bool) {
	t.Helper()
	receipts, err := s.TriggerReceipts(context.Background(), triggerUID, 10)
	if err != nil || len(receipts) != boolIntTest(accepted) {
		t.Fatalf("%s accepted=%t receipts=%+v err=%v", mutation, accepted, receipts, err)
	}
	cursor, eventID, err := s.ProviderCursor(context.Background(), triggerUID)
	if err != nil {
		t.Fatal(err)
	}
	if mutation == "delete" || !accepted {
		if cursor != "" || eventID != "" {
			t.Fatalf("%s accepted=%t cursor=%q event=%q", mutation, accepted, cursor, eventID)
		}
		return
	}
	if cursor != "1" || eventID != "race-event" {
		t.Fatalf("%s accepted=%t cursor=%q event=%q", mutation, accepted, cursor, eventID)
	}
}

func boolIntTest(value bool) int {
	if value {
		return 1
	}
	return 0
}

func applyStoreTrigger(t *testing.T, s *Store, name string) resource.Document {
	return applyStoreTriggerForFlow(t, s, name, "target")
}

func applyStoreTriggerForFlow(t *testing.T, s *Store, name, flowName string) resource.Document {
	t.Helper()
	document, err := resource.DecodeStrict([]byte(fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: %s}
spec:
  flow: %s
  provider: {plugin: github, config: {repository: fixture}}
`, name, flowName)))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := s.Apply(context.Background(), document, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return applied
}

func applyAcceptanceFlow(t *testing.T, s *Store, name, result string) resource.Document {
	t.Helper()
	document, err := resource.DecodeStrict([]byte(fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: %s}
spec:
  nodes: [{id: done, uses: core.noop, with: {result: %s}}]
`, name, result)))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := s.Apply(context.Background(), document, ApplyOptions{RequestID: "acceptance-flow-" + name})
	if err != nil {
		t.Fatal(err)
	}
	return applied
}

func acceptancePlan(document resource.Document, hash string) flow.ExecutionPlan {
	return flow.ExecutionPlan{
		APIVersion: resource.APIVersion, FlowUID: document.Metadata.UID, FlowGeneration: document.Metadata.Generation,
		InterpreterVersion: flow.InterpreterVersion, Timeout: "1h0m0s", MaxParallel: 1,
		Nodes:    []flow.PlanNode{{ID: "done", Name: "done", Uses: "core.noop", Timeout: "1h0m0s", RetryBackoff: "1s"}},
		PlanHash: hash,
	}
}

func TestPluginVersionsActivateAndRollbackWithoutOverwrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	for _, version := range []string{"0.1.0", "0.2.0"} {
		if err := s.PutPlugin(ctx, PluginRecord{
			Name: "exec", Version: version, Digest: "digest-" + version, ManifestJSON: json.RawMessage(`{"name":"exec"}`),
			ContractJSON: json.RawMessage(`{"actions":[]}`), ContractDigest: "contract-" + version,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.PutPlugin(ctx, PluginRecord{Name: "exec", Version: "0.1.0", Digest: "different", ManifestJSON: json.RawMessage(`{}`), ContractJSON: json.RawMessage(`{"actions":[]}`), ContractDigest: "contract-0.1.0"}); err == nil {
		t.Fatal("immutable plugin version was overwritten")
	}
	if err := s.PutPlugin(ctx, PluginRecord{
		Name: "exec", Version: "0.1.0", Digest: "digest-0.1.0", ManifestJSON: json.RawMessage(`{"name":"exec"}`),
		ContractJSON: json.RawMessage(`{"actions":[{"action":"changed"}]}`), ContractDigest: "changed-contract",
	}); err == nil || !strings.Contains(err.Error(), "action contract changed") {
		t.Fatalf("mutable action contract error=%v", err)
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

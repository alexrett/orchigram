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

func TestRunRetentionPreservesRecentAndActiveRuns(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	base := time.Date(2026, 8, 9, 0, 0, 0, 0, time.UTC)
	create := func(uid string, at time.Time, terminal bool) {
		s.now = func() time.Time { return at }
		plan := flow.ExecutionPlan{FlowUID: "flow-" + uid, FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "plan-" + uid}
		if _, err := s.EnsureRun(ctx, StartPayload{RunUID: uid, ReceiptUID: "receipt-" + uid, Input: json.RawMessage(`{}`)}, plan); err != nil {
			t.Fatal(err)
		}
		if terminal {
			if err := s.AppendRunEvent(ctx, uid, "", "run.succeeded", "succeeded", 0, nil); err != nil {
				t.Fatal(err)
			}
		}
	}
	create("old", base, true)
	create("recent", base.Add(time.Hour), true)
	create("active", base.Add(2*time.Hour), false)
	candidates, err := s.PlanRunRetention(ctx, base.Add(24*time.Hour), 1, 100)
	if err != nil || len(candidates) != 1 || candidates[0].UID != "old" {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	if err := s.CollectRetainedRuns(ctx, []string{"old"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRun(ctx, "old"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old Run survived collection: %v", err)
	}
	for _, uid := range []string{"recent", "active"} {
		if _, err := s.GetRun(ctx, uid); err != nil {
			t.Fatalf("preserved Run %s: %v", uid, err)
		}
	}
}

func TestRunRetentionRefusesActiveRun(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	plan := flow.ExecutionPlan{FlowUID: "flow-active", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "plan-active"}
	if _, err := s.EnsureRun(ctx, StartPayload{RunUID: "active", ReceiptUID: "receipt-active", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	if err := s.CollectRetainedRuns(ctx, []string{"active"}); err == nil {
		t.Fatal("active Run was collected")
	}
}

func TestRunRetentionKeepsOccurrenceDeduplicationTombstone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	receipt, err := s.AcceptTrigger(ctx, "manual:default:demo", 0, "stable-occurrence", "demo", "default", json.RawMessage(`{}`), true)
	if err != nil {
		t.Fatal(err)
	}
	command, err := s.ClaimStart(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	plan := flow.ExecutionPlan{FlowUID: "flow", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "plan"}
	if _, err := s.EnsureRun(ctx, command.Payload, plan); err != nil {
		t.Fatal(err)
	}
	if err := s.CompleteOutbox(ctx, command.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendRunEvent(ctx, receipt.RunUID, "", "run.succeeded", "succeeded", 0, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO trigger_receipts(uid,trigger_uid,occurrence_id,payload_json,deduplicated,run_uid,accepted_at) VALUES(?,?,?,?,?,?,?)`, "signal-receipt", "reviews", "review-1", []byte(`{"review":true}`), 1, receipt.RunUID, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	if err := s.CollectRetainedRuns(ctx, []string{receipt.RunUID}); err != nil {
		t.Fatal(err)
	}
	replayed, err := s.AcceptTrigger(ctx, "manual:default:demo", 0, "stable-occurrence", "demo", "default", json.RawMessage(`{}`), true)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Existing || replayed.RunUID != receipt.RunUID || replayed.UID != receipt.UID {
		t.Fatalf("receipt=%+v replayed=%+v", receipt, replayed)
	}
	if _, err := s.ClaimStart(ctx, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("retained occurrence created a second start: %v", err)
	}
	signal, err := s.ReceiptByOccurrence(ctx, "reviews", "review-1")
	if err != nil || signal.UID != "signal-receipt" || signal.RunUID != receipt.RunUID {
		t.Fatalf("signal receipt tombstone=%+v err=%v", signal, err)
	}
	var fullReceipts int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM trigger_receipts WHERE run_uid=?`, receipt.RunUID).Scan(&fullReceipts); err != nil || fullReceipts != 0 {
		t.Fatalf("full receipts=%d err=%v", fullReceipts, err)
	}
}

func TestPluginRetentionKeepsActiveDesiredAndPlanPinnedVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	installedAt := time.Now().Add(-60 * 24 * time.Hour)
	for _, version := range []string{"0.1.0", "0.2.0", "0.3.0", "0.4.0"} {
		if err := s.PutPlugin(ctx, PluginRecord{
			Name: "exec", Version: version, Digest: "digest-" + version, InstalledAt: installedAt,
			ManifestJSON: json.RawMessage(`{"name":"exec"}`), ContractJSON: json.RawMessage(`{"actions":[]}`), ContractDigest: "contract-" + version,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ActivatePlugin(ctx, "exec", "0.2.0"); err != nil {
		t.Fatal(err)
	}
	desired, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: PluginInstallation
metadata: {name: exec-desired}
spec: {plugin: exec, version: 0.3.0, digest: digest-0.3.0, enabled: false}
`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(ctx, desired, ApplyOptions{RequestID: "desired-plugin"}); err != nil {
		t.Fatal(err)
	}
	plan := flow.ExecutionPlan{
		FlowUID: "flow-pinned", FlowGeneration: 1, InterpreterVersion: flow.InterpreterVersion, PlanHash: "plan-pinned",
		Nodes: []flow.PlanNode{{ID: "exec", Uses: "exec.command", Plugin: &flow.PluginBinding{Name: "exec", Version: "0.4.0", Digest: "digest-0.4.0"}}},
	}
	if _, err := s.EnsureRun(ctx, StartPayload{RunUID: "run-pinned", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendRunEvent(ctx, "run-pinned", "", "run.succeeded", "succeeded", 0, nil); err != nil {
		t.Fatal(err)
	}
	candidates, err := s.PlanPluginRetention(ctx, time.Now(), 100)
	if err != nil || len(candidates) != 1 || candidates[0].Version != "0.1.0" {
		t.Fatalf("candidates=%+v err=%v", candidates, err)
	}
	if err := s.CollectRetainedPlugin(ctx, candidates[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Plugin(ctx, "exec", "0.1.0"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unreferenced plugin survived: %v", err)
	}
	for _, version := range []string{"0.2.0", "0.3.0", "0.4.0"} {
		if _, err := s.Plugin(ctx, "exec", version); err != nil {
			t.Fatalf("preserved plugin %s: %v", version, err)
		}
	}
}

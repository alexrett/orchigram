package retention

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/store"
)

type recordingPruner struct{ runs []string }

func (p *recordingPruner) RemoveFinishedRun(_ context.Context, runUID string) error {
	p.runs = append(p.runs, runUID)
	return nil
}

func TestCollectPrunesFrameworkHistoryAndOwnedWorkspace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "orchigram.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	plan := flow.ExecutionPlan{FlowUID: "flow", FlowGeneration: 1, PlanHash: "plan", InterpreterVersion: flow.InterpreterVersion}
	if _, err := state.EnsureRun(ctx, store.StartPayload{RunUID: "run-old", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	if err := state.AppendRunEvent(ctx, "run-old", "", "run.succeeded", "succeeded", 0, nil); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(root, "workspaces", "run-old")
	if err := os.MkdirAll(workspace, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "evidence"), []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	pruner := &recordingPruner{}
	report, err := Apply(ctx, state, root, time.Now(), 0, 0, 10, true, false, pruner)
	if err != nil {
		t.Fatal(err)
	}
	if report.CollectedRuns != 1 || len(pruner.runs) != 1 || pruner.runs[0] != "run-old" {
		t.Fatalf("report=%+v pruned=%v", report, pruner.runs)
	}
	if _, err := os.Stat(workspace); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("workspace survived: %v", err)
	}
}

func TestCollectBackupsPreservesConfiguredRecentSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "orchigram.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	backupRoot := filepath.Join(root, "backups")
	if err := os.MkdirAll(backupRoot, 0o750); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(backupRoot, "old.tar.gz")
	recent := filepath.Join(backupRoot, "recent.tar.gz")
	for _, path := range []string{old, recent} {
		if err := os.WriteFile(path, []byte("backup"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(old, time.Now().Add(-48*time.Hour), time.Now().Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(recent, time.Now().Add(-24*time.Hour), time.Now().Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	report, err := Apply(ctx, state, root, time.Now(), 0, 1, 10, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if report.CollectedFiles != 1 || report.ReclaimedBytes != int64(len("backup")) {
		t.Fatalf("report=%+v", report)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old backup survived: %v", err)
	}
	if _, err := os.Stat(recent); err != nil {
		t.Fatalf("recent backup removed: %v", err)
	}
}

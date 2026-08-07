package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

type fakeEngine struct {
	mu     sync.Mutex
	plans  []string
	unique map[string]string
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
func (f *fakeEngine) Cancel(context.Context, string, string) error                        { return nil }
func (f *fakeEngine) Reconcile(context.Context) error                                     { return nil }
func (f *fakeEngine) Describe(context.Context, string) (store.Run, error)                 { return store.Run{}, nil }
func (f *fakeEngine) Close() error                                                        { return nil }

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

package server

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/plugincontroller"
	"github.com/alexrett/orchigram/internal/pluginmanager"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"google.golang.org/grpc/metadata"
)

func TestInfoAdvertisesDeclarativePluginsOnlyWithController(t *testing.T) {
	t.Parallel()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	without := NewAPI(state, flow.NewCompiler(nil), nil, missingWorkflowEngine{}, nil, nil, t.TempDir())
	info, err := without.Info(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(info.GetCapabilities(), "plugins.declarative.v1") {
		t.Fatalf("capabilities without controller=%v", info.GetCapabilities())
	}
	manager := pluginmanager.New(state, t.TempDir())
	defer manager.Close()
	with := NewAPI(state, flow.NewCompiler(nil), nil, missingWorkflowEngine{}, manager, nil, t.TempDir(), plugincontroller.New(state, manager))
	info, err = with.Info(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(info.GetCapabilities(), "plugins.declarative.v1") {
		t.Fatalf("capabilities with controller=%v", info.GetCapabilities())
	}
}

func TestSecureArtifactPathRejectsEscapes(t *testing.T) {
	t.Parallel()
	inside, err := secureArtifactName("artifacts/run/node/raw.log")
	if err != nil || inside != filepath.Join("artifacts", "run", "node", "raw.log") {
		t.Fatalf("inside=%q err=%v", inside, err)
	}
	for _, candidate := range []string{"", "../outside", filepath.Join("artifacts", "..", "..", "outside"), filepath.Join(string(os.PathSeparator), "outside")} {
		if path, err := secureArtifactName(candidate); err == nil {
			t.Fatalf("escape %q resolved to %q", candidate, path)
		}
	}
}

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
	receipt, err := state.AcceptTrigger(ctx, "manual:default:demo", 0, "cancel-api-race", "demo", "default", json.RawMessage(`{}`), true)
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

func TestResourceApplyAndProjectionShareReferenceDiagnostics(t *testing.T) {
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	api := NewAPI(state, flow.NewCompiler(nil), nil, missingWorkflowEngine{}, nil, nil, t.TempDir())
	for _, fixture := range []struct {
		kind string
		name string
		yaml string
	}{
		{kind: "Repository", name: "repo", yaml: `apiVersion: orchigram.dev/v1alpha1
kind: Repository
metadata: {name: repo}
spec: {cloneURL: "https://example.invalid/repo.git", authSecretRef: missing-token}
`},
		{kind: "AgentProfile", name: "agent", yaml: `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: agent}
spec: {type: command, executable: fake-agent, secretRefs: [TOKEN=missing-token]}
`},
	} {
		response, applyErr := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: []byte(fixture.yaml)})
		if applyErr != nil || len(response.GetDiagnostics()) != 1 || response.GetDiagnostics()[0].GetCode() != "reference_not_found" {
			t.Fatalf("%s missing reference response=%+v err=%v", fixture.kind, response, applyErr)
		}
		if _, getErr := state.Get(ctx, fixture.kind, resource.DefaultNamespace, fixture.name); !errors.Is(getErr, store.ErrNotFound) {
			t.Fatalf("invalid %s was stored: %v", fixture.kind, getErr)
		}
	}
	triggerYAML := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: daily}
spec:
  flow: target
  schedule: {cron: "0 9 * * *", timezone: UTC}
`)
	rejected, err := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: triggerYAML})
	if err != nil || len(rejected.GetDiagnostics()) != 1 || rejected.GetDiagnostics()[0].GetCode() != "reference_not_found" {
		t.Fatalf("missing reference response=%+v err=%v", rejected, err)
	}
	if _, err := state.Get(ctx, "Trigger", resource.DefaultNamespace, "daily"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("invalid Trigger was stored: %v", err)
	}
	flowYAML := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: target}
spec: {nodes: [{id: done, uses: core.noop}]}
`)
	flowResponse, err := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: flowYAML})
	if err != nil || flowResponse.GetResource().GetKey().GetUid() == "" {
		t.Fatalf("Flow apply response=%+v err=%v", flowResponse, err)
	}
	triggerResponse, err := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: triggerYAML})
	if err != nil || len(triggerResponse.GetDiagnostics()) != 0 {
		t.Fatalf("Trigger apply response=%+v err=%v", triggerResponse, err)
	}
	ready, err := api.Get(ctx, &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: "Trigger", Name: "daily"}})
	if err != nil || projectedReadyStatus(t, ready.GetJson()) != "True" {
		t.Fatalf("ready projection=%s err=%v", ready.GetJson(), err)
	}
	if _, err := api.Delete(ctx, &controlv1alpha1.DeleteRequest{
		Key: &controlv1alpha1.ResourceKey{Kind: "Flow", Name: "target"}, ExpectedResourceVersion: flowResponse.GetResource().GetResourceVersion(),
	}); err != nil {
		t.Fatal(err)
	}
	notReady, err := api.Get(ctx, &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: "Trigger", Name: "daily"}})
	if err != nil || projectedReadyStatus(t, notReady.GetJson()) != "False" {
		t.Fatalf("not-ready projection=%s err=%v", notReady.GetJson(), err)
	}
	watchContext, cancelWatch := context.WithCancel(context.Background())
	stream := &captureResourceWatch{ctx: watchContext, cancel: cancelWatch}
	watchErr := api.Watch(&controlv1alpha1.WatchRequest{Kind: "Trigger"}, stream)
	if !errors.Is(watchErr, context.Canceled) || len(stream.events) != 1 || projectedReadyStatus(t, stream.events[0].GetResource().GetJson()) != "False" {
		t.Fatalf("watch events=%+v err=%v", stream.events, watchErr)
	}
}

func projectedReadyStatus(t *testing.T, raw []byte) string {
	t.Helper()
	var projection struct {
		Status struct {
			Conditions []struct {
				Type   string `json:"type"`
				Status string `json:"status"`
			} `json:"conditions"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &projection); err != nil {
		t.Fatal(err)
	}
	for _, condition := range projection.Status.Conditions {
		if condition.Type == "Ready" {
			return condition.Status
		}
	}
	return ""
}

type captureResourceWatch struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	events []*controlv1alpha1.ResourceEvent
}

func (s *captureResourceWatch) Send(event *controlv1alpha1.ResourceEvent) error {
	s.mu.Lock()
	s.events = append(s.events, event)
	s.mu.Unlock()
	s.cancel()
	return nil
}

func (*captureResourceWatch) SetHeader(metadata.MD) error  { return nil }
func (*captureResourceWatch) SendHeader(metadata.MD) error { return nil }
func (*captureResourceWatch) SetTrailer(metadata.MD)       {}
func (s *captureResourceWatch) Context() context.Context   { return s.ctx }
func (*captureResourceWatch) SendMsg(any) error            { return nil }
func (*captureResourceWatch) RecvMsg(any) error            { return nil }

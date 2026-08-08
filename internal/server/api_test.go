package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/plugincontroller"
	"github.com/alexrett/orchigram/internal/pluginmanager"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
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

func TestResourceListPaginationWatchAndYAMLExportContracts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	api := NewAPI(state, flow.NewCompiler(nil), nil, missingWorkflowEngine{}, nil, nil, t.TempDir())
	resources := map[string]*controlv1alpha1.ResourceDocument{}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		team := "platform"
		if name == "gamma" {
			team = "product"
		}
		response, applyErr := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: []byte(`apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata:
  name: ` + name + `
  labels: {environment: production, team: ` + team + `}
spec: {type: command, executable: fake-agent}
`)})
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		resources[name] = response.GetResource()
	}
	request := &controlv1alpha1.ListRequest{
		Kind: "AgentProfile", Namespace: resource.DefaultNamespace,
		Labels: map[string]string{"environment": "production", "team": "platform"}, Limit: 1,
	}
	first, err := api.List(ctx, request)
	if err != nil || len(first.GetResources()) != 1 || first.GetResources()[0].GetKey().GetName() != "alpha" || first.GetContinueToken() == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	secondRequest := &controlv1alpha1.ListRequest{
		Kind: request.GetKind(), Namespace: request.GetNamespace(), Labels: request.GetLabels(), Limit: 1, ContinueToken: first.GetContinueToken(),
	}
	second, err := api.List(ctx, secondRequest)
	if err != nil || len(second.GetResources()) != 1 || second.GetResources()[0].GetKey().GetName() != "beta" || second.GetContinueToken() != "" || second.GetRevision() != first.GetRevision() {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
	mismatch := &controlv1alpha1.ListRequest{Kind: request.GetKind(), Namespace: request.GetNamespace(), Labels: map[string]string{"team": "product"}, Limit: 1, ContinueToken: first.GetContinueToken()}
	if _, err := api.List(ctx, mismatch); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("mismatched token error=%v", err)
	}
	if _, err := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: []byte(`apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: later, labels: {environment: production, team: platform}}
spec: {type: command, executable: fake-agent}
`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := api.List(ctx, secondRequest); status.Code(err) != codes.Aborted {
		t.Fatalf("changed snapshot error=%v", err)
	}

	alpha, err := state.Get(ctx, "AgentProfile", resource.DefaultNamespace, "alpha")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.UpdateResourceStatus(ctx, alpha.Kind, alpha.Metadata.Namespace, alpha.Metadata.Name, alpha.Metadata.Generation, map[string]any{"phase": "Ready"}); err != nil {
		t.Fatal(err)
	}
	exported, err := api.Export(ctx, &controlv1alpha1.ExportRequest{Keys: []*controlv1alpha1.ResourceKey{resources["alpha"].GetKey(), resources["beta"].GetKey()}})
	if err != nil || !strings.Contains(string(exported.GetYaml()), "\n---\n") || strings.Contains(string(exported.GetYaml()), "status:") {
		t.Fatalf("export=%q err=%v", exported.GetYaml(), err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(exported.GetYaml()))
	decoded := 0
	for {
		var value map[string]any
		if err := decoder.Decode(&value); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		encoded, err := yaml.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := resource.DecodeStrict(encoded); err != nil {
			t.Fatalf("strict decode exported document: %v\n%s", err, encoded)
		}
		decoded++
	}
	if decoded != 2 {
		t.Fatalf("decoded export documents=%d", decoded)
	}
	repeated, err := api.Export(ctx, &controlv1alpha1.ExportRequest{Keys: []*controlv1alpha1.ResourceKey{resources["alpha"].GetKey(), resources["beta"].GetKey()}})
	if err != nil || !bytes.Equal(exported.GetYaml(), repeated.GetYaml()) {
		t.Fatalf("export is not deterministic: err=%v\nfirst=%s\nsecond=%s", err, exported.GetYaml(), repeated.GetYaml())
	}

	baseline, err := api.List(ctx, &controlv1alpha1.ListRequest{Kind: "AgentProfile", Namespace: resource.DefaultNamespace, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	deltaResponse, err := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: []byte(`apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: delta}
spec: {type: command, executable: fake-agent}
`)})
	if err != nil {
		t.Fatal(err)
	}
	delta, err := state.UpdateResourceStatus(ctx, "AgentProfile", resource.DefaultNamespace, "delta", deltaResponse.GetResource().GetGeneration(), map[string]any{"phase": "Ready"})
	if err != nil {
		t.Fatal(err)
	}
	updatedDelta, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: delta, resourceVersion: ` + fmt.Sprint(delta.Metadata.ResourceVersion) + `}
spec: {type: command, executable: next-agent}
`))
	if err != nil {
		t.Fatal(err)
	}
	updatedDelta, err = state.Apply(ctx, updatedDelta, store.ApplyOptions{ExpectedResourceVersion: delta.Metadata.ResourceVersion, RequestID: "update-delta"})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Delete(ctx, "AgentProfile", resource.DefaultNamespace, "delta", updatedDelta.Metadata.ResourceVersion, "delete-delta"); err != nil {
		t.Fatal(err)
	}
	watchContext, cancelWatch := context.WithCancel(context.Background())
	stream := &captureResourceWatch{ctx: watchContext, cancel: cancelWatch, target: 4}
	watchErr := api.Watch(&controlv1alpha1.WatchRequest{Kind: "AgentProfile", Namespace: resource.DefaultNamespace, AfterRevision: baseline.GetRevision()}, stream)
	if !errors.Is(watchErr, context.Canceled) || len(stream.events) != 4 {
		t.Fatalf("watch events=%+v err=%v", stream.events, watchErr)
	}
	for index, eventType := range []string{"ADDED", "MODIFIED", "MODIFIED", "DELETED"} {
		if stream.events[index].GetType() != eventType {
			t.Fatalf("watch event %d=%+v", index, stream.events[index])
		}
	}
	deleted := stream.events[3].GetResource()
	if stream.events[1].GetResource().GetGeneration() != stream.events[0].GetResource().GetGeneration() || stream.events[1].GetResource().GetResourceVersion() <= stream.events[0].GetResource().GetResourceVersion() || deleted.GetKey().GetKind() != "AgentProfile" || deleted.GetKey().GetNamespace() != resource.DefaultNamespace || deleted.GetKey().GetName() != "delta" || deleted.GetKey().GetUid() == "" || deleted.GetResourceVersion() != stream.events[3].GetRevision() {
		t.Fatalf("status/delete watch events=%+v", stream.events)
	}
}

func TestRunListComposesFlowPhaseAndLimitFilters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	api := NewAPI(state, flow.NewCompiler(nil), nil, missingWorkflowEngine{}, nil, nil, t.TempDir())
	flowUIDs := map[string]string{}
	for _, name := range []string{"alpha-flow", "beta-flow"} {
		response, applyErr := api.Apply(ctx, &controlv1alpha1.ApplyRequest{Document: []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: ` + name + `}
spec: {nodes: [{id: done, uses: core.noop}]}
`)})
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		flowUIDs[name] = response.GetResource().GetKey().GetUid()
	}
	fixtures := []struct {
		uid, flowName, phase string
	}{{"run-alpha-success", "alpha-flow", "succeeded"}, {"run-alpha-failed", "alpha-flow", "failed"}, {"run-beta-success", "beta-flow", "succeeded"}}
	for _, fixture := range fixtures {
		plan := flow.ExecutionPlan{FlowUID: flowUIDs[fixture.flowName], FlowGeneration: 1, PlanHash: "plan-" + fixture.uid, InterpreterVersion: flow.InterpreterVersion}
		if _, err := state.EnsureRun(ctx, store.StartPayload{RunUID: fixture.uid, Input: json.RawMessage(`{}`)}, plan); err != nil {
			t.Fatal(err)
		}
		if err := state.AppendRunEvent(ctx, fixture.uid, "", "run."+fixture.phase, fixture.phase, 0, nil); err != nil {
			t.Fatal(err)
		}
	}
	filtered, err := api.ListRuns(ctx, &controlv1alpha1.ListRunsRequest{Flow: "alpha-flow", Phase: "succeeded", Limit: 10})
	if err != nil || len(filtered.GetRuns()) != 1 || filtered.GetRuns()[0].GetUid() != "run-alpha-success" {
		t.Fatalf("filtered runs=%+v err=%v", filtered, err)
	}
	byUID, err := api.ListRuns(ctx, &controlv1alpha1.ListRunsRequest{Flow: flowUIDs["alpha-flow"], Limit: 1})
	if err != nil || len(byUID.GetRuns()) != 1 || byUID.GetRuns()[0].GetFlow() != flowUIDs["alpha-flow"] {
		t.Fatalf("UID filtered runs=%+v err=%v", byUID, err)
	}
	if _, err := api.ListRuns(ctx, &controlv1alpha1.ListRunsRequest{Phase: "unknown"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid phase error=%v", err)
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
	target int
	mu     sync.Mutex
	events []*controlv1alpha1.ResourceEvent
}

func (s *captureResourceWatch) Send(event *controlv1alpha1.ResourceEvent) error {
	s.mu.Lock()
	s.events = append(s.events, event)
	count := len(s.events)
	s.mu.Unlock()
	if s.target == 0 || count >= s.target {
		s.cancel()
	}
	return nil
}

func (*captureResourceWatch) SetHeader(metadata.MD) error  { return nil }
func (*captureResourceWatch) SendHeader(metadata.MD) error { return nil }
func (*captureResourceWatch) SetTrailer(metadata.MD)       {}
func (s *captureResourceWatch) Context() context.Context   { return s.ctx }
func (*captureResourceWatch) SendMsg(any) error            { return nil }
func (*captureResourceWatch) RecvMsg(any) error            { return nil }

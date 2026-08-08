package pluginmanager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/githubplugin"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const conformanceVersion = "0.1.0"

func TestMain(m *testing.M) {
	if os.Getenv(pluginsdk.Handshake.MagicCookieKey) == pluginsdk.Handshake.MagicCookieValue {
		executable, _ := os.Executable()
		name := filepath.Base(filepath.Dir(filepath.Dir(executable)))
		catalogPlugin, _ := firstparty.Find(name)
		config := pluginsdk.Config{Metadata: pluginsdk.Metadata{
			Name: name, Version: conformanceVersion, Capabilities: catalogPlugin.Capabilities,
			Actions: catalogPlugin.Actions, Triggers: catalogPlugin.Triggers,
		}}
		switch name {
		case "exec":
			config.Task = &pluginruntime.Exec{Runner: process.NewRunner()}
		case "agent-command":
			config.Agent = &pluginruntime.Agent{Runner: process.NewRunner()}
		case "http":
			config.Task = &pluginruntime.HTTP{}
		case "github":
			githubRuntime := &githubplugin.Runtime{Runner: process.NewRunner()}
			config.Task = githubRuntime
			config.Trigger = githubRuntime
		default:
			os.Exit(2)
		}
		pluginsdk.Serve(config)
		return
	}
	os.Exit(m.Run())
}

func TestFirstPartyPluginConformance(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	stateRoot := t.TempDir()
	manager := New(state, stateRoot)
	defer manager.Close()
	for _, name := range []string{"exec", "agent-command", "http", "github"} {
		bundle := conformanceBundle(t, name, 1, 1)
		record, installErr := manager.Install(context.Background(), bundle)
		if installErr != nil {
			t.Fatalf("install %s: %v", name, installErr)
		}
		if record.Name != name || record.Version != conformanceVersion {
			t.Fatalf("unexpected installation: %+v", record)
		}
		contract, contractErr := pluginsdk.DecodeContract(record.ContractJSON)
		if contractErr != nil || record.ContractDigest == "" || len(contract.Actions) == 0 {
			t.Fatalf("%s contract=%+v digest=%q err=%v", name, contract, record.ContractDigest, contractErr)
		}
		if err := manager.Enable(context.Background(), name, conformanceVersion); err != nil {
			t.Fatalf("enable %s: %v", name, err)
		}
		if err := manager.Doctor(context.Background(), name, conformanceVersion); err != nil {
			t.Fatalf("doctor %s: %v", name, err)
		}
	}
	records, err := manager.List(context.Background())
	if err != nil || len(records) != 4 {
		t.Fatalf("plugin list: len=%d err=%v", len(records), err)
	}

	t.Setenv("ORCHIGRAM_UNRELATED_SECRET", "must-not-reach-plugin")
	output, err := manager.Execute(context.Background(), "run-env", flow.PlanNode{
		ID: "environment", Uses: "exec.run", Timeout: "10s",
		With: map[string]any{"argv": []string{"/usr/bin/env"}},
	}, json.RawMessage(`{}`), nil, "stable-env-key")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(output), "ORCHIGRAM_UNRELATED_SECRET") || !strings.Contains(string(output), "PATH=") {
		t.Fatalf("plugin inherited unsafe environment: %s", output)
	}

	token := "sensitive-test-value"
	t.Setenv("ORCHIGRAM_TEST_AGENT_TOKEN", token)
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: agent-token}
spec: {backend: env, key: ORCHIGRAM_TEST_AGENT_TOKEN}
`)
	for _, profileType := range []string{"codex", "claude", "command"} {
		applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: fake-`+profileType+`}
spec:
  type: `+profileType+`
  executable: /bin/sh
  args: ["-c", "printf '{\"type\":\"result\",\"secret\":\"%s\"}\\n' \"$API_TOKEN\""]
  secretRefs: ["API_TOKEN=agent-token"]
`)
		agentOutput, executeErr := manager.Execute(context.Background(), "run-agent-"+profileType, flow.PlanNode{
			ID: "agent", Namespace: resource.DefaultNamespace, Uses: "agent-command.run", Timeout: "10s", With: map[string]any{"profile": "fake-" + profileType},
		}, json.RawMessage(`{"prompt":"conformance"}`), nil, "stable-agent-"+profileType)
		if executeErr != nil {
			t.Fatalf("%s profile: %v", profileType, executeErr)
		}
		if strings.Contains(string(agentOutput), token) || !strings.Contains(string(agentOutput), "[REDACTED]") {
			t.Fatalf("%s profile output was not redacted: %s", profileType, agentOutput)
		}
		artifact, readErr := os.ReadFile(filepath.Join(stateRoot, "artifacts", "run-agent-"+profileType, "agent", "iteration-0", "attempt-1", "raw.log")) //nolint:gosec // Test-owned artifact path.
		if readErr != nil || strings.Contains(string(artifact), token) {
			t.Fatalf("%s raw artifact leak: %q err=%v", profileType, artifact, readErr)
		}
	}
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: fake-failure}
spec:
  type: command
  executable: /bin/sh
  args: ["-c", "echo expected-agent-failure >&2; exit 17"]
`)
	_, failureErr := manager.Execute(context.Background(), "run-agent-failure", flow.PlanNode{
		ID: "agent", Namespace: resource.DefaultNamespace, Uses: "agent-command.run", Timeout: "10s", With: map[string]any{"profile": "fake-failure"},
	}, json.RawMessage(`{"prompt":"conformance"}`), nil, "stable-agent-failure")
	if failureErr == nil || !strings.Contains(failureErr.Error(), "process exited with status 17") || strings.Contains(failureErr.Error(), "did not end immediately") {
		t.Fatalf("agent failure diagnostic=%v", failureErr)
	}

	seenKeys := []string{}
	seenBodies := []string{}
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seenKeys = append(seenKeys, request.Header.Get("Idempotency-Key"))
		body, _ := io.ReadAll(request.Body)
		seenBodies = append(seenBodies, string(body))
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"accepted":true}`))
	}))
	defer receiver.Close()
	for range 2 {
		if _, err := manager.Execute(context.Background(), "run-http", flow.PlanNode{
			ID: "notify", Uses: "http.request", Timeout: "10s",
			With: map[string]any{"url": receiver.URL, "method": "POST", "body": map[string]any{"hello": "world"}},
		}, json.RawMessage(`{}`), nil, "stable-http-key"); err != nil {
			t.Fatal(err)
		}
	}
	if len(seenKeys) != 2 || seenKeys[0] != "stable-http-key" || seenKeys[1] != seenKeys[0] {
		t.Fatalf("HTTP idempotency keys: %+v", seenKeys)
	}
	t.Setenv("ORCHIGRAM_TEST_WEBHOOK_URL", receiver.URL)
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: webhook-url}
spec: {backend: env, key: ORCHIGRAM_TEST_WEBHOOK_URL}
`)
	if _, err := manager.Execute(context.Background(), "run-mapping", flow.PlanNode{
		ID: "notify", Namespace: resource.DefaultNamespace, Uses: "http.request", Timeout: "10s",
		With: map[string]any{
			"urlSecret": "endpoint", "secretRefs": map[string]any{"endpoint": "webhook-url"},
			"body":     map[string]any{"type": "message", "text": "placeholder"},
			"mappings": []any{map[string]any{"from": "nodes.compose.text", "to": "/body/text"}},
		},
	}, json.RawMessage(`{}`), map[string]any{"compose": map[string]any{"text": "mapped message"}}, "stable-mapping-key"); err != nil {
		t.Fatal(err)
	}
	if len(seenBodies) != 3 || !strings.Contains(seenBodies[2], `"text":"mapped message"`) {
		t.Fatalf("mapped HTTP body: %+v", seenBodies)
	}
	t.Setenv("ORCHIGRAM_TEST_GITHUB_PROVIDER_TOKEN", "provider-token")
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: github-provider-token}
spec: {backend: env, key: ORCHIGRAM_TEST_GITHUB_PROVIDER_TOKEN}
`)
	providerConfig := map[string]any{
		"owner": "acme", "repository": "widget", "tokenSecret": "token",
		"secretRefs": map[string]any{"token": "github-provider-token"},
	}
	if diagnostics := manager.ValidateTriggerProvider(context.Background(), resource.DefaultNamespace, "github", providerConfig); len(diagnostics) != 0 {
		t.Fatalf("valid provider diagnostics=%+v", diagnostics)
	}
	invalidProviderConfig := map[string]any{"repository": "widget", "tokenSecret": "token", "secretRefs": map[string]any{"token": "github-provider-token"}}
	if diagnostics := manager.ValidateTriggerProvider(context.Background(), resource.DefaultNamespace, "github", invalidProviderConfig); len(diagnostics) != 1 || diagnostics[0].Code != "required" {
		t.Fatalf("invalid provider diagnostics=%+v", diagnostics)
	}
	missingSecretConfig := map[string]any{"owner": "acme", "repository": "widget", "tokenSecret": "token", "secretRefs": map[string]any{"token": "missing-token"}}
	if diagnostics := manager.ValidateTriggerProvider(context.Background(), resource.DefaultNamespace, "github", missingSecretConfig); len(diagnostics) != 1 || diagnostics[0].Code != "reference_not_found" {
		t.Fatalf("missing provider SecretRef diagnostics=%+v", diagnostics)
	}
	var providerServer *httptest.Server
	var providerMu sync.Mutex
	providerComments := []map[string]any{}
	providerServer = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer provider-token" {
			writer.WriteHeader(http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/repos/acme/widget/issues/events":
			_, _ = fmt.Fprintf(writer, `[{"id":77,"event":"labeled","issue_url":%q,"created_at":"2026-08-08T10:00:00Z","label":{"name":"orchigram:ready"}}]`, providerServer.URL+"/repos/acme/widget/issues/42")
		case "/repos/acme/widget/issues/42":
			_, _ = writer.Write([]byte(`{"number":42,"title":"provider issue","body":"fixture","html_url":"https://example.invalid/42","state":"open"}`))
		case "/repos/acme/widget/issues/42/comments":
			providerMu.Lock()
			defer providerMu.Unlock()
			if request.Method == http.MethodGet {
				_ = json.NewEncoder(writer).Encode(providerComments)
				return
			}
			var payload map[string]any
			_ = json.NewDecoder(request.Body).Decode(&payload)
			created := map[string]any{"id": 1, "html_url": "https://example.invalid/comment/1", "body": payload["body"]}
			providerComments = append(providerComments, created)
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(created)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer providerServer.Close()
	providerContext, stopProvider := context.WithCancel(context.Background())
	providerAccepted := make(chan *pluginv1alpha1.TriggerEvent, 1)
	providerDone := make(chan error, 1)
	providerActivation := time.Date(2026, 8, 8, 10, 0, 30, 0, time.UTC)
	go func() {
		providerDone <- manager.WatchTrigger(providerContext, "github", "trigger-fixture", resource.DefaultNamespace, map[string]any{
			"owner": "acme", "repository": "widget", "apiBase": providerServer.URL, "label": "orchigram:ready", "pollInterval": "1h", "tokenSecret": "token",
			"secretRefs": map[string]any{"token": "github-provider-token"},
		}, "", providerActivation, func(event *pluginv1alpha1.TriggerEvent) error {
			providerAccepted <- event
			return nil
		})
	}()
	select {
	case event := <-providerAccepted:
		if event.GetProviderEventId() != "github:acme/widget:issue-label-event:77" || event.GetCursor() != "77" || !strings.Contains(string(event.GetPayloadJson()), `"number":42`) {
			t.Fatalf("provider event=%+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GitHub provider did not emit fixture event")
	}
	time.Sleep(100 * time.Millisecond) // allow the post-callback acknowledgement to cross the stream
	stopProvider()
	select {
	case <-providerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("GitHub provider stream did not stop")
	}
	githubCommentNode := flow.PlanNode{ID: "publish", Namespace: resource.DefaultNamespace, Uses: "github.issue.comment", Timeout: "10s", With: map[string]any{
		"owner": "acme", "repository": "widget", "apiBase": providerServer.URL, "tokenSecret": "token", "number": 42, "body": "Reconciled plan",
		"secretRefs": map[string]any{"token": "github-provider-token"},
	}}
	firstGitHubOutput, err := manager.Execute(context.Background(), "run-github-reconcile", githubCommentNode, json.RawMessage(`{}`), nil, "github-comment-stable")
	if err != nil {
		t.Fatal(err)
	}
	manager.evict(installationKey("github", conformanceVersion))
	secondGitHubOutput, err := manager.Execute(context.Background(), "run-github-reconcile", githubCommentNode, json.RawMessage(`{}`), nil, "github-comment-stable")
	if err != nil {
		t.Fatal(err)
	}
	providerMu.Lock()
	githubCommentCount := len(providerComments)
	providerMu.Unlock()
	if githubCommentCount != 1 || !strings.Contains(string(firstGitHubOutput), `"reconciled":false`) || !strings.Contains(string(secondGitHubOutput), `"reconciled":true`) {
		t.Fatalf("GitHub restart reconciliation count=%d first=%s second=%s", githubCommentCount, firstGitHubOutput, secondGitHubOutput)
	}

	cancelContext, cancel := context.WithCancel(context.Background())
	cancelled := make(chan error, 1)
	go func() {
		_, executeErr := manager.Execute(cancelContext, "run-cancel", flow.PlanNode{
			ID: "sleep", Uses: "exec.run", Timeout: "30s",
			With: map[string]any{"argv": []string{"/bin/sh", "-c", "sleep 30 & wait"}},
		}, json.RawMessage(`{}`), nil, "stable-cancel-key")
		cancelled <- executeErr
	}()
	time.Sleep(150 * time.Millisecond)
	cancel()
	select {
	case cancelErr := <-cancelled:
		if cancelErr == nil {
			t.Fatal("cancelled plugin call succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("plugin cancellation did not terminate")
	}
	timeoutContext, stopTimeout := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stopTimeout()
	startedTimeout := time.Now()
	_, timeoutErr := manager.Execute(timeoutContext, "run-timeout", flow.PlanNode{
		ID: "timeout", Uses: "exec.run", Timeout: "30s", With: map[string]any{"argv": []string{"/bin/sleep", "30"}},
	}, json.RawMessage(`{}`), nil, "stable-timeout-key")
	if !errors.Is(timeoutErr, context.DeadlineExceeded) || time.Since(startedTimeout) > 5*time.Second {
		t.Fatalf("timeout outcome=%v elapsed=%s", timeoutErr, time.Since(startedTimeout))
	}

	_, crashErr := manager.Execute(context.Background(), "run-crash", flow.PlanNode{
		ID: "crash", Uses: "exec.run", Timeout: "10s",
		With: map[string]any{"argv": []string{"/bin/sh", "-c", "kill -KILL $PPID"}},
	}, json.RawMessage(`{}`), nil, "stable-crash-key")
	if crashErr == nil {
		t.Fatal("deliberate plugin crash was not observed")
	}
	time.Sleep(100 * time.Millisecond)
	if _, err := manager.Execute(context.Background(), "run-recovered", flow.PlanNode{
		ID: "recover", Uses: "exec.run", Timeout: "10s", With: map[string]any{"argv": []string{"/bin/echo", "recovered"}},
	}, json.RawMessage(`{}`), nil, "stable-recovery-key"); err != nil {
		t.Fatalf("daemon-side manager did not recover plugin: %v", err)
	}
}

func TestProviderEventContractRejectsMalformedPayloadBeforeAcceptance(t *testing.T) {
	t.Parallel()
	catalog, ok := firstparty.Find("github")
	if !ok || len(catalog.Triggers) != 1 {
		t.Fatalf("GitHub trigger contract=%+v found=%t", catalog.Triggers, ok)
	}
	invalid := []byte(`{"repository":{"owner":"acme","name":"widget"},"issue":{"number":"not-an-integer"}}`)
	validation := validateTriggerContractJSON(catalog.Triggers[0].EventSchema, "event", invalid)
	if validation == nil || validation.code != "required" && validation.code != "type_mismatch" || !strings.HasPrefix(validation.path, "event.issue") {
		t.Fatalf("invalid event validation=%+v", validation)
	}
	valid := []byte(`{"repository":{"owner":"acme","name":"widget"},"issue":{"number":42,"title":"title","body":"body","html_url":"https://example.invalid/42","state":"open"}}`)
	if validation := validateTriggerContractJSON(catalog.Triggers[0].EventSchema, "event", valid); validation != nil {
		t.Fatalf("valid event validation=%+v", validation)
	}
}

func TestLegacyResourceFallbackRequiresExplicitNamespace(t *testing.T) {
	ctx := context.Background()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	manager := New(state, t.TempDir())
	t.Setenv("ORCHIGRAM_TEAM_TOKEN", "team-value")
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: token, namespace: team-a}
spec: {backend: env, key: ORCHIGRAM_TEAM_TOKEN}
`)
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: worker, namespace: team-a}
spec: {type: command, executable: fake-agent, secretRefs: [TOKEN=token]}
`)
	if _, err := manager.resolveBoundSecret(ctx, "", "token", nil); err == nil || !strings.Contains(err.Error(), "does not pin a resource namespace") {
		t.Fatalf("namespace-less SecretRef fallback error=%v", err)
	}
	secret, err := manager.resolveBoundSecret(ctx, "team-a", "token", nil)
	if err != nil || string(secret) != "team-value" {
		t.Fatalf("team SecretRef=%q err=%v", secret, err)
	}
	profile, err := manager.boundAgentProfile(ctx, "team-a", nil, "worker")
	if err != nil || profile.Executable != "fake-agent" {
		t.Fatalf("team AgentProfile=%+v err=%v", profile, err)
	}
	wrongNamespaceBindings := []flow.ResourceBinding{
		{Kind: "AgentProfile", Namespace: "team-b", Name: "worker", Spec: json.RawMessage(`{"type":"command","executable":"wrong-agent"}`)},
		{Kind: "SecretRef", Namespace: "team-b", Name: "token", Spec: json.RawMessage(`{"backend":"env","key":"ORCHIGRAM_TEAM_TOKEN"}`)},
	}
	if _, err := manager.boundAgentProfile(ctx, "team-a", wrongNamespaceBindings, "worker"); err == nil || !strings.Contains(err.Error(), "pinned AgentProfile") {
		t.Fatalf("cross-namespace AgentProfile binding error=%v", err)
	}
	if _, err := manager.resolveBoundSecret(ctx, "team-a", "token", wrongNamespaceBindings); err == nil || !strings.Contains(err.Error(), "pinned SecretRef") {
		t.Fatalf("cross-namespace SecretRef binding error=%v", err)
	}
	legacyProfile, err := manager.boundAgentProfile(ctx, "", wrongNamespaceBindings, "worker")
	if err != nil || legacyProfile.Executable != "wrong-agent" {
		t.Fatalf("legacy pinned AgentProfile=%+v err=%v", legacyProfile, err)
	}
}

func TestActivePluginHealthReportsProcessLossThenRestarts(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	manager := New(state, t.TempDir())
	defer manager.Close()
	record, err := manager.Install(context.Background(), conformanceBundle(t, "exec", 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Enable(context.Background(), record.Name, record.Version); err != nil {
		t.Fatal(err)
	}
	if diagnostics := manager.HealthDiagnostics(context.Background()); len(diagnostics) != 0 {
		t.Fatalf("healthy plugin diagnostics=%+v", diagnostics)
	}
	key := installationKey(record.Name, record.Version)
	manager.mu.Lock()
	process := manager.processes[key]
	manager.mu.Unlock()
	if process == nil {
		t.Fatal("enabled plugin process is missing")
	}
	process.Close()
	diagnostics := manager.HealthDiagnostics(context.Background())
	if len(diagnostics) != 1 || diagnostics[0].Path != "plugins/exec@"+record.Version || diagnostics[0].Code != "process_exited" {
		t.Fatalf("process-loss diagnostics=%+v", diagnostics)
	}
	if diagnostics := manager.HealthDiagnostics(context.Background()); len(diagnostics) != 0 {
		t.Fatalf("plugin health did not recover after restart: %+v", diagnostics)
	}
}

func TestCompiledNodePinsInactivePluginAndDeletedResourceProjections(t *testing.T) {
	ctx := context.Background()
	stateRoot := t.TempDir()
	state, err := store.Open(filepath.Join(stateRoot, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	manager := New(state, stateRoot)
	defer manager.Close()
	record, err := manager.Install(ctx, conformanceBundle(t, "agent-command", 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Enable(ctx, record.Name, record.Version); err != nil {
		t.Fatal(err)
	}
	t.Setenv("ORCHIGRAM_PINNED_AGENT_TOKEN", "runtime-only-test-value")
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: pinned-token}
spec: {backend: env, key: ORCHIGRAM_PINNED_AGENT_TOKEN}
`)
	applyResource(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: pinned-profile}
spec:
  type: command
  executable: /bin/sh
  args: ["-c", "printf '{\"type\":\"result\",\"pinned\":true}\\n'"]
  secretRefs: ["API_TOKEN=pinned-token"]
`)
	document, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: pinned-flow}
spec:
  nodes:
    - id: agent
      uses: agent-command.run
      with: {profile: pinned-profile}
`))
	if err != nil {
		t.Fatal(err)
	}
	flowResource, err := resource.DecodeFlow(document.JSON)
	if err != nil {
		t.Fatal(err)
	}
	plan, diagnostics := flow.NewCompiler(manager).Compile(flowResource)
	if len(diagnostics) != 0 || len(plan.Nodes) != 1 {
		t.Fatalf("plan=%+v diagnostics=%+v", plan, diagnostics)
	}
	node := plan.Nodes[0]
	if node.Namespace != resource.DefaultNamespace || node.Plugin == nil || node.Plugin.Version != record.Version || node.Plugin.Digest != record.Digest || node.Contract == nil || node.Contract.Digest != record.ContractDigest || len(node.Contract.ConfigSchema) == 0 || len(node.Contract.OutputSchema) == 0 || len(node.Resources) != 2 {
		t.Fatalf("compiled binding=%+v contract=%+v resources=%+v", node.Plugin, node.Contract, node.Resources)
	}
	if encoded, marshalErr := json.Marshal(plan); marshalErr != nil || strings.Contains(string(encoded), "runtime-only-test-value") {
		t.Fatalf("compiled plan leaked a secret value: %s err=%v", encoded, marshalErr)
	}

	if err := manager.Disable(ctx, "agent-command"); err != nil {
		t.Fatal(err)
	}
	for _, target := range []struct{ kind, name string }{{"AgentProfile", "pinned-profile"}, {"SecretRef", "pinned-token"}} {
		stored, getErr := state.Get(ctx, target.kind, resource.DefaultNamespace, target.name)
		if getErr != nil {
			t.Fatal(getErr)
		}
		if deleteErr := state.Delete(ctx, target.kind, resource.DefaultNamespace, target.name, stored.Metadata.ResourceVersion, "delete-after-compile"); deleteErr != nil {
			t.Fatal(deleteErr)
		}
	}
	output, err := manager.Execute(ctx, "run-pinned-bindings", node, json.RawMessage(`{}`), nil, "pinned-bindings-attempt")
	var result struct {
		Stdout string `json:"stdout"`
	}
	decodeErr := json.Unmarshal(output, &result)
	if err != nil || decodeErr != nil || !strings.Contains(result.Stdout, `"pinned":true`) {
		t.Fatalf("pinned execution output=%s err=%v", output, err)
	}
}

func TestSelfSDLCCExampleRendersFetchedIssueRequirementsForPlanner(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "examples", "self-sdlc", "issue-to-pr-flow.yaml")) //nolint:gosec // Repository-owned fixture.
	if err != nil {
		t.Fatal(err)
	}
	document, err := resource.DecodeStrict(data)
	if err != nil {
		t.Fatal(err)
	}
	flowResource, err := resource.DecodeFlow(document.JSON)
	if err != nil {
		t.Fatal(err)
	}
	plan, diagnostics := flow.NewCompiler(nil).Compile(flowResource)
	if len(diagnostics) != 0 {
		t.Fatalf("compile diagnostics: %+v", diagnostics)
	}
	var planner map[string]any
	for _, node := range plan.Nodes {
		if node.ID == "plan" {
			planner = node.With
			break
		}
	}
	if planner == nil {
		t.Fatal("planner node is missing")
	}
	issueFixture := map[string]any{"issue": map[string]any{
		"number": float64(42), "title": "Preserve durable requirements", "body": "Acceptance: work without GitHub or network access.",
	}}
	if err := renderConfigTemplates(planner, json.RawMessage(`{}`), map[string]any{"fetch_issue": issueFixture}); err != nil {
		t.Fatal(err)
	}
	prompt, _ := planner["prompt"].(string)
	for _, requirement := range []string{"Issue #42: Preserve durable requirements", "Acceptance: work without GitHub or network access."} {
		if !strings.Contains(prompt, requirement) {
			t.Fatalf("rendered planner prompt omitted %q: %s", requirement, prompt)
		}
	}
}

func TestRejectsIncompatibleProtocolBeforeInstallation(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	root := t.TempDir()
	manager := New(state, root)
	if _, err := manager.Install(context.Background(), conformanceBundle(t, "exec", 2, 2)); err == nil {
		t.Fatal("incompatible protocol was installed")
	}
	if _, err := os.Stat(filepath.Join(root, "plugins", "exec", conformanceVersion)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incompatible bundle wrote an installation: %v", err)
	}
}

func TestInstallFailuresDoNotPublishImmutableVersion(t *testing.T) {
	t.Run("launch failure permits corrected same version", func(t *testing.T) {
		stateRoot := t.TempDir()
		state, err := store.Open(filepath.Join(stateRoot, "state.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = state.Close() }()
		manager := New(state, stateRoot)
		broken := bundleWithBinary(t, "exec", conformanceVersion, []string{"task.exec.run"}, []byte("not an executable"))
		if _, err := manager.Install(context.Background(), broken); err == nil {
			t.Fatal("invalid executable installed")
		}
		assertVersionAbsent(t, stateRoot, "exec", conformanceVersion)
		if _, err := manager.Install(context.Background(), conformanceBundle(t, "exec", 1, 1)); err != nil {
			t.Fatalf("corrected bundle was poisoned by failed install: %v", err)
		}
	})

	for name, test := range map[string]struct {
		version      string
		capabilities []string
	}{
		"identity mismatch":   {version: "0.2.0", capabilities: []string{"task.exec.run"}},
		"capability mismatch": {version: conformanceVersion, capabilities: []string{"task.exec.other"}},
	} {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			stateRoot := t.TempDir()
			state, err := store.Open(filepath.Join(stateRoot, "state.sqlite"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = state.Close() }()
			manager := New(state, stateRoot)
			if _, err := manager.Install(context.Background(), bundleWithMetadata(t, "exec", test.version, test.capabilities)); err == nil {
				t.Fatal("negotiation mismatch installed")
			}
			assertVersionAbsent(t, stateRoot, "exec", test.version)
		})
	}

	t.Run("database failure rolls back publication", func(t *testing.T) {
		stateRoot := t.TempDir()
		state, err := store.Open(filepath.Join(stateRoot, "state.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		manager := New(state, stateRoot)
		if err := state.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Install(context.Background(), conformanceBundle(t, "exec", 1, 1)); err == nil {
			t.Fatal("install unexpectedly survived closed database")
		}
		assertVersionAbsent(t, stateRoot, "exec", conformanceVersion)
	})
}

func assertVersionAbsent(t *testing.T, stateRoot, name, version string) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(stateRoot, "plugins", name, version)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed installation published version directory: %v", err)
	}
}

func TestMalformedStreamIsRejected(t *testing.T) {
	valid := func(sequence uint64, eventType, payload string) *pluginv1alpha1.ExecuteEvent {
		return &pluginv1alpha1.ExecuteEvent{Sequence: sequence, Type: eventType, PayloadJson: []byte(payload), OccurredAt: timestamppb.Now()}
	}
	tests := map[string][]*pluginv1alpha1.ExecuteEvent{
		"sequence starts at two": {valid(2, "task.completed", `{}`)},
		"sequence gap":           {valid(1, "task.progress", `{}`), valid(3, "task.completed", `{}`)},
		"empty type":             {valid(1, "", `{}`)},
		"missing timestamp":      {{Sequence: 1, Type: "task.completed", PayloadJson: []byte(`{}`)}},
		"invalid JSON":           {valid(1, "task.completed", `{`)},
		"missing terminal":       {valid(1, "task.progress", `{}`)},
		"event after terminal":   {valid(1, "task.completed", `{}`), valid(2, "task.progress", `{}`)},
		"multiple terminals":     {valid(1, "task.completed", `{}`), valid(2, "task.failed", `{}`)},
	}
	for name, events := range tests {
		t.Run(name, func(t *testing.T) {
			manager := &Manager{artifacts: t.TempDir()}
			if _, err := manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1}, &fakeReceiver{events: events}); err == nil {
				t.Fatal("malformed plugin stream was accepted")
			}
		})
	}
	manager := &Manager{artifacts: t.TempDir()}
	output, err := manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1}, &fakeReceiver{events: []*pluginv1alpha1.ExecuteEvent{valid(1, "task.completed", `{"ok":true}`)}})
	if err != nil || string(output) != `{"ok":true}` {
		t.Fatalf("valid stream output=%s err=%v", output, err)
	}
	_, err = manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1}, &fakeReceiver{
		events: []*pluginv1alpha1.ExecuteEvent{valid(1, "agent.failed", `{"outcome":"exited"}`)},
		endErr: errors.New("process exited with status 17"),
	})
	if err == nil || !strings.Contains(err.Error(), "plugin reported agent.failed") || !strings.Contains(err.Error(), "status 17") {
		t.Fatalf("failed terminal diagnostic=%v", err)
	}
	_, err = manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1}, &fakeReceiver{
		events: []*pluginv1alpha1.ExecuteEvent{valid(1, "agent.completed", `{}`)},
		endErr: errors.New("transport reset"),
	})
	if err == nil || !strings.Contains(err.Error(), "did not end immediately") {
		t.Fatalf("successful terminal transport failure=%v", err)
	}
	secret := []byte("terminal-secret-value")
	_, err = manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1, redactions: [][]byte{secret}}, &fakeReceiver{
		events: []*pluginv1alpha1.ExecuteEvent{valid(1, "agent.failed", `{}`)},
		endErr: status.Error(codes.Internal, "authentication failed for terminal-secret-value"),
	})
	if err == nil || status.Code(err) != codes.Internal || strings.Contains(err.Error(), string(secret)) || !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("redacted terminal diagnostic=%v", err)
	}
	doctorErr := drainDoctor(&fakeReceiver{
		events: []*pluginv1alpha1.ExecuteEvent{valid(1, "agent.failed", `{}`)},
		endErr: status.Error(codes.Internal, "authentication failed for terminal-secret-value"),
	}, [][]byte{secret})
	if doctorErr == nil || status.Code(doctorErr) != codes.Internal || strings.Contains(doctorErr.Error(), string(secret)) || !strings.Contains(doctorErr.Error(), "[REDACTED]") {
		t.Fatalf("redacted doctor diagnostic=%v", doctorErr)
	}
}

func TestPluginEventOutcomePrefersStructuredProcessOutcome(t *testing.T) {
	t.Parallel()
	if got := pluginEventOutcome("task.process", json.RawMessage(`{"exitCode":17,"outcome":"exited"}`)); got != "exited" {
		t.Fatalf("structured outcome=%q", got)
	}
	if got := pluginEventOutcome("task.failed", json.RawMessage(`{"error":"transport"}`)); got != "task.failed" {
		t.Fatalf("terminal fallback=%q", got)
	}
	if got := pluginEventOutcome("task.progress", json.RawMessage(`{"percent":50}`)); got != "" {
		t.Fatalf("nonterminal fallback=%q", got)
	}
}

func TestConsumeRedactsEvidenceBeforeDurablePersistence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	plan := flow.ExecutionPlan{FlowUID: "flow-redaction", FlowGeneration: 1, PlanHash: "plan-redaction", InterpreterVersion: flow.InterpreterVersion}
	if _, err := state.EnsureRun(ctx, store.StartPayload{RunUID: "run-redaction", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.BeginNodeAttempt(ctx, "run-redaction", "effect", 0, 1, "stable-redaction-key", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	secret := []byte("durable-secret-sentinel")
	manager := &Manager{store: state, artifacts: filepath.Join(root, "artifacts")}
	events := []*pluginv1alpha1.ExecuteEvent{
		{Sequence: 1, Type: "task.log.stdout", PayloadJson: []byte(`{"message":"durable-secret-sentinel"}`), RawLog: []byte("durable-secret-sentinel\n"), OccurredAt: timestamppb.Now()},
		{Sequence: 2, Type: "task.completed", PayloadJson: []byte(`{"result":"durable-secret-sentinel","outcome":"exited"}`), OccurredAt: timestamppb.Now()},
	}
	output, err := manager.consume(ctx, runArtifact{runUID: "run-redaction", nodeID: "effect", attempt: 1, redactions: [][]byte{secret}, persist: true}, &fakeReceiver{events: events})
	if err != nil || strings.Contains(string(output), string(secret)) || !strings.Contains(string(output), "[REDACTED]") {
		t.Fatalf("output=%s err=%v", output, err)
	}
	runEvents, err := state.RunEventsAfter(ctx, "run-redaction", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range runEvents {
		if strings.Contains(string(event.Payload), string(secret)) {
			t.Fatalf("event leaked secret: %+v", event)
		}
	}
	artifacts, err := state.ListArtifacts(ctx, "run-redaction", 10)
	if err != nil || len(artifacts) != 1 {
		t.Fatalf("artifacts=%+v err=%v", artifacts, err)
	}
	raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(artifacts[0].RelativePath))) //nolint:gosec // Test reads registered fixture metadata.
	if err != nil || strings.Contains(string(raw), string(secret)) || !strings.Contains(string(raw), "[REDACTED]") {
		t.Fatalf("raw artifact=%q err=%v", raw, err)
	}
	attempt, err := state.NodeAttempt(ctx, "run-redaction", "effect", 0, 1)
	if err != nil || attempt.ExitOutcome != "exited" {
		t.Fatalf("attempt=%+v err=%v", attempt, err)
	}
}

func TestReconcileArtifactsRegistersCrashBoundaryFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	root := t.TempDir()
	state, err := store.Open(filepath.Join(root, "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	plan := flow.ExecutionPlan{FlowUID: "flow-artifact-reconcile", FlowGeneration: 1, PlanHash: "plan-artifact-reconcile", InterpreterVersion: flow.InterpreterVersion}
	if _, err := state.EnsureRun(ctx, store.StartPayload{RunUID: "run-artifact-reconcile", Input: json.RawMessage(`{}`)}, plan); err != nil {
		t.Fatal(err)
	}
	if _, _, err := state.BeginNodeAttempt(ctx, "run-artifact-reconcile", "effect", 2, 1, "stable-artifact-key", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	artifactPath := filepath.Join(root, "artifacts", "run-artifact-reconcile", "effect", "iteration-2", "attempt-1", "raw.log")
	if err := os.MkdirAll(filepath.Dir(artifactPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifactPath, []byte("crash-boundary\n"), 0o600); err != nil { //nolint:gosec // Test-owned recovery fixture.
		t.Fatal(err)
	}
	manager := New(state, root)
	if err := manager.ReconcileArtifacts(ctx); err != nil {
		t.Fatal(err)
	}
	artifacts, err := state.ListArtifacts(ctx, "run-artifact-reconcile", 10)
	if err != nil || len(artifacts) != 1 || artifacts[0].LogicalIteration != 2 || artifacts[0].Attempt != 1 || artifacts[0].SizeBytes != int64(len("crash-boundary\n")) {
		t.Fatalf("reconciled artifacts=%+v err=%v", artifacts, err)
	}
	digest := sha256.Sum256([]byte("crash-boundary\n"))
	if artifacts[0].SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("reconciled digest=%q", artifacts[0].SHA256)
	}
	if _, err := state.CompleteNodeAttempt(ctx, "run-artifact-reconcile", "effect", 2, 1, "succeeded", json.RawMessage(`{}`), "", "exited"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(artifactPath, []byte("tampered\n"), 0o600); err != nil { //nolint:gosec // Test deliberately mutates its fixture.
		t.Fatal(err)
	}
	if err := manager.ReconcileArtifacts(ctx); err == nil || !strings.Contains(err.Error(), "differs from durable metadata") {
		t.Fatalf("tampered completed artifact reconciliation error=%v", err)
	}
}

func TestFailedTerminalPayloadDiagnostic(t *testing.T) {
	terminalEvent := func(payload string) *pluginv1alpha1.ExecuteEvent {
		return &pluginv1alpha1.ExecuteEvent{Sequence: 1, Type: "task.failed", PayloadJson: []byte(payload), OccurredAt: timestamppb.Now()}
	}
	t.Run("error is returned", func(t *testing.T) {
		manager := &Manager{artifacts: t.TempDir()}
		_, err := manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1}, &fakeReceiver{
			events: []*pluginv1alpha1.ExecuteEvent{terminalEvent(`{"error":"HTTP status 502"}`)},
		})
		if err == nil || !strings.Contains(err.Error(), "plugin reported task.failed") || !strings.Contains(err.Error(), "HTTP status 502") {
			t.Fatalf("terminal payload diagnostic=%v", err)
		}
	})
	t.Run("error is redacted", func(t *testing.T) {
		manager := &Manager{artifacts: t.TempDir()}
		secret := []byte("payload-secret-value")
		_, err := manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1, redactions: [][]byte{secret}}, &fakeReceiver{
			events: []*pluginv1alpha1.ExecuteEvent{terminalEvent(`{"error":"request failed for payload-secret-value"}`)},
		})
		if err == nil || strings.Contains(err.Error(), string(secret)) || !strings.Contains(err.Error(), "request failed for [REDACTED]") {
			t.Fatalf("redacted terminal payload diagnostic=%v", err)
		}
	})
}

func TestProviderBootstrapRequiresActivationFenceCapability(t *testing.T) {
	t.Parallel()
	activation := time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)
	oldRecord := store.PluginRecord{Name: "github", ManifestJSON: json.RawMessage(`{"capabilities":["trigger.github.issues"]}`)}
	if err := validateProviderBootstrap(oldRecord, "", time.Time{}, map[string]any{}); err == nil || !strings.Contains(err.Error(), "activation time is required") {
		t.Fatalf("missing activation fence error=%v", err)
	}
	if err := validateProviderBootstrap(oldRecord, "", activation, map[string]any{}); err == nil || !strings.Contains(err.Error(), pluginsdk.ActivationFenceCapability) {
		t.Fatalf("old provider bootstrap error=%v", err)
	}
	if err := validateProviderBootstrap(oldRecord, "77", activation, map[string]any{}); err != nil {
		t.Fatalf("persisted cursor should not require activation fence: %v", err)
	}
	if err := validateProviderBootstrap(oldRecord, "", activation, map[string]any{"replayExisting": true}); err != nil {
		t.Fatalf("explicit replay should allow an old provider: %v", err)
	}
	if err := validateProviderBootstrap(oldRecord, "", time.Time{}, map[string]any{"replayExisting": true}); err != nil {
		t.Fatalf("explicit replay should allow a missing activation time: %v", err)
	}
	newRecord := store.PluginRecord{Name: "github", ManifestJSON: json.RawMessage(`{"capabilities":["trigger.github.issues","trigger.bootstrap.activation-fence"]}`)}
	if err := validateProviderBootstrap(newRecord, "", activation, map[string]any{}); err != nil {
		t.Fatalf("activation-fence provider bootstrap: %v", err)
	}
}

type fakeReceiver struct {
	events []*pluginv1alpha1.ExecuteEvent
	index  int
	endErr error
}

func (f *fakeReceiver) Recv() (*pluginv1alpha1.ExecuteEvent, error) {
	if f.index >= len(f.events) {
		if f.endErr != nil {
			return nil, f.endErr
		}
		return nil, io.EOF
	}
	event := f.events[f.index]
	f.index++
	return event, nil
}

func conformanceBundle(t *testing.T, name string, minimum, maximum uint32) []byte {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable) //nolint:gosec // The test intentionally bundles its own executable.
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	capabilities := map[string][]string{
		"exec": {"task.exec.run"}, "agent-command": {"agent.codex", "agent.claude", "agent.command"},
		"http": {"task.http.request"}, "github": githubplugin.Capabilities,
	}[name]
	manifest := pluginbundle.Manifest{
		APIVersion: pluginbundle.APIVersion, Name: name, Version: conformanceVersion,
		Protocol: pluginbundle.ProtocolRange{Minimum: minimum, Maximum: maximum}, Capabilities: capabilities,
		Platforms: []pluginbundle.Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	bundle, err := pluginbundle.Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func bundleWithMetadata(t *testing.T, name, version string, capabilities []string) []byte {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable) //nolint:gosec // Test intentionally bundles its executable.
	if err != nil {
		t.Fatal(err)
	}
	return bundleWithBinary(t, name, version, capabilities, binary)
}

func bundleWithBinary(t *testing.T, name, version string, capabilities []string, binary []byte) []byte {
	t.Helper()
	digest := sha256.Sum256(binary)
	manifest := pluginbundle.Manifest{
		APIVersion: pluginbundle.APIVersion, Name: name, Version: version,
		Protocol: pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: capabilities,
		Platforms: []pluginbundle.Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	bundle, err := pluginbundle.Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func applyResource(t *testing.T, state *store.Store, manifest string) {
	t.Helper()
	document, err := resource.DecodeStrict([]byte(manifest))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Apply(context.Background(), document, store.ApplyOptions{}); err != nil {
		t.Fatal(err)
	}
}

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
		config := pluginsdk.Config{Metadata: pluginsdk.Metadata{Name: name, Version: conformanceVersion}}
		switch name {
		case "exec":
			config.Metadata.Capabilities = []string{"task.exec.run"}
			config.Task = &pluginruntime.Exec{Runner: process.NewRunner()}
		case "agent-command":
			config.Metadata.Capabilities = []string{"agent.codex", "agent.claude", "agent.command"}
			config.Agent = &pluginruntime.Agent{Runner: process.NewRunner()}
		case "http":
			config.Metadata.Capabilities = []string{"task.http.request"}
			config.Task = &pluginruntime.HTTP{}
		case "github":
			config.Metadata.Capabilities = githubplugin.Capabilities
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
			ID: "agent", Uses: "agent-command.run", Timeout: "10s", With: map[string]any{"profile": "fake-" + profileType},
		}, json.RawMessage(`{"prompt":"conformance"}`), nil, "stable-agent-"+profileType)
		if executeErr != nil {
			t.Fatalf("%s profile: %v", profileType, executeErr)
		}
		if strings.Contains(string(agentOutput), token) || !strings.Contains(string(agentOutput), "[REDACTED]") {
			t.Fatalf("%s profile output was not redacted: %s", profileType, agentOutput)
		}
		artifact, readErr := os.ReadFile(filepath.Join(stateRoot, "artifacts", "run-agent-"+profileType, "agent", "attempt-1", "raw.log")) //nolint:gosec // Test-owned artifact path.
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
		ID: "agent", Uses: "agent-command.run", Timeout: "10s", With: map[string]any{"profile": "fake-failure"},
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
		ID: "notify", Uses: "http.request", Timeout: "10s",
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
	go func() {
		providerDone <- manager.WatchTrigger(providerContext, "github", "trigger-fixture", map[string]any{
			"owner": "acme", "repository": "widget", "apiBase": providerServer.URL, "label": "orchigram:ready", "pollInterval": "1h", "tokenSecret": "token",
			"secretRefs": map[string]any{"token": "github-provider-token"},
		}, "", time.Time{}, func(event *pluginv1alpha1.TriggerEvent) error {
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
	githubCommentNode := flow.PlanNode{ID: "publish", Uses: "github.issue.comment", Timeout: "10s", With: map[string]any{
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

func TestProviderBootstrapRequiresActivationFenceCapability(t *testing.T) {
	t.Parallel()
	activation := time.Date(2026, 8, 8, 10, 0, 0, 0, time.UTC)
	oldRecord := store.PluginRecord{Name: "github", ManifestJSON: json.RawMessage(`{"capabilities":["trigger.github.issues"]}`)}
	if err := validateProviderBootstrap(oldRecord, "", activation, map[string]any{}); err == nil || !strings.Contains(err.Error(), pluginsdk.ActivationFenceCapability) {
		t.Fatalf("old provider bootstrap error=%v", err)
	}
	if err := validateProviderBootstrap(oldRecord, "77", activation, map[string]any{}); err != nil {
		t.Fatalf("persisted cursor should not require activation fence: %v", err)
	}
	if err := validateProviderBootstrap(oldRecord, "", activation, map[string]any{"replayExisting": true}); err != nil {
		t.Fatalf("explicit replay should allow an old provider: %v", err)
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

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
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const conformanceVersion = "0.1.0"

func TestMain(m *testing.M) {
	if os.Getenv(pluginprotocol.Handshake.MagicCookieKey) == pluginprotocol.Handshake.MagicCookieValue {
		executable, _ := os.Executable()
		name := filepath.Base(filepath.Dir(filepath.Dir(executable)))
		info := pluginruntime.Info{Name: name, Version: conformanceVersion}
		servers := pluginprotocol.Servers{}
		switch name {
		case "exec":
			info.Capabilities = []string{"task.exec.run"}
			servers.Task = &pluginruntime.Exec{Runner: process.NewRunner()}
		case "agent-command":
			info.Capabilities = []string{"agent.codex", "agent.claude", "agent.command"}
			servers.Agent = &pluginruntime.Agent{Runner: process.NewRunner()}
		case "http":
			info.Capabilities = []string{"task.http.request"}
			servers.Task = &pluginruntime.HTTP{}
		case "github":
			info.Capabilities = githubplugin.Capabilities
			githubRuntime := &githubplugin.Runtime{Runner: process.NewRunner()}
			servers.Task = githubRuntime
			servers.Trigger = githubRuntime
		default:
			os.Exit(2)
		}
		servers.Control = &pluginruntime.Control{Info: info}
		pluginprotocol.Serve(servers)
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
		}, "", func(event *pluginv1alpha1.TriggerEvent) error {
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

func TestMalformedStreamIsRejected(t *testing.T) {
	manager := &Manager{artifacts: t.TempDir()}
	receiver := &fakeReceiver{events: []*pluginv1alpha1.ExecuteEvent{{Sequence: 2, Type: "task.completed", PayloadJson: []byte(`{}`), OccurredAt: timestamppb.Now()}}}
	if _, err := manager.consume(context.Background(), runArtifact{runUID: "run", nodeID: "node", attempt: 1}, receiver); err == nil {
		t.Fatal("out-of-order plugin stream was accepted")
	}
}

type fakeReceiver struct {
	events []*pluginv1alpha1.ExecuteEvent
	index  int
}

func (f *fakeReceiver) Recv() (*pluginv1alpha1.ExecuteEvent, error) {
	if f.index >= len(f.events) {
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

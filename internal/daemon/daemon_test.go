package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/githubplugin"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMain(m *testing.M) {
	if os.Getenv(pluginprotocol.Handshake.MagicCookieKey) == pluginprotocol.Handshake.MagicCookieValue {
		executable, _ := os.Executable()
		name := filepath.Base(filepath.Dir(filepath.Dir(executable)))
		info := pluginruntime.Info{Name: name, Version: "0.1.0"}
		servers := pluginprotocol.Servers{}
		switch name {
		case "exec":
			info.Capabilities = []string{"task.exec.run"}
			servers.Task = &pluginruntime.Exec{Runner: process.NewRunner()}
		case "agent-command":
			info.Capabilities = []string{"agent.codex", "agent.claude", "agent.command"}
			servers.Agent = &pluginruntime.Agent{Runner: process.NewRunner()}
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

func TestInstalledPluginExecutesThroughDurableDaemon(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-plugin-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	stop := serveTestDaemon(t, cfg)
	defer stop()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()

	bundle := daemonPluginBundle(t, "exec", []string{"task.exec.run"})
	stream, err := client.Plugins.Install(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const chunkSize = 1 << 20
	for offset := 0; offset < len(bundle); offset += chunkSize {
		end := min(offset+chunkSize, len(bundle))
		if err := stream.Send(&controlv1alpha1.PluginUploadRequest{BundleChunk: bundle[offset:end], Final: end == len(bundle)}); err != nil {
			t.Fatal(err)
		}
	}
	installed, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if installed.GetName() != "exec" || installed.GetVersion() != "0.1.0" {
		t.Fatalf("installed plugin: %+v", installed)
	}
	if _, err := client.Plugins.Enable(context.Background(), &controlv1alpha1.PluginRequest{Name: "exec", Version: "0.1.0"}); err != nil {
		t.Fatal(err)
	}
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: plugin-execution}
spec:
  nodes:
    - id: execute
      uses: exec.run
      timeout: 10s
      with:
        argv: [/bin/echo, durable-plugin-flow]
`)
	applied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil {
		t.Fatal(err)
	}
	if len(applied.GetDiagnostics()) != 0 {
		t.Fatalf("apply diagnostics: %+v", applied.GetDiagnostics())
	}
	run, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "plugin-execution", InputJson: []byte(`{}`), IdempotencyKey: "plugin-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, run.GetUid(), 0, "run.succeeded")
	artifact, err := os.ReadFile(filepath.Join(cfg.StateDir, "artifacts", run.GetUid(), "execute", "attempt-1", "raw.log")) //nolint:gosec // Test-owned daemon state path.
	if err != nil || string(artifact) != "durable-plugin-flow\n" {
		t.Fatalf("artifact=%q err=%v", artifact, err)
	}
}

func TestRunApprovalSurvivesDaemonRestart(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))

	stopFirst := serveTestDaemon(t, cfg)
	first := dialReadyClient(t, cfg.SocketPath)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata:
  name: restart-approval
spec:
  policies:
    timeout: 1m
    maxParallel: 1
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 30s}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	apply, err := first.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil {
		t.Fatal(err)
	}
	if len(apply.GetDiagnostics()) != 0 {
		t.Fatalf("apply diagnostics: %+v", apply.GetDiagnostics())
	}
	started, err := first.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{
		Flow:           "restart-approval",
		InputJson:      []byte(`{"scope":"restart-gate"}`),
		IdempotencyKey: "restart-gate-occurrence",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, first, started.GetUid(), 0, "approval.waiting")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	stopFirst()

	stopSecond := serveTestDaemon(t, cfg)
	defer stopSecond()
	second := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = second.Close() }()
	duplicate, err := second.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{
		Flow:           "restart-approval",
		InputJson:      []byte(`{"scope":"different-payload"}`),
		IdempotencyKey: "restart-gate-occurrence",
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.GetUid() != started.GetUid() {
		t.Fatalf("duplicate occurrence created run %s instead of %s", duplicate.GetUid(), started.GetUid())
	}
	if _, err := second.Runs.Approve(context.Background(), &controlv1alpha1.ApprovalRequest{
		RunUid: started.GetUid(), NodeId: "approval", Reason: "restart acceptance gate",
	}); err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, second, started.GetUid(), 0, "run.succeeded")
}

func TestNativeScheduleRestartCreatesExactlyOneRun(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-schedule-restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	now := time.Date(2026, 8, 8, 12, 0, 10, 0, time.UTC)

	firstContext, cancelFirst := context.WithCancel(context.Background())
	first, err := Open(firstContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	applyDaemonResource(t, first, `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: scheduled-noop}
spec:
  nodes: [{id: execute, uses: core.noop}]
`)
	triggerDocument := applyDaemonResource(t, first, `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: every-minute}
spec:
  flow: scheduled-noop
  schedule: {cron: "* * * * *", timezone: UTC, startingDeadline: 1h, concurrencyPolicy: forbid}
`)
	if _, err := first.store.EnsureTriggerState(firstContext, triggerDocument.Metadata.UID, triggerDocument.Metadata.Generation, true, now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := first.triggers.ReconcileSchedules(firstContext, now); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after receipt/outbox commit but before the cursor update
	// became visible. The same occurrence must reconcile to the same receipt.
	if err := first.store.AdvanceTriggerCursor(firstContext, triggerDocument.Metadata.UID, now.Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	cancelFirst()
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	secondContext, cancelSecond := context.WithCancel(context.Background())
	second, err := Open(secondContext, cfg, nil)
	if err != nil {
		cancelSecond()
		t.Fatal(err)
	}
	defer func() {
		cancelSecond()
		if closeErr := second.Close(); closeErr != nil {
			t.Errorf("close second daemon: %v", closeErr)
		}
	}()
	if err := second.triggers.ReconcileSchedules(secondContext, now); err != nil {
		t.Fatal(err)
	}
	receipts, err := second.store.TriggerReceipts(secondContext, triggerDocument.Metadata.UID, 10)
	if err != nil || len(receipts) != 1 {
		t.Fatalf("receipts=%d err=%v", len(receipts), err)
	}
	second.orchestrator.Start(secondContext)
	deadline := time.Now().Add(10 * time.Second)
	for {
		run, runErr := second.store.GetRun(secondContext, receipts[0].RunUID)
		if runErr == nil && run.Phase == "succeeded" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("scheduled run did not succeed: run=%+v err=%v", run, runErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	runs, err := second.store.ListRuns(secondContext, 10)
	if err != nil || len(runs) != 1 || runs[0].UID != receipts[0].RunUID {
		t.Fatalf("runs=%+v err=%v", runs, err)
	}
}

func TestGitHubIssueApprovalToPullRequestTracer(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-github-tracer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	origin := filepath.Join(root, "origin.git")
	seed := filepath.Join(root, "seed")
	runDaemonGit(t, "", "init", "--bare", "--initial-branch=main", origin)
	runDaemonGit(t, "", "init", "--initial-branch=main", seed)
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runDaemonGit(t, seed, "add", "README.md")
	runDaemonGit(t, seed, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed")
	runDaemonGit(t, seed, "remote", "add", "origin", origin)
	runDaemonGit(t, seed, "push", "origin", "main")

	fixture := newGitHubFixture(t)
	defer fixture.server.Close()
	t.Setenv("ORCHIGRAM_TEST_GITHUB_TOKEN", "fixture-token")
	cfg := config.Development(filepath.Join(root, "state"))
	stop := serveTestDaemon(t, cfg)
	defer stop()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()
	for name, capabilities := range map[string][]string{
		"exec": {"task.exec.run"}, "agent-command": {"agent.codex", "agent.claude", "agent.command"}, "github": githubplugin.Capabilities,
	} {
		installDaemonPlugin(t, client, daemonPluginBundle(t, name, capabilities), name)
	}

	applyClientResource(t, client, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: github-token}
spec: {backend: env, key: ORCHIGRAM_TEST_GITHUB_TOKEN}
`)
	applyClientResource(t, client, fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Repository
metadata: {name: fixture-repository}
spec:
  cloneURL: %q
  defaultBranch: main
  workspacePolicy: isolated-run
  authSecretRef: github-token
`, origin))
	applyClientResource(t, client, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: fixture-planner}
spec:
  type: command
  executable: /bin/sh
  args:
    - -c
    - |-
      printf '%s\n' '{"type":"result","result":"Approved fixture plan"}'
    - "{prompt}"
`)
	applyClientResource(t, client, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: fixture-implementer}
spec:
  type: command
  executable: /bin/sh
  args:
    - -c
    - |-
      printf 'implemented\n' > implemented.txt
      printf '%s\n' '{"type":"result","result":"Implemented fixture"}'
    - "{prompt}"
`)
	flowSource := fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: github-fixture}
spec:
  policies: {timeout: 10m, maxParallel: 1}
  nodes:
    - id: fetch
      uses: github.issue.get
      with:
        owner: acme
        repository: widget
        apiBase: %q
        tokenSecret: token
        secretRefs: {token: github-token}
        number: 1
        mappings: [{from: input.issue.number, to: /number}]
    - id: checkout
      uses: github.workspace.checkout
      with:
        repositoryRef: fixture-repository
        issueNumber: 1
        mappings: [{from: input.issue.number, to: /issueNumber}]
    - id: plan
      uses: agent-command.run
      with:
        profile: fixture-planner
        workspace: .
        prompt: "Plan issue ${input.issue.number}: ${input.issue.title}"
        mappings: [{from: nodes.checkout.workspace, to: /workspace}]
    - id: publish
      uses: github.issue.comment
      with:
        owner: acme
        repository: widget
        apiBase: %q
        tokenSecret: token
        secretRefs: {token: github-token}
        number: 1
        body: "Plan: ${nodes.plan.text}"
        mappings: [{from: input.issue.number, to: /number}]
    - {id: approval, uses: core.approval, timeout: 2m}
    - id: implement
      uses: agent-command.run
      with:
        profile: fixture-implementer
        workspace: .
        prompt: "Implement ${nodes.plan.text}"
        mappings: [{from: nodes.checkout.workspace, to: /workspace}]
    - id: tests
      uses: exec.run
      with:
        argv: [/bin/test, -f, implemented.txt]
        directory: .
        mappings: [{from: nodes.checkout.workspace, to: /directory}]
    - id: push
      uses: github.workspace.commit-push
      with:
        workspace: .
        branch: orchigram/issue-1-placeholder
        message: "Implement issue ${input.issue.number}"
        tokenSecret: token
        secretRefs: {token: github-token}
        mappings:
          - {from: nodes.checkout.workspace, to: /workspace}
          - {from: nodes.checkout.branch, to: /branch}
    - id: pr
      uses: github.pr.ensure
      with:
        owner: acme
        repository: widget
        apiBase: %q
        tokenSecret: token
        secretRefs: {token: github-token}
        head: orchigram/issue-1-placeholder
        base: main
        title: "Implement ${input.issue.title}"
        mappings: [{from: nodes.checkout.branch, to: /head}]
    - id: final
      uses: github.issue.comment
      with:
        owner: acme
        repository: widget
        apiBase: %q
        tokenSecret: token
        secretRefs: {token: github-token}
        number: 1
        body: "PR ${nodes.pr.url} is ready."
        mappings: [{from: input.issue.number, to: /number}]
  edges:
    - {from: fetch, to: checkout}
    - {from: checkout, to: plan}
    - {from: plan, to: publish}
    - {from: publish, to: approval}
    - {from: approval, to: implement, when: result.approved}
    - {from: implement, to: tests}
    - {from: tests, to: push}
    - {from: push, to: pr}
    - {from: pr, to: final}
`, fixture.server.URL, fixture.server.URL, fixture.server.URL, fixture.server.URL)
	applyClientResource(t, client, flowSource)

	approved, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "github-fixture", InputJson: []byte(`{"issue":{"number":42,"title":"Implement tracer","body":"fixture"}}`), IdempotencyKey: "github-approved"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, approved.GetUid(), 0, "approval.waiting")
	if _, err := client.Runs.Approve(context.Background(), &controlv1alpha1.ApprovalRequest{RunUid: approved.GetUid(), NodeId: "approval", Reason: "fixture approval"}); err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, approved.GetUid(), 0, "run.succeeded")
	branch := "orchigram/issue-42-" + strings.ReplaceAll(approved.GetUid(), "-", "")[:8]
	if head := strings.TrimSpace(runDaemonGit(t, "", "--git-dir", origin, "rev-parse", "refs/heads/"+branch)); head == "" {
		t.Fatal("approved run did not push its deterministic branch")
	}
	fixture.mu.Lock()
	commentCount, pullCount := len(fixture.comments[42]), len(fixture.pulls)
	fixture.mu.Unlock()
	if commentCount != 2 || pullCount != 1 {
		t.Fatalf("approved GitHub effects: comments=%d pulls=%d", commentCount, pullCount)
	}

	rejected, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "github-fixture", InputJson: []byte(`{"issue":{"number":43,"title":"Reject tracer","body":"fixture"}}`), IdempotencyKey: "github-rejected"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, rejected.GetUid(), 0, "approval.waiting")
	if _, err := client.Runs.Reject(context.Background(), &controlv1alpha1.ApprovalRequest{RunUid: rejected.GetUid(), NodeId: "approval", Reason: "fixture rejection"}); err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, rejected.GetUid(), 0, "run.rejected")
	rejectedBranch := "orchigram/issue-43-" + strings.ReplaceAll(rejected.GetUid(), "-", "")[:8]
	if output := runDaemonGit(t, "", "--git-dir", origin, "for-each-ref", "--format=%(refname)", "refs/heads/"+rejectedBranch); strings.TrimSpace(output) != "" {
		t.Fatalf("rejected run pushed branch: %s", output)
	}
	fixture.mu.Lock()
	rejectedPulls := len(fixture.pulls)
	fixture.mu.Unlock()
	if rejectedPulls != 1 {
		t.Fatalf("rejected run created a PR: pulls=%d", rejectedPulls)
	}
}

func applyDaemonResource(t *testing.T, daemon *Daemon, source string) resource.Document {
	t.Helper()
	document, err := resource.DecodeStrict([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := daemon.store.Apply(context.Background(), document, store.ApplyOptions{RequestID: "daemon-test"})
	if err != nil {
		t.Fatal(err)
	}
	return applied
}

func TestShutdownIsBoundedWithActiveWatch(t *testing.T) {
	t.Parallel()
	root, err := os.MkdirTemp("/tmp", "orchigram-daemon-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	ctx, cancel := context.WithCancel(context.Background())
	d, err := Open(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx) }()
	client, err := clientpkg.DialUnix(ctx, cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	stream, err := client.Resources.Watch(context.Background(), &controlv1alpha1.WatchRequest{})
	if err != nil {
		t.Fatal(err)
	}
	receiving := make(chan error, 1)
	go func() { _, receiveErr := stream.Recv(); receiving <- receiveErr }()
	time.Sleep(100 * time.Millisecond)
	started := time.Now()
	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("daemon shutdown exceeded bounded grace period")
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("shutdown took %s", elapsed)
	}
	select {
	case <-receiving:
	case <-time.After(time.Second):
		t.Fatal("watch was not interrupted")
	}
}

func TestPrepareSocketRefusesRegularFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "orchigram.sock")
	if err := os.WriteFile(path, []byte("do not replace"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepareSocket(path); err == nil {
		t.Fatal("expected refusal")
	}
	data, err := os.ReadFile(path) //nolint:gosec // This test owns the temporary path.
	if err != nil || string(data) != "do not replace" {
		t.Fatalf("file changed: %q err=%v", data, err)
	}
}

func TestDefaultConfigurationOpensNoNetworkListener(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-no-network-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	ctx, cancel := context.WithCancel(context.Background())
	instance, err := Open(ctx, cfg, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if instance.httpIngress != nil {
		t.Fatal("default configuration created an HTTP listener")
	}
	cancel()
	if err := instance.Close(); err != nil {
		t.Fatal(err)
	}
}

func serveTestDaemon(t *testing.T, cfg config.Config) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	instance, err := Open(ctx, cfg, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(ctx) }()
	return func() {
		cancel()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}
}

func dialReadyClient(t *testing.T, socketPath string) *clientpkg.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := clientpkg.DialUnix(ctx, socketPath)
	if err != nil {
		t.Fatal(err)
	}
	for {
		_, healthErr := client.System.Health(ctx, &emptypb.Empty{})
		if healthErr == nil {
			return client
		}
		if ctx.Err() != nil {
			_ = client.Close()
			t.Fatalf("daemon was not ready: %v", healthErr)
		}
		if status.Code(healthErr) != codes.Unavailable {
			_ = client.Close()
			t.Fatal(healthErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForRunEvent(t *testing.T, client *clientpkg.Client, runUID string, after uint64, eventType string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: runUID, AfterSequence: after})
	if err != nil {
		t.Fatal(err)
	}
	seen := []string{}
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			t.Fatalf("wait for %s after %v: %v", eventType, seen, receiveErr)
		}
		seen = append(seen, event.GetType()+":"+string(event.GetPayloadJson()))
		if event.GetType() == eventType {
			return
		}
	}
}

type githubFixture struct {
	server   *httptest.Server
	mu       sync.Mutex
	comments map[int][]map[string]any
	pulls    []map[string]any
}

func newGitHubFixture(t *testing.T) *githubFixture {
	t.Helper()
	fixture := &githubFixture{comments: map[int][]map[string]any{}}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer fixture-token" {
			http.Error(writer, `{"message":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		var issueNumber int
		if _, err := fmt.Sscanf(request.URL.Path, "/repos/acme/widget/issues/%d", &issueNumber); err == nil && !strings.HasSuffix(request.URL.Path, "/comments") {
			_ = json.NewEncoder(writer).Encode(map[string]any{"number": issueNumber, "title": fmt.Sprintf("Issue %d", issueNumber), "body": "fixture", "html_url": fmt.Sprintf("https://example.invalid/issues/%d", issueNumber), "state": "open"})
			return
		}
		if _, err := fmt.Sscanf(request.URL.Path, "/repos/acme/widget/issues/%d/comments", &issueNumber); err == nil {
			switch request.Method {
			case http.MethodGet:
				_ = json.NewEncoder(writer).Encode(fixture.comments[issueNumber])
			case http.MethodPost:
				var payload map[string]any
				_ = json.NewDecoder(request.Body).Decode(&payload)
				created := map[string]any{"id": len(fixture.comments[issueNumber]) + 1, "html_url": fmt.Sprintf("https://example.invalid/comments/%d/%d", issueNumber, len(fixture.comments[issueNumber])+1), "body": payload["body"]}
				fixture.comments[issueNumber] = append(fixture.comments[issueNumber], created)
				writer.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(writer).Encode(created)
			default:
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}
			return
		}
		if request.URL.Path == "/repos/acme/widget/pulls" {
			switch request.Method {
			case http.MethodGet:
				_ = json.NewEncoder(writer).Encode(fixture.pulls)
			case http.MethodPost:
				var payload map[string]any
				_ = json.NewDecoder(request.Body).Decode(&payload)
				created := map[string]any{"number": len(fixture.pulls) + 1, "html_url": fmt.Sprintf("https://example.invalid/pulls/%d", len(fixture.pulls)+1), "body": payload["body"], "head": map[string]any{"ref": payload["head"]}}
				fixture.pulls = append(fixture.pulls, created)
				writer.WriteHeader(http.StatusCreated)
				_ = json.NewEncoder(writer).Encode(created)
			default:
				writer.WriteHeader(http.StatusMethodNotAllowed)
			}
			return
		}
		http.NotFound(writer, request)
	}))
	return fixture
}

func installDaemonPlugin(t *testing.T, client *clientpkg.Client, bundle []byte, name string) {
	t.Helper()
	stream, err := client.Plugins.Install(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	const chunkSize = 1 << 20
	for offset := 0; offset < len(bundle); offset += chunkSize {
		end := min(offset+chunkSize, len(bundle))
		if err := stream.Send(&controlv1alpha1.PluginUploadRequest{BundleChunk: bundle[offset:end], Final: end == len(bundle)}); err != nil {
			t.Fatal(err)
		}
	}
	installed, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatal(err)
	}
	if installed.GetName() != name {
		t.Fatalf("installed %q instead of %q", installed.GetName(), name)
	}
	if _, err := client.Plugins.Enable(context.Background(), &controlv1alpha1.PluginRequest{Name: name, Version: "0.1.0"}); err != nil {
		t.Fatal(err)
	}
}

func applyClientResource(t *testing.T, client *clientpkg.Client, source string) *controlv1alpha1.ResourceDocument {
	t.Helper()
	response, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: []byte(source)})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetDiagnostics()) > 0 {
		t.Fatalf("apply diagnostics: %+v", response.GetDiagnostics())
	}
	return response.GetResource()
}

func runDaemonGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.CommandContext(context.Background(), "git", args...) //nolint:gosec // Test constructs fixed git argv without a shell.
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}

func daemonPluginBundle(t *testing.T, name string, capabilities []string) []byte {
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
	manifest := pluginbundle.Manifest{
		APIVersion: pluginbundle.APIVersion, Name: name, Version: "0.1.0",
		Protocol: pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: capabilities,
		Platforms: []pluginbundle.Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:])}},
	}
	bundle, err := pluginbundle.Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

package daemon

import (
	"bytes"
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
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/backup"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/githubplugin"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/pluginmanager"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	triggercontroller "github.com/alexrett/orchigram/internal/trigger"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestMain(m *testing.M) {
	if os.Getenv(pluginsdk.Handshake.MagicCookieKey) == pluginsdk.Handshake.MagicCookieValue {
		executable, err := os.Executable()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "resolve plugin test executable:", err)
			os.Exit(3)
		}
		if err := os.WriteFile(filepath.Join(filepath.Dir(executable), "process.pid"), []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "write plugin test pid marker:", err)
			os.Exit(3)
		}
		name := filepath.Base(filepath.Dir(filepath.Dir(executable)))
		catalogPlugin, _ := firstparty.Find(name)
		config := pluginsdk.Config{Metadata: pluginsdk.Metadata{
			Name: name, Version: "0.1.0", Capabilities: catalogPlugin.Capabilities,
			Actions: catalogPlugin.Actions, Triggers: catalogPlugin.Triggers,
		}}
		switch name {
		case "exec":
			config.Task = &pluginruntime.Exec{Runner: process.NewRunner()}
		case "agent-command":
			config.Agent = &pluginruntime.Agent{Runner: process.NewRunner()}
		case "github":
			githubRuntime := &githubplugin.Runtime{Runner: process.NewRunner()}
			config.Task = githubRuntime
			config.Trigger = githubRuntime
		case "http":
			config.Task = &pluginruntime.HTTP{}
		default:
			os.Exit(2)
		}
		pluginsdk.Serve(config)
		return
	}
	os.Exit(m.Run())
}

func TestOpenClosesActivatedPluginsWhenLateInitializationFails(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-open-cleanup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	if err := os.MkdirAll(cfg.StateDir, 0o750); err != nil {
		t.Fatal(err)
	}
	state, err := store.Open(filepath.Join(cfg.StateDir, "orchigram.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	manager := pluginmanager.New(state, cfg.StateDir)
	record, err := manager.Install(context.Background(), daemonPluginBundle(t, "exec", []string{"task.exec.run"}))
	if err != nil {
		manager.Close()
		_ = state.Close()
		t.Fatal(err)
	}
	if err := state.ActivatePlugin(context.Background(), record.Name, record.Version); err != nil {
		manager.Close()
		_ = state.Close()
		t.Fatal(err)
	}
	manager.Close()
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(cfg.StateDir, "workflows.sqlite"), 0o750); err != nil {
		t.Fatal(err)
	}
	instance, err := Open(context.Background(), cfg, nil)
	if err == nil {
		_ = instance.Close()
		t.Fatal("expected workflow database initialization to fail")
	}
	pidFile := filepath.Join(cfg.StateDir, "plugins", "exec", record.Version, "process.pid")
	data, readErr := os.ReadFile(pidFile) //nolint:gosec // The test owns the temporary path.
	if readErr != nil {
		t.Fatalf("activated plugin did not report its pid: %v", readErr)
	}
	pid, parseErr := strconv.Atoi(string(data))
	if parseErr != nil {
		t.Fatalf("parse plugin pid %q: %v", data, parseErr)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		signalErr := syscall.Kill(pid, 0)
		if errors.Is(signalErr, syscall.ESRCH) {
			break
		}
		if signalErr != nil {
			t.Fatalf("probe plugin process %d: %v", pid, signalErr)
		}
		if time.Now().After(deadline) {
			t.Fatalf("plugin process %d survived failed daemon initialization", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSlackWeekdayFlowAcceptance(t *testing.T) {
	tests := []struct {
		name              string
		statuses          []int
		wantPhase         string
		wantRequests      int
		wantFailureStatus int
	}{
		{name: "200 ok", statuses: []int{http.StatusOK}, wantPhase: "succeeded", wantRequests: 1},
		{name: "503 then 200", statuses: []int{http.StatusServiceUnavailable, http.StatusOK}, wantPhase: "succeeded", wantRequests: 2},
		{name: "permanent non-2xx", statuses: []int{http.StatusBadGateway}, wantPhase: "failed", wantRequests: 4, wantFailureStatus: http.StatusBadGateway},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runSlackWeekdayFlowCase(t, test.statuses, test.wantPhase, test.wantRequests, test.wantFailureStatus)
		})
	}
}

type slackHTTPRequest struct {
	body           []byte
	contentType    string
	idempotencyKey string
}

func runSlackWeekdayFlowCase(t *testing.T, statuses []int, wantPhase string, wantRequests, wantFailureStatus int) {
	t.Helper()
	const (
		reminder = "Review one risky assumption before you ship today."
	)
	sentinel := strings.Join([]string{"slack", "webhook", "credential", "sentinel"}, "-")
	var mu sync.Mutex
	requests := []slackHTTPRequest{}
	receiver := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		body, err := io.ReadAll(request.Body)
		if err != nil {
			t.Errorf("read receiver body: %v", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		mu.Lock()
		requests = append(requests, slackHTTPRequest{body: append([]byte(nil), body...), contentType: request.Header.Get("Content-Type"), idempotencyKey: request.Header.Get("Idempotency-Key")})
		attempt := len(requests)
		statusCode := statuses[min(attempt-1, len(statuses)-1)]
		mu.Unlock()
		writer.WriteHeader(statusCode)
		if statusCode == http.StatusOK {
			_, _ = writer.Write([]byte("ok"))
		} else {
			_, _ = writer.Write([]byte(http.StatusText(statusCode)))
		}
	}))
	defer receiver.Close()
	webhookURL := receiver.URL + "/" + sentinel
	t.Setenv("ORCHIGRAM_SLACK_WEBHOOK_URL", webhookURL)

	root, err := os.MkdirTemp("/tmp", "orchigram-slack-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	instance, stop := serveTestDaemonInstance(t, cfg)
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()
	installDaemonPlugin(t, client, daemonPluginBundle(t, "agent-command", []string{"agent.codex", "agent.claude", "agent.command"}), "agent-command")
	installDaemonPlugin(t, client, daemonPluginBundle(t, "http", []string{"task.http.request"}), "http")

	projections := [][]byte{}
	apply := func(source string) {
		resourceDocument := applyClientResource(t, client, source)
		projections = append(projections, append([]byte(nil), resourceDocument.GetJson()...))
	}
	apply(`apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: slack-webhook-url}
spec: {backend: env, key: ORCHIGRAM_SLACK_WEBHOOK_URL}
`)
	apply(fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: weekday-reminder-writer}
spec:
  type: command
  executable: /bin/sh
  args:
    - -c
    - %q
    - orchigram-slack-fixture
    - "{prompt}"
`, `printf '%s\n' '{"type":"result","result":"`+reminder+`"}'`))
	flowYAML, err := os.ReadFile(filepath.Join("..", "..", "examples", "slack", "weekday-flow.yaml")) //nolint:gosec // Test loads the shipped Flow.
	if err != nil {
		t.Fatal(err)
	}
	apply(string(flowYAML))
	triggerYAML, err := os.ReadFile(filepath.Join("..", "..", "examples", "slack", "weekday-trigger.yaml")) //nolint:gosec // Test loads the shipped Trigger.
	if err != nil {
		t.Fatal(err)
	}
	triggerDocument := applyClientResource(t, client, string(triggerYAML))
	projections = append(projections, append([]byte(nil), triggerDocument.GetJson()...))
	for _, projection := range projections {
		assertSecretAbsent(t, "resource projection", projection, sentinel, webhookURL)
	}

	triggerResource, err := resource.DecodeTrigger(triggerDocument.GetJson())
	if err != nil {
		t.Fatal(err)
	}
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	tomorrow := time.Now().In(berlin).AddDate(0, 0, 1)
	for tomorrow.Weekday() == time.Saturday || tomorrow.Weekday() == time.Sunday {
		tomorrow = tomorrow.AddDate(0, 0, 1)
	}
	occurrence := time.Date(tomorrow.Year(), tomorrow.Month(), tomorrow.Day(), 9, 0, 0, 0, berlin)
	if _, err := instance.store.EnsureTriggerState(context.Background(), triggerDocument.GetKey().GetUid(), triggerDocument.GetGeneration(), true, occurrence.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := instance.store.AdvanceTriggerCursor(context.Background(), triggerDocument.GetKey().GetUid(), occurrence.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := instance.triggers.ReconcileSchedules(context.Background(), occurrence.Add(30*time.Second)); err != nil {
		t.Fatal(err)
	}
	receipt := waitForTriggerReceipt(t, client, triggerDocument.GetKey().GetUid())
	expectedOccurrence := triggercontroller.OccurrenceIdentity(triggerResource, occurrence)
	if receipt.GetOccurrenceId() != expectedOccurrence {
		t.Fatalf("schedule occurrence=%q want=%q", receipt.GetOccurrenceId(), expectedOccurrence)
	}
	if receipt.GetRunUid() == "" {
		t.Fatal("schedule receipt has no outbox Run UID")
	}
	if !receipt.GetDeduplicated() {
		t.Fatal("schedule receipt does not enforce stable occurrence idempotency")
	}
	runUID := receipt.GetRunUid()
	events := collectRunEventsUntil(t, client, runUID, "run."+wantPhase)
	summary, err := client.Runs.Reconcile(context.Background(), &controlv1alpha1.ReconcileRequest{RunUid: runUID})
	if err != nil {
		t.Fatal(err)
	}
	if summary.GetPhase() != wantPhase {
		t.Fatalf("run phase=%q want=%q", summary.GetPhase(), wantPhase)
	}
	for _, event := range events {
		assertSecretAbsent(t, "run event", event.GetPayloadJson(), sentinel, webhookURL)
	}

	mu.Lock()
	gotRequests := append([]slackHTTPRequest(nil), requests...)
	mu.Unlock()
	if len(gotRequests) != wantRequests {
		t.Fatalf("requests=%d want=%d", len(gotRequests), wantRequests)
	}
	var expectedPayload any
	if err := json.Unmarshal([]byte(`{"text":"`+reminder+`","blocks":[{"type":"section","text":{"type":"plain_text","text":"`+reminder+`","emoji":true}}]}`), &expectedPayload); err != nil {
		t.Fatal(err)
	}
	for index, request := range gotRequests {
		if request.contentType != "application/json" {
			t.Errorf("request %d Content-Type=%q", index+1, request.contentType)
		}
		if request.idempotencyKey == "" {
			t.Errorf("request %d has empty Idempotency-Key", index+1)
		}
		var payload map[string]any
		if err := json.Unmarshal(request.body, &payload); err != nil {
			t.Fatalf("request %d JSON: %v", index+1, err)
		}
		validateSlackPayload(t, payload)
		if !reflect.DeepEqual(any(payload), expectedPayload) {
			t.Errorf("request %d payload=%s", index+1, request.body)
		}
		if index > 0 {
			if !bytes.Equal(request.body, gotRequests[0].body) {
				t.Errorf("request %d payload differs from first attempt", index+1)
			}
			if request.idempotencyKey != gotRequests[0].idempotencyKey {
				t.Errorf("request %d idempotency key differs from first attempt", index+1)
			}
		}
	}

	failedIndex := -1
	succeededIndex := -1
	foundHTTPOutput := false
	failurePayloads := []byte{}
	for index, event := range events {
		if event.GetNodeId() == "notify" && event.GetType() == "node.failed" {
			if failedIndex == -1 {
				failedIndex = index
			}
			failurePayloads = append(failurePayloads, event.GetPayloadJson()...)
		}
		if event.GetType() == "run.succeeded" {
			succeededIndex = index
		}
		if event.GetNodeId() == "notify" && event.GetType() == "node.completed" {
			var output struct {
				Status int    `json:"status"`
				Body   string `json:"body"`
			}
			if err := json.Unmarshal(event.GetPayloadJson(), &output); err != nil {
				t.Fatal(err)
			}
			if output.Status != http.StatusOK || output.Body != "ok" {
				t.Errorf("HTTP output=%+v", output)
			}
			foundHTTPOutput = true
		}
	}
	if wantPhase == "succeeded" && !foundHTTPOutput {
		t.Error("successful run did not expose HTTP 200 ok output")
	}
	if len(statuses) > 1 && statuses[0] != http.StatusOK {
		if failedIndex == -1 || succeededIndex == -1 || failedIndex >= succeededIndex {
			t.Fatalf("retry event order failed=%d succeeded=%d", failedIndex, succeededIndex)
		}
	}
	if wantFailureStatus != 0 {
		statusText := strconv.Itoa(wantFailureStatus)
		if !bytes.Contains(failurePayloads, []byte(statusText)) {
			t.Errorf("failure diagnostics do not contain HTTP status %s: %s", statusText, failurePayloads)
		}
		if len(failurePayloads) == 0 {
			t.Error("permanent failure emitted no node diagnostics")
		}
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	stop()
	stopped = true
	assertFilesDoNotContainSecret(t, root, sentinel, webhookURL)
}

func validateSlackPayload(t *testing.T, payload map[string]any) {
	t.Helper()
	fallback, ok := payload["text"].(string)
	if !ok || strings.TrimSpace(fallback) == "" || utf8.RuneCountInString(fallback) > 4000 {
		t.Fatalf("invalid Slack fallback text %#v", payload["text"])
	}
	blocks, ok := payload["blocks"].([]any)
	if !ok || len(blocks) != 1 {
		t.Fatalf("invalid Slack blocks %#v", payload["blocks"])
	}
	section, ok := blocks[0].(map[string]any)
	if !ok || section["type"] != "section" {
		t.Fatalf("invalid Slack section %#v", blocks[0])
	}
	textObject, ok := section["text"].(map[string]any)
	if !ok || textObject["type"] != "plain_text" || textObject["emoji"] != true {
		t.Fatalf("invalid Slack plain-text object %#v", section["text"])
	}
	text, ok := textObject["text"].(string)
	if !ok || strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > 3000 {
		t.Fatalf("invalid Slack section text %#v", textObject["text"])
	}
}

func collectRunEventsUntil(t *testing.T, client *clientpkg.Client, runUID, terminalType string) []*controlv1alpha1.RunEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: runUID})
	if err != nil {
		t.Fatal(err)
	}
	events := []*controlv1alpha1.RunEvent{}
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			t.Fatalf("watch run events: %v", receiveErr)
		}
		events = append(events, event)
		if event.GetType() == terminalType {
			return events
		}
	}
}

func assertSecretAbsent(t *testing.T, surface string, data []byte, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if secret != "" && bytes.Contains(data, []byte(secret)) {
			t.Errorf("%s contains test webhook sentinel", surface)
		}
	}
}

func assertFilesDoNotContainSecret(t *testing.T, root string, secrets ...string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(path) //nolint:gosec // Test scans its own state, logs, and artifacts.
		if readErr != nil {
			return readErr
		}
		assertSecretAbsent(t, path, data, secrets...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
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
	installations, err := client.Resources.List(context.Background(), &controlv1alpha1.ListRequest{Kind: "PluginInstallation", Limit: 10})
	if err != nil || len(installations.GetResources()) != 1 {
		t.Fatalf("PluginInstallation list=%+v err=%v", installations, err)
	}
	installation := installations.GetResources()[0]
	before := decodePluginInstallationProjection(t, installation.GetJson())
	if before.Spec.Enabled == nil || *before.Spec.Enabled || before.Status.Phase != "Installed" || before.Status.ObservedGeneration != before.Metadata.Generation {
		t.Fatalf("installed projection=%+v", before)
	}
	watchContext, cancelWatch := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelWatch()
	watch, err := client.Resources.Watch(watchContext, &controlv1alpha1.WatchRequest{Kind: "PluginInstallation", AfterRevision: installations.GetRevision()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Plugins.Enable(context.Background(), &controlv1alpha1.PluginRequest{Name: "exec", Version: "0.1.0"}); err != nil {
		t.Fatal(err)
	}
	var active pluginInstallationProjection
	for active.Status.Phase != "Active" {
		event, recvErr := watch.Recv()
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		active = decodePluginInstallationProjection(t, event.GetResource().GetJson())
	}
	if active.Metadata.Generation != before.Metadata.Generation+1 || active.Metadata.ResourceVersion <= installation.GetResourceVersion() || active.Status.ObservedGeneration != active.Metadata.Generation {
		t.Fatalf("active projection=%+v", active)
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
	artifact, err := os.ReadFile(filepath.Join(cfg.StateDir, "artifacts", run.GetUid(), "execute", "iteration-0", "attempt-1", "raw.log")) //nolint:gosec // Test-owned daemon state path.
	if err != nil || string(artifact) != "durable-plugin-flow\n" {
		t.Fatalf("artifact=%q err=%v", artifact, err)
	}
	attempts, err := client.Runs.ListAttempts(context.Background(), &controlv1alpha1.ListAttemptsRequest{RunUid: run.GetUid()})
	if err != nil || len(attempts.GetAttempts()) != 1 {
		t.Fatalf("attempts=%+v err=%v", attempts, err)
	}
	attempt := attempts.GetAttempts()[0]
	if attempt.GetNodeId() != "execute" || attempt.GetLogicalIteration() != 0 || attempt.GetAttempt() != 1 || attempt.GetFrameworkAttempt() != 1 || attempt.GetPhase() != "succeeded" || attempt.GetExitOutcome() != "exited" {
		t.Fatalf("attempt evidence=%+v", attempt)
	}
	artifacts, err := client.Runs.ListArtifacts(context.Background(), &controlv1alpha1.ListArtifactsRequest{RunUid: run.GetUid()})
	if err != nil || len(artifacts.GetArtifacts()) != 1 {
		t.Fatalf("artifacts=%+v err=%v", artifacts, err)
	}
	metadata := artifacts.GetArtifacts()[0]
	if metadata.GetNodeId() != "execute" || metadata.GetAttempt() != 1 || metadata.GetName() != "raw.log" || metadata.GetSizeBytes() != int64(len(artifact)) {
		t.Fatalf("artifact metadata=%+v", metadata)
	}
	digest := sha256.Sum256(artifact)
	if metadata.GetSha256() != hex.EncodeToString(digest[:]) {
		t.Fatalf("artifact digest=%q want=%q", metadata.GetSha256(), hex.EncodeToString(digest[:]))
	}
	download, err := client.Runs.GetArtifact(context.Background(), &controlv1alpha1.GetArtifactRequest{Uid: metadata.GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	var downloaded bytes.Buffer
	for {
		chunk, receiveErr := download.Recv()
		if errors.Is(receiveErr, io.EOF) {
			break
		}
		if receiveErr != nil {
			t.Fatal(receiveErr)
		}
		downloaded.Write(chunk.GetData())
	}
	if !bytes.Equal(downloaded.Bytes(), artifact) {
		t.Fatalf("downloaded artifact=%q want=%q", downloaded.Bytes(), artifact)
	}

	eventContext, cancelEvents := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelEvents()
	events, err := client.Runs.WatchEvents(eventContext, &controlv1alpha1.WatchRunRequest{Uid: run.GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	seenPluginEvents := map[string]bool{}
	for {
		event, receiveErr := events.Recv()
		if receiveErr != nil {
			t.Fatalf("replay persisted plugin events: %v", receiveErr)
		}
		if strings.HasPrefix(event.GetType(), "plugin.") {
			seenPluginEvents[event.GetType()] = true
		}
		if event.GetType() == "run.succeeded" {
			break
		}
	}
	for _, eventType := range []string{"plugin.task.started", "plugin.task.log.stdout", "plugin.task.completed"} {
		if !seenPluginEvents[eventType] {
			t.Fatalf("missing durable %s event; saw %+v", eventType, seenPluginEvents)
		}
	}
}

func TestMissingPluginInstallationIsPersistedWithControllerStatus(t *testing.T) {
	t.Parallel()
	root, err := os.MkdirTemp("/tmp", "orchigram-missing-plugin-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	stop := serveTestDaemon(t, cfg)
	defer stop()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()
	response, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: []byte(`apiVersion: orchigram.dev/v1alpha1
kind: PluginInstallation
metadata: {name: missing-plugin}
spec:
  plugin: missing
  version: 1.0.0
  digest: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  enabled: false
`)})
	if err != nil || len(response.GetDiagnostics()) != 1 || response.GetDiagnostics()[0].GetCode() != "bundle_missing" || response.GetDiagnostics()[0].GetSeverity() != controlv1alpha1.Diagnostic_SEVERITY_WARNING {
		t.Fatalf("apply response=%+v err=%v", response, err)
	}
	var current *controlv1alpha1.ResourceDocument
	var projection pluginInstallationProjection
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		current, err = client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: response.GetResource().GetKey()})
		if err == nil {
			projection = decodePluginInstallationProjection(t, current.GetJson())
			if projection.Status.Phase == "Error" && projection.Status.ObservedGeneration == projection.Metadata.Generation {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	if projection.Status.Phase != "Error" || projection.Status.ObservedGeneration != projection.Metadata.Generation {
		t.Fatalf("projection did not reconcile: %+v err=%v", projection, err)
	}
	healthResponse, err := client.System.Health(context.Background(), &emptypb.Empty{})
	if err != nil || healthResponse.GetReady() {
		t.Fatalf("health=%+v err=%v", healthResponse, err)
	}
	if _, err := client.Resources.Delete(context.Background(), &controlv1alpha1.DeleteRequest{Key: response.GetResource().GetKey(), ExpectedResourceVersion: current.GetResourceVersion()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: response.GetResource().GetKey()}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted missing PluginInstallation error=%v", err)
	}
}

type pluginInstallationProjection struct {
	APIVersion string                          `json:"apiVersion"`
	Kind       string                          `json:"kind"`
	Metadata   resource.ObjectMeta             `json:"metadata"`
	Spec       resource.PluginInstallationSpec `json:"spec"`
	Status     struct {
		ObservedGeneration uint64 `json:"observedGeneration"`
		Phase              string `json:"phase"`
	} `json:"status"`
}

func decodePluginInstallationProjection(t *testing.T, data []byte) pluginInstallationProjection {
	t.Helper()
	var projection pluginInstallationProjection
	if err := json.Unmarshal(data, &projection); err != nil {
		t.Fatal(err)
	}
	return projection
}

func TestFlakyPluginRetriesKeepDistinctEvidenceAcrossRestart(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-plugin-retry-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	marker := filepath.Join(root, "first-attempt-finished")
	script := filepath.Join(root, "flaky-plugin-task")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nif [ -f \"$1\" ]; then\n  printf 'second-attempt\\n'\n  exit 0\nfi\nprintf 'first-attempt\\n'\n: > \"$1\"\nexit 17\n"), 0o700); err != nil { //nolint:gosec // Test-owned fixed fixture executable.
		t.Fatal(err)
	}

	cfg := config.Development(filepath.Join(root, "state"))
	stop := serveTestDaemon(t, cfg)
	client := dialReadyClient(t, cfg.SocketPath)
	installDaemonPlugin(t, client, daemonPluginBundle(t, "exec", []string{"task.exec.run"}), "exec")
	applyClientResource(t, client, fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: plugin-retry-evidence}
spec:
  nodes:
    - id: flaky
      uses: exec.run
      timeout: 10s
      retry: {limit: 1, backoff: 10ms}
      with:
        argv: [%s, %s]
`, strconv.Quote(script), strconv.Quote(marker)))
	run, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "plugin-retry-evidence", InputJson: []byte(`{}`), IdempotencyKey: "plugin-retry-evidence"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, client, run.GetUid(), 0, "run.succeeded")

	attemptResponse, err := client.Runs.ListAttempts(context.Background(), &controlv1alpha1.ListAttemptsRequest{RunUid: run.GetUid()})
	if err != nil || len(attemptResponse.GetAttempts()) != 2 {
		t.Fatalf("attempts=%+v err=%v", attemptResponse, err)
	}
	attempts := attemptResponse.GetAttempts()
	if attempts[0].GetAttempt() != 1 || attempts[0].GetFrameworkAttempt() != 1 || attempts[0].GetPhase() != "failed" || attempts[0].GetExitOutcome() != "exited" || attempts[1].GetAttempt() != 2 || attempts[1].GetFrameworkAttempt() != 2 || attempts[1].GetPhase() != "succeeded" || attempts[1].GetExitOutcome() != "exited" {
		t.Fatalf("attempt evidence=%+v", attempts)
	}
	if attempts[0].GetIdempotencyKey() == "" || attempts[0].GetIdempotencyKey() != attempts[1].GetIdempotencyKey() {
		t.Fatalf("logical idempotency identity changed: %+v", attempts)
	}
	artifactResponse, err := client.Runs.ListArtifacts(context.Background(), &controlv1alpha1.ListArtifactsRequest{RunUid: run.GetUid()})
	if err != nil || len(artifactResponse.GetArtifacts()) != 2 {
		t.Fatalf("artifacts=%+v err=%v", artifactResponse, err)
	}
	artifactByAttempt := map[uint32]string{}
	for _, metadata := range artifactResponse.GetArtifacts() {
		download, downloadErr := client.Runs.GetArtifact(context.Background(), &controlv1alpha1.GetArtifactRequest{Uid: metadata.GetUid()})
		if downloadErr != nil {
			t.Fatal(downloadErr)
		}
		var content bytes.Buffer
		for {
			chunk, receiveErr := download.Recv()
			if errors.Is(receiveErr, io.EOF) {
				break
			}
			if receiveErr != nil {
				t.Fatal(receiveErr)
			}
			content.Write(chunk.GetData())
		}
		artifactByAttempt[metadata.GetAttempt()] = content.String()
	}
	if artifactByAttempt[1] != "first-attempt\n" || artifactByAttempt[2] != "second-attempt\n" {
		t.Fatalf("attempt artifacts=%+v", artifactByAttempt)
	}

	eventContext, cancelEvents := context.WithTimeout(context.Background(), 5*time.Second)
	eventStream, err := client.Runs.WatchEvents(eventContext, &controlv1alpha1.WatchRunRequest{Uid: run.GetUid()})
	if err != nil {
		cancelEvents()
		t.Fatal(err)
	}
	pluginEvents := map[uint32][]string{}
	completedTransitions := 0
	for {
		event, receiveErr := eventStream.Recv()
		if receiveErr != nil {
			cancelEvents()
			t.Fatalf("replay retry evidence: %v", receiveErr)
		}
		if strings.HasPrefix(event.GetType(), "plugin.") {
			pluginEvents[event.GetAttempt()] = append(pluginEvents[event.GetAttempt()], event.GetType())
		}
		if event.GetType() == "node.completed" {
			completedTransitions++
		}
		if event.GetType() == "run.succeeded" {
			break
		}
	}
	cancelEvents()
	if !reflect.DeepEqual(pluginEvents[1], []string{"plugin.task.started", "plugin.task.log.stdout", "plugin.task.process", "plugin.task.failed"}) || !reflect.DeepEqual(pluginEvents[2], []string{"plugin.task.started", "plugin.task.log.stdout", "plugin.task.completed"}) {
		t.Fatalf("structured attempt streams=%+v", pluginEvents)
	}
	if completedTransitions != 1 {
		t.Fatalf("successful local transitions=%d", completedTransitions)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	stop()
	restartedStop := serveTestDaemon(t, cfg)
	defer restartedStop()
	restarted := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = restarted.Close() }()
	replayedRun, err := restarted.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "plugin-retry-evidence", InputJson: []byte(`{}`), IdempotencyKey: "plugin-retry-evidence"})
	if err != nil || replayedRun.GetUid() != run.GetUid() {
		t.Fatalf("idempotent restart run=%+v err=%v", replayedRun, err)
	}
	restartedAttempts, err := restarted.Runs.ListAttempts(context.Background(), &controlv1alpha1.ListAttemptsRequest{RunUid: run.GetUid()})
	if err != nil || len(restartedAttempts.GetAttempts()) != 2 {
		t.Fatalf("restart attempts=%+v err=%v", restartedAttempts, err)
	}
}

func TestRunCancellationTerminatesAgentProcessGroupAndRemainsCancelled(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-cancel-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	stop := serveTestDaemon(t, cfg)
	defer stop()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()
	installDaemonPlugin(t, client, daemonPluginBundle(t, "agent-command", []string{"agent.codex", "agent.claude", "agent.command"}), "agent-command")
	pidFile := filepath.Join(root, "agent-pids")
	applyClientResource(t, client, fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: cancellation-agent}
spec:
  type: command
  executable: /bin/sh
  args: ["-c", "sleep 30 & child=$!; printf '%%s %%s' \"$$\" \"$child\" > \"$1\"; wait", "orchigram-agent", "{prompt}"]
`))
	applyClientResource(t, client, fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: cancellation-flow}
spec:
  nodes:
    - id: agent
      uses: agent-command.run
      timeout: 1m
      with:
        profile: cancellation-agent
        prompt: %q
`, pidFile))
	started, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "cancellation-flow", InputJson: []byte(`{}`), IdempotencyKey: "cancellation-e2e"})
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	deadline := time.Now().Add(10 * time.Second)
	for len(pids) != 2 {
		data, readErr := os.ReadFile(pidFile) //nolint:gosec // Test-owned synchronization file.
		if readErr == nil {
			fields := strings.Fields(string(data))
			if len(fields) == 2 {
				leader, leaderErr := strconv.Atoi(fields[0])
				child, childErr := strconv.Atoi(fields[1])
				if leaderErr == nil && childErr == nil {
					pids = []int{leader, child}
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("agent process group did not start: %v", readErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := client.Runs.Cancel(context.Background(), &controlv1alpha1.CancelRunRequest{RunUid: started.GetUid(), Reason: "operator cancellation test"}); err != nil {
		t.Fatal(err)
	}
	for _, pid := range pids {
		deadline := time.Now().Add(5 * time.Second)
		for {
			err := syscall.Kill(pid, 0)
			if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("agent process %d survived Run cancellation", pid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	time.Sleep(200 * time.Millisecond)
	summary, err := client.Runs.Reconcile(context.Background(), &controlv1alpha1.ReconcileRequest{RunUid: started.GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	if summary.GetPhase() != "cancelled" {
		t.Fatalf("late activity completion regressed run phase to %q", summary.GetPhase())
	}
	waitForRunEvent(t, client, started.GetUid(), 0, "plugin.agent.failed")
	attempts, err := client.Runs.ListAttempts(context.Background(), &controlv1alpha1.ListAttemptsRequest{RunUid: started.GetUid()})
	if err != nil || len(attempts.GetAttempts()) != 1 {
		t.Fatalf("cancelled attempts=%+v err=%v", attempts, err)
	}
	attempt := attempts.GetAttempts()[0]
	if attempt.GetPhase() != "failed" || !strings.HasPrefix(attempt.GetExitOutcome(), "cancelled:") || attempt.GetCompletedAt() == nil {
		t.Fatalf("cancelled attempt evidence=%+v", attempt)
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

func TestBackupRestoreReconcilesWaitingApproval(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-backup-restore-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	state := filepath.Join(root, "state")
	cfg := config.Development(state)
	stopFirst := serveTestDaemon(t, cfg)
	first := dialReadyClient(t, cfg.SocketPath)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: backup-approval}
spec:
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 1m}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	if response, err := first.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument}); err != nil || len(response.GetDiagnostics()) != 0 {
		t.Fatalf("apply response=%+v err=%v", response, err)
	}
	run, err := first.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "backup-approval", InputJson: []byte(`{}`), IdempotencyKey: "backup-approval"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, first, run.GetUid(), 0, "approval.waiting")
	backupResponse, err := first.System.Backup(context.Background(), &controlv1alpha1.BackupRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if backupResponse.GetPath() == "" || len(backupResponse.GetSha256()) != 64 {
		t.Fatalf("backup response=%+v", backupResponse)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	stopFirst()

	restoredState := filepath.Join(root, "restored")
	if err := backup.Restore(context.Background(), backupResponse.GetPath(), restoredState); err != nil {
		t.Fatal(err)
	}
	restoredConfig := config.Development(restoredState)
	stopSecond := serveTestDaemon(t, restoredConfig)
	defer stopSecond()
	second := dialReadyClient(t, restoredConfig.SocketPath)
	defer func() { _ = second.Close() }()
	if _, err := second.Runs.Approve(context.Background(), &controlv1alpha1.ApprovalRequest{RunUid: run.GetUid(), NodeId: "approval", Reason: "restored backup"}); err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, second, run.GetUid(), 0, "run.succeeded")
}

func TestRetryTimerSurvivesDaemonRestart(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-retry-restart-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	stopFirst := serveTestDaemon(t, cfg)
	first := dialReadyClient(t, cfg.SocketPath)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: retry-restart}
spec:
  nodes:
    - id: fail
      uses: core.fail
      timeout: 5s
      retry: {limit: 2, backoff: 750ms}
`)
	if response, err := first.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument}); err != nil || len(response.GetDiagnostics()) != 0 {
		t.Fatalf("apply response=%+v err=%v", response, err)
	}
	run, err := first.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "retry-restart", InputJson: []byte(`{}`), IdempotencyKey: "retry-restart"})
	if err != nil {
		t.Fatal(err)
	}
	waitForRunEvent(t, first, run.GetUid(), 0, "node.failed")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	stopFirst()

	stopSecond := serveTestDaemon(t, cfg)
	defer stopSecond()
	second := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = second.Close() }()
	waitForRunEvent(t, second, run.GetUid(), 0, "run.failed")
	events, err := second.Runs.WatchEvents(context.Background(), &controlv1alpha1.WatchRunRequest{Uid: run.GetUid()})
	if err != nil {
		t.Fatal(err)
	}
	failedAttempts := 0
	for {
		event, err := events.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if event.GetNodeId() == "fail" && event.GetType() == "node.failed" {
			failedAttempts++
		}
		if event.GetType() == "run.failed" {
			break
		}
	}
	// The activity interrupted by shutdown may consume a framework attempt
	// before product code appends node.failed. At least one persisted failure
	// before and after restart proves the timer/history resumed to terminal.
	if failedAttempts < 2 {
		t.Fatalf("failed attempts=%d", failedAttempts)
	}
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
	if err := first.store.AdvanceTriggerCursor(firstContext, triggerDocument.Metadata.UID, now.Add(-2*time.Minute)); err != nil {
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
	stopped := false
	defer func() {
		if !stopped {
			stop()
		}
	}()
	client := dialReadyClient(t, cfg.SocketPath)
	clientClosed := false
	defer func() {
		if !clientClosed {
			_ = client.Close()
		}
	}()
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
  inputSchema:
    type: object
    properties:
      repository:
        type: object
        properties:
          owner: {type: string}
          name: {type: string}
        required: [owner, name]
        additionalProperties: false
      issue:
        type: object
        properties:
          number: {type: integer}
          title: {type: string}
          body: {type: string}
          html_url: {type: string}
          state: {type: string}
        required: [number, title, body, html_url, state]
        additionalProperties: false
    required: [repository, issue]
    additionalProperties: false
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
      retry: {limit: 1, backoff: 10ms}
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
      retry: {limit: 1, backoff: 10ms}
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
      retry: {limit: 1, backoff: 10ms}
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
	trigger := applyClientResource(t, client, fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: github-fixture-ready}
spec:
  flow: github-fixture
  provider:
    plugin: github
    config:
      owner: acme
      repository: widget
      apiBase: %q
      label: orchigram:ready
      pollInterval: 10ms
      replayExisting: true
      tokenSecret: token
      secretRefs: {token: github-token}
`, fixture.server.URL))
	receipt := waitForTriggerReceipt(t, client, trigger.GetKey().GetUid())
	if receipt.GetOccurrenceId() != "github:acme/widget:issue-label-event:7001" {
		t.Fatalf("provider occurrence=%q", receipt.GetOccurrenceId())
	}
	waitForRunEvent(t, client, receipt.GetRunUid(), 0, "approval.waiting")
	workspace := filepath.Join(cfg.StateDir, "workspaces", receipt.GetRunUid())
	if _, err := os.Stat(filepath.Join(workspace, "implemented.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("implementation ran before approval: %v", err)
	}
	branch := "orchigram/issue-42-" + strings.ReplaceAll(receipt.GetRunUid(), "-", "")[:8]
	if output := runDaemonGit(t, "", "--git-dir", origin, "for-each-ref", "--format=%(refname)", "refs/heads/"+branch); strings.TrimSpace(output) != "" {
		t.Fatalf("branch was pushed before approval: %s", output)
	}
	if _, err := client.Runs.Approve(context.Background(), &controlv1alpha1.ApprovalRequest{RunUid: receipt.GetRunUid(), NodeId: "approval", Reason: "fixture approval"}); err != nil {
		t.Fatal(err)
	}
	events := collectRunEventsUntil(t, client, receipt.GetRunUid(), "run.succeeded")
	if head := strings.TrimSpace(runDaemonGit(t, "", "--git-dir", origin, "rev-parse", "refs/heads/"+branch)); head == "" {
		t.Fatal("approved run did not push its deterministic branch")
	}
	assertNodeCompletedBefore(t, events, "tests", "push")
	for _, nodeID := range []string{"publish", "pr", "final"} {
		if !nodeCompletedWithReconciled(t, events, nodeID) {
			t.Errorf("%s did not reconcile its hidden marker after an ambiguous remote success", nodeID)
		}
	}
	fixture.mu.Lock()
	commentCount, pullCount := len(fixture.comments[42]), len(fixture.pulls)
	pullHead := ""
	if pullCount == 1 {
		head, _ := fixture.pulls[0]["head"].(map[string]any)
		pullHead, _ = head["ref"].(string)
	}
	fixture.mu.Unlock()
	if commentCount != 2 || pullCount != 1 || pullHead != branch {
		t.Fatalf("approved GitHub effects: comments=%d pulls=%d head=%q", commentCount, pullCount, pullHead)
	}

	fixture.mu.Lock()
	fixture.events = append(fixture.events, map[string]any{
		"id": 7002, "event": "labeled", "created_at": time.Now().UTC().Format(time.RFC3339),
		"label": map[string]any{"name": "orchigram:ready"},
		"issue": map[string]any{"number": 42, "title": "Reject tracer", "body": "fixture", "html_url": "https://example.invalid/issues/42", "state": "open"},
	})
	fixture.mu.Unlock()
	receipts := waitForTriggerReceipts(t, client, trigger.GetKey().GetUid(), 2)
	var rejectedReceipt *controlv1alpha1.TriggerReceipt
	for _, candidate := range receipts {
		if candidate.GetOccurrenceId() == "github:acme/widget:issue-label-event:7002" {
			rejectedReceipt = candidate
			break
		}
	}
	if rejectedReceipt == nil {
		t.Fatalf("second provider occurrence missing from receipts: %+v", receipts)
	}
	if rejectedReceipt.GetRunUid() == receipt.GetRunUid() {
		t.Fatal("second provider occurrence reused the first Run UID")
	}
	waitForRunEvent(t, client, rejectedReceipt.GetRunUid(), 0, "approval.waiting")
	rejectedWorkspace := filepath.Join(cfg.StateDir, "workspaces", rejectedReceipt.GetRunUid())
	rejectedBranch := "orchigram/issue-42-" + strings.ReplaceAll(rejectedReceipt.GetRunUid(), "-", "")[:8]
	if _, err := client.Runs.Reject(context.Background(), &controlv1alpha1.ApprovalRequest{RunUid: rejectedReceipt.GetRunUid(), NodeId: "approval", Reason: "fixture rejection"}); err != nil {
		t.Fatal(err)
	}
	rejectedEvents := collectRunEventsUntil(t, client, rejectedReceipt.GetRunUid(), "run.rejected")
	if _, err := os.Stat(filepath.Join(rejectedWorkspace, "implemented.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("implementation ran after rejection: %v", err)
	}
	if output := runDaemonGit(t, "", "--git-dir", origin, "for-each-ref", "--format=%(refname)", "refs/heads/"+rejectedBranch); strings.TrimSpace(output) != "" {
		t.Fatalf("rejected run pushed deterministic branch: %s", output)
	}
	if !nodeCompletedWithReconciled(t, rejectedEvents, "publish") {
		t.Error("rejected run did not reconcile its planning comment")
	}
	for _, nodeID := range []string{"implement", "tests", "push", "pr", "final"} {
		for _, event := range rejectedEvents {
			if event.GetNodeId() == nodeID && event.GetType() == "node.completed" {
				t.Errorf("rejected run completed forbidden node %q", nodeID)
			}
		}
	}
	fixture.mu.Lock()
	rejectedCommentCount, rejectedPullCount := len(fixture.comments[42]), len(fixture.pulls)
	fixture.mu.Unlock()
	if rejectedCommentCount != 3 || rejectedPullCount != 1 {
		t.Fatalf("rejected GitHub effects: comments=%d pulls=%d", rejectedCommentCount, rejectedPullCount)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	clientClosed = true
	stop()
	stopped = true

	stopRestarted := serveTestDaemon(t, cfg)
	defer stopRestarted()
	restarted := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = restarted.Close() }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		receiptsContext, cancelReceipts := context.WithTimeout(context.Background(), time.Second)
		receipts, err := restarted.Triggers.Receipts(receiptsContext, &controlv1alpha1.ReceiptRequest{TriggerUid: trigger.GetKey().GetUid(), Limit: 10})
		cancelReceipts()
		if err != nil {
			t.Fatal(err)
		}
		runsContext, cancelRuns := context.WithTimeout(context.Background(), time.Second)
		runs, err := restarted.Runs.List(runsContext, &controlv1alpha1.ListRunsRequest{Limit: 10})
		cancelRuns()
		if err != nil {
			t.Fatal(err)
		}
		seenRuns := map[string]bool{}
		for _, candidate := range receipts.GetReceipts() {
			seenRuns[candidate.GetRunUid()] = true
		}
		if len(receipts.GetReceipts()) != 2 || !seenRuns[receipt.GetRunUid()] || !seenRuns[rejectedReceipt.GetRunUid()] || len(runs.GetRuns()) != 2 {
			t.Fatalf("provider replay created duplicate state: receipts=%d runs=%d", len(receipts.GetReceipts()), len(runs.GetRuns()))
		}
		time.Sleep(25 * time.Millisecond)
	}
	fixture.mu.Lock()
	restartedPulls := len(fixture.pulls)
	fixture.mu.Unlock()
	if restartedPulls != 1 {
		t.Fatalf("provider restart created another PR: pulls=%d", restartedPulls)
	}
}

func waitForTriggerReceipt(t *testing.T, client *clientpkg.Client, triggerUID string) *controlv1alpha1.TriggerReceipt {
	t.Helper()
	return waitForTriggerReceipts(t, client, triggerUID, 1)[0]
}

func waitForTriggerReceipts(t *testing.T, client *clientpkg.Client, triggerUID string, wanted int) []*controlv1alpha1.TriggerReceipt {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		response, err := client.Triggers.Receipts(context.Background(), &controlv1alpha1.ReceiptRequest{TriggerUid: triggerUID, Limit: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(response.GetReceipts()) == wanted {
			return response.GetReceipts()
		}
		if len(response.GetReceipts()) > wanted {
			t.Fatalf("trigger created %d receipts, want %d", len(response.GetReceipts()), wanted)
		}
		if time.Now().After(deadline) {
			t.Fatalf("trigger created %d receipts, want %d", len(response.GetReceipts()), wanted)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertNodeCompletedBefore(t *testing.T, events []*controlv1alpha1.RunEvent, first, second string) {
	t.Helper()
	firstIndex, secondIndex := -1, -1
	for index, event := range events {
		if event.GetType() != "node.completed" {
			continue
		}
		if event.GetNodeId() == first {
			firstIndex = index
		}
		if event.GetNodeId() == second {
			secondIndex = index
		}
	}
	if firstIndex == -1 || secondIndex == -1 || firstIndex >= secondIndex {
		t.Fatalf("node completion order %s=%d %s=%d", first, firstIndex, second, secondIndex)
	}
}

func nodeCompletedWithReconciled(t *testing.T, events []*controlv1alpha1.RunEvent, nodeID string) bool {
	t.Helper()
	for _, event := range events {
		if event.GetNodeId() != nodeID || event.GetType() != "node.completed" {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal(event.GetPayloadJson(), &payload); err != nil {
			t.Fatal(err)
		}
		return payload["reconciled"] == true && strings.Contains(fmt.Sprint(payload["marker"]), "<!-- orchigram:run=")
	}
	return false
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

func TestPublicHealthReflectsControllerFailureAndRecovery(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-health-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	instance, stop := serveTestDaemonInstance(t, cfg)
	defer stop()
	client := dialReadyClient(t, cfg.SocketPath)
	defer func() { _ = client.Close() }()
	document, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: health-overflow}
spec:
  flow: unused
  schedule: {cron: "* * * * *", timezone: UTC, startingDeadline: 1h}
`))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := instance.store.Apply(context.Background(), document, store.ApplyOptions{RequestID: "health-overflow"})
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := resource.DecodeTrigger(applied.JSON)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := instance.store.AdvanceTriggerCursor(context.Background(), trigger.Metadata.UID, now.Add(-20*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	waitForSystemHealth(t, client, false, "controllers/schedules", "reconcile_failed")
	if err := instance.store.AdvanceTriggerCursor(context.Background(), trigger.Metadata.UID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := instance.triggers.ReconcileSchedulesNow(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	waitForSystemHealth(t, client, true, "", "")
}

func serveTestDaemon(t *testing.T, cfg config.Config) func() {
	t.Helper()
	_, stop := serveTestDaemonInstance(t, cfg)
	return stop
}

func serveTestDaemonInstance(t *testing.T, cfg config.Config) (*Daemon, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	instance, err := Open(ctx, cfg, nil)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(ctx) }()
	return instance, func() {
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
		health, healthErr := client.System.Health(ctx, &emptypb.Empty{})
		if healthErr == nil && health.GetReady() {
			return client
		}
		if ctx.Err() != nil {
			_ = client.Close()
			t.Fatalf("daemon was not ready: %v", healthErr)
		}
		if healthErr != nil && status.Code(healthErr) != codes.Unavailable {
			_ = client.Close()
			t.Fatal(healthErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForSystemHealth(t *testing.T, client *clientpkg.Client, ready bool, path, code string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	var health *controlv1alpha1.HealthResponse
	var err error
	for time.Now().Before(deadline) {
		callContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		health, err = client.System.Health(callContext, &emptypb.Empty{})
		cancel()
		if err == nil && health.GetReady() == ready {
			if ready {
				return
			}
			for _, diagnostic := range health.GetDiagnostics() {
				if diagnostic.GetPath() == path && diagnostic.GetCode() == code {
					return
				}
			}
		}
		if err != nil && status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("health probe failed before convergence: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("health did not converge to ready=%t path=%q code=%q: response=%+v err=%v", ready, path, code, health, err)
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
	events   []map[string]any
}

func newGitHubFixture(t *testing.T) *githubFixture {
	t.Helper()
	fixture := &githubFixture{
		comments: map[int][]map[string]any{},
		events: []map[string]any{{
			"id": 7001, "event": "labeled", "created_at": time.Now().UTC().Add(-time.Second).Format(time.RFC3339),
			"label": map[string]any{"name": "orchigram:ready"},
			"issue": map[string]any{"number": 42, "title": "Implement tracer", "body": "fixture", "html_url": "https://example.invalid/issues/42", "state": "open"},
		}},
	}
	fixture.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer fixture-token" {
			http.Error(writer, `{"message":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/repos/acme/widget/issues/events" {
			_ = json.NewEncoder(writer).Encode(fixture.events)
			return
		}
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
				writer.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(writer).Encode(map[string]any{"message": "ambiguous fixture response after storing comment"})
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
				writer.WriteHeader(http.StatusServiceUnavailable)
				_ = json.NewEncoder(writer).Encode(map[string]any{"message": "ambiguous fixture response after storing pull request"})
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
	for _, diagnostic := range response.GetDiagnostics() {
		if diagnostic.GetSeverity() == controlv1alpha1.Diagnostic_SEVERITY_ERROR || diagnostic.GetSeverity() == controlv1alpha1.Diagnostic_SEVERITY_UNSPECIFIED {
			t.Fatalf("apply diagnostics: %+v", response.GetDiagnostics())
		}
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

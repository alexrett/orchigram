package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/daemon"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOperatorCLIThroughRealUnixSocket(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	testRoot, err := os.MkdirTemp("/tmp", "orchigram-cli-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })
	cfg := config.Development(filepath.Join(testRoot, "state"))
	server, err := daemon.Open(ctx, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case serveErr := <-served:
			if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	})

	waitForCLIHealth(t, cfg.SocketPath)
	manifests := testFlow("alpha", "team", "cli", true) + "\n---\n" + testFlow("beta", "team", "cli", false)
	stdout, stderr, err := executeCLI(ctx, cfg.SocketPath, manifests, "apply", "-f", "-")
	if err != nil {
		t.Fatalf("multi-document apply: %v stderr=%s", err, stderr)
	}
	if got := strings.Count(stdout, "apiVersion:"); got != 2 || !strings.Contains(stdout, "---\n") {
		t.Fatalf("multi-document output count=%d:\n%s", got, stdout)
	}

	invalidBatch := testFlow("must-not-exist", "team", "cli", false) + `
---
apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: invalid}
spec:
  unknown: true
  nodes: [{id: done, uses: core.noop}]
`
	_, _, err = executeCLI(ctx, cfg.SocketPath, invalidBatch, "apply", "-f", "-")
	if err == nil || !strings.Contains(err.Error(), "no resources were applied") {
		t.Fatalf("invalid batch error=%v", err)
	}
	_, _, err = executeCLI(ctx, cfg.SocketPath, "", "get", "flow", "must-not-exist")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("validation-first apply persisted a prefix: %v", err)
	}

	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "get", "flows", "-A", "--selector", "team=cli", "--limit", "1")
	if err != nil {
		t.Fatalf("paginated list: %v stderr=%s", err, stderr)
	}
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "\talpha\t") || !strings.Contains(lines[1], "\tbeta\t") {
		t.Fatalf("unexpected paginated list:\n%s", stdout)
	}

	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "export", "flow", "alpha", "beta")
	if err != nil {
		t.Fatalf("export: %v stderr=%s", err, stderr)
	}
	if strings.Contains(stdout, "status:") || strings.Count(stdout, "apiVersion:") != 2 {
		t.Fatalf("export is not desired-state multi-document YAML:\n%s", stdout)
	}
	if _, stderr, err = executeCLI(ctx, cfg.SocketPath, stdout, "apply", "-f", "-"); err != nil {
		t.Fatalf("round-trip exported resources with per-document CAS: %v stderr=%s", err, stderr)
	}

	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "watch", "flow", "-A", "--after-revision", "0", "--count", "1")
	if err != nil {
		t.Fatalf("resource watch replay: %v stderr=%s", err, stderr)
	}
	if !strings.Contains(stdout, "\tADDED\tFlow\tdefault\talpha") {
		t.Fatalf("unexpected resource event: %s", stdout)
	}

	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "flow", "graph", "alpha")
	if err != nil {
		t.Fatalf("flow graph: %v stderr=%s", err, stderr)
	}
	if !strings.Contains(stdout, "[start]--> [finish]") || !strings.Contains(stdout, "NODES") {
		t.Fatalf("unexpected graph:\n%s", stdout)
	}

	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "run", "start", "alpha", "--idempotency-key", "cli-integration")
	if err != nil {
		t.Fatalf("start run: %v stderr=%s", err, stderr)
	}
	runUID := strings.TrimSpace(stdout)
	waitForRunPhase(ctx, t, cfg.SocketPath, runUID, "succeeded")
	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "run", "list", "--flow", "alpha", "--phase", "succeeded")
	if err != nil || !strings.Contains(stdout, runUID) {
		t.Fatalf("run list: %v stderr=%s stdout=%s", err, stderr, stdout)
	}
	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "--output", "json", "run", "describe", runUID)
	if err != nil {
		t.Fatalf("run describe: %v stderr=%s", err, stderr)
	}
	var description struct {
		UID           string          `json:"uid"`
		ExecutionPlan json.RawMessage `json:"executionPlan"`
		Attempts      []any           `json:"attempts"`
		Artifacts     []any           `json:"artifacts"`
	}
	if err := json.Unmarshal([]byte(stdout), &description); err != nil {
		t.Fatalf("decode run description: %v output=%s", err, stdout)
	}
	if description.UID != runUID || len(description.ExecutionPlan) == 0 || description.Attempts == nil || description.Artifacts == nil {
		t.Fatalf("incomplete run description: %+v", description)
	}

	triggerJSON := testTrigger("cli-trigger", "alpha")
	stdout, stderr, err = executeCLI(ctx, cfg.SocketPath, triggerJSON, "--output", "json", "apply", "-f", "-")
	if err != nil {
		t.Fatalf("apply trigger: %v stderr=%s", err, stderr)
	}
	var trigger struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(stdout), &trigger); err != nil || trigger.Metadata.UID == "" {
		t.Fatalf("decode applied trigger: %v output=%s", err, stdout)
	}
	if _, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "trigger", "receipts", trigger.Metadata.UID); err != nil {
		t.Fatalf("trigger receipts: %v stderr=%s", err, stderr)
	}

	_, _, err = executeCLI(ctx, cfg.SocketPath, "", "plugin", "describe", "missing")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("plugin describe did not traverse UDS API: %v", err)
	}
	if _, stderr, err = executeCLI(ctx, cfg.SocketPath, "", "system", "health"); err != nil {
		t.Fatalf("system health: %v stderr=%s", err, stderr)
	}
}

func TestReadManifestDocumentsAndLosslessOutput(t *testing.T) {
	t.Parallel()
	documents, err := readManifestDocuments(strings.NewReader("---\n\n---\n" + testFlow("one", "", "", false) + "\n---\n"))
	if err != nil || len(documents) != 1 {
		t.Fatalf("documents=%d err=%v", len(documents), err)
	}
	data := []byte(`{"apiVersion":"orchigram.dev/v1alpha1","kind":"Flow","metadata":{"name":"max","resourceVersion":18446744073709551615},"spec":{"nodes":[{"id":"done","uses":"core.noop"}]}}`)
	var output bytes.Buffer
	if err := printDocument(&output, data, "yaml"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "resourceVersion: 18446744073709551615") {
		t.Fatalf("uint64 changed in YAML output: %s", output.String())
	}
}

func executeCLI(ctx context.Context, socket, stdin string, args ...string) (string, string, error) {
	command := NewRoot()
	var stdout, stderr bytes.Buffer
	command.SetOut(&stdout)
	command.SetErr(&stderr)
	command.SetIn(strings.NewReader(stdin))
	command.SetArgs(append([]string{"--socket", socket}, args...))
	err := command.ExecuteContext(ctx)
	return stdout.String(), stderr.String(), err
}

func waitForCLIHealth(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, _, err := executeCLI(ctx, socket, "", "system", "health")
		cancel()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("daemon never became healthy: %v", err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func waitForRunPhase(ctx context.Context, t *testing.T, socket, uid, phase string) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		stdout, _, err := executeCLI(ctx, socket, "", "run", "reconcile", uid)
		if err == nil && strings.Contains(stdout, "\t"+phase+"\t") {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never reached %s: %v output=%s", uid, phase, err, stdout)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func testFlow(name, labelKey, labelValue string, edge bool) string {
	labels := ""
	if labelKey != "" {
		labels = fmt.Sprintf("\n  labels: {%s: %s}", labelKey, labelValue)
	}
	nodes := "    - {id: done, uses: core.noop}"
	edges := ""
	if edge {
		nodes = "    - {id: start, uses: core.noop}\n    - {id: finish, uses: core.noop}"
		edges = "\n  edges:\n    - {from: start, to: finish}"
	}
	return fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata:
  name: %s%s
spec:
  nodes:
%s%s
`, name, labels, nodes, edges)
}

func testTrigger(name, flow string) string {
	return fmt.Sprintf(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: %s}
spec:
  flow: %s
  enabled: false
  schedule:
    cron: "0 9 * * 1-5"
    timezone: UTC
`, name, flow)
}

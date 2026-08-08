package workspace

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/process"
)

func TestCheckoutCommitAndPushReconcileDeterministicBranch(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	origin := filepath.Join(root, "origin.git")
	seed := filepath.Join(root, "seed")
	runGit(t, "", "init", "--bare", "--initial-branch=main", origin)
	runGit(t, "", "init", "--initial-branch=main", seed)
	if err := os.WriteFile(filepath.Join(seed, "README.md"), []byte("seed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, seed, "add", "README.md")
	runGit(t, seed, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "seed")
	runGit(t, seed, "remote", "add", "origin", origin)
	runGit(t, seed, "push", "origin", "main")

	manager := &Manager{Root: filepath.Join(root, "workspaces"), Runner: process.NewRunner()}
	checkout, err := manager.Checkout(context.Background(), CheckoutRequest{RequestID: "checkout", RunUID: "12345678-abcd", CloneURL: origin, DefaultBranch: "main", IssueNumber: 42}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if checkout.Branch != "orchigram/issue-42-12345678" {
		t.Fatalf("branch=%q", checkout.Branch)
	}
	if err := os.WriteFile(filepath.Join(checkout.Path, "change.txt"), []byte("implementation\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	first, err := manager.CommitPush(context.Background(), CommitRequest{RequestID: "commit", Path: checkout.Path, Branch: checkout.Branch, Message: "Implement issue #42"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if first.NoChange || first.Commit == "" {
		t.Fatalf("first result=%+v", first)
	}
	second, err := manager.CommitPush(context.Background(), CommitRequest{RequestID: "commit-retry", Path: checkout.Path, Branch: checkout.Branch, Message: "Implement issue #42"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !second.NoChange || second.Commit != first.Commit {
		t.Fatalf("retry result=%+v first=%+v", second, first)
	}
	remoteHead := runGit(t, "", "--git-dir", origin, "rev-parse", "refs/heads/"+checkout.Branch)
	if remoteHead != first.Commit+"\n" {
		t.Fatalf("remote head=%q commit=%q", remoteHead, first.Commit)
	}
}

func TestGitHubSmartHTTPUsesOperationScopedBasicCredential(t *testing.T) {
	t.Parallel()
	token := []byte("fixture-token-must-not-leak")
	seenAuthorization := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		seenAuthorization <- request.Header.Get("Authorization")
		http.Error(writer, "denied", http.StatusUnauthorized)
	}))
	defer server.Close()
	manager := &Manager{Root: t.TempDir(), Runner: process.NewRunner()}
	var output strings.Builder
	_, err := manager.gitResult(context.Background(), "auth-check", "", token, func(chunk process.Output) error {
		output.Write(chunk.Data)
		return nil
	}, "ls-remote", server.URL)
	if err == nil {
		t.Fatal("expected fixture smart-HTTP failure")
	}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("x-access-token:"+string(token)))
	if got := <-seenAuthorization; got != want {
		t.Fatalf("authorization=%q want=%q", got, want)
	}
	if strings.Contains(err.Error(), string(token)) || strings.Contains(output.String(), string(token)) || strings.Contains(server.URL, string(token)) {
		t.Fatalf("credential leaked: err=%q output=%q url=%q", err, output.String(), server.URL)
	}
	repository := filepath.Join(t.TempDir(), "repository")
	runGit(t, "", "init", repository)
	result, err := manager.gitResult(context.Background(), "config-check", repository, token, nil, "status", "--porcelain")
	if err != nil {
		t.Fatal(err)
	}
	config, err := os.ReadFile(filepath.Join(repository, ".git", "config")) //nolint:gosec // Test-owned Git configuration.
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(config), string(token)) || strings.Contains(string(result.Stdout), string(token)) || strings.Contains(string(result.Stderr), string(token)) {
		t.Fatalf("credential persisted or returned: config=%q stdout=%q stderr=%q", config, result.Stdout, result.Stderr)
	}
}

func TestGitOutputRedactsRawAndDerivedBasicCredentials(t *testing.T) {
	token := []byte("fixture-token")
	credential := githubBasicCredential(token)
	redacted := redactGitOutput([]byte("raw=fixture-token header="+credential), token)
	if strings.Contains(string(redacted), string(token)) || strings.Contains(string(redacted), credential) {
		t.Fatalf("git credential remained in redacted output: %q", redacted)
	}
}

func TestCheckoutRejectsHTTPUserinfo(t *testing.T) {
	t.Parallel()
	manager := &Manager{Root: t.TempDir(), Runner: process.NewRunner()}
	_, err := manager.Checkout(context.Background(), CheckoutRequest{RequestID: "request", RunUID: "run-123", CloneURL: "https://user:password@example.invalid/repo.git", IssueNumber: 1}, nil)
	if err == nil || !strings.Contains(err.Error(), "userinfo") {
		t.Fatalf("userinfo error=%v", err)
	}
}

func runGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.CommandContext(context.Background(), "git", args...) //nolint:gosec // Test constructs fixed git argv without a shell.
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}

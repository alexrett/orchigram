package workspace

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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

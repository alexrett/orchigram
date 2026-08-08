// Package workspace owns isolated git worktrees used by one durable run.
package workspace

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/alexrett/orchigram/internal/process"
)

var safeIdentity = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// EventSink receives raw git stdout/stderr without shell interpretation.
type EventSink func(process.Output) error

// Manager creates and reconciles one checkout per Run UID.
type Manager struct {
	Root   string
	Runner *process.Runner
}

// CheckoutRequest describes a deterministic issue branch checkout.
type CheckoutRequest struct {
	RequestID     string
	RunUID        string
	CloneURL      string
	DefaultBranch string
	IssueNumber   int
	Token         []byte
}

// CheckoutResult is safe to pass to later flow nodes.
type CheckoutResult struct {
	Path   string `json:"workspace"`
	Branch string `json:"branch"`
}

// CommitRequest reconciles one commit and deterministic remote branch.
type CommitRequest struct {
	RequestID string
	Path      string
	Branch    string
	Message   string
	Token     []byte
}

// CommitResult describes the pushed branch head.
type CommitResult struct {
	Branch   string `json:"branch"`
	Commit   string `json:"commit"`
	NoChange bool   `json:"noChange"`
}

// Checkout clones or resets the run-owned directory and checks out its stable branch.
func (m *Manager) Checkout(ctx context.Context, request CheckoutRequest, emit EventSink) (CheckoutResult, error) {
	if !safeIdentity.MatchString(request.RunUID) || request.CloneURL == "" || request.IssueNumber <= 0 {
		return CheckoutResult{}, errors.New("run UID, clone URL, and positive issue number are required")
	}
	if request.DefaultBranch == "" {
		request.DefaultBranch = "main"
	}
	if !safeIdentity.MatchString(request.DefaultBranch) {
		return CheckoutResult{}, errors.New("default branch contains unsupported characters")
	}
	cloneURL, err := url.Parse(request.CloneURL)
	if err != nil {
		return CheckoutResult{}, errors.New("clone URL is invalid")
	}
	if (cloneURL.Scheme == "http" || cloneURL.Scheme == "https") && cloneURL.User != nil {
		return CheckoutResult{}, errors.New("HTTP(S) clone URL must not contain userinfo")
	}
	root, err := filepath.Abs(m.Root)
	if err != nil || root == "." || root == string(filepath.Separator) {
		return CheckoutResult{}, errors.New("safe absolute workspace root is required")
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return CheckoutResult{}, err
	}
	directory := filepath.Join(root, request.RunUID)
	if err := ensureWithin(root, directory); err != nil {
		return CheckoutResult{}, err
	}
	branch := fmt.Sprintf("orchigram/issue-%d-%s", request.IssueNumber, shortID(request.RunUID))
	if _, err := os.Stat(filepath.Join(directory, ".git")); errors.Is(err, os.ErrNotExist) {
		if entries, readErr := os.ReadDir(directory); readErr == nil && len(entries) > 0 {
			return CheckoutResult{}, errors.New("workspace exists but is not a git checkout")
		}
		if err := m.git(ctx, request.RequestID, "", request.Token, emit, "clone", "--no-tags", "--single-branch", "--branch", request.DefaultBranch, request.CloneURL, directory); err != nil {
			return CheckoutResult{}, err
		}
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "checkout", "-b", branch); err != nil {
			return CheckoutResult{}, err
		}
	} else if err != nil {
		return CheckoutResult{}, err
	} else {
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "fetch", "--prune", "origin", request.DefaultBranch); err != nil {
			return CheckoutResult{}, err
		}
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "checkout", "-B", branch, "origin/"+request.DefaultBranch); err != nil {
			return CheckoutResult{}, err
		}
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "reset", "--hard", "origin/"+request.DefaultBranch); err != nil {
			return CheckoutResult{}, err
		}
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "clean", "-fdx"); err != nil {
			return CheckoutResult{}, err
		}
	}
	return CheckoutResult{Path: directory, Branch: branch}, nil
}

// CommitPush creates at most one commit for current changes and reconciles the branch push.
func (m *Manager) CommitPush(ctx context.Context, request CommitRequest, emit EventSink) (CommitResult, error) {
	root, err := filepath.Abs(m.Root)
	if err != nil {
		return CommitResult{}, err
	}
	directory, err := filepath.Abs(request.Path)
	if err != nil {
		return CommitResult{}, err
	}
	if err := ensureWithin(root, directory); err != nil {
		return CommitResult{}, err
	}
	if !strings.HasPrefix(request.Branch, "orchigram/issue-") || request.Message == "" {
		return CommitResult{}, errors.New("deterministic Orchigram branch and commit message are required")
	}
	status, err := m.gitResult(ctx, request.RequestID, directory, request.Token, emit, "status", "--porcelain")
	if err != nil {
		return CommitResult{}, err
	}
	noChange := strings.TrimSpace(string(status.Stdout)) == ""
	if !noChange {
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "add", "--all"); err != nil {
			return CommitResult{}, err
		}
		if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "-c", "user.name=Orchigram", "-c", "user.email=orchigram@localhost", "commit", "-m", request.Message); err != nil {
			return CommitResult{}, err
		}
	}
	if err := m.git(ctx, request.RequestID, directory, request.Token, emit, "push", "--set-upstream", "origin", "HEAD:refs/heads/"+request.Branch); err != nil {
		return CommitResult{}, err
	}
	head, err := m.gitResult(ctx, request.RequestID, directory, request.Token, emit, "rev-parse", "HEAD")
	if err != nil {
		return CommitResult{}, err
	}
	return CommitResult{Branch: request.Branch, Commit: strings.TrimSpace(string(head.Stdout)), NoChange: noChange}, nil
}

func (m *Manager) git(ctx context.Context, requestID, directory string, token []byte, emit EventSink, args ...string) error {
	_, err := m.gitResult(ctx, requestID, directory, token, emit, args...)
	return err
}

func (m *Manager) gitResult(ctx context.Context, requestID, directory string, token []byte, emit EventSink, args ...string) (process.Result, error) {
	if m.Runner == nil {
		m.Runner = process.NewRunner()
	}
	base := map[string]string{"PATH": "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin", "LANG": "C.UTF-8"}
	for _, key := range []string{"HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value := os.Getenv(key); value != "" {
			base[key] = value
		}
	}
	if len(token) > 0 {
		base["GIT_CONFIG_COUNT"] = "1"
		base["GIT_CONFIG_KEY_0"] = "http.extraHeader"
		base["GIT_CONFIG_VALUE_0"] = "Authorization: " + githubBasicCredential(token)
		base["GIT_TERMINAL_PROMPT"] = "0"
	}
	safeEmit := emit
	if emit != nil && len(token) > 0 {
		safeEmit = func(output process.Output) error {
			output.Data = redactGitOutput(output.Data, token)
			return emit(output)
		}
	}
	result, err := m.Runner.Run(ctx, requestID, process.Spec{Executable: "git", Args: args, Directory: directory, Environment: process.MinimalEnvironment(base, nil)}, safeEmit)
	if len(token) > 0 {
		result.Stdout = redactGitOutput(result.Stdout, token)
		result.Stderr = redactGitOutput(result.Stderr, token)
	}
	if err != nil {
		return result, fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return result, nil
}

func githubBasicCredential(token []byte) string {
	return "Basic " + base64.StdEncoding.EncodeToString(append([]byte("x-access-token:"), token...))
}

func redactGitOutput(value, token []byte) []byte {
	redacted := bytesReplace(value, token, []byte("[REDACTED]"))
	if len(token) == 0 {
		return redacted
	}
	return bytesReplace(redacted, []byte(githubBasicCredential(token)), []byte("[REDACTED]"))
}

func bytesReplace(value, old, replacement []byte) []byte {
	if len(old) == 0 {
		return append([]byte(nil), value...)
	}
	return []byte(strings.ReplaceAll(string(value), string(old), string(replacement)))
}

func ensureWithin(root, target string) error {
	relative, err := filepath.Rel(root, target)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("workspace path escapes its configured root")
	}
	return nil
}

func shortID(value string) string {
	value = strings.ReplaceAll(value, "-", "")
	if len(value) > 8 {
		return value[:8]
	}
	return value
}

package process

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestCancellationTerminatesDescendants(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	childPID := make(chan int, 1)
	finished := make(chan error, 1)
	go func() {
		_, err := runner.Run(ctx, "tree", Spec{
			Executable:  "/bin/sh",
			Args:        []string{"-c", "sleep 30 & child=$!; echo $child; wait"},
			Environment: []string{"PATH=/usr/bin:/bin"},
		}, func(output Output) error {
			if output.Stream == "stdout" {
				pid, parseErr := strconv.Atoi(strings.TrimSpace(string(output.Data)))
				if parseErr == nil {
					select {
					case childPID <- pid:
					default:
					}
				}
			}
			return nil
		})
		finished <- err
	}()
	var pid int
	select {
	case pid = <-childPID:
	case <-time.After(3 * time.Second):
		t.Fatal("child process did not start")
	}
	cancel()
	select {
	case err := <-finished:
		var exitErr *ExitError
		if !errors.As(err, &exitErr) {
			t.Fatalf("expected exit error, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process tree did not stop")
	}
	deadline := time.Now().Add(time.Second)
	for {
		err := syscall.Kill(pid, 0)
		if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("descendant %d is still alive", pid)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMinimalEnvironmentDoesNotInheritHost(t *testing.T) {
	t.Setenv("ORCHIGRAM_UNRELATED_SECRET", "must-not-leak")
	runner := NewRunner()
	result, err := runner.Run(context.Background(), "environment", Spec{
		Executable:  "/usr/bin/env",
		Environment: MinimalEnvironment(map[string]string{"PATH": "/usr/bin:/bin"}, map[string]string{"VISIBLE": "yes"}),
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	text := string(result.Stdout)
	if strings.Contains(text, "ORCHIGRAM_UNRELATED_SECRET") || !strings.Contains(text, "VISIBLE=yes") {
		t.Fatalf("unexpected environment: %s", text)
	}
}

func TestRunnerCapturesShortLivedOutput(t *testing.T) {
	t.Parallel()
	runner := NewRunner()
	for attempt := range 100 {
		result, err := runner.Run(context.Background(), "short-"+strconv.Itoa(attempt), Spec{
			Executable:  "/bin/sh",
			Args:        []string{"-c", "printf stdout; printf stderr >&2"},
			Environment: []string{"PATH=/usr/bin:/bin"},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if string(result.Stdout) != "stdout" || string(result.Stderr) != "stderr" {
			t.Fatalf("attempt %d: stdout=%q stderr=%q", attempt, result.Stdout, result.Stderr)
		}
	}
}

//go:build darwin || linux

package tui

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/daemon"
	"github.com/creack/pty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRealPTYCreatesFlowAt80x24(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-pty-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil && !errors.Is(serveErr, context.Canceled) {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)

	workingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot, err := filepath.Abs(filepath.Join(workingDirectory, "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(root, "orchigram")
	build := exec.CommandContext(context.Background(), "go", "build", "-o", binary, "./cmd/orchigram") //nolint:gosec // Fixed test argv builds the repository's own CLI.
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}

	command := exec.CommandContext(context.Background(), binary, "--socket", cfg.SocketPath) //nolint:gosec // The test owns both fixed paths.
	command.Env = append(os.Environ(), "TERM=xterm-256color", "HOME="+root)
	terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 24, Cols: 80})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = terminal.Close() }()
	var output synchronizedBuffer
	readDone := make(chan struct{})
	go func() {
		_, _ = output.ReadFrom(terminal)
		close(readDone)
	}()
	waitPTYText(t, &output, "Contexts")
	writePTY(t, terminal, []byte("n"))
	waitPTYText(t, &output, "Create resource")
	writePTY(t, terminal, []byte("\x1b[B\r"))
	waitPTYText(t, &output, "New Flow (strict YAML)")
	writePTY(t, terminal, []byte{0x13})
	waitPTYText(t, &output, "Created Flow/new-flow")

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, getErr := client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "new-flow"}})
		if getErr == nil {
			break
		}
		if status.Code(getErr) != codes.NotFound || time.Now().After(deadline) {
			t.Fatalf("created Flow not found: %v", getErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	writePTY(t, terminal, []byte("q"))
	exited := make(chan error, 1)
	go func() { exited <- command.Wait() }()
	select {
	case waitErr := <-exited:
		if waitErr != nil {
			t.Fatalf("TUI exit: %v", waitErr)
		}
	case <-time.After(5 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("TUI did not exit after q")
	}
	_ = terminal.Close()
	select {
	case <-readDone:
	case <-time.After(time.Second):
		t.Fatal("PTY reader did not stop")
	}
}

type synchronizedBuffer struct {
	mu sync.RWMutex
	bytes.Buffer
}

func (b *synchronizedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(data)
}

func (b *synchronizedBuffer) ReadFrom(file *os.File) (int64, error) {
	var total int64
	buffer := make([]byte, 32*1024)
	for {
		count, err := file.Read(buffer)
		if count > 0 {
			written, writeErr := b.Write(buffer[:count])
			total += int64(written)
			if writeErr != nil {
				return total, writeErr
			}
		}
		if err != nil {
			return total, err
		}
	}
}

func (b *synchronizedBuffer) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.Buffer.String()
}

func waitPTYText(t *testing.T, output *synchronizedBuffer, expected string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		text := output.String()
		if strings.Contains(text, expected) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("PTY output did not contain %q:\n%s", expected, text)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func writePTY(t *testing.T, terminal *os.File, data []byte) {
	t.Helper()
	if _, err := terminal.Write(data); err != nil {
		t.Fatal(err)
	}
}

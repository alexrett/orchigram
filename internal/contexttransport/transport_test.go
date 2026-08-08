package contexttransport

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/contextcfg"
)

func TestSSHArgumentsUseStreamLocalAndOptionTerminator(t *testing.T) {
	t.Parallel()
	arguments := sshArguments(contextcfg.SSHContext{
		Destination: "operator@example.test",
		Socket:      "/run/orchigram/orchigram.sock",
		Identity:    "/tmp/key with spaces",
	}, "/tmp/local.sock")
	wantTail := []string{"-i", "/tmp/key with spaces", "-L", "/tmp/local.sock:/run/orchigram/orchigram.sock", "--", "operator@example.test"}
	if !slices.Equal(arguments[len(arguments)-len(wantTail):], wantTail) {
		t.Fatalf("arguments tail = %#v", arguments)
	}
	if !slices.Contains(arguments, "StreamLocalBindUnlink=yes") {
		t.Fatalf("StreamLocalBindUnlink missing: %#v", arguments)
	}
}

func TestSSHContextReconnectsAtTheSameSocket(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "starts")
	t.Setenv("ORCHIGRAM_SSH_HELPER", "1")
	t.Setenv("ORCHIGRAM_SSH_HELPER_MARKER", marker)
	previousExecutable, previousPrefix := sshExecutable, sshArgumentPrefix
	sshExecutable = os.Args[0]
	sshArgumentPrefix = []string{"-test.run=^TestSSHHelperProcess$", "--"}
	t.Cleanup(func() {
		sshExecutable = previousExecutable
		sshArgumentPrefix = previousPrefix
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := Connect(ctx, contextcfg.Context{SSH: &contextcfg.SSHContext{
		Destination: "operator@example.test",
		Socket:      "/run/orchigram/orchigram.sock",
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connection.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		data, _ := os.ReadFile(marker) //nolint:gosec // Test reads its private reconnect marker.
		if len(data) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("SSH supervisor did not reconnect, marker=%q", data)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSSHHelperProcess(_ *testing.T) {
	if os.Getenv("ORCHIGRAM_SSH_HELPER") != "1" {
		return
	}
	arguments := os.Args
	forward := ""
	for index, argument := range arguments {
		if argument == "-L" && index+1 < len(arguments) {
			forward = arguments[index+1]
			break
		}
	}
	localSocket, _, ok := strings.Cut(forward, ":")
	if !ok {
		os.Exit(2)
	}
	var listenConfig net.ListenConfig
	listener, err := listenConfig.Listen(context.Background(), "unix", localSocket)
	if err != nil {
		os.Exit(3)
	}
	marker := os.Getenv("ORCHIGRAM_SSH_HELPER_MARKER")
	existing, _ := os.ReadFile(marker) //nolint:gosec // Helper uses a test-selected private marker.
	if err := os.WriteFile(marker, append(existing, 'x'), 0o600); err != nil {
		os.Exit(4)
	}
	if len(existing) == 0 {
		time.Sleep(100 * time.Millisecond)
		_ = listener.Close()
		os.Exit(5)
	}
	for {
		time.Sleep(time.Second)
	}
}

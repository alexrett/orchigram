// Package contexttransport connects the shared gRPC client through local Unix
// sockets or supervised OpenSSH StreamLocal forwarding.
package contexttransport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/contextcfg"
)

var (
	sshExecutable     = "ssh"
	sshArgumentPrefix []string
)

// Connection owns a client and any SSH forwarding process behind it.
type Connection struct {
	Client *clientpkg.Client
	cancel context.CancelFunc
	done   chan struct{}
	temp   string
}

// Connect resolves one context. SSH reconnects preserve the local socket path,
// allowing gRPC's normal backoff to recover the same ClientConn.
func Connect(ctx context.Context, selected contextcfg.Context) (*Connection, error) {
	if selected.SSH == nil {
		client, err := clientpkg.DialUnix(ctx, selected.Socket)
		if err != nil {
			return nil, err
		}
		return &Connection{Client: client}, nil
	}
	temporary, err := os.MkdirTemp("", "orchigram-ssh-")
	if err != nil {
		return nil, err
	}
	localSocket := filepath.Join(temporary, "o.sock")
	tunnelContext, cancel := context.WithCancel(ctx)
	connection := &Connection{cancel: cancel, done: make(chan struct{}), temp: temporary}
	ready := make(chan error, 1)
	go supervise(tunnelContext, *selected.SSH, localSocket, ready, connection.done)
	select {
	case err := <-ready:
		if err != nil {
			cancel()
			<-connection.done
			_ = os.RemoveAll(temporary)
			return nil, err
		}
	case <-ctx.Done():
		cancel()
		<-connection.done
		_ = os.RemoveAll(temporary)
		return nil, ctx.Err()
	}
	client, err := clientpkg.DialUnix(ctx, localSocket)
	if err != nil {
		cancel()
		<-connection.done
		_ = os.RemoveAll(temporary)
		return nil, err
	}
	connection.Client = client
	return connection, nil
}

// Close stops the tunnel, closes gRPC, and removes its private local socket.
func (c *Connection) Close() error {
	var result error
	if c.Client != nil {
		result = errors.Join(result, c.Client.Close())
	}
	if c.cancel != nil {
		c.cancel()
		if c.done != nil {
			select {
			case <-c.done:
			case <-time.After(5 * time.Second):
				result = errors.Join(result, errors.New("timed out stopping SSH context"))
			}
		}
	}
	if c.temp != "" {
		result = errors.Join(result, os.RemoveAll(c.temp))
	}
	return result
}

func supervise(ctx context.Context, selected contextcfg.SSHContext, localSocket string, initial chan<- error, done chan<- struct{}) {
	defer close(done)
	first := true
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		_ = os.Remove(localSocket)
		command, wait, stderr, err := startSSH(ctx, selected, localSocket)
		if err == nil {
			err = waitForSocket(ctx, localSocket, wait, 10*time.Second)
		}
		if first {
			first = false
			if err != nil {
				initial <- fmt.Errorf("open SSH context: %w: %s", err, stderr.String())
				return
			}
			initial <- nil
		}
		if err == nil {
			err = <-wait
			if err == nil {
				err = errors.New("SSH forwarding process exited")
			}
		}
		if command != nil && command.Process != nil && ctx.Err() != nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		slog.Warn("SSH context disconnected; reconnecting", "destination", selected.Destination, "error", err)
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 10*time.Second {
			backoff *= 2
		}
	}
}

func startSSH(ctx context.Context, selected contextcfg.SSHContext, localSocket string) (*exec.Cmd, <-chan error, *bytes.Buffer, error) {
	arguments := sshArguments(selected, localSocket)
	commandArguments := append([]string{}, sshArgumentPrefix...)
	commandArguments = append(commandArguments, arguments...)
	command := exec.CommandContext(ctx, sshExecutable, commandArguments...) //nolint:gosec // Validated context fields become distinct SSH argv; no shell is used.
	stderr := &bytes.Buffer{}
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return nil, nil, stderr, err
	}
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	return command, wait, stderr, nil
}

func sshArguments(selected contextcfg.SSHContext, localSocket string) []string {
	arguments := []string{
		"-N", "-T", "-o", "BatchMode=yes", "-o", "ExitOnForwardFailure=yes",
		"-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=3",
		"-o", "StreamLocalBindUnlink=yes",
	}
	if selected.Identity != "" {
		arguments = append(arguments, "-i", selected.Identity)
	}
	return append(arguments, "-L", localSocket+":"+selected.Socket, "--", selected.Destination)
}

func waitForSocket(ctx context.Context, path string, process <-chan error, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-process:
			if err == nil {
				return errors.New("SSH exited before opening its forwarding socket")
			}
			return err
		case <-timer.C:
			return errors.New("timed out waiting for SSH forwarding socket")
		case <-ticker.C:
			info, err := os.Stat(path)
			if err == nil && info.Mode()&os.ModeSocket != 0 {
				return nil
			}
		}
	}
}

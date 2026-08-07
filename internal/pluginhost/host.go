// Package pluginhost owns one isolated go-plugin client process.
package pluginhost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/hashicorp/go-hclog"
	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Process is a negotiated plugin connection and its lifecycle owner.
type Process struct {
	clients *pluginprotocol.Clients
	client  *hplugin.Client
	command *exec.Cmd
	once    sync.Once
}

// Launch verifies the executable digest, starts it with AutoMTLS, and negotiates protocol v1.
func Launch(ctx context.Context, executable, binaryDigest string) (*Process, *pluginv1alpha1.DescribeResponse, error) {
	checksum, err := hex.DecodeString(binaryDigest)
	if err != nil || len(checksum) != sha256.Size {
		return nil, nil, errors.New("plugin binary digest is invalid")
	}
	command := exec.CommandContext(context.WithoutCancel(ctx), executable)
	command.Env = process.MinimalEnvironment(hostEnvironment(), nil)
	configureProcessGroup(command)
	client := hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: pluginprotocol.Handshake,
		Plugins:         pluginprotocol.PluginSet(pluginprotocol.Servers{}),
		Cmd:             command,
		AllowedProtocols: []hplugin.Protocol{
			hplugin.ProtocolGRPC,
		},
		AutoMTLS:     true,
		SkipHostEnv:  true,
		StartTimeout: 10 * time.Second,
		Logger:       hclog.NewNullLogger(),
		SecureConfig: &hplugin.SecureConfig{Checksum: checksum, Hash: sha256.New()},
		SyncStdout:   nil,
		SyncStderr:   nil,
	})
	rpcClient, err := client.Client()
	if err != nil {
		client.Kill()
		return nil, nil, fmt.Errorf("start plugin: %w", err)
	}
	dispensed, err := rpcClient.Dispense(pluginprotocol.DispenseName)
	if err != nil {
		client.Kill()
		return nil, nil, fmt.Errorf("dispense plugin: %w", err)
	}
	clients, ok := dispensed.(*pluginprotocol.Clients)
	if !ok {
		client.Kill()
		return nil, nil, errors.New("plugin returned an unexpected client type")
	}
	describeContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	description, err := clients.Control.Describe(describeContext, &pluginv1alpha1.DescribeRequest{HostProtocol: &pluginv1alpha1.ProtocolRange{Minimum: 1, Maximum: 1}})
	if err != nil {
		client.Kill()
		return nil, nil, fmt.Errorf("negotiate plugin: %w", err)
	}
	return &Process{clients: clients, client: client, command: command}, description, nil
}

// Clients returns the business protocol clients for this process.
func (p *Process) Clients() *pluginprotocol.Clients { return p.clients }

// Exited reports process loss without propagating it to the daemon.
func (p *Process) Exited() bool { return p == nil || p.client.Exited() }

// Close asks the plugin to stop, then guarantees process cleanup.
func (p *Process) Close() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, _ = p.clients.Control.Shutdown(ctx, &pluginv1alpha1.ShutdownRequest{Deadline: timestamppb.New(time.Now().Add(2 * time.Second))})
		cancel()
		_ = signalProcessGroup(p.command, false)
		p.client.Kill()
	})
}

func hostEnvironment() map[string]string {
	result := map[string]string{"PATH": "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin", "LANG": "C.UTF-8"}
	for _, key := range []string{"HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value := os.Getenv(key); value != "" {
			result[key] = value
		}
	}
	return result
}

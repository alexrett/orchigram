// Package client connects CLI and TUI clients to local or forwarded daemon sockets.
package client

import (
	"context"
	"fmt"
	"net"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client exposes typed service clients and their shared connection.
type Client struct {
	Connection *grpc.ClientConn
	Resources  controlv1alpha1.ResourceServiceClient
	Flows      controlv1alpha1.FlowServiceClient
	Runs       controlv1alpha1.RunServiceClient
	Triggers   controlv1alpha1.TriggerServiceClient
	Plugins    controlv1alpha1.PluginServiceClient
	System     controlv1alpha1.SystemServiceClient
}

// DialUnix connects to a local Unix socket. SSH contexts forward to a temporary
// local Unix socket and use this exact same function.
func DialUnix(_ context.Context, socketPath string) (*Client, error) {
	connection, err := grpc.NewClient(
		"passthrough:///orchigram",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", socketPath)
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}
	return &Client{
		Connection: connection,
		Resources:  controlv1alpha1.NewResourceServiceClient(connection),
		Flows:      controlv1alpha1.NewFlowServiceClient(connection),
		Runs:       controlv1alpha1.NewRunServiceClient(connection),
		Triggers:   controlv1alpha1.NewTriggerServiceClient(connection),
		Plugins:    controlv1alpha1.NewPluginServiceClient(connection),
		System:     controlv1alpha1.NewSystemServiceClient(connection),
	}, nil
}

// Close closes the shared gRPC connection.
func (c *Client) Close() error { return c.Connection.Close() }

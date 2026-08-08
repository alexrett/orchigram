// Package plugin is the supported authoring SDK for Orchigram plugins.
package plugin

import (
	"context"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

const (
	// ProtocolVersion is the current host/plugin bootstrap protocol.
	ProtocolVersion = 1
	// DispenseName is the go-plugin capability containing Orchigram services.
	DispenseName = "orchigram"
)

// Handshake is shared by the public SDK and the Orchigram host.
var Handshake = hplugin.HandshakeConfig{
	ProtocolVersion:  ProtocolVersion,
	MagicCookieKey:   "ORCHIGRAM_PLUGIN_MAGIC_COOKIE",
	MagicCookieValue: "orchigram-plugin-v1",
}

// Servers is the protobuf service set registered in a plugin process. Most
// authors should use Config; Servers exists for host integration and advanced
// protobuf implementations.
type Servers struct {
	Control pluginv1alpha1.PluginControlServer
	Task    pluginv1alpha1.TaskProviderServer
	Trigger pluginv1alpha1.TriggerProviderServer
	Agent   pluginv1alpha1.AgentRuntimeServer
}

// Clients is the host-side typed view of a running plugin process.
type Clients struct {
	Control pluginv1alpha1.PluginControlClient
	Task    pluginv1alpha1.TaskProviderClient
	Trigger pluginv1alpha1.TriggerProviderClient
	Agent   pluginv1alpha1.AgentRuntimeClient
}

// Bridge registers Orchigram protobuf services with HashiCorp go-plugin.
type Bridge struct {
	hplugin.NetRPCUnsupportedPlugin
	Servers Servers
}

// GRPCServer registers every supplied service.
func (p *Bridge) GRPCServer(_ *hplugin.GRPCBroker, server *grpc.Server) error {
	if p.Servers.Control != nil {
		pluginv1alpha1.RegisterPluginControlServer(server, p.Servers.Control)
	}
	if p.Servers.Task != nil {
		pluginv1alpha1.RegisterTaskProviderServer(server, p.Servers.Task)
	}
	if p.Servers.Trigger != nil {
		pluginv1alpha1.RegisterTriggerProviderServer(server, p.Servers.Trigger)
	}
	if p.Servers.Agent != nil {
		pluginv1alpha1.RegisterAgentRuntimeServer(server, p.Servers.Agent)
	}
	return nil
}

// GRPCClient constructs typed clients over the negotiated connection.
func (*Bridge) GRPCClient(_ context.Context, _ *hplugin.GRPCBroker, connection *grpc.ClientConn) (any, error) {
	return &Clients{
		Control: pluginv1alpha1.NewPluginControlClient(connection),
		Task:    pluginv1alpha1.NewTaskProviderClient(connection),
		Trigger: pluginv1alpha1.NewTriggerProviderClient(connection),
		Agent:   pluginv1alpha1.NewAgentRuntimeClient(connection),
	}, nil
}

// Set returns a fresh transport bridge set.
func Set(servers Servers) hplugin.PluginSet {
	return hplugin.PluginSet{DispenseName: &Bridge{Servers: servers}}
}

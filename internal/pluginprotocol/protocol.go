// Package pluginprotocol binds Orchigram's protobuf services to HashiCorp
// go-plugin process bootstrap without leaking framework types into resources.
package pluginprotocol

import (
	"context"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc"
)

const (
	// ProtocolVersion is the current host/plugin bootstrap protocol.
	ProtocolVersion = 1
	// DispenseName is the single go-plugin capability containing all gRPC services.
	DispenseName = "orchigram"
)

// Handshake is intentionally independent from the public business protocol.
var Handshake = hplugin.HandshakeConfig{
	ProtocolVersion:  ProtocolVersion,
	MagicCookieKey:   "ORCHIGRAM_PLUGIN_MAGIC_COOKIE",
	MagicCookieValue: "orchigram-plugin-v1",
}

// Servers contains the services implemented by a plugin binary.
type Servers struct {
	Control pluginv1alpha1.PluginControlServer
	Task    pluginv1alpha1.TaskProviderServer
	Trigger pluginv1alpha1.TriggerProviderServer
	Agent   pluginv1alpha1.AgentRuntimeServer
}

// Clients is the typed host view of one running plugin process.
type Clients struct {
	Control pluginv1alpha1.PluginControlClient
	Task    pluginv1alpha1.TaskProviderClient
	Trigger pluginv1alpha1.TriggerProviderClient
	Agent   pluginv1alpha1.AgentRuntimeClient
}

// Bridge implements go-plugin's transport adapter and registers protobuf services.
type Bridge struct {
	hplugin.NetRPCUnsupportedPlugin
	Servers Servers
}

// GRPCServer registers every service supplied by the plugin runtime.
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

// GRPCClient creates typed clients over go-plugin's negotiated connection.
func (*Bridge) GRPCClient(_ context.Context, _ *hplugin.GRPCBroker, connection *grpc.ClientConn) (any, error) {
	return &Clients{
		Control: pluginv1alpha1.NewPluginControlClient(connection),
		Task:    pluginv1alpha1.NewTaskProviderClient(connection),
		Trigger: pluginv1alpha1.NewTriggerProviderClient(connection),
		Agent:   pluginv1alpha1.NewAgentRuntimeClient(connection),
	}, nil
}

// PluginSet returns a fresh bridge for host-side dispensing.
func PluginSet(servers Servers) hplugin.PluginSet {
	return hplugin.PluginSet{DispenseName: &Bridge{Servers: servers}}
}

// Serve starts one isolated plugin process and never returns.
func Serve(servers Servers) {
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         PluginSet(servers),
		GRPCServer:      hplugin.DefaultGRPCServer,
	})
}

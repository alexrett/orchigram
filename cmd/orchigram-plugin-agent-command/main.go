// Command orchigram-plugin-agent-command runs agent CLIs behind the plugin protocol.
package main

import (
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: "agent-command", Version: version.Semver(), Capabilities: []string{"agent.codex", "agent.claude", "agent.command"}}},
		Agent:   &pluginruntime.Agent{Runner: process.NewRunner()},
	})
}

// Command orchigram-plugin-agent-command runs agent CLIs behind the plugin protocol.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	plugin, _ := firstparty.Find("agent-command")
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities}},
		Agent:   &pluginruntime.Agent{Runner: process.NewRunner()},
	})
}

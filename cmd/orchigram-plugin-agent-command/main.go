// Command orchigram-plugin-agent-command runs agent CLIs behind the plugin protocol.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

func main() {
	plugin, _ := firstparty.Find("agent-command")
	pluginsdk.Serve(pluginsdk.Config{
		Metadata: pluginsdk.Metadata{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities, Actions: plugin.Actions},
		Agent:    &pluginruntime.Agent{Runner: process.NewRunner()},
	})
}

// Command orchigram-plugin-github integrates GitHub triggers and actions.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/githubplugin"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	plugin, _ := firstparty.Find("github")
	runtime := &githubplugin.Runtime{Runner: process.NewRunner()}
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities}},
		Task:    runtime,
		Trigger: runtime,
	})
}

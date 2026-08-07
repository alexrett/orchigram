// Command orchigram-plugin-github integrates GitHub triggers and actions.
package main

import (
	"github.com/alexrett/orchigram/internal/githubplugin"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	runtime := &githubplugin.Runtime{Runner: process.NewRunner()}
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: "github", Version: version.Version, Capabilities: githubplugin.Capabilities}},
		Task:    runtime,
		Trigger: runtime,
	})
}

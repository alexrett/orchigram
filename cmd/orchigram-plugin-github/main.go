// Command orchigram-plugin-github integrates GitHub triggers and actions.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/githubplugin"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

func main() {
	plugin, _ := firstparty.Find("github")
	runtime := &githubplugin.Runtime{Runner: process.NewRunner()}
	pluginsdk.Serve(pluginsdk.Config{
		Metadata: pluginsdk.Metadata{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities},
		Task:     runtime,
		Trigger:  runtime,
	})
}

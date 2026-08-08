// Command orchigram-plugin-exec runs deterministic argv tasks.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

func main() {
	plugin, _ := firstparty.Find("exec")
	pluginsdk.Serve(pluginsdk.Config{
		Metadata: pluginsdk.Metadata{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities, Actions: plugin.Actions},
		Task:     &pluginruntime.Exec{Runner: process.NewRunner()},
	})
}

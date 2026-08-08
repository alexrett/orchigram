// Command orchigram-plugin-http delivers idempotent outbound HTTP actions.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/version"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

func main() {
	plugin, _ := firstparty.Find("http")
	pluginsdk.Serve(pluginsdk.Config{
		Metadata: pluginsdk.Metadata{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities},
		Task:     &pluginruntime.HTTP{},
	})
}

// Command orchigram-plugin-http delivers idempotent outbound HTTP actions.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	plugin, _ := firstparty.Find("http")
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities}},
		Task:    &pluginruntime.HTTP{},
	})
}

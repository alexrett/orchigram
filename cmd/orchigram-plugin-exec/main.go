// Command orchigram-plugin-exec runs deterministic argv tasks.
package main

import (
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	plugin, _ := firstparty.Find("exec")
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: plugin.Name, Version: version.Semver(), Capabilities: plugin.Capabilities}},
		Task:    &pluginruntime.Exec{Runner: process.NewRunner()},
	})
}

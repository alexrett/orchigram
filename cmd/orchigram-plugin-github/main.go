// Command orchigram-plugin-github integrates GitHub triggers and actions.
package main

import (
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: "github", Version: version.Version, Capabilities: []string{"task.github.request"}}},
		Task:    &pluginruntime.HTTP{Action: "github.request"},
	})
}

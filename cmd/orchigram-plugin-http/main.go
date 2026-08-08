// Command orchigram-plugin-http delivers idempotent outbound HTTP actions.
package main

import (
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: "http", Version: version.Semver(), Capabilities: []string{"task.http.request"}}},
		Task:    &pluginruntime.HTTP{},
	})
}

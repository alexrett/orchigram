// Command orchigram-plugin-exec runs deterministic argv tasks.
package main

import (
	"github.com/alexrett/orchigram/internal/pluginprotocol"
	"github.com/alexrett/orchigram/internal/pluginruntime"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/version"
)

func main() {
	pluginprotocol.Serve(pluginprotocol.Servers{
		Control: &pluginruntime.Control{Info: pluginruntime.Info{Name: "exec", Version: version.Version, Capabilities: []string{"task.exec.run"}}},
		Task:    &pluginruntime.Exec{Runner: process.NewRunner()},
	})
}

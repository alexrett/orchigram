// Package firstparty is the canonical catalog of plugins shipped with Orchigram.
package firstparty

import (
	"sort"

	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

// Plugin describes one independently built first-party plugin.
type Plugin struct {
	Name         string
	Command      string
	Capabilities []string
	Actions      []pluginsdk.ActionDescriptor
	Triggers     []pluginsdk.TriggerDescriptor
}

var catalog = []Plugin{
	{Name: "agent-command", Command: "orchigram-plugin-agent-command", Capabilities: []string{"agent.codex", "agent.claude", "agent.command"}, Actions: agentActionDescriptors()},
	{Name: "exec", Command: "orchigram-plugin-exec", Capabilities: []string{"task.exec.run"}, Actions: execActionDescriptors()},
	{Name: "github", Command: "orchigram-plugin-github", Capabilities: []string{
		"trigger.github.issues", pluginsdk.ActivationFenceCapability, "task.github.issue.get", "task.github.issue.comment",
		"task.github.workspace.checkout", "task.github.workspace.commit-push", "task.github.pr.ensure",
	}, Actions: githubActionDescriptors(), Triggers: githubTriggerDescriptors()},
	{Name: "http", Command: "orchigram-plugin-http", Capabilities: []string{"task.http.request"}, Actions: httpActionDescriptors()},
}

// All returns a stable copy of the release catalog.
func All() []Plugin {
	result := make([]Plugin, len(catalog))
	for index, plugin := range catalog {
		result[index] = clone(plugin)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Name < result[j].Name })
	return result
}

// Find returns a copy of a catalog entry.
func Find(name string) (Plugin, bool) {
	for _, plugin := range catalog {
		if plugin.Name == name {
			return clone(plugin), true
		}
	}
	return Plugin{}, false
}

func clone(plugin Plugin) Plugin {
	return Plugin{
		Name: plugin.Name, Command: plugin.Command, Capabilities: append([]string(nil), plugin.Capabilities...),
		Actions: cloneActions(plugin.Actions), Triggers: cloneTriggers(plugin.Triggers),
	}
}

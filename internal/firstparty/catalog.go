// Package firstparty is the canonical catalog of plugins shipped with Orchigram.
package firstparty

import "sort"

// Plugin describes one independently built first-party plugin.
type Plugin struct {
	Name         string
	Command      string
	Capabilities []string
}

var catalog = []Plugin{
	{Name: "agent-command", Command: "orchigram-plugin-agent-command", Capabilities: []string{"agent.codex", "agent.claude", "agent.command"}},
	{Name: "exec", Command: "orchigram-plugin-exec", Capabilities: []string{"task.exec.run"}},
	{Name: "github", Command: "orchigram-plugin-github", Capabilities: []string{
		"trigger.github.issues", "task.github.issue.get", "task.github.issue.comment",
		"task.github.workspace.checkout", "task.github.workspace.commit-push", "task.github.pr.ensure",
	}},
	{Name: "http", Command: "orchigram-plugin-http", Capabilities: []string{"task.http.request"}},
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
	return Plugin{Name: plugin.Name, Command: plugin.Command, Capabilities: append([]string(nil), plugin.Capabilities...)}
}

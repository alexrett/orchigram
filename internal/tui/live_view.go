package tui

import (
	"fmt"
	"sort"
	"strings"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/rivo/tview"
)

func resourcesForKind(snapshot liveSnapshot, kind string) []*controlv1alpha1.ResourceDocument {
	result := make([]*controlv1alpha1.ResourceDocument, 0)
	for _, document := range snapshot.Resources {
		if document.GetKey().GetKind() == kind {
			result = append(result, document)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].GetKey(), result[j].GetKey()
		if left.GetNamespace() != right.GetNamespace() {
			return left.GetNamespace() < right.GetNamespace()
		}
		return left.GetName() < right.GetName()
	})
	return result
}

func resourceDisplayName(key *controlv1alpha1.ResourceKey) string {
	if key.GetNamespace() == "" || key.GetNamespace() == "default" {
		return key.GetName()
	}
	return key.GetNamespace() + "/" + key.GetName()
}

func sortedRuns(snapshot liveSnapshot) []*controlv1alpha1.RunSummary {
	result := make([]*controlv1alpha1.RunSummary, 0, len(snapshot.Runs))
	for _, run := range snapshot.Runs {
		result = append(result, run)
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].GetCreatedAt().AsTime(), result[j].GetCreatedAt().AsTime()
		if !left.Equal(right) {
			return left.After(right)
		}
		return result[i].GetUid() > result[j].GetUid()
	})
	return result
}

func sortedPlugins(snapshot liveSnapshot) []*controlv1alpha1.PluginInfo {
	result := make([]*controlv1alpha1.PluginInfo, 0, len(snapshot.Plugins))
	for _, plugin := range snapshot.Plugins {
		result = append(result, plugin)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].GetName() != result[j].GetName() {
			return result[i].GetName() < result[j].GetName()
		}
		return result[i].GetVersion() < result[j].GetVersion()
	})
	return result
}

func applyRunEventStatuses(graph *Graph, events []*controlv1alpha1.RunEvent) {
	for _, event := range events {
		status := ""
		switch event.GetType() {
		case "node.started":
			status = "running"
		case "node.completed", "approval.approved", "event.received":
			status = "completed"
		case "node.failed":
			status = "failed"
		case "approval.waiting", "event.waiting", "event.duplicate":
			status = "waiting"
		case "approval.rejected":
			status = "rejected"
		case "node.skipped":
			status = "skipped"
		}
		if status != "" {
			graph.SetStatus(event.GetNodeId(), status)
		}
	}
}

func showLiveStatus(view *tview.TextView, snapshot liveSnapshot, selectedRun string) {
	reconnecting := make([]string, 0)
	for component, state := range snapshot.Connections {
		if state != "connected" {
			reconnecting = append(reconnecting, component)
		}
	}
	sort.Strings(reconnecting)
	connection := "[green]connected[-]"
	if len(reconnecting) > 0 {
		connection = "[yellow]reconnecting[-] " + escape(strings.Join(reconnecting, ", "))
	}
	text := fmt.Sprintf("Control %s  revision=%d  resources=%d  runs=%d", connection, snapshot.Revision, len(snapshot.Resources), len(snapshot.Runs))
	if events := snapshot.RunEvents[selectedRun]; selectedRun != "" && len(events) > 0 {
		last := events[len(events)-1]
		text += fmt.Sprintf("\nRun %s  sequence=%d  %s  %s", escape(short(selectedRun)), last.GetSequence(), escape(last.GetNodeId()), escape(last.GetType()))
	}
	view.SetText(text)
}

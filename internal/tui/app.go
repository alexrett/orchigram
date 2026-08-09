package tui

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/contextcfg"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/protobuf/types/known/emptypb"
	"gopkg.in/yaml.v3"
)

// Run opens the stateless operator TUI on an existing daemon connection.
func Run(ctx context.Context, client *clientpkg.Client) error {
	application := tview.NewApplication().EnableMouse(true)
	return runWithApplicationContext(ctx, client, application, "local", nil)
}

func runWithApplication(ctx context.Context, client *clientpkg.Client, application *tview.Application) error {
	return runWithApplicationContext(ctx, client, application, "test", nil)
}

// RunWithContext opens the TUI and identifies the selected local route.
func RunWithContext(ctx context.Context, client *clientpkg.Client, contextName string) error {
	application := tview.NewApplication().EnableMouse(true)
	return runWithApplicationContext(ctx, client, application, contextName, nil)
}

// RunWithContexts opens the TUI with the complete local routing projection.
func RunWithContexts(ctx context.Context, client *clientpkg.Client, contextName string, contexts contextcfg.File) error {
	application := tview.NewApplication().EnableMouse(true)
	return runWithApplicationContext(ctx, client, application, contextName, &contexts)
}

type navigationEntry struct {
	key      string
	label    string
	activate func()
}

func runWithApplicationContext(ctx context.Context, client *clientpkg.Client, application *tview.Application, contextName string, contexts *contextcfg.File) error {
	pages := tview.NewPages()
	graph := NewGraph()
	inspector := tview.NewTextView().SetDynamicColors(true).SetWrap(true)
	inspector.SetBorder(true).SetTitle(" Inspector ")
	events := tview.NewTextView().SetDynamicColors(true)
	events.SetBorder(true).SetTitle(" Notifications ")
	liveStatus := tview.NewTextView().SetDynamicColors(true)
	help := tview.NewTextView().SetDynamicColors(true).SetText(" [yellow]:/[-] commands/filter  [yellow]?[-] help  [yellow]Enter[-] inspect  [yellow]g[-] graph  [yellow]n/x/E[-] create/delete/edit  [yellow]S[-] start  [yellow]i[-] install  [yellow]e/t/f/l[-] evidence  [yellow]a/r/c[-] decide/cancel  [yellow]q[-] quit")
	navigation := tview.NewList().ShowSecondaryText(false).SetUseStyleTags(false, false)
	navigation.SetBorder(true).SetTitle(" Resources ")
	live := newLiveController(client)
	latest, err := live.bootstrap(ctx)
	if err != nil {
		return err
	}
	currentRun := ""
	var currentResource *controlv1alpha1.ResourceDocument
	entries := []navigationEntry{}
	visibleEntryKeys := []string{}
	currentFilter := ""
	activeNavigationKey := ""
	switchContext := ""
	rebuild := func(filter string) {
		currentFilter = strings.ToLower(strings.TrimSpace(filter))
		preserveKey := activeNavigationKey
		currentIndex := navigation.GetCurrentItem()
		if currentIndex >= 0 && currentIndex < len(visibleEntryKeys) {
			preserveKey = visibleEntryKeys[currentIndex]
		}
		navigation.Clear()
		visibleEntryKeys = visibleEntryKeys[:0]
		targetIndex := -1
		for _, entry := range entries {
			if currentFilter == "" || strings.Contains(strings.ToLower(entry.label), currentFilter) {
				if entry.key == preserveKey {
					targetIndex = len(visibleEntryKeys)
				}
				visibleEntryKeys = append(visibleEntryKeys, entry.key)
				navigation.AddItem(entry.label, "", 0, entry.activate)
			}
		}
		if targetIndex >= 0 {
			navigation.SetCurrentItem(targetIndex)
		} else if currentIndex >= 0 && len(visibleEntryKeys) > 0 {
			navigation.SetCurrentItem(min(currentIndex, len(visibleEntryKeys)-1))
		}
	}

	setInspector := func(node flow.PlanNode) {
		inspector.SetText(fmt.Sprintf("[yellow::b]%s[-:-:-]\n\n[gray]ID[-]       %s\n[gray]Action[-]   %s\n[gray]Timeout[-]  %s\n[gray]Retries[-]  %d\n", escape(node.Name), escape(node.ID), escape(node.Uses), escape(node.Timeout), node.RetryLimit))
	}
	setEdgeInspector := func(edge flow.PlanEdge, _ int) {
		condition := edge.Condition
		if condition == "" {
			condition = "always"
		}
		inspector.SetText(fmt.Sprintf("[yellow::b]Edge[-:-:-]\n\n[gray]From[-]       %s\n[gray]To[-]         %s\n[gray]Condition[-]  %s\n", escape(edge.From), escape(edge.To), escape(condition)))
	}
	graph.SetOnSelect(setInspector).SetOnOpen(func(node flow.PlanNode) {
		if currentRun == "" && currentResource != nil && currentResource.GetKey().GetKind() == "Flow" {
			openFlowNodeForm(ctx, application, pages, client, currentResource, node, events, graph)
			return
		}
		modal := tview.NewModal().SetText(tview.Escape(fmt.Sprintf("%s\n\nID: %s\nAction: %s\nTimeout: %s", node.Name, node.ID, node.Uses, node.Timeout))).AddButtons([]string{"Close"})
		modal.SetDoneFunc(func(_ int, _ string) { pages.RemovePage("node"); application.SetFocus(graph) })
		pages.AddPage("node", centered(modal, 60, 14), true, true)
		application.SetFocus(modal)
	}).SetOnSelectEdge(setEdgeInspector).SetOnOpenEdge(func(edge flow.PlanEdge, index int) {
		if currentRun == "" && currentResource != nil && currentResource.GetKey().GetKind() == "Flow" {
			openFlowEdgeForm(ctx, application, pages, client, currentResource, edge, index, events, graph)
			return
		}
		modal := tview.NewModal().SetText(tview.Escape(fmt.Sprintf("Edge\n\nFrom: %s\nTo: %s\nCondition: %s", edge.From, edge.To, valueOr(edge.Condition, "always")))).AddButtons([]string{"Close"})
		modal.SetDoneFunc(func(_ int, _ string) { pages.RemovePage("edge"); application.SetFocus(graph) })
		pages.AddPage("edge", centered(modal, 68, 14), true, true)
		application.SetFocus(modal)
	})

	var handleSnapshot func(liveSnapshot)
	buildEntries := func(snapshot liveSnapshot) []navigationEntry {
		result := []navigationEntry{}
		add := func(key, label string, activate func()) {
			if activate != nil {
				operation := activate
				activate = func() {
					activeNavigationKey = key
					operation()
				}
			}
			result = append(result, navigationEntry{key: key, label: label, activate: activate})
		}

		add("heading/contexts", "Contexts", nil)
		contextNames := []string{contextName}
		if contexts != nil {
			contextNames = contextNames[:0]
			for name := range contexts.Contexts {
				contextNames = append(contextNames, name)
			}
			sort.Strings(contextNames)
		}
		for _, name := range contextNames {
			name := name
			selected := contextcfg.Context{}
			if contexts != nil {
				selected = contexts.Contexts[name]
			}
			marker := ""
			if name == contextName {
				marker = "  [connected]"
			}
			add("context/"+name, "  "+name+marker, func() {
				currentRun, currentResource = "", nil
				transport := "the active gRPC route"
				if selected.Socket != "" {
					transport = "Unix socket " + selected.Socket
				}
				if selected.SSH != nil {
					transport = "OpenSSH StreamLocal to " + selected.SSH.Destination + ":" + selected.SSH.Socket
				}
				inspector.SetText(fmt.Sprintf("[yellow::b]Context %s[-:-:-]\n\n%s\n\nThis process is connected to the marked context.", escape(name), escape(transport)))
				if name != contextName {
					modal := tview.NewModal().SetText(tview.Escape(fmt.Sprintf("Switch from %s to %s?\n\nThe current transport will close and the stateless TUI will reconnect.", contextName, name))).AddButtons([]string{"Connect", "Cancel"})
					modal.SetDoneFunc(func(buttonIndex int, _ string) {
						pages.RemovePage("context-switch")
						if buttonIndex == 0 {
							switchContext = name
							application.Stop()
							return
						}
						application.SetFocus(navigation)
					})
					pages.AddPage("context-switch", centered(modal, 70, 13), true, true)
					application.SetFocus(modal)
				}
			})
		}

		resourceGroups := []struct{ kind, heading string }{
			{kind: "Flow", heading: "Flows"}, {kind: "Trigger", heading: "Triggers"}, {kind: "Repository", heading: "Repositories"},
			{kind: "AgentProfile", heading: "AgentProfiles"}, {kind: "PluginInstallation", heading: "PluginInstallations"},
		}
		for _, group := range resourceGroups {
			add("heading/resources/"+group.kind, group.heading, nil)
			for _, document := range resourcesForKind(snapshot, group.kind) {
				document := document
				key := "resource/" + resourceLiveKey(document.GetKey())
				label := "  " + resourceDisplayName(document.GetKey())
				switch group.kind {
				case "Flow":
					add(key, label, func() {
						currentRun, currentResource = "", cloneMessage(document)
						plan, compileErr := compileFlowPlan(ctx, client, document)
						if compileErr != nil {
							events.SetText("[red]" + escape(compileErr.Error()))
							return
						}
						graph.SetPlan(plan)
						events.SetText("[green]Opened Flow/" + escape(document.GetKey().GetName()))
						if selectedNode, ok := graph.Selected(); ok {
							setInspector(selectedNode)
						}
						application.SetFocus(graph)
					})
				case "Trigger":
					add(key, label, func() {
						currentRun, currentResource = "", cloneMessage(document)
						openTriggerDetail(ctx, application, pages, client, currentResource, inspector, events, navigation)
					})
				default:
					add(key, label, func() {
						currentRun, currentResource = "", cloneMessage(document)
						showResourceInspector(inspector, currentResource)
						openResourceDetail(application, pages, client, currentResource, inspector, events, navigation)
					})
				}
			}
		}

		add("heading/runs", "Runs", nil)
		for _, run := range sortedRuns(snapshot) {
			run := run
			key := "run/" + run.GetUid()
			add(key, fmt.Sprintf("  %s  [%s]", short(run.GetUid()), run.GetPhase()), func() {
				currentRun, currentResource = run.GetUid(), nil
				inspector.SetText(runInspectorText(run))
				planContext, cancel := context.WithTimeout(ctx, 10*time.Second)
				defer cancel()
				planResponse, planErr := client.Runs.Plan(planContext, &controlv1alpha1.RunRequest{Uid: run.GetUid()})
				if planErr != nil {
					events.SetText("[red]Unable to load the pinned run plan")
					return
				}
				var plan flow.ExecutionPlan
				if decodeErr := json.Unmarshal(planResponse.GetExecutionPlanJson(), &plan); decodeErr != nil {
					events.SetText("[red]Pinned run plan is invalid")
					return
				}
				graph.SetPlan(plan)
				applyRunEventStatuses(graph, snapshot.RunEvents[run.GetUid()])
				live.ensureRunHistory(ctx, run.GetUid(), handleSnapshot)
				application.SetFocus(graph)
			})
		}

		add("heading/plugins", "Plugins", nil)
		for _, plugin := range sortedPlugins(snapshot) {
			plugin := plugin
			key := "plugin/" + pluginLiveKey(plugin)
			add(key, fmt.Sprintf("  %s:%s [%s]", plugin.GetName(), plugin.GetVersion(), plugin.GetState()), func() {
				currentRun, currentResource = "", nil
				inspector.SetText(fmt.Sprintf("[yellow::b]Plugin %s[-:-:-]\n\nVersion: %s\nState: %s\nDigest: %s\nCapabilities: %s", escape(plugin.GetName()), escape(plugin.GetVersion()), escape(plugin.GetState()), escape(plugin.GetDigest()), escape(strings.Join(plugin.GetCapabilities(), ", "))))
				openPluginDetail(ctx, application, pages, client, plugin, sortedPlugins(snapshot), events, navigation)
			})
		}
		add("heading/resources/SecretRef", "SecretRefs", nil)
		for _, document := range resourcesForKind(snapshot, "SecretRef") {
			document := document
			key := "resource/" + resourceLiveKey(document.GetKey())
			add(key, "  "+resourceDisplayName(document.GetKey()), func() {
				currentRun, currentResource = "", cloneMessage(document)
				showResourceInspector(inspector, currentResource)
				openResourceDetail(application, pages, client, currentResource, inspector, events, navigation)
			})
		}

		info := snapshot.System
		health := "unknown"
		if snapshot.Health != nil {
			health = "degraded"
			if snapshot.Health.GetReady() {
				health = "ready"
			}
		}
		add("system", "System  ["+health+"]", func() {
			currentRun, currentResource = "", nil
			if info == nil {
				inspector.SetText("[yellow]System information is reconnecting")
				return
			}
			inspector.SetText(fmt.Sprintf("[yellow::b]Orchigram %s[-:-:-]\n\nHost: %s\nOS/Arch: %s/%s\nPID: %d\nProtocol: %s\nCapabilities: %s", escape(info.GetVersion()), escape(info.GetHostname()), escape(info.GetOs()), escape(info.GetArchitecture()), info.GetProcessId(), escape(info.GetProtocolVersion()), escape(strings.Join(info.GetCapabilities(), ", "))))
			openSystemDetail(ctx, application, pages, client, info, events, navigation)
		})
		return result
	}

	handleSnapshot = func(snapshot liveSnapshot) {
		application.QueueUpdateDraw(func() {
			latest = snapshot
			entries = buildEntries(snapshot)
			if currentResource != nil {
				selectedGeneration := currentResource.GetGeneration()
				key := resourceLiveKey(currentResource.GetKey())
				if updated := snapshot.Resources[key]; updated != nil {
					if updated.GetResourceVersion() < currentResource.GetResourceVersion() {
						showLiveStatus(liveStatus, snapshot, currentRun)
						rebuild(currentFilter)
						return
					}
					currentResource = cloneMessage(updated)
					if currentResource.GetKey().GetKind() == "Flow" && currentResource.GetGeneration() != selectedGeneration {
						plan, compileErr := compileFlowPlan(ctx, client, currentResource)
						if compileErr != nil {
							events.SetText("[red]Unable to compile the updated Flow: " + escape(compileErr.Error()))
						} else {
							graph.SetPlan(plan)
							if selectedNode, ok := graph.Selected(); ok {
								setInspector(selectedNode)
							} else if selectedEdge, index, ok := graph.SelectedEdge(); ok {
								setEdgeInspector(selectedEdge, index)
							}
						}
					} else if currentResource.GetKey().GetKind() != "Flow" {
						showResourceInspector(inspector, currentResource)
					}
				} else {
					deleted := currentResource.GetKey()
					currentResource = nil
					activeNavigationKey = ""
					inspector.SetText("[yellow]The selected resource was deleted")
					events.SetText("[yellow]Deleted " + escape(deleted.GetKind()+"/"+resourceDisplayName(deleted)))
				}
			}
			if currentRun != "" {
				if run := snapshot.Runs[currentRun]; run != nil {
					applyRunEventStatuses(graph, snapshot.RunEvents[currentRun])
				} else {
					currentRun = ""
					activeNavigationKey = ""
					inspector.SetText("[yellow]The selected run is no longer retained")
				}
			}
			showLiveStatus(liveStatus, snapshot, currentRun)
			rebuild(currentFilter)
		})
	}
	entries = buildEntries(latest)
	rebuild("")
	showLiveStatus(liveStatus, latest, "")
	if flows := resourcesForKind(latest, "Flow"); len(flows) > 0 {
		if plan, compileErr := compileFlowPlan(ctx, client, flows[0]); compileErr == nil {
			graph.SetPlan(plan)
			if selectedNode, ok := graph.Selected(); ok {
				setInspector(selectedNode)
			}
		}
	}

	body := tview.NewFlex().AddItem(navigation, 25, 0, false).AddItem(graph, 0, 1, true).AddItem(inspector, 34, 0, false)
	root := tview.NewFlex().SetDirection(tview.FlexRow).AddItem(help, 1, 0, false).AddItem(body, 0, 1, true).AddItem(events, 3, 0, false).AddItem(liveStatus, 2, 0, false)
	pages.AddPage("main", root, true, true)
	compact := false
	application.SetBeforeDrawFunc(func(screen tcell.Screen) bool {
		width, _ := screen.Size()
		wantCompact := width < 105
		if wantCompact == compact {
			return false
		}
		compact = wantCompact
		if compact {
			body.ResizeItem(navigation, 22, 0)
			body.ResizeItem(inspector, 0, 0)
		} else {
			body.ResizeItem(navigation, 25, 0)
			body.ResizeItem(inspector, 34, 0)
		}
		screen.Clear()
		return false
	})
	application.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc {
			if name, _ := pages.GetFrontPage(); name != "main" {
				pages.RemovePage(name)
				application.SetFocus(graph)
				return nil
			}
			application.SetFocus(navigation)
			return nil
		}
		if name, _ := pages.GetFrontPage(); name != "main" {
			return event
		}
		if event.Key() == tcell.KeyRune {
			switch event.Rune() {
			case ':':
				openCommandField(application, pages, navigation, entries, rebuild)
				return nil
			case '/':
				openFilterField(application, pages, navigation, rebuild)
				return nil
			case 'q':
				application.Stop()
				return nil
			case 'a', 'r':
				if currentRun == "" {
					return event
				}
				decision := "approve"
				if event.Rune() == 'r' {
					decision = "reject"
				}
				openDecisionForm(application, pages, client, currentRun, decision, events, graph)
				return nil
			case 'c':
				if currentRun != "" {
					openCancelForm(application, pages, client, currentRun, events, graph)
					return nil
				}
			case 'g':
				application.SetFocus(graph)
				return nil
			case 'n':
				openCreateResource(ctx, application, pages, client, events, navigation)
				return nil
			case 'x':
				if currentResource != nil {
					openDeleteResource(ctx, application, pages, client, currentResource, events, navigation)
					return nil
				}
			case 'S':
				if currentResource != nil && currentResource.GetKey().GetKind() == "Flow" {
					openStartFlow(ctx, application, pages, client, currentResource, events, graph)
					return nil
				}
			case 'i':
				openPluginInstall(ctx, application, pages, client, events, navigation)
				return nil
			case 'e':
				if currentRun != "" {
					openRunEvents(application, pages, latest, currentRun, graph)
					return nil
				}
				application.SetFocus(events)
				return nil
			case 't':
				if currentRun != "" {
					openRunAttempts(ctx, application, pages, client, currentRun, events)
					return nil
				}
			case 'f', 'l':
				if currentRun != "" {
					openRunArtifacts(ctx, application, pages, client, currentRun, event.Rune() == 'l', events)
					return nil
				}
			case 'y':
				if currentResource != nil {
					openYAML(application, pages, currentResource, navigation)
					return nil
				}
			case 'E':
				if currentResource != nil {
					openResourceForm(application, pages, client, currentResource, inspector, events, navigation)
					return nil
				}
			case 'd':
				if currentResource != nil {
					showResourceInspector(inspector, currentResource)
					application.SetFocus(inspector)
					return nil
				}
			case '?':
				modal := tview.NewModal().SetText("Orchigram keys\n\n: command palette, / filter\nEnter: inspect, edit selected Flow node/edge, or switch context\nTab/Shift-Tab: select graph nodes and edges\nh/j/k/l or arrows: directional graph navigation\nH/J/K/L: pan graph\nn: create resource, x: CAS delete, E: edit form\nS: start selected Flow, i: install plugin bundle\na/r: approve or reject selected run, c: cancel\ne: events, t: attempts, f: artifacts, l: logs\ny: YAML, g: graph\nEsc: go back, q: quit").AddButtons([]string{"Close"})
				modal.SetDoneFunc(func(_ int, _ string) { pages.RemovePage("help"); application.SetFocus(graph) })
				pages.AddPage("help", centered(modal, 58, 16), true, true)
				application.SetFocus(modal)
				return nil
			}
		}
		return event
	})
	liveContext, stopLive := context.WithCancel(ctx)
	defer stopLive()
	live.run(liveContext, handleSnapshot)
	go func() { <-ctx.Done(); application.QueueUpdateDraw(application.Stop) }()
	if err := application.SetRoot(pages, true).SetFocus(navigation).Run(); err != nil {
		return err
	}
	if switchContext != "" {
		return &ContextSwitchError{Name: switchContext}
	}
	return nil
}

func openPluginDetail(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, plugin *controlv1alpha1.PluginInfo, plugins []*controlv1alpha1.PluginInfo, events *tview.TextView, returnFocus tview.Primitive) {
	modal := tview.NewModal().SetText(tview.Escape(fmt.Sprintf("Plugin %s\n\nVersion: %s\nState: %s\nCapabilities: %s", plugin.GetName(), plugin.GetVersion(), plugin.GetState(), strings.Join(plugin.GetCapabilities(), ", ")))).AddButtons([]string{"Doctor", "Activate", "Disable", "Rollback", "Close"})
	modal.SetDoneFunc(func(buttonIndex int, _ string) {
		if buttonIndex == 3 {
			pages.RemovePage("plugin-detail")
			openPluginRollback(ctx, application, pages, client, plugin, plugins, events, returnFocus)
			return
		}
		operationContext, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		var err error
		switch buttonIndex {
		case 0:
			var result *controlv1alpha1.DoctorResponse
			result, err = client.Plugins.Doctor(operationContext, &controlv1alpha1.PluginRequest{Name: plugin.GetName(), Version: plugin.GetVersion()})
			if err == nil && len(result.GetDiagnostics()) > 0 {
				err = fmt.Errorf("%s: %s", result.GetDiagnostics()[0].GetPath(), result.GetDiagnostics()[0].GetMessage())
			}
		case 1:
			_, err = client.Plugins.Enable(operationContext, &controlv1alpha1.PluginRequest{Name: plugin.GetName(), Version: plugin.GetVersion()})
		case 2:
			_, err = client.Plugins.Disable(operationContext, &controlv1alpha1.PluginRequest{Name: plugin.GetName()})
		default:
			pages.RemovePage("plugin-detail")
			application.SetFocus(returnFocus)
			return
		}
		if err != nil {
			events.SetText("[red]" + escape(err.Error()))
		} else {
			events.SetText("[green]Plugin operation completed")
		}
		pages.RemovePage("plugin-detail")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("plugin-detail", centered(modal, 76, 18), true, true)
	application.SetFocus(modal)
}

func openSystemDetail(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, info *controlv1alpha1.SystemInfo, events *tview.TextView, returnFocus tview.Primitive) {
	modal := tview.NewModal().SetText(fmt.Sprintf("Orchigram %s\n\nHost: %s\nOS/Arch: %s/%s\nProtocol: %s", info.GetVersion(), info.GetHostname(), info.GetOs(), info.GetArchitecture(), info.GetProtocolVersion())).AddButtons([]string{"Health", "Backup", "Close"})
	modal.SetDoneFunc(func(buttonIndex int, _ string) {
		operationContext, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		var err error
		switch buttonIndex {
		case 0:
			var health *controlv1alpha1.HealthResponse
			health, err = client.System.Health(operationContext, &emptypb.Empty{})
			if err == nil {
				if health.GetReady() {
					events.SetText("[green]System healthy[-] ready=true")
				} else {
					lines := []string{"[red]System degraded[-] ready=false"}
					for _, diagnostic := range health.GetDiagnostics() {
						lines = append(lines, fmt.Sprintf("%s  %s: %s", escape(diagnostic.GetPath()), escape(diagnostic.GetCode()), escape(diagnostic.GetMessage())))
					}
					events.SetText(strings.Join(lines, "\n"))
				}
			}
		case 1:
			var result *controlv1alpha1.BackupResponse
			result, err = client.System.Backup(operationContext, &controlv1alpha1.BackupRequest{})
			if err == nil {
				events.SetText(fmt.Sprintf("[green]Backup created[-] %s\nsha256 %s", escape(result.GetPath()), escape(result.GetSha256())))
			}
		default:
			pages.RemovePage("system-detail")
			application.SetFocus(returnFocus)
			return
		}
		if err != nil {
			events.SetText("[red]" + escape(err.Error()))
		}
		pages.RemovePage("system-detail")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("system-detail", centered(modal, 72, 16), true, true)
	application.SetFocus(modal)
}

func showResourceInspector(inspector *tview.TextView, document *controlv1alpha1.ResourceDocument) {
	inspector.SetText(fmt.Sprintf("[yellow::b]%s %s[-:-:-]\n\n[gray]UID[-]              %s\n[gray]Resource version[-] %d\n[gray]Generation[-]       %d\n\nPress Enter for schema fields or y for the read-only YAML projection.", escape(document.GetKey().GetKind()), escape(document.GetKey().GetName()), escape(document.GetKey().GetUid()), document.GetResourceVersion(), document.GetGeneration()))
}

func runInspectorText(run *controlv1alpha1.RunSummary) string {
	return fmt.Sprintf("[yellow::b]Run %s[-:-:-]\n\nFlow: %s\nPhase: %s\nPlan: %s\nInterpreter: %s", escape(run.GetUid()), escape(run.GetFlow()), escape(run.GetPhase()), escape(run.GetPlanHash()), escape(run.GetInterpreterVersion()))
}

func openResourceDetail(application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, inspector, events *tview.TextView, returnFocus tview.Primitive) {
	modal := tview.NewModal().SetText(fmt.Sprintf("%s %s\n\nUID: %s\nResource version: %d\nGeneration: %d", document.GetKey().GetKind(), document.GetKey().GetName(), document.GetKey().GetUid(), document.GetResourceVersion(), document.GetGeneration())).AddButtons([]string{"Edit", "YAML", "Close"})
	modal.SetDoneFunc(func(buttonIndex int, _ string) {
		switch buttonIndex {
		case 0:
			pages.RemovePage("resource-detail")
			openResourceForm(application, pages, client, document, inspector, events, returnFocus)
			return
		case 1:
			pages.RemovePage("resource-detail")
			openYAML(application, pages, document, returnFocus)
			return
		}
		pages.RemovePage("resource-detail")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("resource-detail", centered(modal, 72, 16), true, true)
	application.SetFocus(modal)
}

type formField struct {
	label    string
	path     []string
	typeName string
}

func openResourceForm(application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, inspector, events *tview.TextView, returnFocus tview.Primitive) {
	var projection map[string]any
	if err := json.Unmarshal(document.GetJson(), &projection); err != nil {
		events.SetText("[red]" + escape(err.Error()))
		return
	}
	delete(projection, "status")
	fields := resourceFormFields(document.GetKey().GetKind(), projection)
	if len(fields) == 0 {
		events.SetText("[yellow]This resource has no scalar form fields; inspect its YAML projection.")
		application.SetFocus(returnFocus)
		return
	}
	form := tview.NewForm()
	form.SetBorder(true).SetTitle(" Edit " + tview.Escape(document.GetKey().GetKind()) + "/" + tview.Escape(document.GetKey().GetName()) + " ")
	for _, descriptor := range fields {
		initial := scalarText(readPath(projection, descriptor.path))
		if initial == "" && descriptor.typeName == "bool" {
			initial = "true"
		}
		if initial == "" && descriptor.typeName == "int" {
			initial = "1"
		}
		form.AddInputField(descriptor.label, initial, 48, nil, nil)
	}
	form.AddButton("Save", func() {
		for index, descriptor := range fields {
			text := form.GetFormItem(index).(*tview.InputField).GetText()
			var value any = text
			switch descriptor.typeName {
			case "bool":
				parsed, err := strconv.ParseBool(text)
				if err != nil {
					events.SetText("[red]" + escape(descriptor.label+": "+err.Error()))
					return
				}
				value = parsed
			case "int":
				parsed, err := strconv.Atoi(text)
				if err != nil {
					events.SetText("[red]" + escape(descriptor.label+": "+err.Error()))
					return
				}
				value = parsed
			}
			writePath(projection, descriptor.path, value)
		}
		encoded, err := json.Marshal(projection)
		if err != nil {
			events.SetText("[red]" + escape(err.Error()))
			return
		}
		applyContext, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		response, err := client.Resources.Apply(applyContext, &controlv1alpha1.ApplyRequest{Document: encoded, ExpectedResourceVersion: document.GetResourceVersion()})
		if err != nil {
			events.SetText("[red]CAS conflict or validation failure: " + escape(err.Error()))
			return
		}
		if len(response.GetDiagnostics()) > 0 {
			events.SetText("[red]" + escape(response.GetDiagnostics()[0].GetMessage()))
			return
		}
		*document = *response.GetResource()
		showResourceInspector(inspector, document)
		events.SetText(fmt.Sprintf("[green]Applied resource version %d", document.GetResourceVersion()))
		pages.RemovePage("resource-form")
		application.SetFocus(returnFocus)
	}).AddButton("Cancel", func() {
		pages.RemovePage("resource-form")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("resource-form", centered(form, 76, min(28, len(fields)+8)), true, true)
	application.SetFocus(form)
}

func resourceFormFields(kind string, projection map[string]any) []formField {
	fields := map[string][]formField{
		"Flow":         {{label: "Timeout", path: []string{"spec", "policies", "timeout"}}, {label: "Max parallel", path: []string{"spec", "policies", "maxParallel"}, typeName: "int"}},
		"Repository":   {{label: "Clone URL", path: []string{"spec", "cloneURL"}}, {label: "Default branch", path: []string{"spec", "defaultBranch"}}, {label: "Workspace policy", path: []string{"spec", "workspacePolicy"}}, {label: "Auth SecretRef", path: []string{"spec", "authSecretRef"}}},
		"AgentProfile": {{label: "Type", path: []string{"spec", "type"}}, {label: "Executable", path: []string{"spec", "executable"}}, {label: "Model", path: []string{"spec", "model"}}, {label: "Profile", path: []string{"spec", "profile"}}, {label: "Effort", path: []string{"spec", "effort"}}, {label: "Sandbox", path: []string{"spec", "sandbox"}}},
		"SecretRef":    {{label: "Backend", path: []string{"spec", "backend"}}, {label: "Reference key/path", path: []string{"spec", "key"}}},
		"Trigger":      {{label: "Flow", path: []string{"spec", "flow"}}, {label: "Enabled", path: []string{"spec", "enabled"}, typeName: "bool"}},
	}[kind]
	if kind == "Trigger" {
		spec, _ := projection["spec"].(map[string]any)
		switch {
		case spec["schedule"] != nil:
			fields = append(fields, formField{label: "Cron", path: []string{"spec", "schedule", "cron"}}, formField{label: "Timezone", path: []string{"spec", "schedule", "timezone"}}, formField{label: "Starting deadline", path: []string{"spec", "schedule", "startingDeadline"}}, formField{label: "Concurrency", path: []string{"spec", "schedule", "concurrencyPolicy"}})
		case spec["webhook"] != nil:
			fields = append(fields, formField{label: "Bearer SecretRef", path: []string{"spec", "webhook", "bearerSecretRef"}})
		case spec["provider"] != nil:
			fields = append(fields, formField{label: "Provider plugin", path: []string{"spec", "provider", "plugin"}})
		}
	}
	return fields
}

func readPath(root map[string]any, path []string) any {
	var current any = root
	for _, segment := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = object[segment]
	}
	return current
}

func writePath(root map[string]any, path []string, value any) {
	current := root
	for _, segment := range path[:len(path)-1] {
		next, ok := current[segment].(map[string]any)
		if !ok {
			next = map[string]any{}
			current[segment] = next
		}
		current = next
	}
	current[path[len(path)-1]] = value
}

func scalarText(value any) string {
	if value == nil {
		return ""
	}
	return fmt.Sprint(value)
}

func openYAML(application *tview.Application, pages *tview.Pages, document *controlv1alpha1.ResourceDocument, returnFocus tview.Primitive) {
	var value any
	_ = json.Unmarshal(document.GetJson(), &value)
	encoded, _ := yaml.Marshal(value)
	view := tview.NewTextView().SetDynamicColors(false).SetScrollable(true).SetWrap(false).SetText(string(encoded))
	view.SetBorder(true).SetTitle(" YAML " + tview.Escape(document.GetKey().GetKind()) + "/" + tview.Escape(document.GetKey().GetName()) + " (read-only) ")
	view.SetDoneFunc(func(_ tcell.Key) {
		pages.RemovePage("yaml")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("yaml", view, true, true)
	application.SetFocus(view)
}

func openFilterField(application *tview.Application, pages *tview.Pages, navigation *tview.List, rebuild func(string)) {
	field := tview.NewInputField().SetLabel("Filter: ")
	field.SetChangedFunc(rebuild)
	field.SetDoneFunc(func(_ tcell.Key) {
		pages.RemovePage("filter")
		application.SetFocus(navigation)
	})
	field.SetBorder(true).SetTitle(" Resource filter ")
	pages.AddPage("filter", centered(field, 64, 3), true, true)
	application.SetFocus(field)
}

func openCommandField(application *tview.Application, pages *tview.Pages, navigation *tview.List, _ []navigationEntry, rebuild func(string)) {
	field := tview.NewInputField().SetLabel(":")
	field.SetDoneFunc(func(_ tcell.Key) {
		command := strings.TrimSpace(strings.ToLower(field.GetText()))
		pages.RemovePage("command")
		switch command {
		case "q", "quit":
			application.Stop()
			return
		case "flows", "triggers", "runs", "repositories", "plugins", "secretrefs", "system", "contexts":
			rebuild("")
			for index := 0; index < navigation.GetItemCount(); index++ {
				label, _ := navigation.GetItemText(index)
				if strings.EqualFold(strings.TrimSpace(label), command) {
					navigation.SetCurrentItem(index)
					break
				}
			}
		}
		application.SetFocus(navigation)
	})
	field.SetBorder(true).SetTitle(" Command ")
	pages.AddPage("command", centered(field, 64, 3), true, true)
	application.SetFocus(field)
}

func openTriggerDetail(ctx context.Context, application *tview.Application, pages *tview.Pages, client *clientpkg.Client, document *controlv1alpha1.ResourceDocument, inspector, events *tview.TextView, returnFocus tview.Primitive) {
	trigger, err := resource.DecodeTrigger(document.GetJson())
	if err != nil {
		events.SetText("[red]" + escape(err.Error()))
		return
	}
	source := "unknown"
	switch {
	case trigger.Spec.Schedule != nil:
		source = fmt.Sprintf("schedule %s (%s)", trigger.Spec.Schedule.Cron, valueOr(trigger.Spec.Schedule.Timezone, "UTC"))
	case trigger.Spec.Webhook != nil:
		source = "webhook (bearer SecretRef: " + trigger.Spec.Webhook.BearerSecretRef + ")"
	case trigger.Spec.Provider != nil:
		source = "provider " + trigger.Spec.Provider.Plugin
	}
	detail := fmt.Sprintf("[yellow::b]%s[-:-:-]\n\n[gray]UID[-]     %s\n[gray]Flow[-]    %s\n[gray]Source[-]  %s", escape(trigger.Metadata.Name), escape(trigger.Metadata.UID), escape(trigger.Spec.Flow), escape(source))
	if trigger.Spec.Schedule != nil {
		response, nextErr := client.Triggers.Next(ctx, &controlv1alpha1.NextOccurrencesRequest{Uid: trigger.Metadata.UID, Count: 1})
		if nextErr == nil && len(response.GetOccurrences()) > 0 {
			detail += "\n[gray]Next[-]    " + response.GetOccurrences()[0].GetScheduledAt().AsTime().Format(time.RFC3339)
		}
	}
	receipts, receiptsErr := client.Triggers.Receipts(ctx, &controlv1alpha1.ReceiptRequest{TriggerUid: trigger.Metadata.UID, Limit: 1})
	if receiptsErr == nil {
		if len(receipts.GetReceipts()) > 0 {
			last := receipts.GetReceipts()[0]
			detail += fmt.Sprintf("\n[gray]Last run[-] %s\n[gray]Receipt[-]  %s", escape(last.GetRunUid()), escape(last.GetOccurrenceId()))
		}
		if len(receipts.GetSkips()) > 0 {
			last := receipts.GetSkips()[0]
			detail += fmt.Sprintf("\n[gray]Last skip[-] %s (%s)", escape(last.GetReason()), last.GetScheduledAt().AsTime().Format(time.RFC3339))
		}
	}
	inspector.SetText(detail)
	modal := tview.NewModal().SetText(tview.Escape(fmt.Sprintf("Trigger %s\n\nFlow: %s\nSource: %s\n\nEnable/Disable changes durable controller state without rewriting YAML.", trigger.Metadata.Name, trigger.Spec.Flow, source))).AddButtons([]string{"Enable", "Disable", "Edit", "Close"})
	modal.SetDoneFunc(func(buttonIndex int, _ string) {
		if buttonIndex < 2 {
			operationContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			request := &controlv1alpha1.TriggerRequest{Uid: trigger.Metadata.UID}
			var operationErr error
			if buttonIndex == 0 {
				_, operationErr = client.Triggers.Enable(operationContext, request)
			} else {
				_, operationErr = client.Triggers.Disable(operationContext, request)
			}
			if operationErr != nil {
				events.SetText("[red]" + escape(operationErr.Error()))
			} else {
				state := "enabled"
				if buttonIndex == 1 {
					state = "disabled"
				}
				events.SetText("[green]Trigger " + escape(trigger.Metadata.Name) + " " + state)
			}
		}
		if buttonIndex == 2 {
			pages.RemovePage("trigger")
			openResourceForm(application, pages, client, document, inspector, events, returnFocus)
			return
		}
		pages.RemovePage("trigger")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("trigger", centered(modal, 76, 18), true, true)
	application.SetFocus(modal)
}

func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func openDecisionForm(application *tview.Application, pages *tview.Pages, client *clientpkg.Client, runUID, decision string, events *tview.TextView, returnFocus tview.Primitive) {
	form := tview.NewForm().AddInputField("Node", "approval", 32, nil, nil).AddInputField("Reason", "", 48, nil, nil)
	title := strings.ToUpper(decision[:1]) + decision[1:]
	form.SetBorder(true).SetTitle(" " + title + " run ")
	form.AddButton(title, func() {
		node := form.GetFormItemByLabel("Node").(*tview.InputField).GetText()
		reason := form.GetFormItemByLabel("Reason").(*tview.InputField).GetText()
		request := &controlv1alpha1.ApprovalRequest{RunUid: runUID, NodeId: node, Reason: reason}
		decisionContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var err error
		if decision == "approve" {
			_, err = client.Runs.Approve(decisionContext, request)
		} else {
			_, err = client.Runs.Reject(decisionContext, request)
		}
		if err != nil {
			events.SetText("[red]" + escape(err.Error()))
			return
		}
		pages.RemovePage("decision")
		application.SetFocus(returnFocus)
	}).AddButton("Cancel", func() {
		pages.RemovePage("decision")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("decision", centered(form, 68, 12), true, true)
	application.SetFocus(form)
}

func openCancelForm(application *tview.Application, pages *tview.Pages, client *clientpkg.Client, runUID string, events *tview.TextView, returnFocus tview.Primitive) {
	form := tview.NewForm().AddInputField("Reason", "operator cancellation", 48, nil, nil)
	form.SetBorder(true).SetTitle(" Cancel run ")
	form.AddButton("Cancel run", func() {
		reason := form.GetFormItemByLabel("Reason").(*tview.InputField).GetText()
		cancelContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Runs.Cancel(cancelContext, &controlv1alpha1.CancelRunRequest{RunUid: runUID, Reason: reason}); err != nil {
			events.SetText("[red]" + escape(err.Error()))
			return
		}
		pages.RemovePage("cancel-run")
		application.SetFocus(returnFocus)
	}).AddButton("Close", func() {
		pages.RemovePage("cancel-run")
		application.SetFocus(returnFocus)
	})
	pages.AddPage("cancel-run", centered(form, 68, 10), true, true)
	application.SetFocus(form)
}

func centered(primitive tview.Primitive, width, height int) tview.Primitive {
	return tview.NewFlex().SetDirection(tview.FlexRow).AddItem(nil, 0, 1, false).AddItem(tview.NewFlex().AddItem(nil, 0, 1, false).AddItem(primitive, width, 0, true).AddItem(nil, 0, 1, false), height, 0, true).AddItem(nil, 0, 1, false)
}

func escape(value string) string { return tview.Escape(value) }
func short(value string) string {
	if len(value) <= 8 {
		return value
	}
	return value[:8]
}

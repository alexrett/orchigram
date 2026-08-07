package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

// Run opens the stateless operator TUI on an existing daemon connection.
func Run(ctx context.Context, client *clientpkg.Client) error {
	application := tview.NewApplication().EnableMouse(true)
	return runWithApplication(ctx, client, application)
}

func runWithApplication(ctx context.Context, client *clientpkg.Client, application *tview.Application) error {
	pages := tview.NewPages()
	graph := NewGraph()
	inspector := tview.NewTextView().SetDynamicColors(true).SetWrap(true)
	inspector.SetBorder(true).SetTitle(" Inspector ")
	events := tview.NewTextView().SetDynamicColors(true)
	events.SetBorder(true).SetTitle(" Events ")
	help := tview.NewTextView().SetDynamicColors(true).SetText(" [yellow]:[-] commands  [yellow]/[-] filter  [yellow]?[-] help  [yellow]Enter[-] inspect  [yellow]g[-] graph  [yellow]l[-] logs  [yellow]e[-] events  [yellow]q[-] quit")
	navigation := tview.NewList().ShowSecondaryText(false)
	navigation.SetBorder(true).SetTitle(" Resources ")
	currentRun := ""
	var cancelRunWatch context.CancelFunc

	setInspector := func(node flow.PlanNode) {
		inspector.SetText(fmt.Sprintf("[yellow::b]%s[-:-:-]\n\n[gray]ID[-]       %s\n[gray]Action[-]   %s\n[gray]Timeout[-]  %s\n[gray]Retries[-]  %d\n", escape(node.Name), escape(node.ID), escape(node.Uses), escape(node.Timeout), node.RetryLimit))
	}
	graph.SetOnSelect(setInspector).SetOnOpen(func(node flow.PlanNode) {
		modal := tview.NewModal().SetText(fmt.Sprintf("%s\n\nID: %s\nAction: %s\nTimeout: %s", node.Name, node.ID, node.Uses, node.Timeout)).AddButtons([]string{"Close"})
		modal.SetDoneFunc(func(_ int, _ string) { pages.RemovePage("node"); application.SetFocus(graph) })
		pages.AddPage("node", centered(modal, 60, 14), true, true)
		application.SetFocus(modal)
	})

	flows, err := client.Resources.List(ctx, &controlv1alpha1.ListRequest{Kind: "Flow", Namespace: "default", Limit: 200})
	if err != nil {
		return err
	}
	navigation.AddItem("Flows", "", 0, nil)
	for _, item := range flows.GetResources() {
		item := item
		navigation.AddItem("  "+item.GetKey().GetName(), "", 0, func() {
			currentRun = ""
			if cancelRunWatch != nil {
				cancelRunWatch()
			}
			flowResource, decodeErr := resource.DecodeFlow(item.GetJson())
			if decodeErr != nil {
				events.SetText("[red]" + escape(decodeErr.Error()))
				return
			}
			plan, diagnostics := flow.NewCompiler(nil).Compile(flowResource)
			if len(diagnostics) > 0 {
				events.SetText("[red]" + escape(diagnostics[0].Message))
				return
			}
			graph.SetPlan(plan)
			if selected, ok := graph.Selected(); ok {
				setInspector(selected)
			}
			application.SetFocus(graph)
		})
	}
	runs, err := client.Runs.List(ctx, &controlv1alpha1.ListRunsRequest{Limit: 200})
	if err != nil {
		return err
	}
	navigation.AddItem("Runs", "", 0, nil)
	for _, run := range runs.GetRuns() {
		run := run
		navigation.AddItem(fmt.Sprintf("  %s  [%s]", short(run.GetUid()), run.GetPhase()), "", 0, func() {
			currentRun = run.GetUid()
			inspector.SetText(fmt.Sprintf("[yellow::b]Run %s[-:-:-]\n\nFlow: %s\nPhase: %s\nPlan: %s\nInterpreter: %s", run.GetUid(), run.GetFlow(), run.GetPhase(), run.GetPlanHash(), run.GetInterpreterVersion()))
			if cancelRunWatch != nil {
				cancelRunWatch()
			}
			watchContext, cancel := context.WithCancel(ctx)
			cancelRunWatch = cancel
			go watchRun(watchContext, application, client, graph, events, run.GetUid())
			application.SetFocus(graph)
		})
	}
	if len(flows.GetResources()) > 0 {
		flowResource, decodeErr := resource.DecodeFlow(flows.GetResources()[0].GetJson())
		if decodeErr == nil {
			plan, diagnostics := flow.NewCompiler(nil).Compile(flowResource)
			if len(diagnostics) == 0 {
				graph.SetPlan(plan)
				if selected, ok := graph.Selected(); ok {
					setInspector(selected)
				}
			}
		}
	}

	body := tview.NewFlex().AddItem(navigation, 25, 0, false).AddItem(graph, 0, 1, true).AddItem(inspector, 34, 0, false)
	root := tview.NewFlex().SetDirection(tview.FlexRow).AddItem(help, 1, 0, false).AddItem(body, 0, 1, true).AddItem(events, 4, 0, false)
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
			body.ResizeItem(navigation, 0, 0)
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
		if event.Key() == tcell.KeyRune {
			switch event.Rune() {
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
			case 'g':
				application.SetFocus(graph)
				return nil
			case 'e', 'l':
				application.SetFocus(events)
				return nil
			case '?':
				modal := tview.NewModal().SetText("Orchigram keys\n\nEnter: inspect selected node\nh/j/k/l or arrows: select\nw/a/s/d: pan graph\na/r: approve or reject selected run\nEsc: go back\nq: quit").AddButtons([]string{"Close"})
				modal.SetDoneFunc(func(_ int, _ string) { pages.RemovePage("help"); application.SetFocus(graph) })
				pages.AddPage("help", centered(modal, 58, 16), true, true)
				application.SetFocus(modal)
				return nil
			}
		}
		return event
	})
	go func() { <-ctx.Done(); application.QueueUpdateDraw(application.Stop) }()
	return application.SetRoot(pages, true).SetFocus(navigation).Run()
}

func watchRun(ctx context.Context, application *tview.Application, client *clientpkg.Client, graph *Graph, events *tview.TextView, runUID string) {
	stream, err := client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: runUID})
	if err != nil {
		application.QueueUpdateDraw(func() { events.SetText("[red]" + escape(err.Error())) })
		return
	}
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			return
		}
		status := ""
		switch event.GetType() {
		case "node.started":
			status = "running"
		case "node.completed", "approval.approved":
			status = "completed"
		case "node.failed":
			status = "failed"
		case "approval.waiting":
			status = "waiting"
		case "approval.rejected":
			status = "rejected"
		case "node.skipped":
			status = "skipped"
		}
		application.QueueUpdateDraw(func() {
			if status != "" {
				graph.SetStatus(event.GetNodeId(), status)
			}
			_, _ = fmt.Fprintf(events, "[gray]%d[-] [yellow]%s[-] %s\n", event.GetSequence(), escape(event.GetNodeId()), escape(event.GetType()))
		})
		if strings.HasPrefix(event.GetType(), "run.") && event.GetType() != "run.accepted" {
			return
		}
	}
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

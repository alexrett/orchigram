package tui

import (
	"fmt"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

func TestGraphDrawsAndNavigatesAtSupportedSizes(t *testing.T) {
	t.Parallel()
	plan := flow.ExecutionPlan{Nodes: []flow.PlanNode{{ID: "start", Name: "Start", Uses: "core.noop"}, {ID: "approval", Name: "Approval", Uses: "core.approval"}, {ID: "finish", Name: "Finish", Uses: "core.noop"}}, Edges: []flow.PlanEdge{{From: "start", To: "approval"}, {From: "approval", To: "finish"}}}
	for _, size := range [][2]int{{80, 24}, {120, 40}, {160, 50}} {
		size := size
		t.Run(fmt.Sprintf("%dx%d", size[0], size[1]), func(t *testing.T) {
			screen := tcell.NewSimulationScreen("UTF-8")
			if err := screen.Init(); err != nil {
				t.Fatal(err)
			}
			defer screen.Fini()
			screen.SetSize(size[0], size[1])
			graph := NewGraph().SetPlan(plan)
			graph.SetRect(0, 0, size[0], size[1])
			graph.Draw(screen)
			before, _ := graph.Selected()
			graph.InputHandler()(tcell.NewEventKey(tcell.KeyTab, 0, 0), nil)
			after, _ := graph.Selected()
			if before.ID == after.ID {
				t.Fatal("Tab did not change selection")
			}
		})
	}
}

func TestGraphMouseSelectsAndOpensNode(t *testing.T) {
	t.Parallel()
	plan := flow.ExecutionPlan{Nodes: []flow.PlanNode{
		{ID: "start", Name: "Start", Uses: "core.noop"},
		{ID: "finish", Name: "Finish", Uses: "core.noop"},
	}, Edges: []flow.PlanEdge{{From: "start", To: "finish"}}}
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	defer screen.Fini()
	screen.SetSize(120, 40)
	opened := ""
	graph := NewGraph().SetPlan(plan).SetOnOpen(func(node flow.PlanNode) { opened = node.ID })
	graph.SetRect(0, 0, 120, 40)
	graph.Draw(screen)
	innerX, innerY, _, _ := graph.GetInnerRect()
	target := graph.rects["finish"]
	event := tcell.NewEventMouse(innerX+target.x+1, innerY+target.y+1, tcell.Button1, tcell.ModNone)
	handled, _ := graph.MouseHandler()(tview.MouseLeftClick, event, nil)
	if selected, _ := graph.Selected(); !handled || selected.ID != "finish" {
		t.Fatalf("handled=%v selected=%s", handled, selected.ID)
	}
	graph.MouseHandler()(tview.MouseLeftDoubleClick, event, nil)
	if opened != "finish" {
		t.Fatalf("opened=%q", opened)
	}
}

func TestGraphPlanReplacementClearsLiveOverlay(t *testing.T) {
	t.Parallel()
	graph := NewGraph().SetPlan(flow.ExecutionPlan{Nodes: []flow.PlanNode{{ID: "old", Uses: "core.noop"}}})
	graph.SetStatus("old", "completed")
	graph.SetPlan(flow.ExecutionPlan{Nodes: []flow.PlanNode{{ID: "new", Uses: "core.noop"}}})
	if len(graph.status) != 0 {
		t.Fatalf("stale live overlay survived plan replacement: %+v", graph.status)
	}
}

func TestSecretRefFormExposesOnlyReferenceMetadata(t *testing.T) {
	t.Parallel()
	fields := resourceFormFields("SecretRef", map[string]any{})
	if len(fields) != 2 || fields[0].label != "Backend" || fields[1].label != "Reference key/path" {
		t.Fatalf("secret form fields = %+v", fields)
	}
}

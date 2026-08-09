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

func TestGraphSelectsAndOpensEdgeByKeyboardAndMouse(t *testing.T) {
	t.Parallel()
	plan := flow.ExecutionPlan{Nodes: []flow.PlanNode{
		{ID: "start", Name: "Start", Uses: "core.noop"},
		{ID: "finish", Name: "Finish", Uses: "core.noop"},
	}, Edges: []flow.PlanEdge{{From: "start", To: "finish", Condition: "result.ready"}}}
	for _, size := range [][2]int{{80, 24}, {120, 40}, {160, 50}} {
		size := size
		t.Run(fmt.Sprintf("keyboard-%dx%d", size[0], size[1]), func(t *testing.T) {
			opened := -1
			graph := NewGraph().SetPlan(plan).SetOnOpenEdge(func(_ flow.PlanEdge, index int) { opened = index })
			graph.InputHandler()(tcell.NewEventKey(tcell.KeyTab, 0, 0), nil)
			graph.InputHandler()(tcell.NewEventKey(tcell.KeyTab, 0, 0), nil)
			edge, index, ok := graph.SelectedEdge()
			if !ok || index != 0 || edge.Condition != "result.ready" {
				t.Fatalf("selected edge=%+v index=%d ok=%v", edge, index, ok)
			}
			graph.InputHandler()(tcell.NewEventKey(tcell.KeyEnter, 0, 0), nil)
			if opened != 0 {
				t.Fatalf("opened edge=%d", opened)
			}
		})
	}

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	defer screen.Fini()
	screen.SetSize(120, 40)
	opened := -1
	graph := NewGraph().SetPlan(plan).SetOnOpenEdge(func(_ flow.PlanEdge, index int) { opened = index })
	graph.SetRect(0, 0, 120, 40)
	graph.Draw(screen)
	innerX, innerY, _, _ := graph.GetInnerRect()
	start := graph.rects["start"]
	edgeEvent := tcell.NewEventMouse(innerX+start.x+start.width+2, innerY+start.y+start.height/2, tcell.Button1, tcell.ModNone)
	if handled, _ := graph.MouseHandler()(tview.MouseLeftClick, edgeEvent, nil); !handled {
		t.Fatal("edge click was not handled")
	}
	if _, index, ok := graph.SelectedEdge(); !ok || index != 0 {
		t.Fatalf("mouse selected edge index=%d ok=%v", index, ok)
	}
	graph.MouseHandler()(tview.MouseLeftDoubleClick, edgeEvent, nil)
	if opened != 0 {
		t.Fatalf("mouse opened edge=%d", opened)
	}
}

func TestGraphPlanReplacementPreservesExistingSelection(t *testing.T) {
	t.Parallel()
	plan := flow.ExecutionPlan{Nodes: []flow.PlanNode{{ID: "start", Uses: "core.noop"}, {ID: "finish", Uses: "core.noop"}}, Edges: []flow.PlanEdge{{From: "start", To: "finish"}}}
	graph := NewGraph().SetPlan(plan)
	graph.InputHandler()(tcell.NewEventKey(tcell.KeyTab, 0, 0), nil)
	graph.SetPlan(plan)
	if selected, ok := graph.Selected(); !ok || selected.ID != "finish" {
		t.Fatalf("selection was not preserved: %+v ok=%v", selected, ok)
	}
}

func TestGraphPlanReplacementPreservesDuplicateEdgeIndex(t *testing.T) {
	t.Parallel()
	edge := flow.PlanEdge{From: "start", To: "finish", Condition: "true"}
	plan := flow.ExecutionPlan{Nodes: []flow.PlanNode{{ID: "start"}, {ID: "finish"}}, Edges: []flow.PlanEdge{edge, edge}}
	graph := NewGraph().SetPlan(plan)
	for range 3 {
		graph.InputHandler()(tcell.NewEventKey(tcell.KeyTab, 0, 0), nil)
	}
	if _, index, ok := graph.SelectedEdge(); !ok || index != 1 {
		t.Fatalf("initial duplicate edge index=%d ok=%t", index, ok)
	}
	graph.SetPlan(plan)
	if _, index, ok := graph.SelectedEdge(); !ok || index != 1 {
		t.Fatalf("replacement duplicate edge index=%d ok=%t", index, ok)
	}
}

func TestGraphMouseSelectsBackwardAndSelfLoopEdges(t *testing.T) {
	t.Parallel()
	plan := flow.ExecutionPlan{
		Nodes: []flow.PlanNode{{ID: "alpha", Uses: "core.noop"}, {ID: "beta", Uses: "core.noop"}},
		Edges: []flow.PlanEdge{{From: "alpha", To: "beta"}, {From: "beta", To: "alpha"}, {From: "alpha", To: "alpha"}},
	}
	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	defer screen.Fini()
	screen.SetSize(80, 24)
	graph := NewGraph().SetPlan(plan)
	graph.SetRect(0, 0, 80, 24)
	graph.Draw(screen)
	innerX, innerY, _, _ := graph.GetInnerRect()

	click := func(index, x, y int) {
		t.Helper()
		event := tcell.NewEventMouse(innerX+x, innerY+y, tcell.Button1, tcell.ModNone)
		if handled, _ := graph.MouseHandler()(tview.MouseLeftClick, event, nil); !handled {
			t.Fatalf("edge %d click was not handled", index)
		}
		if _, selected, ok := graph.SelectedEdge(); !ok || selected != index {
			t.Fatalf("edge %d selected=%d ok=%t", index, selected, ok)
		}
	}
	alpha, beta := graph.rects["alpha"], graph.rects["beta"]
	click(0, alpha.x+alpha.width+1, alpha.y+alpha.height/2)
	click(1, beta.x+beta.width+1, beta.y+beta.height/2)
	click(2, rightmost(graph.rects)+2+2*2-1, alpha.y+alpha.height-2)
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

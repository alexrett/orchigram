package tui

import (
	"fmt"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/gdamore/tcell/v2"
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

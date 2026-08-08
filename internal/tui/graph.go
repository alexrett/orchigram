// Package tui implements Orchigram's keyboard-first terminal operator surface.
package tui

import (
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
)

const (
	boxWidth  = 20
	boxHeight = 5
	columnGap = 7
	rowGap    = 2
)

type nodeRect struct{ x, y, width, height int }

func (r nodeRect) center() (int, int) { return r.x + r.width/2, r.y + r.height/2 }

// Graph is a custom tview primitive for the compiled workflow graph.
type Graph struct {
	*tview.Box
	mu       sync.RWMutex
	plan     flow.ExecutionPlan
	rects    map[string]nodeRect
	order    []string
	selected string
	offsetX  int
	offsetY  int
	onOpen   func(flow.PlanNode)
	onSelect func(flow.PlanNode)
	status   map[string]string
}

// NewGraph constructs an empty focusable graph.
func NewGraph() *Graph {
	return &Graph{Box: tview.NewBox().SetBorder(true).SetTitle(" Graph "), rects: map[string]nodeRect{}, status: map[string]string{}}
}

// SetPlan replaces the immutable definition and recalculates layout.
func (g *Graph) SetPlan(plan flow.ExecutionPlan) *Graph {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.plan = plan
	g.status = map[string]string{}
	g.rects, g.order = layout(plan)
	if len(g.order) > 0 {
		g.selected = g.order[0]
	} else {
		g.selected = ""
	}
	g.offsetX, g.offsetY = 0, 0
	return g
}

// SetStatus overlays live or replayed node status.
func (g *Graph) SetStatus(nodeID, status string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.status[nodeID] = status
}

// SetOnOpen registers Enter/double-click drill-down.
func (g *Graph) SetOnOpen(callback func(flow.PlanNode)) *Graph { g.onOpen = callback; return g }

// SetOnSelect registers selection inspection.
func (g *Graph) SetOnSelect(callback func(flow.PlanNode)) *Graph { g.onSelect = callback; return g }

// Selected returns the current node.
func (g *Graph) Selected() (flow.PlanNode, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, node := range g.plan.Nodes {
		if node.ID == g.selected {
			return node, true
		}
	}
	return flow.PlanNode{}, false
}

// Draw renders clipped boxes and ASCII edges into the current viewport.
func (g *Graph) Draw(screen tcell.Screen) {
	g.DrawForSubclass(screen, g)
	x, y, width, height := g.GetInnerRect()
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, edge := range g.plan.Edges {
		from, fromOK := g.rects[edge.From]
		to, toOK := g.rects[edge.To]
		if !fromOK || !toOK {
			continue
		}
		fromX, fromY := from.x+from.width-g.offsetX, from.y+from.height/2-g.offsetY
		toX, toY := to.x-g.offsetX, to.y+to.height/2-g.offsetY
		drawEdge(screen, x, y, width, height, fromX, fromY, toX, toY)
	}
	for _, id := range g.order {
		rect := g.rects[id]
		node, ok := g.nodeByID(id)
		if !ok {
			continue
		}
		drawNode(screen, x, y, width, height, rect.x-g.offsetX, rect.y-g.offsetY, node, id == g.selected, g.status[id])
	}
}

// InputHandler implements keyboard navigation, drill-down, and panning.
func (g *Graph) InputHandler() func(*tcell.EventKey, func(tview.Primitive)) {
	return g.WrapInputHandler(func(event *tcell.EventKey, _ func(tview.Primitive)) {
		switch event.Key() { //nolint:exhaustive // Unhandled terminal keys intentionally pass through.
		case tcell.KeyEnter:
			g.open()
		case tcell.KeyTab, tcell.KeyRight, tcell.KeyDown:
			g.step(1)
		case tcell.KeyBacktab, tcell.KeyLeft, tcell.KeyUp:
			g.step(-1)
		case tcell.KeyRune:
			switch event.Rune() {
			case 'h':
				g.moveDirectional(-1, 0)
			case 'j':
				g.moveDirectional(0, 1)
			case 'k':
				g.moveDirectional(0, -1)
			case 'l':
				g.moveDirectional(1, 0)
			case 'a':
				g.pan(-4, 0)
			case 'd':
				g.pan(4, 0)
			case 'w':
				g.pan(0, -2)
			case 's':
				g.pan(0, 2)
			}
		default:
		}
	})
}

// MouseHandler supports selection, double-click drill-down, and wheel panning.
func (g *Graph) MouseHandler() func(tview.MouseAction, *tcell.EventMouse, func(tview.Primitive)) (bool, tview.Primitive) {
	return g.WrapMouseHandler(func(action tview.MouseAction, event *tcell.EventMouse, _ func(tview.Primitive)) (bool, tview.Primitive) {
		x, y := event.Position()
		innerX, innerY, _, _ := g.GetInnerRect()
		switch action { //nolint:exhaustive // Unsupported mouse actions are intentionally ignored.
		case tview.MouseLeftClick, tview.MouseLeftDoubleClick:
			g.mu.Lock()
			for id, rect := range g.rects {
				drawX, drawY := innerX+rect.x-g.offsetX, innerY+rect.y-g.offsetY
				if x >= drawX && x < drawX+rect.width && y >= drawY && y < drawY+rect.height {
					g.selected = id
					break
				}
			}
			g.mu.Unlock()
			g.notifySelection()
			if action == tview.MouseLeftDoubleClick {
				g.open()
			}
			return true, g
		case tview.MouseScrollUp:
			g.pan(0, -2)
			return true, g
		case tview.MouseScrollDown:
			g.pan(0, 2)
			return true, g
		case tview.MouseScrollLeft:
			g.pan(-4, 0)
			return true, g
		case tview.MouseScrollRight:
			g.pan(4, 0)
			return true, g
		default:
		}
		return false, nil
	})
}

func (g *Graph) step(delta int) {
	g.mu.Lock()
	if len(g.order) == 0 {
		g.mu.Unlock()
		return
	}
	index := 0
	for i, id := range g.order {
		if id == g.selected {
			index = i
			break
		}
	}
	index = (index + delta + len(g.order)) % len(g.order)
	g.selected = g.order[index]
	g.mu.Unlock()
	g.notifySelection()
}

func (g *Graph) moveDirectional(dx, dy int) {
	g.mu.Lock()
	current, ok := g.rects[g.selected]
	if !ok {
		g.mu.Unlock()
		return
	}
	cx, cy := current.center()
	best, score := "", math.MaxFloat64
	for _, id := range g.order {
		if id == g.selected {
			continue
		}
		x, y := g.rects[id].center()
		deltaX, deltaY := x-cx, y-cy
		if dx != 0 && deltaX*dx <= 0 {
			continue
		}
		if dy != 0 && deltaY*dy <= 0 {
			continue
		}
		candidate := float64(deltaX*deltaX + deltaY*deltaY)
		if candidate < score {
			best, score = id, candidate
		}
	}
	if best != "" {
		g.selected = best
	}
	g.mu.Unlock()
	g.notifySelection()
}

func (g *Graph) pan(dx, dy int) {
	g.mu.Lock()
	g.offsetX = max(0, g.offsetX+dx)
	g.offsetY = max(0, g.offsetY+dy)
	g.mu.Unlock()
}

func (g *Graph) open() {
	if node, ok := g.Selected(); ok && g.onOpen != nil {
		g.onOpen(node)
	}
}
func (g *Graph) notifySelection() {
	if node, ok := g.Selected(); ok && g.onSelect != nil {
		g.onSelect(node)
	}
}
func (g *Graph) nodeByID(id string) (flow.PlanNode, bool) {
	for _, node := range g.plan.Nodes {
		if node.ID == id {
			return node, true
		}
	}
	return flow.PlanNode{}, false
}

func layout(plan flow.ExecutionPlan) (map[string]nodeRect, []string) {
	indegree := map[string]int{}
	adjacency := map[string][]string{}
	for _, node := range plan.Nodes {
		indegree[node.ID] = 0
	}
	for _, edge := range plan.Edges {
		indegree[edge.To]++
		adjacency[edge.From] = append(adjacency[edge.From], edge.To)
	}
	for id := range adjacency {
		sort.Strings(adjacency[id])
	}
	queue := []string{}
	for id, degree := range indegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}
	sort.Strings(queue)
	rank := map[string]int{}
	order := []string{}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		for _, next := range adjacency[id] {
			if rank[next] < rank[id]+1 {
				rank[next] = rank[id] + 1
			}
			indegree[next]--
			if indegree[next] == 0 {
				queue = append(queue, next)
				sort.Strings(queue)
			}
		}
	}
	if len(order) < len(plan.Nodes) {
		seen := map[string]bool{}
		for _, id := range order {
			seen[id] = true
		}
		remaining := []string{}
		for _, node := range plan.Nodes {
			if !seen[node.ID] {
				remaining = append(remaining, node.ID)
			}
		}
		sort.Strings(remaining)
		order = append(order, remaining...)
	}
	rows := map[int]int{}
	rects := map[string]nodeRect{}
	for _, id := range order {
		column := rank[id]
		row := rows[column]
		rows[column]++
		rects[id] = nodeRect{x: column * (boxWidth + columnGap), y: row * (boxHeight + rowGap), width: boxWidth, height: boxHeight}
	}
	return rects, order
}

func drawNode(screen tcell.Screen, viewportX, viewportY, viewportWidth, viewportHeight, x, y int, node flow.PlanNode, selected bool, status string) {
	color := tcell.ColorDarkCyan
	if selected {
		color = tcell.ColorYellow
	}
	switch status {
	case "completed":
		color = tcell.ColorGreen
	case "failed", "rejected":
		color = tcell.ColorRed
	case "running", "waiting":
		color = tcell.ColorBlue
	}
	for dx := 0; dx < boxWidth; dx++ {
		put(screen, viewportX, viewportY, viewportWidth, viewportHeight, x+dx, y, horizontal(dx, boxWidth), color)
		put(screen, viewportX, viewportY, viewportWidth, viewportHeight, x+dx, y+boxHeight-1, horizontal(dx, boxWidth), color)
	}
	for dy := 1; dy < boxHeight-1; dy++ {
		put(screen, viewportX, viewportY, viewportWidth, viewportHeight, x, y+dy, '│', color)
		put(screen, viewportX, viewportY, viewportWidth, viewportHeight, x+boxWidth-1, y+dy, '│', color)
	}
	label := truncate(node.Name, boxWidth-4)
	action := truncate(node.Uses, boxWidth-4)
	printAt(screen, viewportX, viewportY, viewportWidth, viewportHeight, x+2, y+1, label, color)
	printAt(screen, viewportX, viewportY, viewportWidth, viewportHeight, x+2, y+2, action, tcell.ColorGray)
}

func horizontal(index, width int) rune {
	if index == 0 {
		return '┌'
	}
	if index == width-1 {
		return '┐'
	}
	return '─'
}

func drawEdge(screen tcell.Screen, vx, vy, vw, vh, fromX, fromY, toX, toY int) {
	if toX > fromX {
		for x := fromX; x < toX; x++ {
			put(screen, vx, vy, vw, vh, x, fromY, '─', tcell.ColorGray)
		}
		if toY != fromY {
			step := 1
			if toY < fromY {
				step = -1
			}
			for y := fromY; y != toY; y += step {
				put(screen, vx, vy, vw, vh, toX-1, y, '│', tcell.ColorGray)
			}
		}
		put(screen, vx, vy, vw, vh, toX-1, toY, '▶', tcell.ColorGray)
	}
}

func put(screen tcell.Screen, vx, vy, vw, vh, x, y int, r rune, color tcell.Color) {
	if x < 0 || y < 0 || x >= vw || y >= vh {
		return
	}
	screen.SetContent(vx+x, vy+y, r, nil, tcell.StyleDefault.Foreground(color))
}

func printAt(screen tcell.Screen, vx, vy, vw, vh, x, y int, text string, color tcell.Color) {
	for _, r := range text {
		put(screen, vx, vy, vw, vh, x, y, r, color)
		x++
	}
}

func truncate(value string, width int) string {
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= width {
		return string(runes)
	}
	if width <= 1 {
		return string(runes[:width])
	}
	return string(runes[:width-1]) + "…"
}

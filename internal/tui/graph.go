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
	// selectedEdge is -1 while a node is selected.
	selectedEdge int
	offsetX      int
	offsetY      int
	onOpen       func(flow.PlanNode)
	onOpenEdge   func(flow.PlanEdge, int)
	onSelect     func(flow.PlanNode)
	onSelectEdge func(flow.PlanEdge, int)
	status       map[string]string
}

// NewGraph constructs an empty focusable graph.
func NewGraph() *Graph {
	return &Graph{Box: tview.NewBox().SetBorder(true).SetTitle(" Graph "), rects: map[string]nodeRect{}, selectedEdge: -1, status: map[string]string{}}
}

// SetPlan replaces the immutable definition and recalculates layout.
func (g *Graph) SetPlan(plan flow.ExecutionPlan) *Graph {
	g.mu.Lock()
	defer g.mu.Unlock()
	previousNode := g.selected
	var previousEdge flow.PlanEdge
	hadEdge := g.selectedEdge >= 0 && g.selectedEdge < len(g.plan.Edges)
	if hadEdge {
		previousEdge = g.plan.Edges[g.selectedEdge]
	}
	g.plan = plan
	g.status = map[string]string{}
	g.rects, g.order = layout(plan)
	g.selected, g.selectedEdge = "", -1
	if _, exists := g.rects[previousNode]; previousNode != "" && exists {
		g.selected = previousNode
	} else if hadEdge {
		for index, edge := range plan.Edges {
			if edge == previousEdge {
				g.selectedEdge = index
				break
			}
		}
	} else if len(g.order) > 0 {
		g.selected = g.order[0]
	}
	if g.selected == "" && g.selectedEdge < 0 && len(g.order) > 0 {
		g.selected = g.order[0]
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

// SetOnOpenEdge registers Enter/double-click drill-down for an edge.
func (g *Graph) SetOnOpenEdge(callback func(flow.PlanEdge, int)) *Graph {
	g.onOpenEdge = callback
	return g
}

// SetOnSelect registers selection inspection.
func (g *Graph) SetOnSelect(callback func(flow.PlanNode)) *Graph { g.onSelect = callback; return g }

// SetOnSelectEdge registers edge selection inspection.
func (g *Graph) SetOnSelectEdge(callback func(flow.PlanEdge, int)) *Graph {
	g.onSelectEdge = callback
	return g
}

// Selected returns the current node.
func (g *Graph) Selected() (flow.PlanNode, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.selectedEdge >= 0 {
		return flow.PlanNode{}, false
	}
	for _, node := range g.plan.Nodes {
		if node.ID == g.selected {
			return node, true
		}
	}
	return flow.PlanNode{}, false
}

// SelectedEdge returns the current edge and its stable plan index.
func (g *Graph) SelectedEdge() (flow.PlanEdge, int, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.selectedEdge < 0 || g.selectedEdge >= len(g.plan.Edges) {
		return flow.PlanEdge{}, -1, false
	}
	return g.plan.Edges[g.selectedEdge], g.selectedEdge, true
}

// Draw renders clipped boxes and ASCII edges into the current viewport.
func (g *Graph) Draw(screen tcell.Screen) {
	g.DrawForSubclass(screen, g)
	x, y, width, height := g.GetInnerRect()
	g.mu.RLock()
	defer g.mu.RUnlock()
	graphRight := rightmost(g.rects)
	for index, edge := range g.plan.Edges {
		from, fromOK := g.rects[edge.From]
		to, toOK := g.rects[edge.To]
		if !fromOK || !toOK {
			continue
		}
		from.x, from.y = from.x-g.offsetX, from.y-g.offsetY
		to.x, to.y = to.x-g.offsetX, to.y-g.offsetY
		drawEdge(screen, x, y, width, height, from, to, graphRight-g.offsetX, index, index == g.selectedEdge)
	}
	for _, id := range g.order {
		rect := g.rects[id]
		node, ok := g.nodeByID(id)
		if !ok {
			continue
		}
		drawNode(screen, x, y, width, height, rect.x-g.offsetX, rect.y-g.offsetY, node, g.selectedEdge < 0 && id == g.selected, g.status[id])
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
			case 'H':
				g.pan(-4, 0)
			case 'L':
				g.pan(4, 0)
			case 'K':
				g.pan(0, -2)
			case 'J':
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
			selected := false
			for id, rect := range g.rects {
				drawX, drawY := innerX+rect.x-g.offsetX, innerY+rect.y-g.offsetY
				if x >= drawX && x < drawX+rect.width && y >= drawY && y < drawY+rect.height {
					g.selected = id
					g.selectedEdge = -1
					selected = true
					break
				}
			}
			if !selected {
				localX, localY := x-innerX+g.offsetX, y-innerY+g.offsetY
				graphRight := rightmost(g.rects)
				for index, edge := range g.plan.Edges {
					from, fromOK := g.rects[edge.From]
					to, toOK := g.rects[edge.To]
					if fromOK && toOK && edgeContains(from, to, graphRight, index, localX, localY) {
						g.selected, g.selectedEdge = "", index
						selected = true
						break
					}
				}
			}
			g.mu.Unlock()
			if !selected {
				return false, nil
			}
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
	count := len(g.order) + len(g.plan.Edges)
	if count == 0 {
		g.mu.Unlock()
		return
	}
	index := 0
	if g.selectedEdge >= 0 {
		index = len(g.order) + g.selectedEdge
	} else {
		for i, id := range g.order {
			if id == g.selected {
				index = i
				break
			}
		}
	}
	index = (index + delta + count) % count
	if index < len(g.order) {
		g.selected, g.selectedEdge = g.order[index], -1
	} else {
		g.selected, g.selectedEdge = "", index-len(g.order)
	}
	g.mu.Unlock()
	g.notifySelection()
}

func (g *Graph) moveDirectional(dx, dy int) {
	g.mu.Lock()
	if g.selectedEdge >= 0 && g.selectedEdge < len(g.plan.Edges) {
		edge := g.plan.Edges[g.selectedEdge]
		g.selected = edge.To
		if dx < 0 || dy < 0 {
			g.selected = edge.From
		}
		g.selectedEdge = -1
		g.mu.Unlock()
		g.notifySelection()
		return
	}
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
		g.selectedEdge = -1
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
	if edge, index, ok := g.SelectedEdge(); ok {
		if g.onOpenEdge != nil {
			g.onOpenEdge(edge, index)
		}
		return
	}
	if node, ok := g.Selected(); ok && g.onOpen != nil {
		g.onOpen(node)
	}
}
func (g *Graph) notifySelection() {
	if edge, index, ok := g.SelectedEdge(); ok {
		if g.onSelectEdge != nil {
			g.onSelectEdge(edge, index)
		}
		return
	}
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

func drawEdge(screen tcell.Screen, vx, vy, vw, vh int, from, to nodeRect, graphRight, index int, selected bool) {
	color := tcell.ColorGray
	if selected {
		color = tcell.ColorYellow
	}
	fromX, fromY := from.x+from.width, from.y+from.height/2
	toX, toY := to.x, to.y+to.height/2
	if toX > fromX {
		for x := fromX; x < toX; x++ {
			put(screen, vx, vy, vw, vh, x, fromY, '─', color)
		}
		if toY != fromY {
			step := 1
			if toY < fromY {
				step = -1
			}
			for y := fromY; y != toY; y += step {
				put(screen, vx, vy, vw, vh, toX-1, y, '│', color)
			}
		}
		put(screen, vx, vy, vw, vh, toX-1, toY, '▶', color)
		return
	}
	gutterX := graphRight + 2 + index*2
	for x := fromX; x <= gutterX; x++ {
		put(screen, vx, vy, vw, vh, x, fromY, '─', color)
	}
	if from == to {
		returnY := from.y + from.height - 2
		for y := min(fromY, returnY); y <= max(fromY, returnY); y++ {
			put(screen, vx, vy, vw, vh, gutterX, y, '│', color)
		}
		for x := fromX; x <= gutterX; x++ {
			put(screen, vx, vy, vw, vh, x, returnY, '─', color)
		}
		put(screen, vx, vy, vw, vh, fromX, returnY, '◀', color)
		return
	}
	targetY := to.y + 1
	if index%2 == 1 {
		targetY = to.y + to.height - 2
	}
	for y := min(fromY, targetY); y <= max(fromY, targetY); y++ {
		put(screen, vx, vy, vw, vh, gutterX, y, '│', color)
	}
	targetRight := to.x + to.width
	for x := targetRight; x <= gutterX; x++ {
		put(screen, vx, vy, vw, vh, x, targetY, '─', color)
	}
	put(screen, vx, vy, vw, vh, targetRight, targetY, '◀', color)
}

func edgeContains(from, to nodeRect, graphRight, index, x, y int) bool {
	fromX, fromY := from.x+from.width, from.y+from.height/2
	toX, toY := to.x, to.y+to.height/2
	if toX > fromX {
		if y == fromY && x >= fromX && x < toX {
			return true
		}
		lowY, highY := min(fromY, toY), max(fromY, toY)
		return x == toX-1 && y >= lowY && y <= highY
	}
	gutterX := graphRight + 2 + index*2
	if y == fromY && x >= fromX && x <= gutterX {
		return true
	}
	if from == to {
		returnY := from.y + from.height - 2
		return x == gutterX && y >= min(fromY, returnY) && y <= max(fromY, returnY) ||
			y == returnY && x >= fromX && x <= gutterX
	}
	targetRight := to.x + to.width
	targetY := to.y + 1
	if index%2 == 1 {
		targetY = to.y + to.height - 2
	}
	return x == gutterX && y >= min(fromY, targetY) && y <= max(fromY, targetY) ||
		y == targetY && x >= targetRight && x <= gutterX
}

func rightmost(rects map[string]nodeRect) int {
	result := 0
	for _, rect := range rects {
		result = max(result, rect.x+rect.width)
	}
	return result
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

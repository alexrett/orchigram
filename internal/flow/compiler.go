// Package flow compiles declarative Flow resources into immutable plans.
package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/alexrett/orchigram/internal/resource"
	"github.com/google/cel-go/cel"
)

const (
	// InterpreterVersion is pinned by every v0.1 run.
	InterpreterVersion = "generic-v1"
	defaultTimeout     = time.Hour
)

var nodeID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

// CapabilityResolver resolves plugin action availability at compile time.
type CapabilityResolver interface {
	HasAction(action string) bool
}

// ActionValidator optionally performs plugin-owned configuration validation.
type ActionValidator interface {
	ValidateAction(action string, config map[string]any) []Diagnostic
}

// Diagnostic is a stable compiler diagnostic.
type Diagnostic struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// PlanNode is a fully defaulted immutable execution node.
type PlanNode struct {
	ID                string         `json:"id"`
	Name              string         `json:"name"`
	Uses              string         `json:"uses"`
	With              map[string]any `json:"with,omitempty"`
	RetryLimit        int            `json:"retryLimit"`
	RetryBackoff      string         `json:"retryBackoff"`
	Timeout           string         `json:"timeout"`
	LoopMaxIterations int            `json:"loopMaxIterations,omitempty"`
}

// PlanEdge is a validated plan transition.
type PlanEdge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Condition string `json:"condition,omitempty"`
}

// ExecutionPlan is the immutable data interpreted by the durable engine.
type ExecutionPlan struct {
	APIVersion         string     `json:"apiVersion"`
	FlowUID            string     `json:"flowUID"`
	FlowGeneration     uint64     `json:"flowGeneration"`
	InterpreterVersion string     `json:"interpreterVersion"`
	Timeout            string     `json:"timeout"`
	MaxParallel        int        `json:"maxParallel"`
	Nodes              []PlanNode `json:"nodes"`
	Edges              []PlanEdge `json:"edges"`
	Components         [][]string `json:"components"`
	PlanHash           string     `json:"planHash"`
}

// Compiler validates and canonicalizes Flow graphs.
type Compiler struct {
	capabilities CapabilityResolver
}

// EvaluateEdges evaluates already type-checked CEL conditions at an activity boundary.
func EvaluateEdges(edges []PlanEdge, input, result, nodes map[string]any) ([]bool, error) {
	env, err := cel.NewEnv(cel.Variable("input", cel.DynType), cel.Variable("result", cel.DynType), cel.Variable("nodes", cel.DynType))
	if err != nil {
		return nil, err
	}
	activated := make([]bool, len(edges))
	for i, edge := range edges {
		if edge.Condition == "" {
			activated[i] = true
			continue
		}
		ast, issues := env.Compile(edge.Condition)
		if issues != nil && issues.Err() != nil {
			return nil, fmt.Errorf("compile edge %s -> %s: %w", edge.From, edge.To, issues.Err())
		}
		program, err := env.Program(ast)
		if err != nil {
			return nil, err
		}
		value, _, err := program.Eval(map[string]any{"input": input, "result": result, "nodes": nodes})
		if err != nil {
			return nil, fmt.Errorf("evaluate edge %s -> %s: %w", edge.From, edge.To, err)
		}
		boolean, ok := value.Value().(bool)
		if !ok {
			return nil, fmt.Errorf("edge %s -> %s did not evaluate to bool", edge.From, edge.To)
		}
		activated[i] = boolean
	}
	return activated, nil
}

// NewCompiler creates a compiler with an optional plugin capability resolver.
func NewCompiler(capabilities CapabilityResolver) *Compiler {
	return &Compiler{capabilities: capabilities}
}

// Compile returns a stable plan or all deterministic validation diagnostics.
func (c *Compiler) Compile(input resource.Flow) (ExecutionPlan, []Diagnostic) {
	diagnostics := make([]Diagnostic, 0)
	timeout, err := resource.ParseDuration(input.Spec.Policies.Timeout, defaultTimeout)
	if err != nil {
		diagnostics = append(diagnostics, Diagnostic{Path: "spec.policies.timeout", Code: "invalid_duration", Message: err.Error()})
		timeout = defaultTimeout
	}
	maxParallel := input.Spec.Policies.MaxParallel
	if maxParallel == 0 {
		maxParallel = 1
	}
	if maxParallel < 1 || maxParallel > 128 {
		diagnostics = append(diagnostics, Diagnostic{Path: "spec.policies.maxParallel", Code: "out_of_range", Message: "must be between 1 and 128"})
	}

	nodes := make(map[string]resource.FlowNode, len(input.Spec.Nodes))
	planNodes := make([]PlanNode, 0, len(input.Spec.Nodes))
	for i, node := range input.Spec.Nodes {
		path := fmt.Sprintf("spec.nodes[%d]", i)
		if !nodeID.MatchString(node.ID) {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".id", Code: "invalid_node_id", Message: "must match ^[a-z][a-z0-9_-]{0,62}$"})
		}
		if _, exists := nodes[node.ID]; exists {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".id", Code: "duplicate_node_id", Message: fmt.Sprintf("node %q is duplicated", node.ID)})
			continue
		}
		nodes[node.ID] = node
		if !validAction(node.Uses) { //nolint:gocritic // Ordered diagnostics are clearer than a tagged switch here.
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".uses", Code: "invalid_action", Message: "action must be core.<name> or <plugin>.<action>"})
		} else if !strings.HasPrefix(node.Uses, "core.") && c.capabilities != nil && !c.capabilities.HasAction(node.Uses) {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".uses", Code: "unknown_action", Message: fmt.Sprintf("no enabled plugin provides %q", node.Uses)})
		} else if !strings.HasPrefix(node.Uses, "core.") {
			if validator, ok := c.capabilities.(ActionValidator); ok {
				for _, diagnostic := range validator.ValidateAction(node.Uses, node.With) {
					diagnostic.Path = path + ".with." + strings.TrimPrefix(diagnostic.Path, "config.")
					diagnostics = append(diagnostics, diagnostic)
				}
			}
		}
		nodeTimeout, timeoutErr := resource.ParseDuration(node.Timeout, timeout)
		if timeoutErr != nil {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".timeout", Code: "invalid_duration", Message: timeoutErr.Error()})
			nodeTimeout = timeout
		}
		retryLimit := 0
		retryBackoff := "1s"
		if node.Retry != nil {
			retryLimit = node.Retry.Limit
			if retryLimit < 0 || retryLimit > 100 {
				diagnostics = append(diagnostics, Diagnostic{Path: path + ".retry.limit", Code: "out_of_range", Message: "must be between 0 and 100"})
			}
			if node.Retry.Backoff != "" {
				if _, parseErr := resource.ParseDuration(node.Retry.Backoff, time.Second); parseErr != nil {
					diagnostics = append(diagnostics, Diagnostic{Path: path + ".retry.backoff", Code: "invalid_duration", Message: parseErr.Error()})
				} else {
					retryBackoff = node.Retry.Backoff
				}
			}
		}
		loopMax := 0
		if node.Loop != nil {
			loopMax = node.Loop.MaxIterations
			if loopMax < 1 || loopMax > 1000 {
				diagnostics = append(diagnostics, Diagnostic{Path: path + ".loop.maxIterations", Code: "out_of_range", Message: "must be between 1 and 1000"})
			}
		}
		name := node.Name
		if name == "" {
			name = node.ID
		}
		planNodes = append(planNodes, PlanNode{ID: node.ID, Name: name, Uses: node.Uses, With: node.With, RetryLimit: retryLimit, RetryBackoff: retryBackoff, Timeout: nodeTimeout.String(), LoopMaxIterations: loopMax})
	}

	celEnv, celErr := cel.NewEnv(cel.Variable("input", cel.DynType), cel.Variable("result", cel.DynType), cel.Variable("nodes", cel.DynType))
	if celErr != nil {
		panic(celErr)
	}
	edges := make([]PlanEdge, 0, len(input.Spec.Edges))
	adjacency := make(map[string][]string, len(nodes))
	for i, edge := range input.Spec.Edges {
		path := fmt.Sprintf("spec.edges[%d]", i)
		if _, ok := nodes[edge.From]; !ok {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".from", Code: "unknown_node", Message: fmt.Sprintf("node %q does not exist", edge.From)})
		}
		if _, ok := nodes[edge.To]; !ok {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".to", Code: "unknown_node", Message: fmt.Sprintf("node %q does not exist", edge.To)})
		}
		if edge.When != "" {
			ast, issues := celEnv.Compile(edge.When)
			if issues != nil && issues.Err() != nil {
				diagnostics = append(diagnostics, Diagnostic{Path: path + ".when", Code: "invalid_cel", Message: issues.Err().Error()})
			} else if ast.OutputType() != cel.BoolType && ast.OutputType() != cel.DynType {
				diagnostics = append(diagnostics, Diagnostic{Path: path + ".when", Code: "cel_not_boolean", Message: "condition must produce bool"})
			}
		}
		if _, fromOK := nodes[edge.From]; fromOK {
			if _, toOK := nodes[edge.To]; toOK {
				adjacency[edge.From] = append(adjacency[edge.From], edge.To)
			}
		}
		edges = append(edges, PlanEdge{From: edge.From, To: edge.To, Condition: edge.When})
	}
	for id := range nodes {
		sort.Strings(adjacency[id])
	}
	for i, node := range input.Spec.Nodes {
		diagnostics = append(diagnostics, validateMappings(i, node, nodes)...)
	}
	components := stronglyConnected(sortedKeys(nodes), adjacency)
	for _, component := range components {
		cyclic := len(component) > 1
		if len(component) == 1 {
			for _, target := range adjacency[component[0]] {
				if target == component[0] {
					cyclic = true
				}
			}
		}
		if cyclic {
			finite := false
			for _, id := range component {
				if nodes[id].Loop != nil && nodes[id].Loop.MaxIterations > 0 {
					finite = true
				}
			}
			if !finite {
				diagnostics = append(diagnostics, Diagnostic{Path: "spec.edges", Code: "unbounded_cycle", Message: fmt.Sprintf("cycle %s requires a finite loop policy", strings.Join(component, " -> "))})
			}
		}
	}

	sort.Slice(planNodes, func(i, j int) bool { return planNodes[i].ID < planNodes[j].ID })
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From == edges[j].From {
			if edges[i].To == edges[j].To {
				return edges[i].Condition < edges[j].Condition
			}
			return edges[i].To < edges[j].To
		}
		return edges[i].From < edges[j].From
	})
	plan := ExecutionPlan{APIVersion: resource.APIVersion, FlowUID: input.Metadata.UID, FlowGeneration: input.Metadata.Generation, InterpreterVersion: InterpreterVersion, Timeout: timeout.String(), MaxParallel: maxParallel, Nodes: planNodes, Edges: edges, Components: components}
	if len(diagnostics) == 0 {
		encoded, marshalErr := json.Marshal(plan)
		if marshalErr != nil {
			panic(marshalErr)
		}
		digest := sha256.Sum256(encoded)
		plan.PlanHash = hex.EncodeToString(digest[:])
	}
	return plan, diagnostics
}

func validateMappings(nodeIndex int, node resource.FlowNode, nodes map[string]resource.FlowNode) []Diagnostic {
	raw, exists := node.With["mappings"]
	if !exists {
		return nil
	}
	path := fmt.Sprintf("spec.nodes[%d].with.mappings", nodeIndex)
	items, ok := raw.([]any)
	if !ok {
		return []Diagnostic{{Path: path, Code: "invalid_mapping", Message: "must be a list of {from,to} mappings"}}
	}
	diagnostics := []Diagnostic{}
	for index, item := range items {
		itemPath := fmt.Sprintf("%s[%d]", path, index)
		mapping, ok := item.(map[string]any)
		if !ok {
			diagnostics = append(diagnostics, Diagnostic{Path: itemPath, Code: "invalid_mapping", Message: "must be an object"})
			continue
		}
		for key := range mapping {
			if key != "from" && key != "to" {
				diagnostics = append(diagnostics, Diagnostic{Path: itemPath + "." + key, Code: "unknown_field", Message: "only from and to are allowed"})
			}
		}
		from, fromOK := mapping["from"].(string)
		to, toOK := mapping["to"].(string)
		if !fromOK || from == "" || (from != "input" && !strings.HasPrefix(from, "input.") && !strings.HasPrefix(from, "nodes.")) {
			diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".from", Code: "invalid_source", Message: "must start with input or nodes.<nodeID>"})
		} else if strings.HasPrefix(from, "nodes.") {
			parts := strings.Split(from, ".")
			if len(parts) < 2 {
				diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".from", Code: "invalid_source", Message: "node output path is incomplete"})
			} else if _, exists := nodes[parts[1]]; !exists {
				diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".from", Code: "unknown_node", Message: fmt.Sprintf("node %q does not exist", parts[1])})
			}
		}
		if !toOK || !strings.HasPrefix(to, "/") || to == "/" {
			diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".to", Code: "invalid_target", Message: "must be a non-root JSON pointer"})
		}
	}
	return diagnostics
}

func validAction(action string) bool {
	parts := strings.Split(action, ".")
	return len(parts) >= 2 && nodeID.MatchString(parts[0]) && nodeID.MatchString(strings.Join(parts[1:], "_"))
}

func sortedKeys(nodes map[string]resource.FlowNode) []string {
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func stronglyConnected(ids []string, adjacency map[string][]string) [][]string {
	index := 0
	stack := make([]string, 0, len(ids))
	onStack := map[string]bool{}
	indices := map[string]int{}
	low := map[string]int{}
	components := make([][]string, 0)
	var visit func(string)
	visit = func(v string) {
		indices[v], low[v] = index, index
		index++
		stack = append(stack, v)
		onStack[v] = true
		for _, w := range adjacency[v] {
			if _, seen := indices[w]; !seen {
				visit(w)
				if low[w] < low[v] {
					low[v] = low[w]
				}
			} else if onStack[w] && indices[w] < low[v] {
				low[v] = indices[w]
			}
		}
		if low[v] == indices[v] {
			component := []string{}
			for {
				last := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				onStack[last] = false
				component = append(component, last)
				if last == v {
					break
				}
			}
			sort.Strings(component)
			components = append(components, component)
		}
	}
	for _, id := range ids {
		if _, seen := indices[id]; !seen {
			visit(id)
		}
	}
	sort.Slice(components, func(i, j int) bool { return components[i][0] < components[j][0] })
	return components
}

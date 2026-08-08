// Package flow compiles declarative Flow resources into immutable plans.
package flow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alexrett/orchigram/internal/actioncontract"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/google/cel-go/cel"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
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

// ActionBinder resolves the exact executable and resource projections used by
// a non-core node. Bindings are private execution-plan data, never public Flow
// schema fields.
type ActionBinder interface {
	BindAction(namespace, action string, config map[string]any) (ActionBinding, []Diagnostic)
}

// Diagnostic is a stable compiler diagnostic.
type Diagnostic struct {
	Severity string `json:"severity,omitempty"`
	Path     string `json:"path"`
	Code     string `json:"code"`
	Message  string `json:"message"`
}

const (
	// SeverityError marks a diagnostic that prevents Flow storage.
	SeverityError = "error"
	// SeverityWarning marks a diagnostic that remains runtime-validated.
	SeverityWarning = "warning"
)

// IsError treats the zero value as an error for backward-compatible binders.
func (d Diagnostic) IsError() bool { return d.Severity == "" || d.Severity == SeverityError }

// PluginBinding pins one immutable installed plugin binary.
type PluginBinding struct {
	Name            string `json:"name"`
	Version         string `json:"version"`
	Digest          string `json:"digest"`
	ProtocolVersion uint32 `json:"protocolVersion"`
}

// ResourceBinding pins one referenced configuration projection. SecretRef
// specs contain only backend coordinates; secret values are never compiled.
type ResourceBinding struct {
	Kind            string          `json:"kind"`
	Namespace       string          `json:"namespace"`
	Name            string          `json:"name"`
	UID             string          `json:"uid"`
	ResourceVersion uint64          `json:"resourceVersion"`
	Generation      uint64          `json:"generation"`
	Spec            json.RawMessage `json:"spec"`
}

// ActionContract pins the canonical schemas accepted with the Flow. Replay
// never consults a mutable current plugin descriptor.
type ActionContract struct {
	Digest       string          `json:"digest"`
	ConfigSchema json.RawMessage `json:"configSchema"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
}

// ActionBinding is the compiler-facing resolved action contract.
type ActionBinding struct {
	Plugin    PluginBinding
	Contract  ActionContract
	Config    map[string]any
	Resources []ResourceBinding
}

// PlanNode is a fully defaulted immutable execution node.
type PlanNode struct {
	ID                string            `json:"id"`
	Name              string            `json:"name"`
	Uses              string            `json:"uses"`
	With              map[string]any    `json:"with,omitempty"`
	RetryLimit        int               `json:"retryLimit"`
	RetryBackoff      string            `json:"retryBackoff"`
	Timeout           string            `json:"timeout"`
	LoopMaxIterations int               `json:"loopMaxIterations,omitempty"`
	Plugin            *PluginBinding    `json:"plugin,omitempty"`
	Contract          *ActionContract   `json:"contract,omitempty"`
	Resources         []ResourceBinding `json:"resources,omitempty"`
}

// PlanEdge is a validated plan transition.
type PlanEdge struct {
	From      string `json:"from"`
	To        string `json:"to"`
	Condition string `json:"condition,omitempty"`
}

// ExecutionPlan is the immutable data interpreted by the durable engine.
type ExecutionPlan struct {
	APIVersion         string          `json:"apiVersion"`
	FlowUID            string          `json:"flowUID"`
	FlowGeneration     uint64          `json:"flowGeneration"`
	InterpreterVersion string          `json:"interpreterVersion"`
	Timeout            string          `json:"timeout"`
	MaxParallel        int             `json:"maxParallel"`
	InputSchema        json.RawMessage `json:"inputSchema"`
	Nodes              []PlanNode      `json:"nodes"`
	Edges              []PlanEdge      `json:"edges"`
	Components         [][]string      `json:"components"`
	PlanHash           string          `json:"planHash"`
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

// ValidateRunInput enforces the input schema pinned into an accepted plan.
func ValidateRunInput(plan ExecutionPlan, raw json.RawMessage) error {
	if len(plan.InputSchema) == 0 {
		return nil
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return errors.New("run input is not valid JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("run input must contain exactly one JSON value")
	}
	schema, err := actioncontract.Compile(plan.InputSchema)
	if err != nil {
		return errors.New("pinned run input schema is invalid")
	}
	violations := schema.Validate(value)
	if len(violations) == 0 {
		return nil
	}
	path := "input"
	if len(violations[0].Path) > 0 {
		path += "." + strings.Join(violations[0].Path, ".")
	}
	return fmt.Errorf("%s violates the Flow input schema (%s)", path, violations[0].Code)
}

// NewCompiler creates a compiler with an optional plugin capability resolver.
func NewCompiler(capabilities CapabilityResolver) *Compiler {
	return &Compiler{capabilities: capabilities}
}

// Compile returns a stable plan or all deterministic validation diagnostics.
func (c *Compiler) Compile(input resource.Flow) (ExecutionPlan, []Diagnostic) {
	diagnostics := make([]Diagnostic, 0)
	inputSchemaJSON := json.RawMessage(`{"type":"object","additionalProperties":true}`)
	if len(input.Spec.InputSchema) > 0 {
		encoded, marshalErr := json.Marshal(input.Spec.InputSchema)
		if marshalErr != nil {
			diagnostics = append(diagnostics, Diagnostic{Path: "spec.inputSchema", Code: "invalid_schema", Message: marshalErr.Error()})
		} else {
			inputSchemaJSON = encoded
		}
	}
	compiledInput, schemaErr := actioncontract.Compile(inputSchemaJSON)
	if schemaErr != nil {
		diagnostics = append(diagnostics, Diagnostic{Path: "spec.inputSchema", Code: "invalid_schema", Message: schemaErr.Error()})
		compiledInput, _ = actioncontract.Compile(json.RawMessage(`{"type":"object","additionalProperties":true}`))
	}
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
	nodeContracts := make(map[string]ActionContract, len(input.Spec.Nodes))
	nodeConfigs := make(map[string]map[string]any, len(input.Spec.Nodes))
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
		var binding *ActionBinding
		if !validAction(node.Uses) { //nolint:gocritic // Ordered diagnostics are clearer than a tagged switch here.
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".uses", Code: "invalid_action", Message: "action must be core.<name> or <plugin>.<action>"})
		} else if !strings.HasPrefix(node.Uses, "core.") && c.capabilities != nil && !c.capabilities.HasAction(node.Uses) {
			diagnostics = append(diagnostics, Diagnostic{Path: path + ".uses", Code: "unknown_action", Message: fmt.Sprintf("no enabled plugin provides %q", node.Uses)})
		} else if !strings.HasPrefix(node.Uses, "core.") {
			if binder, ok := c.capabilities.(ActionBinder); ok {
				resolved, bindingDiagnostics := binder.BindAction(input.Metadata.Namespace, node.Uses, node.With)
				for _, diagnostic := range bindingDiagnostics {
					diagnostic.Path = nodeConfigDiagnosticPath(path, diagnostic.Path)
					diagnostics = append(diagnostics, diagnostic)
				}
				if len(bindingDiagnostics) == 0 {
					binding = &resolved
				}
			} else if validator, ok := c.capabilities.(ActionValidator); ok {
				for _, diagnostic := range validator.ValidateAction(node.Uses, node.With) {
					diagnostic.Path = nodeConfigDiagnosticPath(path, diagnostic.Path)
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
		planNode := PlanNode{ID: node.ID, Name: name, Uses: node.Uses, With: node.With, RetryLimit: retryLimit, RetryBackoff: retryBackoff, Timeout: nodeTimeout.String(), LoopMaxIterations: loopMax}
		if binding != nil {
			planNode.With = binding.Config
			planNode.Plugin = &binding.Plugin
			planNode.Contract = &binding.Contract
			planNode.Resources = binding.Resources
			nodeContracts[node.ID] = binding.Contract
			if len(binding.Contract.InputSchema) > 0 {
				actionInput, inputErr := actioncontract.Compile(binding.Contract.InputSchema)
				if inputErr != nil {
					diagnostics = append(diagnostics, Diagnostic{Path: path + ".uses", Code: "invalid_action_schema", Message: "pinned action input schema is invalid"})
				} else if compatible, _ := actioncontract.Compatible(compiledInput.ResolveDot(nil), actionInput.ResolveDot(nil)); !compatible {
					diagnostics = append(diagnostics, Diagnostic{Path: path + ".uses", Code: "action_input_type_mismatch", Message: "Flow input type is incompatible with the action input schema"})
				}
			}
		} else if strings.HasPrefix(node.Uses, "core.") {
			nodeContracts[node.ID] = coreActionContract(node)
		}
		planNodes = append(planNodes, planNode)
		nodeConfigs[node.ID] = planNode.With
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
			switch {
			case issues != nil && issues.Err() != nil:
				diagnostics = append(diagnostics, Diagnostic{Path: path + ".when", Code: "invalid_cel", Message: issues.Err().Error()})
			case ast.OutputType() != cel.BoolType && ast.OutputType() != cel.DynType:
				diagnostics = append(diagnostics, Diagnostic{Path: path + ".when", Code: "cel_not_boolean", Message: "condition must produce bool"})
			default:
				checked, checkedErr := cel.AstToCheckedExpr(ast)
				if checkedErr != nil {
					diagnostics = append(diagnostics, Diagnostic{Path: path + ".when", Code: "invalid_cel", Message: "condition could not be converted to a checked expression"})
				} else {
					diagnostics = append(diagnostics, typedCELDiagnostics(path+".when", checked.GetExpr(), edge.From, compiledInput, nodeContracts)...)
				}
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
		diagnostics = append(diagnostics, validateMappings(i, node, nodes, adjacency, compiledInput, nodeContracts, nodeConfigs[node.ID])...)
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
	plan := ExecutionPlan{APIVersion: resource.APIVersion, FlowUID: input.Metadata.UID, FlowGeneration: input.Metadata.Generation, InterpreterVersion: InterpreterVersion, Timeout: timeout.String(), MaxParallel: maxParallel, InputSchema: inputSchemaJSON, Nodes: planNodes, Edges: edges, Components: components}
	if !hasErrorDiagnostics(diagnostics) {
		encoded, marshalErr := json.Marshal(plan)
		if marshalErr != nil {
			panic(marshalErr)
		}
		digest := sha256.Sum256(encoded)
		plan.PlanHash = hex.EncodeToString(digest[:])
	}
	return plan, diagnostics
}

func nodeConfigDiagnosticPath(nodePath, pluginPath string) string {
	if pluginPath == "" || pluginPath == "config" {
		return nodePath + ".with"
	}
	return nodePath + ".with." + strings.TrimPrefix(pluginPath, "config.")
}

func validateMappings(nodeIndex int, node resource.FlowNode, nodes map[string]resource.FlowNode, adjacency map[string][]string, inputSchema *actioncontract.Schema, contracts map[string]ActionContract, boundConfig map[string]any) []Diagnostic {
	path := fmt.Sprintf("spec.nodes[%d].with", nodeIndex)
	contract, hasContract := contracts[node.ID]
	if !hasContract || len(contract.ConfigSchema) == 0 {
		return validateMappingShapeOnly(nodeIndex, node, nodes)
	}
	configSchema, err := actioncontract.Compile(contract.ConfigSchema)
	if err != nil {
		return []Diagnostic{{Path: path, Code: "invalid_action_schema", Message: "pinned action config schema is invalid"}}
	}
	projected, err := cloneMap(boundConfig)
	if err != nil {
		return []Diagnostic{{Path: path, Code: "invalid_config", Message: err.Error()}}
	}
	delete(projected, "secretRefs")
	rawMappings, hasMappings := projected["mappings"]
	delete(projected, "mappings")
	diagnostics := []Diagnostic{}
	if hasMappings {
		items, ok := rawMappings.([]any)
		if !ok {
			return []Diagnostic{{Path: path + ".mappings", Code: "invalid_mapping", Message: "must be a list of {from,to} mappings"}}
		}
		for index, item := range items {
			itemPath := fmt.Sprintf("%s.mappings[%d]", path, index)
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
			source, sourceOK, sourceDiagnostics := mappingSource(itemPath+".from", from, fromOK, node.ID, nodes, adjacency, inputSchema, contracts)
			diagnostics = append(diagnostics, sourceDiagnostics...)
			if !toOK || !strings.HasPrefix(to, "/") || to == "/" {
				diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".to", Code: "invalid_target", Message: "must be a non-root JSON pointer"})
				continue
			}
			target := configSchema.ResolvePointer(to)
			if !target.Exists {
				diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".to", Code: "unknown_target", Message: fmt.Sprintf("target %q is not declared by the action config schema", to)})
				continue
			}
			if sourceOK {
				compatible, dynamic := actioncontract.Compatible(source, target)
				if !compatible {
					diagnostics = append(diagnostics, Diagnostic{Path: itemPath, Code: "mapping_type_mismatch", Message: "mapping source type is incompatible with the destination action field"})
				} else if dynamic {
					diagnostics = append(diagnostics, Diagnostic{Severity: SeverityWarning, Path: itemPath, Code: "dynamic_mapping", Message: "mapping crosses an explicitly open schema region and will be validated again at runtime"})
				}
			}
			if !hasErrorAt(diagnostics, itemPath) {
				if err := actioncontract.SetPointer(projected, to, actioncontract.Placeholder(target)); err != nil {
					diagnostics = append(diagnostics, Diagnostic{Path: itemPath + ".to", Code: "invalid_target", Message: err.Error()})
				}
			}
		}
	}
	for _, violation := range configSchema.Validate(projected) {
		diagnostics = append(diagnostics, schemaDiagnostic(path, violation))
	}
	return diagnostics
}

func mappingSource(path, from string, fromOK bool, destination string, nodes map[string]resource.FlowNode, adjacency map[string][]string, inputSchema *actioncontract.Schema, contracts map[string]ActionContract) (actioncontract.Resolution, bool, []Diagnostic) {
	if !fromOK || from == "" || (from != "input" && !strings.HasPrefix(from, "input.") && !strings.HasPrefix(from, "nodes.")) {
		return actioncontract.Resolution{}, false, []Diagnostic{{Path: path, Code: "invalid_source", Message: "must start with input or nodes.<nodeID>"}}
	}
	if from == "input" || strings.HasPrefix(from, "input.") {
		parts := []string{}
		if from != "input" {
			parts = strings.Split(strings.TrimPrefix(from, "input."), ".")
		}
		resolution := inputSchema.ResolveDot(parts)
		if !resolution.Exists {
			return resolution, false, []Diagnostic{{Path: path, Code: "unknown_source_field", Message: fmt.Sprintf("source %q is not declared by spec.inputSchema", from)}}
		}
		return resolution, true, nil
	}
	parts := strings.Split(strings.TrimPrefix(from, "nodes."), ".")
	if len(parts) < 2 || parts[0] == "" {
		return actioncontract.Resolution{}, false, []Diagnostic{{Path: path, Code: "invalid_source", Message: "node output source must include nodes.<nodeID>.<field>"}}
	}
	sourceNode := parts[0]
	if _, exists := nodes[sourceNode]; !exists {
		return actioncontract.Resolution{}, false, []Diagnostic{{Path: path, Code: "unknown_node", Message: fmt.Sprintf("node %q does not exist", sourceNode)}}
	}
	if !reaches(sourceNode, destination, adjacency) {
		return actioncontract.Resolution{}, false, []Diagnostic{{Path: path, Code: "source_not_predecessor", Message: fmt.Sprintf("node %q is not a predecessor of %q", sourceNode, destination)}}
	}
	contract, exists := contracts[sourceNode]
	if !exists || len(contract.OutputSchema) == 0 {
		return actioncontract.Resolution{Exists: true, Dynamic: true}, true, nil
	}
	schema, err := actioncontract.Compile(contract.OutputSchema)
	if err != nil {
		return actioncontract.Resolution{}, false, []Diagnostic{{Path: path, Code: "invalid_action_schema", Message: "source node output schema is invalid"}}
	}
	resolution := schema.ResolveDot(parts[1:])
	if !resolution.Exists {
		return resolution, false, []Diagnostic{{Path: path, Code: "unknown_source_field", Message: fmt.Sprintf("source %q is not declared by node %q output schema", from, sourceNode)}}
	}
	return resolution, true, nil
}

func validateMappingShapeOnly(nodeIndex int, node resource.FlowNode, nodes map[string]resource.FlowNode) []Diagnostic {
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

func typedCELDiagnostics(path string, expression *exprpb.Expr, resultNode string, inputSchema *actioncontract.Schema, contracts map[string]ActionContract) []Diagnostic {
	inferred, exists, dynamic := inferCELType(expression, resultNode, inputSchema, contracts)
	if !exists {
		return []Diagnostic{{Path: path, Code: "unknown_cel_field", Message: "condition references a field that is absent from its declared schema"}}
	}
	if inferred != actioncontract.TypeUnknown && inferred&actioncontract.TypeBoolean == 0 {
		return []Diagnostic{{Path: path, Code: "cel_not_boolean", Message: "condition resolves to a non-boolean schema type"}}
	}
	if inferred == actioncontract.TypeUnknown || dynamic {
		return []Diagnostic{{Severity: SeverityWarning, Path: path, Code: "dynamic_cel", Message: "condition crosses an explicitly open schema region and will be type-checked again at runtime"}}
	}
	return nil
}

func inferCELType(expression *exprpb.Expr, resultNode string, inputSchema *actioncontract.Schema, contracts map[string]ActionContract) (actioncontract.Type, bool, bool) {
	if root, parts, ok := celSelectPath(expression); ok {
		var schema *actioncontract.Schema
		switch root {
		case "input":
			schema = inputSchema
		case "result":
			schema = outputSchema(contracts[resultNode])
		case "nodes":
			if len(parts) == 0 {
				return actioncontract.TypeObject, true, true
			}
			schema = outputSchema(contracts[parts[0]])
			parts = parts[1:]
		default:
			return actioncontract.TypeUnknown, true, true
		}
		if schema == nil {
			return actioncontract.TypeUnknown, true, true
		}
		resolution := schema.ResolveDot(parts)
		return resolution.Type, resolution.Exists, resolution.Dynamic
	}
	if constant := expression.GetConstExpr(); constant != nil {
		switch constant.GetConstantKind().(type) {
		case *exprpb.Constant_BoolValue:
			return actioncontract.TypeBoolean, true, false
		case *exprpb.Constant_Int64Value, *exprpb.Constant_Uint64Value:
			return actioncontract.TypeInteger, true, false
		case *exprpb.Constant_DoubleValue:
			return actioncontract.TypeNumber, true, false
		case *exprpb.Constant_StringValue, *exprpb.Constant_BytesValue:
			return actioncontract.TypeString, true, false
		case *exprpb.Constant_NullValue:
			return actioncontract.TypeNull, true, false
		}
	}
	if call := expression.GetCallExpr(); call != nil {
		switch call.GetFunction() {
		case "_==_", "_!=_", "_<_", "_<=_", "_>_", "_>=_", "_&&_", "_||_", "!_", "@in", "_in_":
			dynamic := false
			arguments := append([]*exprpb.Expr(nil), call.GetArgs()...)
			if call.GetTarget() != nil {
				arguments = append(arguments, call.GetTarget())
			}
			for _, argument := range arguments {
				_, exists, argumentDynamic := inferCELType(argument, resultNode, inputSchema, contracts)
				if !exists {
					return actioncontract.TypeBoolean, false, false
				}
				dynamic = dynamic || argumentDynamic
			}
			return actioncontract.TypeBoolean, true, dynamic
		case "_?_:_":
			arguments := call.GetArgs()
			if len(arguments) == 3 {
				left, leftExists, leftDynamic := inferCELType(arguments[1], resultNode, inputSchema, contracts)
				right, rightExists, rightDynamic := inferCELType(arguments[2], resultNode, inputSchema, contracts)
				return left | right, leftExists && rightExists, leftDynamic || rightDynamic
			}
		}
	}
	return actioncontract.TypeUnknown, true, true
}

func celSelectPath(expression *exprpb.Expr) (string, []string, bool) {
	if identifier := expression.GetIdentExpr(); identifier != nil {
		return identifier.GetName(), nil, true
	}
	selection := expression.GetSelectExpr()
	if selection == nil {
		return "", nil, false
	}
	root, parts, ok := celSelectPath(selection.GetOperand())
	if !ok {
		return "", nil, false
	}
	return root, append(parts, selection.GetField()), true
}

func outputSchema(contract ActionContract) *actioncontract.Schema {
	if len(contract.OutputSchema) == 0 {
		return nil
	}
	schema, err := actioncontract.Compile(contract.OutputSchema)
	if err != nil {
		return nil
	}
	return schema
}

func coreActionContract(node resource.FlowNode) ActionContract {
	switch node.Uses {
	case "core.approval":
		return ActionContract{
			Digest:       "core.approval/v1",
			ConfigSchema: json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
			InputSchema:  json.RawMessage(`{"type":"object","additionalProperties":true}`),
			OutputSchema: json.RawMessage(`{"type":"object","properties":{"approved":{"type":"boolean"},"state":{"type":"string"},"reason":{"type":"string"}},"required":["approved","state","reason"],"additionalProperties":false}`),
		}
	case "core.noop":
		output := any(map[string]any{"ok": true})
		if configured, exists := node.With["result"]; exists {
			output = configured
		}
		return ActionContract{Digest: "core.noop/v1", OutputSchema: schemaFromValue(output)}
	default:
		return ActionContract{}
	}
}

func schemaFromValue(value any) json.RawMessage {
	var schema any
	switch typed := value.(type) {
	case nil:
		schema = map[string]any{"type": "null"}
	case bool:
		schema = map[string]any{"type": "boolean"}
	case string:
		schema = map[string]any{"type": "string"}
	case float64:
		kind := "number"
		if typed == float64(int64(typed)) {
			kind = "integer"
		}
		schema = map[string]any{"type": kind}
	case map[string]any:
		properties := make(map[string]any, len(typed))
		required := make([]string, 0, len(typed))
		for key, child := range typed {
			var childSchema any
			_ = json.Unmarshal(schemaFromValue(child), &childSchema)
			properties[key] = childSchema
			required = append(required, key)
		}
		sort.Strings(required)
		schema = map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
	case []any:
		items := any(map[string]any{})
		if len(typed) > 0 {
			_ = json.Unmarshal(schemaFromValue(typed[0]), &items)
		}
		schema = map[string]any{"type": "array", "items": items}
	default:
		schema = map[string]any{}
	}
	encoded, _ := json.Marshal(schema)
	return encoded
}

func schemaDiagnostic(base string, violation actioncontract.Violation) Diagnostic {
	path := base
	for _, segment := range violation.Path {
		if _, err := strconv.Atoi(segment); err == nil {
			path += "[" + segment + "]"
		} else {
			path += "." + segment
		}
	}
	return Diagnostic{Path: path, Code: violation.Code, Message: violation.Message}
}

func cloneMap(source map[string]any) (map[string]any, error) {
	if source == nil {
		return map[string]any{}, nil
	}
	encoded, err := json.Marshal(source)
	if err != nil {
		return nil, err
	}
	var result map[string]any
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func reaches(source, target string, adjacency map[string][]string) bool {
	queue := []string{source}
	seen := map[string]bool{source: true}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, next := range adjacency[current] {
			if next == target {
				return true
			}
			if !seen[next] {
				seen[next] = true
				queue = append(queue, next)
			}
		}
	}
	return false
}

func hasErrorAt(diagnostics []Diagnostic, path string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.IsError() && strings.HasPrefix(diagnostic.Path, path) {
			return true
		}
	}
	return false
}

func hasErrorDiagnostics(diagnostics []Diagnostic) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.IsError() {
			return true
		}
	}
	return false
}

// HasErrors reports whether diagnostics contain a compile-blocking error.
func HasErrors(diagnostics []Diagnostic) bool { return hasErrorDiagnostics(diagnostics) }

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

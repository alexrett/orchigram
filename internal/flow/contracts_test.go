package flow

import (
	"encoding/json"
	"testing"

	"github.com/alexrett/orchigram/internal/resource"
)

type contractBinder map[string]ActionContract

func (b contractBinder) HasAction(action string) bool { _, exists := b[action]; return exists }
func (b contractBinder) BindAction(_, action string, config map[string]any) (ActionBinding, []Diagnostic) {
	contract, exists := b[action]
	if !exists {
		return ActionBinding{}, []Diagnostic{{Path: "config", Code: "unknown", Message: "unknown action"}}
	}
	return ActionBinding{
		Plugin:   PluginBinding{Name: "fixture", Version: "0.1.0", Digest: "binary-digest", ProtocolVersion: 1},
		Contract: contract, Config: config,
	}, nil
}

func TestCompilerEnforcesMappedActionContracts(t *testing.T) {
	t.Parallel()
	binder := contractBinder{
		"fixture.source": fixtureContract(`{"type":"object","properties":{},"additionalProperties":false}`, `{"type":"object","properties":{"text":{"type":"string"},"count":{"type":"integer"}},"required":["text","count"],"additionalProperties":false}`),
		"fixture.dest":   fixtureContract(`{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}`, `{"type":"object"}`),
	}
	tests := []struct {
		name string
		from string
		to   string
		want string
	}{
		{name: "unknown source", from: "nodes.source.missing", to: "/message", want: "unknown_source_field"},
		{name: "unknown target", from: "nodes.source.text", to: "/missing", want: "unknown_target"},
		{name: "incompatible type", from: "nodes.source.count", to: "/message", want: "mapping_type_mismatch"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			flowResource := decodeContractFlow(t, test.from, test.to, true)
			_, diagnostics := NewCompiler(binder).Compile(flowResource)
			if !containsDiagnostic(diagnostics, test.want) {
				t.Fatalf("diagnostics=%+v", diagnostics)
			}
		})
	}
	valid := decodeContractFlow(t, "nodes.source.text", "/message", true)
	plan, diagnostics := NewCompiler(binder).Compile(valid)
	if HasErrors(diagnostics) || plan.PlanHash == "" || plan.Nodes[1].Contract == nil || plan.Nodes[1].Contract.Digest == "" {
		t.Fatalf("plan=%+v diagnostics=%+v", plan, diagnostics)
	}
}

func TestCompilerValidatesLiteralAndMappedRequiredFieldsTogether(t *testing.T) {
	t.Parallel()
	binder := contractBinder{"fixture.dest": fixtureContract(`{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}`, `{"type":"object"}`)}
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: missing-required}
spec:
  nodes:
    - id: dest
      uses: fixture.dest
      with: {surprise: true}
`))
	if err != nil {
		t.Fatal(err)
	}
	_, diagnostics := NewCompiler(binder).Compile(flowResource)
	if !containsDiagnostic(diagnostics, "required") || !containsDiagnostic(diagnostics, "unknown_field") {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
}

func TestCompilerRejectsUnknownFlowInputAndNonPredecessorSources(t *testing.T) {
	t.Parallel()
	binder := contractBinder{
		"fixture.source": fixtureContract(`{"type":"object","additionalProperties":false}`, `{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`),
		"fixture.dest":   fixtureContract(`{"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false}`, `{"type":"object"}`),
	}
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: invalid-sources}
spec:
  inputSchema:
    type: object
    properties: {known: {type: string}}
    additionalProperties: false
  nodes:
    - {id: source, uses: fixture.source}
    - id: dest
      uses: fixture.dest
      with:
        mappings: [{from: input.missing, to: /message}]
`))
	if err != nil {
		t.Fatal(err)
	}
	_, diagnostics := NewCompiler(binder).Compile(flowResource)
	if !containsDiagnostic(diagnostics, "unknown_source_field") {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
	flowResource.Spec.Nodes[1].With["mappings"] = []any{map[string]any{"from": "nodes.source.text", "to": "/message"}}
	_, diagnostics = NewCompiler(binder).Compile(flowResource)
	if !containsDiagnostic(diagnostics, "source_not_predecessor") {
		t.Fatalf("non-predecessor diagnostics=%+v", diagnostics)
	}
}

func TestCompilerUsesSchemasToTypeCEL(t *testing.T) {
	t.Parallel()
	binder := contractBinder{
		"fixture.source": fixtureContract(`{"type":"object","additionalProperties":false}`, `{"type":"object","properties":{"count":{"type":"integer"}},"required":["count"],"additionalProperties":false}`),
		"fixture.dest":   fixtureContract(`{"type":"object","additionalProperties":false}`, `{"type":"object"}`),
	}
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: typed-cel}
spec:
  nodes:
    - {id: source, uses: fixture.source}
    - {id: dest, uses: fixture.dest}
  edges:
    - {from: source, to: dest, when: result.count}
`))
	if err != nil {
		t.Fatal(err)
	}
	_, diagnostics := NewCompiler(binder).Compile(flowResource)
	if !containsDiagnostic(diagnostics, "cel_not_boolean") {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
	flowResource.Spec.Edges[0].When = "result.missing == 1"
	_, diagnostics = NewCompiler(binder).Compile(flowResource)
	if !containsDiagnostic(diagnostics, "unknown_cel_field") {
		t.Fatalf("missing-field diagnostics=%+v", diagnostics)
	}
}

func TestValidateRunInputUsesPinnedSchema(t *testing.T) {
	t.Parallel()
	plan := ExecutionPlan{InputSchema: json.RawMessage(`{"type":"object","properties":{"issue":{"type":"integer"}},"required":["issue"],"additionalProperties":false}`)}
	if err := ValidateRunInput(plan, json.RawMessage(`{"issue":"wrong"}`)); err == nil {
		t.Fatal("invalid run input was accepted")
	}
	if err := ValidateRunInput(plan, json.RawMessage(`{"issue":42}`)); err != nil {
		t.Fatal(err)
	}
	if err := ValidateRunInput(plan, json.RawMessage(`{"issue":42}{}`)); err == nil {
		t.Fatal("trailing JSON value was accepted")
	}
}

func fixtureContract(config, output string) ActionContract {
	return ActionContract{
		Digest: "contract-digest", ConfigSchema: json.RawMessage(config),
		InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(output),
	}
}

func decodeContractFlow(t *testing.T, from, to string, edge bool) resource.Flow {
	t.Helper()
	edges := ""
	if edge {
		edges = "  edges: [{from: source, to: dest}]\n"
	}
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: mapped-contract}
spec:
  nodes:
    - {id: source, uses: fixture.source}
    - id: dest
      uses: fixture.dest
      with:
        mappings:
          - {from: ` + from + `, to: ` + to + `}
` + edges))
	if err != nil {
		t.Fatal(err)
	}
	return flowResource
}

func containsDiagnostic(diagnostics []Diagnostic, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

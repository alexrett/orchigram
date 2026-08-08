package flow

import (
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/resource"
)

func compileYAML(t *testing.T, value string) (ExecutionPlan, []Diagnostic) {
	t.Helper()
	flowResource, err := resource.DecodeFlow([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return NewCompiler(nil).Compile(flowResource)
}

func TestCompilerHashIsStable(t *testing.T) {
	t.Parallel()
	source := `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: stable, uid: flow-1, generation: 7}
spec:
  nodes:
    - {id: first, uses: core.noop}
    - {id: approval, uses: core.approval}
  edges:
    - {from: first, to: approval, when: "result.ok == true"}
`
	first, diagnostics := compileYAML(t, source)
	if len(diagnostics) != 0 {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
	second, _ := compileYAML(t, source)
	if first.PlanHash == "" || first.PlanHash != second.PlanHash {
		t.Fatalf("unstable hashes: %q %q", first.PlanHash, second.PlanHash)
	}
}

func TestCompilerRejectsUnboundedCycle(t *testing.T) {
	t.Parallel()
	_, diagnostics := compileYAML(t, `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: cycle}
spec:
  nodes:
    - {id: a, uses: core.noop}
    - {id: b, uses: core.noop}
  edges:
    - {from: a, to: b}
    - {from: b, to: a}
`)
	if len(diagnostics) != 1 || diagnostics[0].Code != "unbounded_cycle" {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
}

func TestCompilerAcceptsFiniteCycleAndRejectsBadCEL(t *testing.T) {
	t.Parallel()
	_, diagnostics := compileYAML(t, `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: repair}
spec:
  nodes:
    - {id: test, uses: core.noop}
    - id: repair
      uses: core.noop
      loop: {maxIterations: 2}
  edges:
    - {from: test, to: repair, when: "result["}
    - {from: repair, to: test}
`)
	if len(diagnostics) != 1 || diagnostics[0].Code != "invalid_cel" || !strings.Contains(diagnostics[0].Message, "ERROR") {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
}

type testCapabilities struct{}

func (testCapabilities) HasAction(action string) bool { return action == "exec.run" }
func (testCapabilities) ValidateAction(_ string, _ map[string]any) []Diagnostic {
	return []Diagnostic{{Path: "config.argv", Code: "required", Message: "argv is required"}}
}

type rootDiagnosticBinder struct{ testCapabilities }

func (rootDiagnosticBinder) BindAction(_ string, _ map[string]any) (ActionBinding, []Diagnostic) {
	return ActionBinding{}, []Diagnostic{{Path: "config", Code: "invalid", Message: "configuration is invalid"}}
}

func TestCompilerResolvesPluginCapabilityAndValidation(t *testing.T) {
	t.Parallel()
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: plugin-validation}
spec:
  nodes:
    - {id: execute, uses: exec.run}
`))
	if err != nil {
		t.Fatal(err)
	}
	_, diagnostics := NewCompiler(testCapabilities{}).Compile(flowResource)
	if len(diagnostics) != 1 || diagnostics[0].Path != "spec.nodes[0].with.argv" || diagnostics[0].Code != "required" {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
}

func TestCompilerMapsRootPluginDiagnosticToNodeWith(t *testing.T) {
	t.Parallel()
	flowResource, err := resource.DecodeFlow([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: root-diagnostic}
spec:
  nodes:
    - {id: execute, uses: exec.run}
`))
	if err != nil {
		t.Fatal(err)
	}
	_, diagnostics := NewCompiler(rootDiagnosticBinder{}).Compile(flowResource)
	if len(diagnostics) != 1 || diagnostics[0].Path != "spec.nodes[0].with" {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
}

func TestCompilerValidatesDeclarativeMappings(t *testing.T) {
	t.Parallel()
	_, diagnostics := compileYAML(t, `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: mappings}
spec:
  nodes:
    - {id: compose, uses: core.noop}
    - id: notify
      uses: core.noop
      with:
        mappings:
          - {from: nodes.missing.text, to: body.text}
  edges: [{from: compose, to: notify}]
`)
	if len(diagnostics) != 2 || diagnostics[0].Code != "unknown_node" || diagnostics[1].Code != "invalid_target" {
		t.Fatalf("diagnostics: %+v", diagnostics)
	}
}

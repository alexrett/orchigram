package tui

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
)

func TestActionConfigFieldsDeriveStableScalarForm(t *testing.T) {
	node := flow.PlanNode{Contract: &flow.ActionContract{ConfigSchema: json.RawMessage(`{
  "type":"object",
  "properties":{"retries":{"type":"integer"},"enabled":{"type":"boolean"},"name":{"type":"string"}},
  "required":["name"],
  "additionalProperties":false
}`)}}
	fields, complete := actionConfigFields(node)
	if !complete {
		t.Fatal("scalar schema unexpectedly required JSON fallback")
	}
	want := []actionConfigField{{name: "enabled", typeName: "boolean"}, {name: "name", typeName: "string", required: true}, {name: "retries", typeName: "integer"}}
	if !reflect.DeepEqual(fields, want) {
		t.Fatalf("fields=%+v want=%+v", fields, want)
	}
	if value, present, err := parseConfigField(fields[0], "true"); err != nil || !present || value != true {
		t.Fatalf("boolean parse value=%v present=%t err=%v", value, present, err)
	}
	if _, _, err := parseConfigField(fields[1], ""); err == nil {
		t.Fatal("required empty scalar was accepted")
	}
}

func TestActionConfigFieldsFallsBackForNestedSchema(t *testing.T) {
	node := flow.PlanNode{Contract: &flow.ActionContract{ConfigSchema: json.RawMessage(`{"type":"object","properties":{"argv":{"type":"array","items":{"type":"string"}}}}`)}}
	if fields, complete := actionConfigFields(node); complete || fields != nil {
		t.Fatalf("nested schema fields=%+v complete=%t", fields, complete)
	}
}

func TestSourceEdgeIndexUsesStablePlanIndexForDuplicateEdges(t *testing.T) {
	edges := []resource.FlowEdge{{From: "start", To: "finish", When: "true"}, {From: "start", To: "finish", When: "true"}}
	selected := flow.PlanEdge{From: "start", To: "finish", Condition: "true"}
	if index := sourceEdgeIndex(edges, selected, 1); index != 1 {
		t.Fatalf("source edge index=%d want=1", index)
	}
}

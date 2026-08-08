package actioncontract

import (
	"encoding/json"
	"testing"
)

func TestSchemaResolvesClosedPathsAndTypes(t *testing.T) {
	t.Parallel()
	schema, err := Compile(json.RawMessage(`{
  "type":"object",
  "properties":{"message":{"type":"string"},"nested":{"type":"object","properties":{"count":{"type":"integer"}},"additionalProperties":false}},
  "required":["message"],
  "additionalProperties":false
}`))
	if err != nil {
		t.Fatal(err)
	}
	if resolution := schema.ResolvePointer("/nested/count"); !resolution.Exists || resolution.Type != TypeInteger || resolution.Dynamic {
		t.Fatalf("resolution=%+v", resolution)
	}
	if resolution := schema.ResolvePointer("/nested/missing"); resolution.Exists {
		t.Fatalf("closed field resolved: %+v", resolution)
	}
	violations := schema.Validate(map[string]any{"surprise": true})
	if len(violations) != 2 || violations[0].Code != "required" || violations[1].Code != "unknown_field" {
		t.Fatalf("violations=%+v", violations)
	}
}

func TestSchemaRunsCompleteDraftValidationAfterStaticChecks(t *testing.T) {
	t.Parallel()
	schema, err := Compile(json.RawMessage(`{
  "type":"object",
  "properties":{"url":{"type":"string"},"urlSecret":{"type":"string"}},
  "oneOf":[{"required":["url"]},{"required":["urlSecret"]}]
}`))
	if err != nil {
		t.Fatal(err)
	}
	if violations := schema.Validate(map[string]any{"url": "x", "urlSecret": "y"}); len(violations) != 1 || violations[0].Code != "schema_invalid" {
		t.Fatalf("violations=%+v", violations)
	}
}

func TestCompatibilityMarksOpenRegionsDynamic(t *testing.T) {
	t.Parallel()
	sourceSchema, _ := Compile(json.RawMessage(`{"type":"object","properties":{"count":{"type":"integer"}},"additionalProperties":false}`))
	targetSchema, _ := Compile(json.RawMessage(`{"type":"object","properties":{"message":{"type":"string"},"body":{}},"additionalProperties":false}`))
	if compatible, dynamic := Compatible(sourceSchema.ResolveDot([]string{"count"}), targetSchema.ResolvePointer("/message")); compatible || dynamic {
		t.Fatalf("integer unexpectedly compatible with string: compatible=%v dynamic=%v", compatible, dynamic)
	}
	if compatible, dynamic := Compatible(sourceSchema.ResolveDot([]string{"count"}), targetSchema.ResolvePointer("/body/value")); !compatible || !dynamic {
		t.Fatalf("open region did not remain explicit: compatible=%v dynamic=%v", compatible, dynamic)
	}
}

func TestCompileRejectsExternalReferencesWithoutNetworkAccess(t *testing.T) {
	t.Parallel()
	if _, err := Compile(json.RawMessage(`{"$ref":"https://example.invalid/schema"}`)); err == nil {
		t.Fatal("external schema reference was accepted")
	}
	if _, err := Compile(json.RawMessage(`{"$dynamicRef":"https://example.invalid/schema"}`)); err == nil {
		t.Fatal("external dynamic schema reference was accepted")
	}
}

func TestCompileAllowsReferenceKeywordAsInstanceProperty(t *testing.T) {
	t.Parallel()
	if _, err := Compile(json.RawMessage(`{
  "type":"object",
  "properties":{"$ref":{"type":"string"},"payload":{"const":{"$ref":"instance value"}}},
  "$defs":{"$schema":{"type":"string"}}
}`)); err != nil {
		t.Fatalf("instance property names were treated as schema keywords: %v", err)
	}
}

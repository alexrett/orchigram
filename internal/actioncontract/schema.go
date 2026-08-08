// Package actioncontract validates and statically inspects immutable plugin
// JSON Schema contracts without exposing the schema library in public APIs.
package actioncontract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

// Type is the statically knowable JSON value type set.
type Type uint16

const (
	// TypeUnknown identifies an explicitly open or otherwise non-inferable schema region.
	TypeUnknown Type = 0
	// TypeNull identifies JSON null.
	TypeNull Type = 1 << iota
	// TypeBoolean identifies a JSON boolean.
	TypeBoolean
	// TypeInteger identifies an integral JSON number.
	TypeInteger
	// TypeNumber identifies any JSON number.
	TypeNumber
	// TypeString identifies a JSON string.
	TypeString
	// TypeObject identifies a JSON object.
	TypeObject
	// TypeArray identifies a JSON array.
	TypeArray
)

// Violation is a secret-safe deterministic schema diagnostic.
type Violation struct {
	Path    []string
	Code    string
	Message string
}

// Resolution describes one statically inspected schema path.
type Resolution struct {
	Schema  *jsonschema.Schema
	Type    Type
	Exists  bool
	Dynamic bool
}

// Schema is a compiled Draft 2020-12 contract.
type Schema struct {
	compiled *jsonschema.Schema
}

// Compile parses and compiles one already host-validated schema.
func Compile(raw json.RawMessage) (*Schema, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, errors.New("schema must contain exactly one JSON value")
	}
	if err := rejectExternalReferences(document); err != nil {
		return nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	const location = "urn:orchigram:action-contract"
	if err := compiler.AddResource(location, document); err != nil {
		return nil, err
	}
	compiled, err := compiler.Compile(location)
	if err != nil {
		return nil, err
	}
	return &Schema{compiled: compiled}, nil
}

func rejectExternalReferences(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$schema" {
				if draft, ok := child.(string); !ok || draft != "https://json-schema.org/draft/2020-12/schema" {
					return errors.New("$schema must select Draft 2020-12")
				}
			}
			if key == "$ref" {
				if reference, ok := child.(string); !ok || !strings.HasPrefix(reference, "#") {
					return errors.New("external $ref values are not allowed")
				}
			}
			if err := rejectExternalReferences(child); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := rejectExternalReferences(child); err != nil {
				return err
			}
		}
	}
	return nil
}

// Validate checks required fields, closed objects, and value types with stable
// paths, then delegates the remaining Draft 2020-12 assertions to the library.
func (s *Schema) Validate(value any) []Violation {
	normalized, err := normalize(value)
	if err != nil {
		return []Violation{{Code: "schema_invalid", Message: "value cannot be represented as JSON"}}
	}
	violations := make([]Violation, 0)
	validateShape(s.compiled, normalized, nil, &violations)
	if len(violations) == 0 {
		if err := s.compiled.Validate(normalized); err != nil {
			path := []string{}
			var validation *jsonschema.ValidationError
			if errors.As(err, &validation) {
				path = append(path, validation.InstanceLocation...)
			}
			violations = append(violations, Violation{Path: path, Code: "schema_invalid", Message: "value does not satisfy the declared schema"})
		}
	}
	sort.Slice(violations, func(i, j int) bool {
		left, right := strings.Join(violations[i].Path, "\x00"), strings.Join(violations[j].Path, "\x00")
		if left != right {
			return left < right
		}
		return violations[i].Code < violations[j].Code
	})
	return violations
}

// ResolveDot resolves object fields or numeric array offsets.
func (s *Schema) ResolveDot(parts []string) Resolution {
	return resolve(s.compiled, parts, false)
}

// ResolvePointer resolves a non-root RFC 6901 JSON Pointer.
func (s *Schema) ResolvePointer(pointer string) Resolution {
	if !strings.HasPrefix(pointer, "/") || pointer == "/" {
		return Resolution{}
	}
	encoded := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	parts := make([]string, 0, len(encoded))
	for _, part := range encoded {
		part = strings.ReplaceAll(strings.ReplaceAll(part, "~1", "/"), "~0", "~")
		parts = append(parts, part)
	}
	return resolve(s.compiled, parts, false)
}

// Placeholder returns a schema-compatible representative used only to prove
// that mapped required fields are present during compile-time validation.
func Placeholder(resolution Resolution) any {
	switch {
	case resolution.Type&TypeString != 0:
		return "mapped-value"
	case resolution.Type&TypeInteger != 0:
		return json.Number("1")
	case resolution.Type&TypeNumber != 0:
		return json.Number("1")
	case resolution.Type&TypeBoolean != 0:
		return false
	case resolution.Type&TypeObject != 0:
		return map[string]any{}
	case resolution.Type&TypeArray != 0:
		return []any{}
	case resolution.Type&TypeNull != 0:
		return nil
	default:
		return "mapped-value"
	}
}

// Compatible reports whether every statically known source type can be
// assigned to at least one destination type. Unknown types require a warning.
func Compatible(source, target Resolution) (compatible, dynamic bool) {
	if source.Dynamic || target.Dynamic || source.Type == TypeUnknown || target.Type == TypeUnknown {
		return true, true
	}
	accepted := target.Type
	if target.Type&TypeNumber != 0 {
		accepted |= TypeInteger
	}
	return source.Type&^accepted == 0, false
}

// SetPointer applies one compile-time placeholder to a copied config object.
func SetPointer(root map[string]any, pointer string, value any) error {
	if !strings.HasPrefix(pointer, "/") || pointer == "/" {
		return errors.New("mapping target must be a non-root JSON pointer")
	}
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	for index := range parts {
		parts[index] = strings.ReplaceAll(strings.ReplaceAll(parts[index], "~1", "/"), "~0", "~")
	}
	var current any = root
	for index, part := range parts {
		last := index == len(parts)-1
		switch container := current.(type) {
		case map[string]any:
			if last {
				container[part] = value
				return nil
			}
			next, exists := container[part]
			if !exists || next == nil {
				next = map[string]any{}
				container[part] = next
			}
			current = next
		case []any:
			offset, err := strconv.Atoi(part)
			if err != nil || offset < 0 || offset >= len(container) {
				return fmt.Errorf("mapping target array index %q is unavailable", part)
			}
			if last {
				container[offset] = value
				return nil
			}
			current = container[offset]
		default:
			return fmt.Errorf("mapping target traverses a non-container at %q", part)
		}
	}
	return nil
}

func resolve(schema *jsonschema.Schema, parts []string, dynamic bool) Resolution {
	schema = dereference(schema)
	if schema == nil {
		return Resolution{Exists: true, Dynamic: true}
	}
	if len(parts) == 0 {
		return Resolution{Schema: schema, Type: schemaType(schema), Exists: true, Dynamic: dynamic || schemaType(schema) == TypeUnknown}
	}
	part := parts[0]
	types := schemaType(schema)
	if types == TypeUnknown {
		return Resolution{Schema: schema, Exists: true, Dynamic: true}
	}
	if types&TypeObject != 0 {
		if property, exists := schema.Properties[part]; exists {
			return resolve(property, parts[1:], dynamic)
		}
		switch additional := schema.AdditionalProperties.(type) {
		case bool:
			if !additional {
				return Resolution{}
			}
			return Resolution{Exists: true, Dynamic: true}
		case *jsonschema.Schema:
			return resolve(additional, parts[1:], true)
		case nil:
			return Resolution{Exists: true, Dynamic: true}
		}
	}
	if types&TypeArray != 0 {
		if _, err := strconv.Atoi(part); err != nil {
			return Resolution{}
		}
		item := schema.Items2020
		if item == nil {
			if typed, ok := schema.Items.(*jsonschema.Schema); ok {
				item = typed
			}
		}
		if item == nil {
			return Resolution{Exists: true, Dynamic: true}
		}
		return resolve(item, parts[1:], dynamic)
	}
	return Resolution{}
}

func dereference(schema *jsonschema.Schema) *jsonschema.Schema {
	for schema != nil && schema.Ref != nil {
		schema = schema.Ref
	}
	return schema
}

func schemaType(schema *jsonschema.Schema) Type {
	schema = dereference(schema)
	if schema == nil || schema.Types == nil || schema.Types.IsEmpty() {
		return TypeUnknown
	}
	var result Type
	for _, name := range schema.Types.ToStrings() {
		switch name {
		case "null":
			result |= TypeNull
		case "boolean":
			result |= TypeBoolean
		case "integer":
			result |= TypeInteger
		case "number":
			result |= TypeNumber
		case "string":
			result |= TypeString
		case "object":
			result |= TypeObject
		case "array":
			result |= TypeArray
		}
	}
	return result
}

func validateShape(schema *jsonschema.Schema, value any, path []string, violations *[]Violation) {
	schema = dereference(schema)
	if schema == nil {
		return
	}
	want := schemaType(schema)
	got := valueType(value)
	if want != TypeUnknown && !valueMatches(got, want) {
		*violations = append(*violations, Violation{Path: append([]string(nil), path...), Code: "type_mismatch", Message: "value type is incompatible with the declared schema"})
		return
	}
	switch typed := value.(type) {
	case map[string]any:
		for _, required := range schema.Required {
			if _, exists := typed[required]; !exists {
				*violations = append(*violations, Violation{Path: append(append([]string(nil), path...), required), Code: "required", Message: "required field is missing"})
			}
		}
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			child, exists := schema.Properties[key]
			if !exists {
				switch additional := schema.AdditionalProperties.(type) {
				case bool:
					if !additional {
						*violations = append(*violations, Violation{Path: append(append([]string(nil), path...), key), Code: "unknown_field", Message: "field is not declared by the closed schema"})
						continue
					}
				case *jsonschema.Schema:
					child = additional
				}
			}
			if child != nil {
				validateShape(child, typed[key], append(append([]string(nil), path...), key), violations)
			}
		}
	case []any:
		item := schema.Items2020
		if item == nil {
			item, _ = schema.Items.(*jsonschema.Schema)
		}
		if item != nil {
			for index, child := range typed {
				validateShape(item, child, append(append([]string(nil), path...), strconv.Itoa(index)), violations)
			}
		}
	}
}

func normalize(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}

func valueType(value any) Type {
	switch typed := value.(type) {
	case nil:
		return TypeNull
	case bool:
		return TypeBoolean
	case string:
		return TypeString
	case json.Number:
		if _, err := typed.Int64(); err == nil {
			return TypeInteger
		}
		return TypeNumber
	case float64:
		if math.Trunc(typed) == typed {
			return TypeInteger
		}
		return TypeNumber
	case map[string]any:
		return TypeObject
	case []any:
		return TypeArray
	default:
		return TypeUnknown
	}
}

func valueMatches(got, want Type) bool {
	if got&want != 0 {
		return true
	}
	return got == TypeInteger && want&TypeNumber != 0
}

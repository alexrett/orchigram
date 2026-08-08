package plugin

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
)

const draft2020Schema = "https://json-schema.org/draft/2020-12/schema"

type compiledActionContract struct {
	config *jsonschema.Schema
	input  *jsonschema.Schema
	output *jsonschema.Schema
}

// ValidateDescription applies the same fail-closed metadata rules on the host
// side and returns a canonical contract plus its stable SHA-256 digest.
func ValidateDescription(description *pluginv1alpha1.DescribeResponse) (Contract, string, error) {
	if description == nil || description.GetProtocol() == nil || description.GetProtocol().GetMinimum() == 0 || description.GetProtocol().GetMaximum() < description.GetProtocol().GetMinimum() {
		return Contract{}, "", errors.New("plugin description has an invalid protocol range")
	}
	metadata := Metadata{
		Name: description.GetName(), Version: description.GetVersion(), Capabilities: append([]string(nil), description.GetCapabilities()...),
		InputSchema: append(json.RawMessage(nil), description.GetInputSchemaJson()...), OutputSchema: append(json.RawMessage(nil), description.GetOutputSchemaJson()...),
	}
	for _, action := range description.GetActions() {
		if action == nil {
			return Contract{}, "", errors.New("plugin description contains an empty action descriptor")
		}
		metadata.Actions = append(metadata.Actions, ActionDescriptor{
			Action: action.GetAction(), ConfigSchema: append(json.RawMessage(nil), action.GetConfigSchemaJson()...),
			InputSchema: append(json.RawMessage(nil), action.GetInputSchemaJson()...), OutputSchema: append(json.RawMessage(nil), action.GetOutputSchemaJson()...),
		})
	}
	for _, trigger := range description.GetTriggers() {
		if trigger == nil {
			return Contract{}, "", errors.New("plugin description contains an empty trigger descriptor")
		}
		metadata.Triggers = append(metadata.Triggers, TriggerDescriptor{
			Source: trigger.GetSource(), ConfigSchema: append(json.RawMessage(nil), trigger.GetConfigSchemaJson()...),
			EventSchema: append(json.RawMessage(nil), trigger.GetEventSchemaJson()...),
		})
	}
	hasTask, hasTrigger, hasAgent := false, false, false
	for _, capability := range metadata.Capabilities {
		switch {
		case strings.HasPrefix(capability, "task."):
			hasTask = true
		case strings.HasPrefix(capability, "trigger.") && capability != ActivationFenceCapability:
			hasTrigger = true
		case strings.HasPrefix(capability, "agent."):
			hasAgent = true
		}
	}
	validated, _, _, err := validateMetadata(metadata, hasTask, hasTrigger, hasAgent)
	if err != nil {
		return Contract{}, "", err
	}
	contract := Contract{Actions: validated.Actions, Triggers: validated.Triggers}
	encoded, err := json.Marshal(contract)
	if err != nil {
		return Contract{}, "", err
	}
	digest := sha256.Sum256(encoded)
	return contract, hex.EncodeToString(digest[:]), nil
}

// DecodeContract parses a previously validated canonical contract.
func DecodeContract(raw json.RawMessage) (Contract, error) {
	var contract Contract
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&contract); err != nil {
		return Contract{}, fmt.Errorf("decode plugin contract: %w", err)
	}
	return contract, nil
}

func validateActionDescriptors(metadata Metadata, declared map[string]struct{}, hasAgent bool) ([]ActionDescriptor, map[string]compiledActionContract, error) {
	descriptors := append([]ActionDescriptor(nil), metadata.Actions...)
	seen := make(map[string]struct{}, len(descriptors))
	contracts := make(map[string]compiledActionContract, len(descriptors))
	for index := range descriptors {
		descriptor := &descriptors[index]
		if !capabilityName.MatchString(descriptor.Action) || !strings.HasPrefix(descriptor.Action, metadata.Name+".") {
			return nil, nil, fmt.Errorf("invalid action descriptor %q", descriptor.Action)
		}
		if _, duplicate := seen[descriptor.Action]; duplicate {
			return nil, nil, fmt.Errorf("duplicate action descriptor %q", descriptor.Action)
		}
		seen[descriptor.Action] = struct{}{}
		_, taskAction := declared[descriptor.Action]
		agentAction := hasAgent && descriptor.Action == metadata.Name+".run"
		if !taskAction && !agentAction {
			return nil, nil, fmt.Errorf("action descriptor %q has no matching capability", descriptor.Action)
		}
		config, canonicalConfig, err := compileSchema(descriptor.Action+" config", descriptor.ConfigSchema, true)
		if err != nil {
			return nil, nil, err
		}
		input, canonicalInput, err := compileSchema(descriptor.Action+" input", descriptor.InputSchema, false)
		if err != nil {
			return nil, nil, err
		}
		output, canonicalOutput, err := compileSchema(descriptor.Action+" output", descriptor.OutputSchema, false)
		if err != nil {
			return nil, nil, err
		}
		descriptor.ConfigSchema, descriptor.InputSchema, descriptor.OutputSchema = canonicalConfig, canonicalInput, canonicalOutput
		contracts[descriptor.Action] = compiledActionContract{config: config, input: input, output: output}
	}
	for action := range declared {
		if _, exists := seen[action]; !exists {
			return nil, nil, fmt.Errorf("task action %q is missing an action descriptor", action)
		}
	}
	if hasAgent {
		action := metadata.Name + ".run"
		if _, exists := seen[action]; !exists {
			return nil, nil, fmt.Errorf("agent runtime is missing action descriptor %q", action)
		}
	}
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].Action < descriptors[j].Action })
	return descriptors, contracts, nil
}

func validateTriggerDescriptors(metadata Metadata, hasTrigger bool) ([]TriggerDescriptor, error) {
	descriptors := append([]TriggerDescriptor(nil), metadata.Triggers...)
	seen := make(map[string]struct{}, len(descriptors))
	declared := map[string]struct{}{}
	for _, capability := range metadata.Capabilities {
		if strings.HasPrefix(capability, "trigger.") && capability != ActivationFenceCapability {
			declared[strings.TrimPrefix(capability, "trigger.")] = struct{}{}
		}
	}
	for index := range descriptors {
		descriptor := &descriptors[index]
		if !capabilityName.MatchString(descriptor.Source) || !strings.HasPrefix(descriptor.Source, metadata.Name+".") {
			return nil, fmt.Errorf("invalid trigger descriptor %q", descriptor.Source)
		}
		if _, duplicate := seen[descriptor.Source]; duplicate {
			return nil, fmt.Errorf("duplicate trigger descriptor %q", descriptor.Source)
		}
		seen[descriptor.Source] = struct{}{}
		if _, exists := declared[descriptor.Source]; !exists {
			return nil, fmt.Errorf("trigger descriptor %q has no matching capability", descriptor.Source)
		}
		_, canonicalConfig, err := compileSchema(descriptor.Source+" trigger config", descriptor.ConfigSchema, true)
		if err != nil {
			return nil, err
		}
		_, canonicalEvent, err := compileSchema(descriptor.Source+" trigger event", descriptor.EventSchema, false)
		if err != nil {
			return nil, err
		}
		descriptor.ConfigSchema, descriptor.EventSchema = canonicalConfig, canonicalEvent
	}
	if hasTrigger {
		for source := range declared {
			if _, exists := seen[source]; !exists {
				return nil, fmt.Errorf("trigger source %q is missing a trigger descriptor", source)
			}
		}
	}
	sort.Slice(descriptors, func(i, j int) bool { return descriptors[i].Source < descriptors[j].Source })
	return descriptors, nil
}

func compileSchema(label string, raw json.RawMessage, requireObject bool) (*jsonschema.Schema, json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil, fmt.Errorf("%s schema is required", label)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, nil, fmt.Errorf("%s schema must be valid JSON: %w", label, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, nil, fmt.Errorf("%s schema must contain exactly one JSON value", label)
	}
	if err := rejectExternalReferences(document); err != nil {
		return nil, nil, fmt.Errorf("%s schema: %w", label, err)
	}
	if requireObject && !declaresObject(document) {
		return nil, nil, fmt.Errorf("%s schema must declare an object root", label)
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return nil, nil, fmt.Errorf("canonicalize %s schema: %w", label, err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	resourceURL := "urn:orchigram:schema"
	if err := compiler.AddResource(resourceURL, document); err != nil {
		return nil, nil, fmt.Errorf("load %s schema: %w", label, err)
	}
	compiled, err := compiler.Compile(resourceURL)
	if err != nil {
		return nil, nil, fmt.Errorf("compile %s schema: %w", label, err)
	}
	return compiled, canonical, nil
}

func declaresObject(document any) bool {
	object, ok := document.(map[string]any)
	if !ok {
		return false
	}
	typeName, _ := object["type"].(string)
	return typeName == "object"
}

func rejectExternalReferences(value any) error {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "$schema" {
				if schema, ok := child.(string); !ok || schema != draft2020Schema {
					return fmt.Errorf("$schema must be %q", draft2020Schema)
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

func (c compiledActionContract) validateConfig(raw json.RawMessage) *pluginv1alpha1.ValidationIssue {
	return validateRaw(c.config, raw, "config")
}

func (c compiledActionContract) validateInput(raw json.RawMessage) *pluginv1alpha1.ValidationIssue {
	return validateRaw(c.input, raw, "input")
}

func (c compiledActionContract) validateOutputValue(value any) *pluginv1alpha1.ValidationIssue {
	encoded, err := json.Marshal(value)
	if err != nil {
		return &pluginv1alpha1.ValidationIssue{Path: "output", Code: "schema_invalid", Message: "output cannot be encoded as JSON"}
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return &pluginv1alpha1.ValidationIssue{Path: "output", Code: "schema_invalid", Message: "output cannot be encoded as JSON"}
	}
	if err := c.output.Validate(normalized); err != nil {
		return schemaIssue(err, "output")
	}
	return nil
}

func validateRaw(schema *jsonschema.Schema, raw json.RawMessage, root string) *pluginv1alpha1.ValidationIssue {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return &pluginv1alpha1.ValidationIssue{Path: root, Code: "schema_invalid", Message: root + " is not valid JSON"}
	}
	if err := schema.Validate(value); err != nil {
		return schemaIssue(err, root)
	}
	return nil
}

func schemaIssue(err error, root string) *pluginv1alpha1.ValidationIssue {
	path := root
	var validation *jsonschema.ValidationError
	if errors.As(err, &validation) && len(validation.InstanceLocation) > 0 {
		path += "." + strings.Join(validation.InstanceLocation, ".")
	}
	return &pluginv1alpha1.ValidationIssue{Path: path, Code: "schema_invalid", Message: root + " does not satisfy its declared action schema"}
}

func actionDescriptorsPB(descriptors []ActionDescriptor) []*pluginv1alpha1.ActionDescriptor {
	result := make([]*pluginv1alpha1.ActionDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		result = append(result, &pluginv1alpha1.ActionDescriptor{
			Action: descriptor.Action, ConfigSchemaJson: append([]byte(nil), descriptor.ConfigSchema...),
			InputSchemaJson: append([]byte(nil), descriptor.InputSchema...), OutputSchemaJson: append([]byte(nil), descriptor.OutputSchema...),
		})
	}
	return result
}

func triggerDescriptorsPB(descriptors []TriggerDescriptor) []*pluginv1alpha1.TriggerDescriptor {
	result := make([]*pluginv1alpha1.TriggerDescriptor, 0, len(descriptors))
	for _, descriptor := range descriptors {
		result = append(result, &pluginv1alpha1.TriggerDescriptor{
			Source: descriptor.Source, ConfigSchemaJson: append([]byte(nil), descriptor.ConfigSchema...), EventSchemaJson: append([]byte(nil), descriptor.EventSchema...),
		})
	}
	return result
}

package firstparty

import (
	"encoding/json"

	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

func agentActionDescriptors() []pluginsdk.ActionDescriptor {
	return []pluginsdk.ActionDescriptor{{
		Action: "agent-command.run",
		ConfigSchema: objectSchema(map[string]any{
			"profile": stringSchema(), "prompt": stringSchema(), "workspace": stringSchema(),
		}, []string{"profile"}, false),
		InputSchema: objectSchema(nil, nil, true),
		OutputSchema: objectSchema(map[string]any{
			"exitCode": integerSchema(), "outcome": stringSchema(), "stdout": stringSchema(), "text": stringSchema(),
		}, []string{"exitCode", "outcome", "stdout"}, false),
	}}
}

func execActionDescriptors() []pluginsdk.ActionDescriptor {
	return []pluginsdk.ActionDescriptor{{
		Action: "exec.run",
		ConfigSchema: objectSchema(map[string]any{
			"argv":      map[string]any{"type": "array", "items": stringSchema(), "minItems": 1},
			"directory": stringSchema(), "environment": stringMapSchema(), "stdin": stringSchema(),
		}, []string{"argv"}, false),
		InputSchema: objectSchema(nil, nil, true),
		OutputSchema: objectSchema(map[string]any{
			"exitCode": integerSchema(), "outcome": stringSchema(), "stdout": stringSchema(), "stderr": stringSchema(),
		}, []string{"exitCode", "outcome", "stdout", "stderr"}, false),
	}}
}

func httpActionDescriptors() []pluginsdk.ActionDescriptor {
	config := map[string]any{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type":    "object",
		"properties": map[string]any{
			"method": stringSchema(), "url": stringSchema(), "urlSecret": stringSchema(),
			"headers": stringMapSchema(), "secretHeaders": stringMapSchema(), "body": map[string]any{},
		},
		"additionalProperties": false,
		"oneOf": []any{
			map[string]any{"required": []string{"url"}, "not": map[string]any{"required": []string{"urlSecret"}}},
			map[string]any{"required": []string{"urlSecret"}, "not": map[string]any{"required": []string{"url"}}},
		},
	}
	return []pluginsdk.ActionDescriptor{{
		Action: "http.request", ConfigSchema: mustSchema(config), InputSchema: objectSchema(nil, nil, true),
		OutputSchema: objectSchema(map[string]any{"status": integerSchema(), "body": stringSchema()}, []string{"status", "body"}, false),
	}}
}

func githubActionDescriptors() []pluginsdk.ActionDescriptor {
	repository := map[string]any{
		"owner": stringSchema(), "repository": stringSchema(), "apiBase": stringSchema(), "tokenSecret": stringSchema(),
	}
	issueConfig := copyProperties(repository)
	issueConfig["number"] = positiveIntegerSchema()
	commentConfig := copyProperties(issueConfig)
	commentConfig["body"] = nonEmptyStringSchema()
	pullConfig := copyProperties(repository)
	for _, field := range []string{"head", "base", "title", "body"} {
		pullConfig[field] = stringSchema()
	}
	return []pluginsdk.ActionDescriptor{
		{
			Action: "github.issue.get", ConfigSchema: objectSchema(issueConfig, []string{"owner", "repository", "tokenSecret", "number"}, false),
			InputSchema: objectSchema(nil, nil, true), OutputSchema: objectSchema(map[string]any{"issue": issueSchema()}, []string{"issue"}, false),
		},
		{
			Action: "github.issue.comment", ConfigSchema: objectSchema(commentConfig, []string{"owner", "repository", "tokenSecret", "number", "body"}, false),
			InputSchema: objectSchema(nil, nil, true), OutputSchema: mutationOutputSchema(false),
		},
		{
			Action: "github.workspace.checkout",
			ConfigSchema: objectSchema(map[string]any{
				"cloneURL": nonEmptyStringSchema(), "defaultBranch": stringSchema(), "issueNumber": positiveIntegerSchema(),
				"workspaceRoot": nonEmptyStringSchema(), "tokenSecret": stringSchema(),
			}, []string{"cloneURL", "issueNumber", "workspaceRoot"}, false),
			InputSchema:  objectSchema(nil, nil, true),
			OutputSchema: objectSchema(map[string]any{"workspace": stringSchema(), "branch": stringSchema()}, []string{"workspace", "branch"}, false),
		},
		{
			Action: "github.workspace.commit-push",
			ConfigSchema: objectSchema(map[string]any{
				"workspace": nonEmptyStringSchema(), "workspaceRoot": nonEmptyStringSchema(), "branch": nonEmptyStringSchema(),
				"message": nonEmptyStringSchema(), "tokenSecret": stringSchema(),
			}, []string{"workspace", "workspaceRoot", "branch", "message"}, false),
			InputSchema:  objectSchema(nil, nil, true),
			OutputSchema: objectSchema(map[string]any{"branch": stringSchema(), "commit": stringSchema(), "noChange": boolSchema()}, []string{"branch", "commit", "noChange"}, false),
		},
		{
			Action: "github.pr.ensure", ConfigSchema: objectSchema(pullConfig, []string{"owner", "repository", "tokenSecret", "head", "base", "title"}, false),
			InputSchema: objectSchema(nil, nil, true), OutputSchema: mutationOutputSchema(true),
		},
	}
}

func githubTriggerDescriptors() []pluginsdk.TriggerDescriptor {
	repository := objectSchemaValue(map[string]any{"owner": stringSchema(), "name": stringSchema()}, []string{"owner", "name"}, false)
	providerConfig := map[string]any{
		"owner": nonEmptyStringSchema(), "repository": nonEmptyStringSchema(), "apiBase": stringSchema(),
		"tokenSecret": nonEmptyStringSchema(), "pollInterval": stringSchema(), "replayExisting": boolSchema(),
	}
	return []pluginsdk.TriggerDescriptor{
		{
			Source: "github.issues",
			ConfigSchema: objectSchema(func() map[string]any {
				properties := copyProperties(providerConfig)
				properties["label"] = stringSchema()
				return properties
			}(), []string{"owner", "repository", "tokenSecret"}, false),
			EventSchema: objectSchema(map[string]any{"repository": repository, "issue": issueSchema()}, []string{"repository", "issue"}, false),
		},
		{
			Source:       "github.reviews",
			ConfigSchema: objectSchema(copyProperties(providerConfig), []string{"owner", "repository", "tokenSecret"}, false),
			EventSchema: objectSchema(map[string]any{
				"repository": repository,
				"pull": objectSchemaValue(map[string]any{
					"number": positiveIntegerSchema(), "html_url": stringSchema(), "head_sha": nonEmptyStringSchema(),
				}, []string{"number", "html_url", "head_sha"}, false),
				"review": objectSchemaValue(map[string]any{
					"id": positiveIntegerSchema(), "state": nonEmptyStringSchema(), "body": stringSchema(), "author": nonEmptyStringSchema(),
					"submitted_at": nonEmptyStringSchema(), "commit_id": nonEmptyStringSchema(),
				}, []string{"id", "state", "body", "author", "submitted_at", "commit_id"}, false),
			}, []string{"repository", "pull", "review"}, false),
		},
	}
}

func issueSchema() map[string]any {
	return objectSchemaValue(map[string]any{
		"number": positiveIntegerSchema(), "title": stringSchema(), "body": stringSchema(), "html_url": stringSchema(), "state": stringSchema(),
	}, []string{"number", "title", "body", "html_url", "state"}, false)
}

func mutationOutputSchema(withNumber bool) json.RawMessage {
	properties := map[string]any{"url": stringSchema(), "reconciled": boolSchema(), "marker": stringSchema()}
	required := []string{"url", "reconciled", "marker"}
	if withNumber {
		properties["number"] = positiveIntegerSchema()
		required = append(required, "number")
	} else {
		properties["id"] = positiveIntegerSchema()
		required = append(required, "id")
	}
	return objectSchema(properties, required, false)
}

func objectSchema(properties map[string]any, required []string, additional bool) json.RawMessage {
	value := objectSchemaValue(properties, required, additional)
	value["$schema"] = "https://json-schema.org/draft/2020-12/schema"
	return mustSchema(value)
}

func objectSchemaValue(properties map[string]any, required []string, additional bool) map[string]any {
	if properties == nil {
		properties = map[string]any{}
	}
	value := map[string]any{"type": "object", "properties": properties, "additionalProperties": additional}
	if len(required) > 0 {
		value["required"] = append([]string(nil), required...)
	}
	return value
}

func stringSchema() map[string]any          { return map[string]any{"type": "string"} }
func nonEmptyStringSchema() map[string]any  { return map[string]any{"type": "string", "minLength": 1} }
func integerSchema() map[string]any         { return map[string]any{"type": "integer"} }
func positiveIntegerSchema() map[string]any { return map[string]any{"type": "integer", "minimum": 1} }
func boolSchema() map[string]any            { return map[string]any{"type": "boolean"} }
func stringMapSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": stringSchema()}
}

func copyProperties(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func mustSchema(value any) json.RawMessage {
	encoded, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return encoded
}

func cloneActions(source []pluginsdk.ActionDescriptor) []pluginsdk.ActionDescriptor {
	result := make([]pluginsdk.ActionDescriptor, len(source))
	for index, descriptor := range source {
		result[index] = descriptor
		result[index].ConfigSchema = append(json.RawMessage(nil), descriptor.ConfigSchema...)
		result[index].InputSchema = append(json.RawMessage(nil), descriptor.InputSchema...)
		result[index].OutputSchema = append(json.RawMessage(nil), descriptor.OutputSchema...)
	}
	return result
}

func cloneTriggers(source []pluginsdk.TriggerDescriptor) []pluginsdk.TriggerDescriptor {
	result := make([]pluginsdk.TriggerDescriptor, len(source))
	for index, descriptor := range source {
		result[index] = descriptor
		result[index].ConfigSchema = append(json.RawMessage(nil), descriptor.ConfigSchema...)
		result[index].EventSchema = append(json.RawMessage(nil), descriptor.EventSchema...)
	}
	return result
}

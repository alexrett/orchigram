package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/alexrett/orchigram/sdk/plugin"
)

type handler struct{}

type config struct {
	Prefix       string `json:"prefix,omitempty"`
	EmitProgress bool   `json:"emitProgress,omitempty"`
}

type input struct {
	Message string `json:"message"`
}

func (handler) ValidateAction(_ context.Context, action string, raw json.RawMessage) []plugin.ValidationIssue {
	if action != "echo.echo" {
		return []plugin.ValidationIssue{{Path: "action", Code: "unsupported", Message: "expected echo.echo"}}
	}
	var value config
	if err := json.Unmarshal(raw, &value); err != nil {
		return []plugin.ValidationIssue{{Path: "config", Code: "invalid", Message: err.Error()}}
	}
	return nil
}

func (handler) Execute(_ context.Context, request plugin.TaskRequest, sink plugin.EventSink) (any, error) {
	var settings config
	if err := json.Unmarshal(request.Config, &settings); err != nil {
		return nil, err
	}
	var value input
	if err := json.Unmarshal(request.Input, &value); err != nil {
		return nil, err
	}
	if strings.TrimSpace(value.Message) == "" {
		return nil, errors.New("input.message is required")
	}
	if settings.EmitProgress {
		if err := sink.Emit("echo.progress", map[string]int{"percent": 100}); err != nil {
			return nil, err
		}
	}
	return map[string]string{"message": settings.Prefix + value.Message}, nil
}

func main() {
	plugin.Serve(plugin.Config{
		Metadata: plugin.Metadata{Name: "echo", Version: "0.1.0", Capabilities: []string{"task.echo.echo"}},
		Task:     handler{},
	})
}

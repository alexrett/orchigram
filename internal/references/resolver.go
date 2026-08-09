// Package references resolves desired-resource dependencies without reading
// secret values or launching plugin processes.
package references

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

// Lookup returns one canonical desired resource.
type Lookup interface {
	Get(context.Context, string, string, string) (resource.Document, error)
}

// ProviderValidator validates an installed provider contract and its metadata
// references without launching the provider process.
type ProviderValidator interface {
	ValidateTriggerProvider(context.Context, string, string, map[string]any) []flow.Diagnostic
}

// Resolver owns the stable cross-resource diagnostic vocabulary.
type Resolver struct {
	resources Lookup
	providers ProviderValidator
}

// New constructs a reference resolver.
func New(resources Lookup, providers ProviderValidator) *Resolver {
	return &Resolver{resources: resources, providers: providers}
}

// Supports reports whether a resource kind owns cross-resource desired state.
func Supports(kind string) bool {
	switch kind {
	case "Trigger", "Repository", "AgentProfile":
		return true
	default:
		return false
	}
}

// Diagnostics returns deterministic, secret-safe dependency failures.
func (r *Resolver) Diagnostics(ctx context.Context, document resource.Document) []flow.Diagnostic {
	namespace := document.Metadata.Namespace
	if namespace == "" {
		namespace = resource.DefaultNamespace
	}
	diagnostics := make([]flow.Diagnostic, 0)
	switch document.Kind {
	case "Trigger":
		trigger, err := resource.DecodeTrigger(document.JSON)
		if err != nil {
			return []flow.Diagnostic{{Path: "spec", Code: "invalid", Message: "Trigger cannot be decoded"}}
		}
		diagnostics = append(diagnostics, r.require(ctx, namespace, "Flow", trigger.Spec.Flow, "spec.flow")...)
		if trigger.Spec.Delivery != nil && trigger.Spec.Delivery.Mode == "signal" {
			diagnostics = append(diagnostics, r.signalTarget(ctx, namespace, trigger.Spec.Flow, trigger.Spec.Delivery.Node)...)
		}
		if trigger.Spec.Webhook != nil {
			diagnostics = append(diagnostics, r.require(ctx, namespace, "SecretRef", trigger.Spec.Webhook.BearerSecretRef, "spec.webhook.bearerSecretRef")...)
		}
		if trigger.Spec.Provider != nil {
			if r.providers == nil {
				diagnostics = append(diagnostics, flow.Diagnostic{Path: "spec.provider.plugin", Code: "provider_unavailable", Message: "provider validation is unavailable"})
			} else {
				for _, diagnostic := range r.providers.ValidateTriggerProvider(ctx, namespace, trigger.Spec.Provider.Plugin, trigger.Spec.Provider.Config) {
					diagnostic.Path = "spec.provider." + strings.TrimPrefix(diagnostic.Path, ".")
					diagnostics = append(diagnostics, diagnostic)
				}
			}
		}
	case "Repository":
		repository, err := resource.DecodeRepository(document.JSON)
		if err != nil {
			return []flow.Diagnostic{{Path: "spec", Code: "invalid", Message: "Repository cannot be decoded"}}
		}
		if repository.Spec.AuthSecretRef != "" {
			diagnostics = append(diagnostics, r.require(ctx, namespace, "SecretRef", repository.Spec.AuthSecretRef, "spec.authSecretRef")...)
		}
	case "AgentProfile":
		profile, err := resource.DecodeAgentProfile(document.JSON)
		if err != nil {
			return []flow.Diagnostic{{Path: "spec", Code: "invalid", Message: "AgentProfile cannot be decoded"}}
		}
		for index, binding := range profile.Spec.SecretRefs {
			name := secretName(binding)
			path := fmt.Sprintf("spec.secretRefs[%d]", index)
			if name == "" {
				diagnostics = append(diagnostics, flow.Diagnostic{Path: path, Code: "invalid_reference", Message: "SecretRef binding must contain a resource name"})
				continue
			}
			diagnostics = append(diagnostics, r.require(ctx, namespace, "SecretRef", name, path)...)
		}
	}
	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Path != diagnostics[j].Path {
			return diagnostics[i].Path < diagnostics[j].Path
		}
		return diagnostics[i].Code < diagnostics[j].Code
	})
	return diagnostics
}

func (r *Resolver) signalTarget(ctx context.Context, namespace, flowName, nodeID string) []flow.Diagnostic {
	if r.resources == nil || strings.TrimSpace(flowName) == "" {
		return nil
	}
	document, err := r.resources.Get(ctx, "Flow", namespace, flowName)
	if errors.Is(err, store.ErrNotFound) {
		return nil // spec.flow already owns the missing-reference diagnostic.
	}
	if err != nil {
		return []flow.Diagnostic{{Path: "spec.delivery.node", Code: "reference_unavailable", Message: "Flow signal target state is temporarily unavailable"}}
	}
	definition, err := resource.DecodeFlow(document.JSON)
	if err != nil {
		return []flow.Diagnostic{{Path: "spec.delivery.node", Code: "invalid_reference", Message: "referenced Flow cannot be decoded"}}
	}
	for _, node := range definition.Spec.Nodes {
		if node.ID == nodeID {
			if node.Uses == "core.event" {
				return nil
			}
			return []flow.Diagnostic{{Path: "spec.delivery.node", Code: "invalid_reference", Message: fmt.Sprintf("Flow node %q must use core.event", nodeID)}}
		}
	}
	return []flow.Diagnostic{{Path: "spec.delivery.node", Code: "reference_not_found", Message: fmt.Sprintf("Flow node %q is not available in Flow %q", nodeID, flowName)}}
}

func (r *Resolver) require(ctx context.Context, namespace, kind, name, path string) []flow.Diagnostic {
	if strings.TrimSpace(name) == "" {
		return []flow.Diagnostic{{Path: path, Code: "invalid_reference", Message: kind + " reference must contain a resource name"}}
	}
	if r.resources == nil {
		return []flow.Diagnostic{{Path: path, Code: "reference_unavailable", Message: "resource reference validation is unavailable"}}
	}
	if _, err := r.resources.Get(ctx, kind, namespace, name); errors.Is(err, store.ErrNotFound) {
		return []flow.Diagnostic{{Path: path, Code: "reference_not_found", Message: fmt.Sprintf("%s %q is not available in namespace %q", kind, name, namespace)}}
	} else if err != nil {
		return []flow.Diagnostic{{Path: path, Code: "reference_unavailable", Message: kind + " reference state is temporarily unavailable"}}
	}
	return nil
}

func secretName(binding string) string {
	if parts := strings.SplitN(binding, "=", 2); len(parts) == 2 {
		return strings.TrimSpace(parts[1])
	}
	return strings.TrimSpace(binding)
}

// Status projects one server-owned Ready condition and redacted diagnostics.
func Status(document resource.Document, diagnostics []flow.Diagnostic) map[string]any {
	ready := "True"
	reason := "ReferencesResolved"
	message := "all required references are available"
	firstError := -1
	for index, diagnostic := range diagnostics {
		if diagnostic.IsError() {
			firstError = index
			break
		}
	}
	if firstError >= 0 {
		ready = "False"
		switch diagnostics[firstError].Code {
		case "reference_not_found":
			reason = "MissingReference"
		case "provider_unavailable":
			reason = "ProviderUnavailable"
		default:
			reason = "ValidationFailed"
		}
		message = diagnostics[firstError].Message
	} else if len(diagnostics) > 0 {
		reason = "ValidationWarnings"
		message = diagnostics[0].Message
	}
	condition := map[string]any{
		"type": "Ready", "status": ready, "reason": reason,
		"message": message, "observedGeneration": document.Metadata.Generation,
	}
	status := map[string]any{"conditions": []any{condition}}
	if len(diagnostics) > 0 {
		projected := make([]any, 0, len(diagnostics))
		for _, diagnostic := range diagnostics {
			projected = append(projected, map[string]any{"path": diagnostic.Path, "code": diagnostic.Code, "message": diagnostic.Message})
		}
		status["diagnostics"] = projected
	}
	return status
}

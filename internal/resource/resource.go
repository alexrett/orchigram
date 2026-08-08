// Package resource defines Orchigram's public, framework-agnostic resources.
package resource

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"gopkg.in/yaml.v3"
)

const (
	// APIVersion is the canonical v0.1 resource API.
	APIVersion = "orchigram.dev/v1alpha1"
	// DefaultNamespace is used when metadata.namespace is omitted.
	DefaultNamespace = "default"
)

var dnsName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// TypeMeta identifies a resource schema.
type TypeMeta struct {
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
}

// ObjectMeta contains identity and optimistic concurrency metadata.
type ObjectMeta struct {
	Name            string            `json:"name" yaml:"name"`
	Namespace       string            `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	UID             string            `json:"uid,omitempty" yaml:"uid,omitempty"`
	ResourceVersion uint64            `json:"resourceVersion,omitempty" yaml:"resourceVersion,omitempty"`
	Generation      uint64            `json:"generation,omitempty" yaml:"generation,omitempty"`
	Labels          map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`
}

// Policies are inherited defaults for a Flow.
type Policies struct {
	Timeout     string `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	MaxParallel int    `json:"maxParallel,omitempty" yaml:"maxParallel,omitempty"`
}

// RetryPolicy controls bounded activity retries.
type RetryPolicy struct {
	Limit   int    `json:"limit,omitempty" yaml:"limit,omitempty"`
	Backoff string `json:"backoff,omitempty" yaml:"backoff,omitempty"`
}

// LoopPolicy makes a strongly connected component finite.
type LoopPolicy struct {
	MaxIterations int `json:"maxIterations" yaml:"maxIterations"`
}

// FlowNode is one typed action in a Flow graph.
type FlowNode struct {
	ID      string         `json:"id" yaml:"id"`
	Name    string         `json:"name,omitempty" yaml:"name,omitempty"`
	Uses    string         `json:"uses" yaml:"uses"`
	With    map[string]any `json:"with,omitempty" yaml:"with,omitempty"`
	Retry   *RetryPolicy   `json:"retry,omitempty" yaml:"retry,omitempty"`
	Timeout string         `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Loop    *LoopPolicy    `json:"loop,omitempty" yaml:"loop,omitempty"`
}

// FlowEdge connects two nodes and may carry a CEL condition.
type FlowEdge struct {
	From string `json:"from" yaml:"from"`
	To   string `json:"to" yaml:"to"`
	When string `json:"when,omitempty" yaml:"when,omitempty"`
}

// FlowSpec is the declarative graph source.
type FlowSpec struct {
	Inputs   map[string]any `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Policies Policies       `json:"policies,omitempty" yaml:"policies,omitempty"`
	Nodes    []FlowNode     `json:"nodes" yaml:"nodes"`
	Edges    []FlowEdge     `json:"edges,omitempty" yaml:"edges,omitempty"`
}

// Flow is an editable workflow definition.
type Flow struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     FlowSpec       `json:"spec" yaml:"spec"`
	Status   map[string]any `json:"status,omitempty" yaml:"status,omitempty"`
}

// ScheduleTrigger defines a native five-field cron source.
type ScheduleTrigger struct {
	Cron              string `json:"cron" yaml:"cron"`
	Timezone          string `json:"timezone,omitempty" yaml:"timezone,omitempty"`
	MisfirePolicy     string `json:"misfirePolicy,omitempty" yaml:"misfirePolicy,omitempty"`
	StartingDeadline  string `json:"startingDeadline,omitempty" yaml:"startingDeadline,omitempty"`
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty" yaml:"concurrencyPolicy,omitempty"`
}

// WebhookTrigger defines an opt-in generic hook.
type WebhookTrigger struct {
	BearerSecretRef string `json:"bearerSecretRef" yaml:"bearerSecretRef"`
}

// ProviderTrigger defines a plugin subscription.
type ProviderTrigger struct {
	Plugin string         `json:"plugin" yaml:"plugin"`
	Config map[string]any `json:"config,omitempty" yaml:"config,omitempty"`
}

// TriggerSpec maps one external source to a Flow.
type TriggerSpec struct {
	Flow     string           `json:"flow" yaml:"flow"`
	Enabled  *bool            `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	Schedule *ScheduleTrigger `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	Webhook  *WebhookTrigger  `json:"webhook,omitempty" yaml:"webhook,omitempty"`
	Provider *ProviderTrigger `json:"provider,omitempty" yaml:"provider,omitempty"`
}

// Trigger is a schedule, webhook, or provider subscription.
type Trigger struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     TriggerSpec    `json:"spec" yaml:"spec"`
	Status   map[string]any `json:"status,omitempty" yaml:"status,omitempty"`
}

// RepositorySpec configures a source checkout.
type RepositorySpec struct {
	CloneURL        string `json:"cloneURL" yaml:"cloneURL"`
	DefaultBranch   string `json:"defaultBranch,omitempty" yaml:"defaultBranch,omitempty"`
	WorkspacePolicy string `json:"workspacePolicy,omitempty" yaml:"workspacePolicy,omitempty"`
	AuthSecretRef   string `json:"authSecretRef,omitempty" yaml:"authSecretRef,omitempty"`
}

// Repository is a reusable repository checkout definition.
type Repository struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     RepositorySpec `json:"spec" yaml:"spec"`
	Status   map[string]any `json:"status,omitempty" yaml:"status,omitempty"`
}

// AgentProfileSpec configures one agent-command profile.
type AgentProfileSpec struct {
	Type        string            `json:"type" yaml:"type"`
	Executable  string            `json:"executable,omitempty" yaml:"executable,omitempty"`
	Args        []string          `json:"args,omitempty" yaml:"args,omitempty"`
	Model       string            `json:"model,omitempty" yaml:"model,omitempty"`
	Profile     string            `json:"profile,omitempty" yaml:"profile,omitempty"`
	Effort      string            `json:"effort,omitempty" yaml:"effort,omitempty"`
	Sandbox     string            `json:"sandbox,omitempty" yaml:"sandbox,omitempty"`
	Environment map[string]string `json:"environment,omitempty" yaml:"environment,omitempty"`
	SecretRefs  []string          `json:"secretRefs,omitempty" yaml:"secretRefs,omitempty"`
}

// AgentProfile is a named agent runtime configuration.
type AgentProfile struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta       `json:"metadata" yaml:"metadata"`
	Spec     AgentProfileSpec `json:"spec" yaml:"spec"`
	Status   map[string]any   `json:"status,omitempty" yaml:"status,omitempty"`
}

// PluginInstallationSpec selects an immutable installed plugin version.
type PluginInstallationSpec struct {
	Plugin  string `json:"plugin" yaml:"plugin"`
	Version string `json:"version" yaml:"version"`
	Digest  string `json:"digest" yaml:"digest"`
	Enabled *bool  `json:"enabled,omitempty" yaml:"enabled,omitempty"`
}

// PluginInstallation projects daemon-owned plugin installation state.
type PluginInstallation struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta             `json:"metadata" yaml:"metadata"`
	Spec     PluginInstallationSpec `json:"spec" yaml:"spec"`
	Status   map[string]any         `json:"status,omitempty" yaml:"status,omitempty"`
}

// SecretRefSpec points at secret material without containing its value.
type SecretRefSpec struct {
	Backend string `json:"backend" yaml:"backend"`
	Key     string `json:"key" yaml:"key"`
}

// SecretRef identifies secret material resolved only at execution time.
type SecretRef struct {
	TypeMeta `json:",inline" yaml:",inline"`
	Metadata ObjectMeta     `json:"metadata" yaml:"metadata"`
	Spec     SecretRefSpec  `json:"spec" yaml:"spec"`
	Status   map[string]any `json:"status,omitempty" yaml:"status,omitempty"`
}

// Document is the canonical storage projection shared by all kinds.
type Document struct {
	APIVersion string
	Kind       string
	Metadata   ObjectMeta
	Spec       json.RawMessage
	Status     json.RawMessage
	JSON       []byte
}

type typeProbe struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
}

// DecodeStrict parses one YAML or JSON document and rejects unknown fields.
func DecodeStrict(data []byte) (Document, error) {
	var probe typeProbe
	if err := yaml.Unmarshal(data, &probe); err != nil {
		return Document{}, fmt.Errorf("decode type metadata: %w", err)
	}
	if probe.APIVersion != APIVersion {
		return Document{}, fmt.Errorf("unsupported apiVersion %q", probe.APIVersion)
	}
	var value any
	switch probe.Kind {
	case "Flow":
		value = &Flow{}
	case "Trigger":
		value = &Trigger{}
	case "Repository":
		value = &Repository{}
	case "AgentProfile":
		value = &AgentProfile{}
	case "PluginInstallation":
		value = &PluginInstallation{}
	case "SecretRef":
		value = &SecretRef{}
	default:
		return Document{}, fmt.Errorf("unsupported kind %q", probe.Kind)
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(value); err != nil {
		return Document{}, fmt.Errorf("strict decode %s: %w", probe.Kind, err)
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return Document{}, fmt.Errorf("canonicalize %s: %w", probe.Kind, err)
	}
	var envelope struct {
		APIVersion string          `json:"apiVersion"`
		Kind       string          `json:"kind"`
		Metadata   ObjectMeta      `json:"metadata"`
		Spec       json.RawMessage `json:"spec"`
		Status     json.RawMessage `json:"status"`
	}
	if err := json.Unmarshal(canonical, &envelope); err != nil {
		return Document{}, fmt.Errorf("decode canonical envelope: %w", err)
	}
	if envelope.Metadata.Namespace == "" {
		envelope.Metadata.Namespace = DefaultNamespace
		canonical, err = setMetadata(canonical, envelope.Metadata)
		if err != nil {
			return Document{}, err
		}
	}
	if err := ValidateMetadata(envelope.Metadata); err != nil {
		return Document{}, err
	}
	if err := validateKind(value); err != nil {
		return Document{}, err
	}
	if len(envelope.Status) == 0 || bytes.Equal(envelope.Status, []byte("null")) {
		envelope.Status = []byte("{}")
	}
	return Document{APIVersion: envelope.APIVersion, Kind: envelope.Kind, Metadata: envelope.Metadata, Spec: envelope.Spec, Status: envelope.Status, JSON: canonical}, nil
}

// DecodeFlow decodes and returns a concrete Flow.
func DecodeFlow(data []byte) (Flow, error) {
	doc, err := DecodeStrict(data)
	if err != nil {
		return Flow{}, err
	}
	if doc.Kind != "Flow" {
		return Flow{}, fmt.Errorf("expected Flow, got %s", doc.Kind)
	}
	var flow Flow
	if err := json.Unmarshal(doc.JSON, &flow); err != nil {
		return Flow{}, err
	}
	return flow, nil
}

// DecodeAgentProfile decodes and returns a concrete AgentProfile.
func DecodeAgentProfile(data []byte) (AgentProfile, error) {
	doc, err := DecodeStrict(data)
	if err != nil {
		return AgentProfile{}, err
	}
	if doc.Kind != "AgentProfile" {
		return AgentProfile{}, fmt.Errorf("expected AgentProfile, got %s", doc.Kind)
	}
	var profile AgentProfile
	if err := json.Unmarshal(doc.JSON, &profile); err != nil {
		return AgentProfile{}, err
	}
	return profile, nil
}

// DecodeRepository decodes and returns a concrete Repository projection.
func DecodeRepository(data []byte) (Repository, error) {
	doc, err := DecodeStrict(data)
	if err != nil {
		return Repository{}, err
	}
	if doc.Kind != "Repository" {
		return Repository{}, fmt.Errorf("expected Repository, got %s", doc.Kind)
	}
	var repository Repository
	if err := json.Unmarshal(doc.JSON, &repository); err != nil {
		return Repository{}, err
	}
	return repository, nil
}

// DecodeSecretRef decodes and returns a concrete SecretRef projection.
func DecodeSecretRef(data []byte) (SecretRef, error) {
	doc, err := DecodeStrict(data)
	if err != nil {
		return SecretRef{}, err
	}
	if doc.Kind != "SecretRef" {
		return SecretRef{}, fmt.Errorf("expected SecretRef, got %s", doc.Kind)
	}
	var secret SecretRef
	if err := json.Unmarshal(doc.JSON, &secret); err != nil {
		return SecretRef{}, err
	}
	return secret, nil
}

// DecodeTrigger decodes and returns a concrete Trigger.
func DecodeTrigger(data []byte) (Trigger, error) {
	doc, err := DecodeStrict(data)
	if err != nil {
		return Trigger{}, err
	}
	if doc.Kind != "Trigger" {
		return Trigger{}, fmt.Errorf("expected Trigger, got %s", doc.Kind)
	}
	var trigger Trigger
	if err := json.Unmarshal(doc.JSON, &trigger); err != nil {
		return Trigger{}, err
	}
	return trigger, nil
}

// ValidateMetadata enforces stable DNS-like resource keys.
func ValidateMetadata(meta ObjectMeta) error {
	if !dnsName.MatchString(meta.Name) || len(meta.Name) > 63 {
		return fmt.Errorf("metadata.name %q must be a DNS label", meta.Name)
	}
	if !dnsName.MatchString(meta.Namespace) || len(meta.Namespace) > 63 {
		return fmt.Errorf("metadata.namespace %q must be a DNS label", meta.Namespace)
	}
	return nil
}

// WithServerMetadata replaces server-owned metadata fields in canonical JSON.
func (d Document) WithServerMetadata(meta ObjectMeta) (Document, error) {
	b, err := setMetadata(d.JSON, meta)
	if err != nil {
		return Document{}, err
	}
	d.Metadata = meta
	d.JSON = b
	return d, nil
}

// WithServerStatus replaces the server-owned status projection. Clients may
// round-trip a projected resource, but status is never accepted as desired
// configuration.
func (d Document) WithServerStatus(status map[string]any) (Document, error) {
	var value map[string]any
	if err := json.Unmarshal(d.JSON, &value); err != nil {
		return Document{}, fmt.Errorf("decode canonical resource: %w", err)
	}
	if len(status) == 0 {
		delete(value, "status")
		d.Status = []byte("{}")
	} else {
		value["status"] = status
		encoded, err := json.Marshal(status)
		if err != nil {
			return Document{}, err
		}
		d.Status = encoded
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return Document{}, err
	}
	d.JSON = encoded
	return d, nil
}

func setMetadata(data []byte, meta ObjectMeta) ([]byte, error) {
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		return nil, fmt.Errorf("decode canonical resource: %w", err)
	}
	encoded, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	var metadata any
	if err := json.Unmarshal(encoded, &metadata); err != nil {
		return nil, err
	}
	value["metadata"] = metadata
	return json.Marshal(value)
}

func validateKind(value any) error {
	switch v := value.(type) {
	case *Flow:
		if len(v.Spec.Nodes) == 0 {
			return errors.New("flow spec nodes must not be empty")
		}
	case *Trigger:
		count := 0
		if v.Spec.Schedule != nil {
			count++
		}
		if v.Spec.Webhook != nil {
			count++
		}
		if v.Spec.Provider != nil {
			count++
		}
		if count != 1 {
			return errors.New("trigger spec must configure exactly one of schedule, webhook, or provider")
		}
		if strings.TrimSpace(v.Spec.Flow) == "" {
			return errors.New("trigger spec flow is required")
		}
		if v.Spec.Schedule != nil {
			if len(strings.Fields(v.Spec.Schedule.Cron)) != 5 {
				return errors.New("trigger schedule cron must contain exactly five fields")
			}
			if _, err := cron.ParseStandard(v.Spec.Schedule.Cron); err != nil {
				return fmt.Errorf("trigger schedule cron is invalid: %w", err)
			}
			timezone := v.Spec.Schedule.Timezone
			if timezone == "" {
				timezone = "UTC"
			}
			if _, err := time.LoadLocation(timezone); err != nil {
				return fmt.Errorf("trigger schedule timezone %q is invalid", timezone)
			}
			if v.Spec.Schedule.MisfirePolicy != "" && v.Spec.Schedule.MisfirePolicy != "fireOnce" {
				return fmt.Errorf("trigger schedule misfirePolicy %q is unsupported", v.Spec.Schedule.MisfirePolicy)
			}
			if v.Spec.Schedule.ConcurrencyPolicy != "" && v.Spec.Schedule.ConcurrencyPolicy != "forbid" {
				return fmt.Errorf("trigger schedule concurrencyPolicy %q is unsupported", v.Spec.Schedule.ConcurrencyPolicy)
			}
			if _, err := ParseDuration(v.Spec.Schedule.StartingDeadline, time.Hour); err != nil {
				return fmt.Errorf("trigger schedule startingDeadline: %w", err)
			}
		}
		if v.Spec.Webhook != nil && strings.TrimSpace(v.Spec.Webhook.BearerSecretRef) == "" {
			return errors.New("trigger webhook bearerSecretRef is required")
		}
		if v.Spec.Provider != nil && strings.TrimSpace(v.Spec.Provider.Plugin) == "" {
			return errors.New("trigger provider plugin is required")
		}
	case *Repository:
		if strings.TrimSpace(v.Spec.CloneURL) == "" {
			return errors.New("Repository.spec.cloneURL is required")
		}
		if v.Spec.WorkspacePolicy != "" && v.Spec.WorkspacePolicy != "isolated-run" {
			return fmt.Errorf("Repository.spec.workspacePolicy %q is unsupported", v.Spec.WorkspacePolicy)
		}
	case *AgentProfile:
		if v.Spec.Type != "codex" && v.Spec.Type != "claude" && v.Spec.Type != "command" {
			return fmt.Errorf("AgentProfile.spec.type %q is unsupported", v.Spec.Type)
		}
		if v.Spec.Type == "command" && strings.TrimSpace(v.Spec.Executable) == "" {
			return errors.New("AgentProfile command type requires an executable")
		}
	case *PluginInstallation:
		if v.Spec.Plugin == "" || v.Spec.Version == "" || v.Spec.Digest == "" {
			return errors.New("PluginInstallation plugin, version, and digest are required")
		}
	case *SecretRef:
		if v.Spec.Backend == "" || v.Spec.Key == "" {
			return errors.New("SecretRef backend and key are required")
		}
		if v.Spec.Backend != "env" && v.Spec.Backend != "environment" && v.Spec.Backend != "file" {
			return fmt.Errorf("SecretRef backend %q is unsupported", v.Spec.Backend)
		}
	}
	return nil
}

// ParseDuration applies a default and returns a validated duration.
func ParseDuration(value string, fallback time.Duration) (time.Duration, error) {
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("invalid positive duration %q", value)
	}
	return d, nil
}

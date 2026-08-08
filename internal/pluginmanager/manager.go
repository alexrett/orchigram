// Package pluginmanager installs, activates, supervises, and invokes plugins.
package pluginmanager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/pluginhost"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxSecretSize = 1 << 20

var templateExpression = regexp.MustCompile(`\$\{([^{}]+)\}`)

// Manager is both the plugin control plane and engine TaskExecutor.
type Manager struct {
	store      *store.Store
	root       string
	artifacts  string
	workspaces string
	installMu  sync.Mutex
	mu         sync.Mutex
	processes  map[string]*pluginhost.Process
	active     map[string]*activeProviderCall
}

type activeProviderCall struct {
	process *pluginhost.Process
	meta    *pluginv1alpha1.CallMeta
	kind    string
	cancel  context.CancelFunc
	stop    func() bool
	once    sync.Once
}

// New creates a manager rooted under the daemon's private state directory.
func New(state *store.Store, stateRoot string) *Manager {
	return &Manager{
		store: state, root: filepath.Join(stateRoot, "plugins"),
		artifacts: filepath.Join(stateRoot, "artifacts"), workspaces: filepath.Join(stateRoot, "workspaces"), processes: map[string]*pluginhost.Process{}, active: map[string]*activeProviderCall{},
	}
}

// Install validates an archive, executes protocol negotiation, then records it.
func (m *Manager) Install(ctx context.Context, bundle []byte) (store.PluginRecord, error) {
	m.installMu.Lock()
	defer m.installMu.Unlock()
	manifest, _, _, err := pluginbundle.Parse(bundle)
	if err != nil {
		return store.PluginRecord{}, err
	}
	if manifest.Protocol.Minimum > 1 || manifest.Protocol.Maximum < 1 {
		return store.PluginRecord{}, fmt.Errorf("plugin protocol range %d-%d is incompatible with host protocol 1", manifest.Protocol.Minimum, manifest.Protocol.Maximum)
	}
	installed, err := pluginbundle.Stage(m.root, bundle)
	if err != nil {
		return store.PluginRecord{}, err
	}
	stagingDirectory := installed.Directory
	defer func() { _ = os.RemoveAll(stagingDirectory) }()
	platform, err := installed.Manifest.CurrentPlatform()
	if err != nil {
		return store.PluginRecord{}, err
	}
	process, description, err := pluginhost.Launch(ctx, installed.Executable, platform.SHA256)
	if err != nil {
		return store.PluginRecord{}, err
	}
	process.Close()
	if description.GetName() != installed.Manifest.Name || description.GetVersion() != installed.Manifest.Version {
		return store.PluginRecord{}, fmt.Errorf("plugin identity %s@%s does not match manifest %s@%s", description.GetName(), description.GetVersion(), installed.Manifest.Name, installed.Manifest.Version)
	}
	if !sameCapabilities(description.GetCapabilities(), installed.Manifest.Capabilities) {
		return store.PluginRecord{}, errors.New("plugin capabilities do not match manifest")
	}
	manifestJSON, err := json.Marshal(installed.Manifest)
	if err != nil {
		return store.PluginRecord{}, err
	}
	record := store.PluginRecord{Name: installed.Manifest.Name, Version: installed.Manifest.Version, Digest: installed.Digest, ManifestJSON: manifestJSON, State: "installed"}
	installed, err = pluginbundle.Publish(installed)
	if err != nil {
		return store.PluginRecord{}, err
	}
	if err := m.store.PutPlugin(ctx, record); err != nil {
		if installed.Published {
			_ = os.RemoveAll(installed.Directory)
		}
		return store.PluginRecord{}, err
	}
	return m.store.Plugin(ctx, record.Name, record.Version)
}

// Enable launches and health-checks a version before atomically activating it.
func (m *Manager) Enable(ctx context.Context, name, version string) error {
	record, err := m.store.Plugin(ctx, name, version)
	if err != nil {
		return err
	}
	process, err := m.launch(ctx, record)
	if err != nil {
		return err
	}
	healthContext, cancel := context.WithTimeout(ctx, 5*time.Second)
	health, err := process.Clients().Control.Health(healthContext, &emptypb.Empty{})
	cancel()
	if err != nil || !health.GetReady() {
		process.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("plugin %s@%s is not ready: %s", name, version, health.GetMessage())
	}
	if err := m.store.ActivatePlugin(ctx, name, version); err != nil {
		process.Close()
		return err
	}
	key := installationKey(name, version)
	m.mu.Lock()
	for candidate, running := range m.processes {
		if strings.HasPrefix(candidate, name+"@") && candidate != key {
			running.Close()
			delete(m.processes, candidate)
		}
	}
	m.processes[key] = process
	m.mu.Unlock()
	return nil
}

// Disable stops the active process and leaves immutable versions installed.
func (m *Manager) Disable(ctx context.Context, name string) error {
	record, err := m.store.Plugin(ctx, name, "")
	if err != nil {
		return err
	}
	if err := m.store.DisablePlugin(ctx, name); err != nil {
		return err
	}
	m.evict(installationKey(name, record.Version))
	return nil
}

// List returns every installed immutable version.
func (m *Manager) List(ctx context.Context) ([]store.PluginRecord, error) {
	return m.store.ListPlugins(ctx)
}

// HasAction implements flow.CapabilityResolver against active installations.
func (m *Manager) HasAction(action string) bool {
	name := strings.SplitN(action, ".", 2)[0]
	record, err := m.store.Plugin(context.Background(), name, "")
	if err != nil {
		return false
	}
	var manifest pluginbundle.Manifest
	if json.Unmarshal(record.ManifestJSON, &manifest) != nil {
		return false
	}
	for _, capability := range manifest.Capabilities {
		if capability == action || capability == "task."+action {
			return true
		}
		if name == "agent-command" && action == "agent-command.run" && strings.HasPrefix(capability, "agent.") {
			return true
		}
	}
	return false
}

// ValidateAction asks the active provider to validate configuration at compile time.
func (m *Manager) ValidateAction(action string, config map[string]any) []flow.Diagnostic {
	name := strings.SplitN(action, ".", 2)[0]
	if name == "agent-command" {
		profile, _ := config["profile"].(string)
		if profile == "" {
			return []flow.Diagnostic{{Path: "config.profile", Code: "required", Message: "agent-command action requires a profile"}}
		}
		if _, err := m.store.Get(context.Background(), "AgentProfile", resource.DefaultNamespace, profile); err != nil {
			return []flow.Diagnostic{{Path: "config.profile", Code: "not_found", Message: fmt.Sprintf("AgentProfile %q is not available", profile)}}
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	process, _, err := m.activeProcess(ctx, name)
	if err != nil {
		return []flow.Diagnostic{{Path: "config", Code: "plugin_unavailable", Message: err.Error()}}
	}
	copyConfig := make(map[string]any, len(config))
	for key, value := range config {
		if key != "secretRefs" && key != "mappings" {
			copyConfig[key] = value
		}
	}
	if action == "github.workspace.checkout" {
		if err := m.expandRepositoryConfig(context.Background(), copyConfig); err != nil {
			return []flow.Diagnostic{{Path: "config.repositoryRef", Code: "not_found", Message: err.Error()}}
		}
	}
	if strings.HasPrefix(action, "github.workspace.") {
		copyConfig["workspaceRoot"] = m.workspaces
	}
	delete(copyConfig, "secretRefs")
	configJSON, err := json.Marshal(copyConfig)
	if err != nil {
		return []flow.Diagnostic{{Path: "config", Code: "invalid", Message: err.Error()}}
	}
	response, err := process.Clients().Task.ValidateAction(ctx, &pluginv1alpha1.ValidateActionRequest{Action: action, ConfigJson: configJSON})
	if err != nil {
		return []flow.Diagnostic{{Path: "config", Code: "validation_failed", Message: err.Error()}}
	}
	diagnostics := make([]flow.Diagnostic, 0, len(response.GetIssues()))
	for _, issue := range response.GetIssues() {
		diagnostics = append(diagnostics, flow.Diagnostic{Path: issue.GetPath(), Code: issue.GetCode(), Message: issue.GetMessage()})
	}
	return diagnostics
}

// Describe returns one installed or active version.
func (m *Manager) Describe(ctx context.Context, name, version string) (store.PluginRecord, error) {
	return m.store.Plugin(ctx, name, version)
}

// ResolveSecret resolves one operation-scoped SecretRef for daemon controllers.
func (m *Manager) ResolveSecret(ctx context.Context, name string) ([]byte, error) {
	return m.resolveSecret(ctx, name)
}

// SecretStatus reports only reference availability, never secret material.
func (m *Manager) SecretStatus(ctx context.Context, name string) (string, string) {
	document, err := m.store.Get(ctx, "SecretRef", resource.DefaultNamespace, name)
	if err != nil {
		return "Missing", "unknown"
	}
	reference, err := resource.DecodeSecretRef(document.JSON)
	if err != nil {
		return "Missing", "unknown"
	}
	switch reference.Spec.Backend {
	case "env", "environment":
		if value, exists := os.LookupEnv(reference.Spec.Key); exists && value != "" {
			return "Configured", "env"
		}
		return "Missing", "env"
	case "file":
		info, statErr := os.Stat(reference.Spec.Key)
		if statErr == nil && info.Mode().IsRegular() && info.Size() > 0 && info.Size() <= maxSecretSize {
			return "Configured", "file"
		}
		return "Missing", "file"
	default:
		return "Missing", reference.Spec.Backend
	}
}

// Doctor launches the selected plugin and verifies negotiation plus health.
func (m *Manager) Doctor(ctx context.Context, name, version string) error {
	record, err := m.store.Plugin(ctx, name, version)
	if err != nil {
		return err
	}
	process, err := m.launch(ctx, record)
	if err != nil {
		return err
	}
	defer process.Close()
	health, err := process.Clients().Control.Health(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	if !health.GetReady() {
		return fmt.Errorf("plugin is not ready: %s", health.GetMessage())
	}
	if name == "agent-command" {
		profiles, _, err := m.store.List(ctx, "AgentProfile", resource.DefaultNamespace, 1000)
		if err != nil {
			return err
		}
		for _, document := range profiles {
			profile, err := resource.DecodeAgentProfile(document.JSON)
			if err != nil {
				return err
			}
			secrets, err := m.resolveProfileSecrets(ctx, profile.Spec.SecretRefs)
			if err != nil {
				return fmt.Errorf("agent profile %s: %w", profile.Metadata.Name, err)
			}
			profileJSON, err := json.Marshal(profile.Spec)
			if err != nil {
				return err
			}
			meta := &pluginv1alpha1.CallMeta{RequestId: uuid.NewString(), RunUid: "doctor", NodeId: profile.Metadata.Name, Attempt: 1, IdempotencyKey: "doctor/" + profile.Metadata.UID, Deadline: timestamppb.New(time.Now().Add(30 * time.Second))}
			stream, err := process.Clients().Agent.Execute(ctx, &pluginv1alpha1.AgentRequest{Meta: meta, ProfileType: profile.Spec.Type, ProfileJson: profileJSON, InputJson: []byte(`{"doctor":true}`), Secrets: secrets})
			if err != nil {
				return fmt.Errorf("agent profile %s authentication: %w", profile.Metadata.Name, err)
			}
			if err := drainDoctor(stream, secretValues(secrets)); err != nil {
				return fmt.Errorf("agent profile %s authentication: %w", profile.Metadata.Name, err)
			}
		}
	}
	return nil
}

// Execute implements engine.TaskExecutor for non-core Flow nodes.
func (m *Manager) Execute(ctx context.Context, runUID string, node flow.PlanNode, input json.RawMessage, nodes map[string]any, idempotencyKey string) (json.RawMessage, error) {
	pluginName := strings.SplitN(node.Uses, ".", 2)[0]
	process, record, err := m.activeProcess(ctx, pluginName)
	if err != nil {
		return nil, err
	}
	requestID := uuid.NewString()
	deadline := time.Now().Add(30 * time.Minute)
	if parsed, parseErr := time.ParseDuration(node.Timeout); parseErr == nil {
		deadline = time.Now().Add(parsed)
	}
	callMeta := &pluginv1alpha1.CallMeta{
		RequestId: requestID, RunUid: runUID, NodeId: node.ID, Attempt: 1,
		IdempotencyKey: idempotencyKey, Deadline: timestamppb.New(deadline),
	}
	callContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	kind := "task"
	if pluginName == "agent-command" {
		kind = "agent"
	}
	call := m.registerActiveCall(callContext, process, callMeta, kind, cancel)
	defer m.releaseActiveCall(callMeta.GetRequestId(), call)
	if pluginName == "agent-command" {
		output, executeErr := m.executeAgent(callContext, process, callMeta, node, input, nodes)
		if executeErr != nil && process.Exited() {
			m.evict(installationKey(record.Name, record.Version))
		}
		return output, executeErr
	}
	output, executeErr := m.executeTask(callContext, process, callMeta, node, input, nodes)
	if executeErr != nil && process.Exited() {
		m.evict(installationKey(record.Name, record.Version))
	}
	return output, executeErr
}

// WatchTrigger runs one bidirectional provider stream and acknowledges an
// event only after the controller callback has durably persisted it.
func (m *Manager) WatchTrigger(ctx context.Context, pluginName, triggerUID string, config map[string]any, cursor string, activatedAt time.Time, accept func(*pluginv1alpha1.TriggerEvent) error) error {
	process, record, err := m.activeProcess(ctx, pluginName)
	if err != nil {
		return err
	}
	configCopy := make(map[string]any, len(config))
	for key, value := range config {
		configCopy[key] = value
	}
	if err := validateProviderBootstrap(record, cursor, activatedAt, configCopy); err != nil {
		return err
	}
	secrets, err := m.resolveNodeSecrets(ctx, configCopy)
	if err != nil {
		return err
	}
	configJSON, err := json.Marshal(configCopy)
	if err != nil {
		return err
	}
	stream, err := process.Clients().Trigger.Watch(ctx)
	if err != nil {
		return err
	}
	start := &pluginv1alpha1.WatchStart{InstallationUid: triggerUID, Cursor: cursor, ConfigJson: configJSON, Secrets: secrets}
	if !activatedAt.IsZero() {
		start.ActivatedAt = timestamppb.New(activatedAt)
	}
	if err := stream.Send(&pluginv1alpha1.TriggerCommand{Value: &pluginv1alpha1.TriggerCommand_Start{Start: start}}); err != nil {
		return err
	}
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			if process.Exited() {
				m.evict(installationKey(record.Name, record.Version))
			}
			return receiveErr
		}
		if err := accept(event); err != nil {
			return err
		}
		if err := stream.Send(&pluginv1alpha1.TriggerCommand{Value: &pluginv1alpha1.TriggerCommand_Ack{Ack: &pluginv1alpha1.TriggerAck{ProviderEventId: event.GetProviderEventId(), Cursor: event.GetCursor()}}}); err != nil {
			return err
		}
	}
}

// Close stops every supervised plugin process.
func (m *Manager) Close() {
	m.mu.Lock()
	processes := make([]*pluginhost.Process, 0, len(m.processes))
	for _, process := range m.processes {
		processes = append(processes, process)
	}
	m.processes = map[string]*pluginhost.Process{}
	m.mu.Unlock()
	for _, process := range processes {
		process.Close()
	}
}

func (m *Manager) executeTask(ctx context.Context, process *pluginhost.Process, meta *pluginv1alpha1.CallMeta, node flow.PlanNode, input json.RawMessage, nodes map[string]any) (json.RawMessage, error) {
	config := make(map[string]any, len(node.With))
	for key, value := range node.With {
		config[key] = value
	}
	if err := renderConfigTemplates(config, input, nodes); err != nil {
		return nil, err
	}
	if err := applyMappings(config, input, nodes); err != nil {
		return nil, err
	}
	if node.Uses == "github.workspace.checkout" {
		if err := m.expandRepositoryConfig(ctx, config); err != nil {
			return nil, err
		}
	}
	if strings.HasPrefix(node.Uses, "github.workspace.") {
		config["workspaceRoot"] = m.workspaces
	}
	secrets, err := m.resolveNodeSecrets(ctx, config)
	if err != nil {
		return nil, err
	}
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	stream, err := process.Clients().Task.Execute(ctx, &pluginv1alpha1.ExecuteRequest{Meta: meta, Action: node.Uses, InputJson: input, ConfigJson: configJSON, Secrets: secrets})
	if err != nil {
		return nil, err
	}
	return m.consume(ctx, runArtifact{runUID: meta.GetRunUid(), nodeID: meta.GetNodeId(), attempt: meta.GetAttempt(), redactions: secretValues(secrets)}, stream)
}

func (m *Manager) expandRepositoryConfig(ctx context.Context, config map[string]any) error {
	name, _ := config["repositoryRef"].(string)
	if name == "" {
		return errors.New("repositoryRef is required")
	}
	document, err := m.store.Get(ctx, "Repository", resource.DefaultNamespace, name)
	if err != nil {
		return fmt.Errorf("resolve Repository %q: %w", name, err)
	}
	repository, err := resource.DecodeRepository(document.JSON)
	if err != nil {
		return err
	}
	delete(config, "repositoryRef")
	config["cloneURL"] = repository.Spec.CloneURL
	config["defaultBranch"] = repository.Spec.DefaultBranch
	if repository.Spec.AuthSecretRef != "" {
		bindings, exists := config["secretRefs"]
		if !exists {
			bindings = map[string]any{}
			config["secretRefs"] = bindings
		}
		bindingMap, ok := bindings.(map[string]any)
		if !ok {
			return errors.New("with.secretRefs must be a string map")
		}
		if _, exists := bindingMap["token"]; !exists {
			bindingMap["token"] = repository.Spec.AuthSecretRef
		}
		if _, exists := config["tokenSecret"]; !exists {
			config["tokenSecret"] = "token"
		}
	}
	return nil
}

func (m *Manager) executeAgent(ctx context.Context, process *pluginhost.Process, meta *pluginv1alpha1.CallMeta, node flow.PlanNode, input json.RawMessage, nodes map[string]any) (json.RawMessage, error) {
	profileName, _ := node.With["profile"].(string)
	if profileName == "" {
		return nil, errors.New("agent-command node requires with.profile")
	}
	document, err := m.store.Get(ctx, "AgentProfile", resource.DefaultNamespace, profileName)
	if err != nil {
		return nil, err
	}
	profile, err := resource.DecodeAgentProfile(document.JSON)
	if err != nil {
		return nil, err
	}
	secrets, err := m.resolveProfileSecrets(ctx, profile.Spec.SecretRefs)
	if err != nil {
		return nil, err
	}
	profileJSON, err := json.Marshal(profile.Spec)
	if err != nil {
		return nil, err
	}
	config := make(map[string]any, len(node.With))
	for key, value := range node.With {
		if key != "profile" {
			config[key] = value
		}
	}
	if err := renderConfigTemplates(config, input, nodes); err != nil {
		return nil, err
	}
	if err := applyMappings(config, input, nodes); err != nil {
		return nil, err
	}
	invocation := map[string]any{}
	var decodedInput any
	if len(input) > 0 && json.Unmarshal(input, &decodedInput) == nil {
		invocation["input"] = decodedInput
	}
	for _, key := range []string{"prompt", "workspace"} {
		if value, exists := config[key]; exists {
			invocation[key] = value
			delete(config, key)
		}
	}
	if len(config) != 0 {
		return nil, fmt.Errorf("unsupported agent-command configuration fields: %v", sortedMapKeys(config))
	}
	agentInput, err := json.Marshal(invocation)
	if err != nil {
		return nil, err
	}
	stream, err := process.Clients().Agent.Execute(ctx, &pluginv1alpha1.AgentRequest{Meta: meta, ProfileType: profile.Spec.Type, ProfileJson: profileJSON, InputJson: agentInput, Secrets: secrets})
	if err != nil {
		return nil, err
	}
	return m.consume(ctx, runArtifact{runUID: meta.GetRunUid(), nodeID: meta.GetNodeId(), attempt: meta.GetAttempt(), redactions: secretValues(secrets)}, stream)
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type eventReceiver interface {
	Recv() (*pluginv1alpha1.ExecuteEvent, error)
}

type runArtifact struct {
	runUID     string
	nodeID     string
	attempt    uint32
	redactions [][]byte
}

func (m *Manager) consume(ctx context.Context, artifact runArtifact, stream eventReceiver) (json.RawMessage, error) {
	var expected uint64 = 1
	var output json.RawMessage
	terminal := ""
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if terminal == "" {
				return nil, errors.New("plugin stream ended without a completion event")
			}
			if strings.HasSuffix(terminal, ".failed") {
				return nil, fmt.Errorf("plugin reported %s", terminal)
			}
			return output, nil
		}
		if err != nil {
			if terminal != "" {
				err = sanitizePluginError(err, artifact.redactions)
				if strings.HasSuffix(terminal, ".failed") {
					return nil, fmt.Errorf("plugin reported %s: %w", terminal, err)
				}
				return nil, fmt.Errorf("plugin stream did not end immediately after its terminal event: %w", err)
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, sanitizePluginError(err, artifact.redactions)
		}
		if terminal != "" {
			return nil, errors.New("plugin emitted an event after its terminal event")
		}
		if event.GetSequence() != expected || strings.TrimSpace(event.GetType()) == "" || event.GetOccurredAt() == nil || !event.GetOccurredAt().IsValid() {
			return nil, fmt.Errorf("malformed plugin stream at sequence %d", expected)
		}
		expected++
		if len(event.GetRawLog()) > 0 {
			if err := m.appendArtifact(artifact, redact(event.GetRawLog(), artifact.redactions)); err != nil {
				return nil, err
			}
		}
		payload := redact(event.GetPayloadJson(), artifact.redactions)
		if !json.Valid(payload) {
			return nil, fmt.Errorf("plugin event %d contains invalid JSON", event.GetSequence())
		}
		if strings.HasSuffix(event.GetType(), ".failed") || strings.HasSuffix(event.GetType(), ".completed") {
			terminal = event.GetType()
			output = append(output[:0], payload...)
		}
	}
}

func (m *Manager) activeProcess(ctx context.Context, name string) (*pluginhost.Process, store.PluginRecord, error) {
	record, err := m.store.Plugin(ctx, name, "")
	if err != nil {
		return nil, store.PluginRecord{}, fmt.Errorf("active plugin %q: %w", name, err)
	}
	key := installationKey(name, record.Version)
	m.mu.Lock()
	if running := m.processes[key]; running != nil && !running.Exited() {
		m.mu.Unlock()
		return running, record, nil
	}
	m.mu.Unlock()
	process, err := m.launch(ctx, record)
	if err != nil {
		return nil, store.PluginRecord{}, err
	}
	m.mu.Lock()
	if existing := m.processes[key]; existing != nil && !existing.Exited() {
		m.mu.Unlock()
		process.Close()
		return existing, record, nil
	}
	m.processes[key] = process
	m.mu.Unlock()
	return process, record, nil
}

func (m *Manager) launch(ctx context.Context, record store.PluginRecord) (*pluginhost.Process, error) {
	var manifest pluginbundle.Manifest
	if err := json.Unmarshal(record.ManifestJSON, &manifest); err != nil {
		return nil, err
	}
	platform, err := manifest.CurrentPlatform()
	if err != nil {
		return nil, err
	}
	executable := filepath.Join(m.root, record.Name, record.Version, "plugin")
	process, description, err := pluginhost.Launch(ctx, executable, platform.SHA256)
	if err != nil {
		return nil, err
	}
	if description.GetName() != record.Name || description.GetVersion() != record.Version {
		process.Close()
		return nil, errors.New("running plugin identity does not match installation")
	}
	return process, nil
}

func (m *Manager) resolveNodeSecrets(ctx context.Context, config map[string]any) (map[string][]byte, error) {
	value, exists := config["secretRefs"]
	if !exists {
		return nil, nil
	}
	delete(config, "secretRefs")
	references, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("with.secretRefs must be a string map")
	}
	result := make(map[string][]byte, len(references))
	for target, untyped := range references {
		name, ok := untyped.(string)
		if !ok || name == "" {
			return nil, fmt.Errorf("secret reference for %s must be a resource name", target)
		}
		secret, err := m.resolveSecret(ctx, name)
		if err != nil {
			return nil, err
		}
		result[target] = secret
	}
	return result, nil
}

type valueMapping struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func renderConfigTemplates(config map[string]any, input json.RawMessage, nodes map[string]any) error {
	sources, err := mappingSources(input, nodes)
	if err != nil {
		return err
	}
	for key, value := range config {
		rendered, renderErr := renderValue(value, sources)
		if renderErr != nil {
			return fmt.Errorf("render config %s: %w", key, renderErr)
		}
		config[key] = rendered
	}
	return nil
}

func renderValue(value any, sources map[string]any) (any, error) {
	switch typed := value.(type) {
	case string:
		var renderErr error
		rendered := templateExpression.ReplaceAllStringFunc(typed, func(match string) string {
			path := templateExpression.FindStringSubmatch(match)[1]
			resolved, err := lookupMappingValue(sources, path)
			if err != nil {
				renderErr = err
				return match
			}
			if text, ok := resolved.(string); ok {
				return text
			}
			encoded, err := json.Marshal(resolved)
			if err != nil {
				renderErr = err
				return match
			}
			return string(encoded)
		})
		return rendered, renderErr
	case map[string]any:
		for key, child := range typed {
			rendered, err := renderValue(child, sources)
			if err != nil {
				return nil, err
			}
			typed[key] = rendered
		}
		return typed, nil
	case []any:
		for index, child := range typed {
			rendered, err := renderValue(child, sources)
			if err != nil {
				return nil, err
			}
			typed[index] = rendered
		}
		return typed, nil
	default:
		return value, nil
	}
}

func applyMappings(config map[string]any, input json.RawMessage, nodes map[string]any) error {
	raw, exists := config["mappings"]
	if !exists {
		return nil
	}
	delete(config, "mappings")
	encoded, err := json.Marshal(raw)
	if err != nil {
		return fmt.Errorf("encode mappings: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	var mappings []valueMapping
	if err := decoder.Decode(&mappings); err != nil {
		return fmt.Errorf("decode mappings: %w", err)
	}
	sources, err := mappingSources(input, nodes)
	if err != nil {
		return err
	}
	for index, mapping := range mappings {
		value, err := lookupMappingValue(sources, mapping.From)
		if err != nil {
			return fmt.Errorf("mapping %d from %q: %w", index, mapping.From, err)
		}
		if err := setJSONPointer(config, mapping.To, value); err != nil {
			return fmt.Errorf("mapping %d to %q: %w", index, mapping.To, err)
		}
	}
	return nil
}

func mappingSources(input json.RawMessage, nodes map[string]any) (map[string]any, error) {
	var decodedInput any
	if len(input) > 0 {
		if err := json.Unmarshal(input, &decodedInput); err != nil {
			return nil, fmt.Errorf("decode mapping input: %w", err)
		}
	}
	return map[string]any{"input": decodedInput, "nodes": nodes}, nil
}

func lookupMappingValue(root any, path string) (any, error) {
	if path == "" {
		return nil, errors.New("source path is empty")
	}
	current := root
	for _, segment := range strings.Split(path, ".") {
		switch value := current.(type) {
		case map[string]any:
			var exists bool
			current, exists = value[segment]
			if !exists {
				return nil, fmt.Errorf("field %q does not exist", segment)
			}
		case []any:
			index, err := strconv.Atoi(segment)
			if err != nil || index < 0 || index >= len(value) {
				return nil, fmt.Errorf("array index %q is invalid", segment)
			}
			current = value[index]
		default:
			return nil, fmt.Errorf("cannot traverse %q", segment)
		}
	}
	return current, nil
}

func setJSONPointer(root map[string]any, pointer string, value any) error {
	if pointer == "" || !strings.HasPrefix(pointer, "/") {
		return errors.New("target must be a non-empty JSON pointer")
	}
	tokens := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	for index := range tokens {
		tokens[index] = strings.ReplaceAll(strings.ReplaceAll(tokens[index], "~1", "/"), "~0", "~")
	}
	_, err := setPointerValue(root, tokens, value)
	return err
}

func setPointerValue(current any, tokens []string, value any) (any, error) {
	if len(tokens) == 0 {
		return value, nil
	}
	token := tokens[0]
	switch typed := current.(type) {
	case map[string]any:
		child, exists := typed[token]
		if !exists {
			if len(tokens) == 1 {
				typed[token] = value
				return typed, nil
			}
			child = map[string]any{}
		}
		updated, err := setPointerValue(child, tokens[1:], value)
		if err != nil {
			return nil, err
		}
		typed[token] = updated
		return typed, nil
	case []any:
		index, err := strconv.Atoi(token)
		if err != nil || index < 0 || index >= len(typed) {
			return nil, fmt.Errorf("array index %q is invalid", token)
		}
		updated, err := setPointerValue(typed[index], tokens[1:], value)
		if err != nil {
			return nil, err
		}
		typed[index] = updated
		return typed, nil
	default:
		return nil, fmt.Errorf("cannot traverse target segment %q", token)
	}
}

func (m *Manager) resolveProfileSecrets(ctx context.Context, references []string) (map[string][]byte, error) {
	result := make(map[string][]byte, len(references))
	for _, binding := range references {
		target, name := "", binding
		if parts := strings.SplitN(binding, "=", 2); len(parts) == 2 {
			target, name = parts[0], parts[1]
		}
		if target == "" {
			target = secretEnvironmentName(name)
		}
		secret, err := m.resolveSecret(ctx, name)
		if err != nil {
			return nil, err
		}
		result[target] = secret
	}
	return result, nil
}

func (m *Manager) resolveSecret(ctx context.Context, name string) ([]byte, error) {
	document, err := m.store.Get(ctx, "SecretRef", resource.DefaultNamespace, name)
	if err != nil {
		return nil, fmt.Errorf("resolve SecretRef %q: %w", name, err)
	}
	reference, err := resource.DecodeSecretRef(document.JSON)
	if err != nil {
		return nil, err
	}
	switch reference.Spec.Backend {
	case "environment", "env":
		value, exists := os.LookupEnv(reference.Spec.Key)
		if !exists {
			return nil, fmt.Errorf("SecretRef %q is not configured", name)
		}
		return []byte(value), nil
	case "file":
		file, err := os.Open(filepath.Clean(reference.Spec.Key)) //nolint:gosec // The operator explicitly configures this SecretRef path.
		if err != nil {
			return nil, fmt.Errorf("SecretRef %q is not configured", name)
		}
		defer func() { _ = file.Close() }()
		data, err := io.ReadAll(io.LimitReader(file, maxSecretSize+1))
		if err != nil || len(data) > maxSecretSize {
			return nil, fmt.Errorf("SecretRef %q could not be read within the size limit", name)
		}
		return data, nil
	default:
		return nil, fmt.Errorf("SecretRef %q uses unsupported backend %q", name, reference.Spec.Backend)
	}
}

func (m *Manager) appendArtifact(artifact runArtifact, data []byte) error {
	directory := filepath.Join(m.artifacts, artifact.runUID, artifact.nodeID, fmt.Sprintf("attempt-%d", artifact.attempt))
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(directory, "raw.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) //nolint:gosec // Run and node identifiers are compiler-validated path components.
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	_, err = file.Write(data)
	return err
}

func (m *Manager) registerActiveCall(ctx context.Context, process *pluginhost.Process, meta *pluginv1alpha1.CallMeta, kind string, cancel context.CancelFunc) *activeProviderCall {
	call := &activeProviderCall{process: process, meta: meta, kind: kind, cancel: cancel}
	m.mu.Lock()
	m.active[meta.GetRequestId()] = call
	m.mu.Unlock()
	call.stop = context.AfterFunc(ctx, func() { m.cancelProviderCall(call, contextCause(ctx)) })
	return call
}

func (m *Manager) releaseActiveCall(requestID string, call *activeProviderCall) {
	if call.stop != nil {
		call.stop()
	}
	m.mu.Lock()
	if m.active[requestID] == call {
		delete(m.active, requestID)
	}
	m.mu.Unlock()
}

func contextCause(ctx context.Context) string {
	if err := context.Cause(ctx); err != nil {
		return err.Error()
	}
	return "cancelled"
}

func (m *Manager) cancelProviderCall(call *activeProviderCall, reason string) {
	call.once.Do(func() {
		cancelContext, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		request := &pluginv1alpha1.CancelRequest{Meta: call.meta, Reason: reason}
		if call.kind == "agent" {
			_, _ = call.process.Clients().Agent.Cancel(cancelContext, request)
		} else {
			_, _ = call.process.Clients().Task.Cancel(cancelContext, request)
		}
		cancel()
		call.cancel()
	})
}

// CancelRun propagates durable Run cancellation to every active task or agent call.
func (m *Manager) CancelRun(ctx context.Context, runUID, reason string) error {
	m.mu.Lock()
	calls := make([]*activeProviderCall, 0)
	for _, call := range m.active {
		if call.meta.GetRunUid() == runUID {
			calls = append(calls, call)
		}
	}
	m.mu.Unlock()
	var wg sync.WaitGroup
	for _, call := range calls {
		wg.Add(1)
		go func(call *activeProviderCall) {
			defer wg.Done()
			m.cancelProviderCall(call, reason)
		}(call)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) evict(key string) {
	m.mu.Lock()
	process := m.processes[key]
	delete(m.processes, key)
	m.mu.Unlock()
	if process != nil {
		process.Close()
	}
}

func sameCapabilities(actual, expected []string) bool {
	actual = append([]string(nil), actual...)
	expected = append([]string(nil), expected...)
	sort.Strings(actual)
	sort.Strings(expected)
	return strings.Join(actual, "\x00") == strings.Join(expected, "\x00")
}

func drainDoctor(stream eventReceiver, redactions [][]byte) error {
	var expected uint64 = 1
	terminal := ""
	for {
		event, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			if terminal == "" {
				return errors.New("doctor stream ended without completion")
			}
			if strings.HasSuffix(terminal, ".failed") {
				return errors.New("doctor command failed")
			}
			return nil
		}
		if err != nil {
			if terminal != "" {
				err = sanitizePluginError(err, redactions)
				if strings.HasSuffix(terminal, ".failed") {
					return fmt.Errorf("doctor command failed: %w", err)
				}
				return fmt.Errorf("doctor stream did not end immediately after completion: %w", err)
			}
			return sanitizePluginError(err, redactions)
		}
		if terminal != "" {
			return errors.New("doctor stream emitted an event after completion")
		}
		if event.GetSequence() != expected || strings.TrimSpace(event.GetType()) == "" || event.GetOccurredAt() == nil || !event.GetOccurredAt().IsValid() || !json.Valid(event.GetPayloadJson()) {
			return errors.New("doctor stream sequence is malformed")
		}
		expected++
		if strings.HasSuffix(event.GetType(), ".failed") || strings.HasSuffix(event.GetType(), ".completed") {
			terminal = event.GetType()
		}
	}
}

func validateProviderBootstrap(record store.PluginRecord, cursor string, activatedAt time.Time, config map[string]any) error {
	replayExisting, _ := config["replayExisting"].(bool)
	if cursor != "" || activatedAt.IsZero() || replayExisting {
		return nil
	}
	var manifest pluginbundle.Manifest
	if err := json.Unmarshal(record.ManifestJSON, &manifest); err != nil {
		return fmt.Errorf("decode plugin manifest: %w", err)
	}
	for _, capability := range manifest.Capabilities {
		if capability == pluginsdk.ActivationFenceCapability {
			return nil
		}
	}
	return fmt.Errorf("plugin %q does not declare %s; refusing a non-replay provider bootstrap", record.Name, pluginsdk.ActivationFenceCapability)
}

func sanitizePluginError(err error, secrets [][]byte) error {
	if err == nil {
		return nil
	}
	if pluginStatus, ok := status.FromError(err); ok {
		message := redact([]byte(pluginStatus.Message()), secrets)
		return status.Error(pluginStatus.Code(), string(message))
	}
	return errors.New(string(redact([]byte(err.Error()), secrets)))
}

func secretValues(secrets map[string][]byte) [][]byte {
	result := make([][]byte, 0, len(secrets))
	for _, value := range secrets {
		if len(value) > 0 {
			result = append(result, append([]byte(nil), value...))
		}
	}
	return result
}

func redact(data []byte, secrets [][]byte) []byte {
	result := append([]byte(nil), data...)
	for _, secret := range secrets {
		encoded, _ := json.Marshal(string(secret))
		if len(encoded) >= 2 {
			result = []byte(strings.ReplaceAll(string(result), string(encoded[1:len(encoded)-1]), "[REDACTED]"))
		}
		result = []byte(strings.ReplaceAll(string(result), string(secret), "[REDACTED]"))
	}
	return result
}

func secretEnvironmentName(name string) string {
	var builder strings.Builder
	for _, character := range name {
		if unicode.IsLetter(character) || unicode.IsDigit(character) {
			builder.WriteRune(unicode.ToUpper(character))
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func installationKey(name, version string) string { return name + "@" + version }

// StatusCode classifies plugin failures for control-plane diagnostics.
func StatusCode(err error) codes.Code { return status.Code(err) }

package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Masterminds/semver/v3"
	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	hplugin "github.com/hashicorp/go-plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	pluginName     = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	capabilityName = regexp.MustCompile(`^[a-z][a-z0-9-]*(\.[a-z][a-z0-9-]*)+$`)
	// ErrEventSinkClosed is returned when a handler emits after completion.
	ErrEventSinkClosed = errors.New("plugin event sink is closed")
)

// Metadata is the stable public identity and contract of a plugin.
type Metadata struct {
	Name         string
	Version      string
	Capabilities []string
	Actions      []ActionDescriptor
	Triggers     []TriggerDescriptor
	// InputSchema and OutputSchema are retained only for protocol-v1 wire
	// compatibility. New plugins must publish action-specific descriptors.
	InputSchema  json.RawMessage
	OutputSchema json.RawMessage
}

// ActionDescriptor declares the complete data contract for one Flow action.
// Schemas use JSON Schema draft 2020-12 and are immutable plugin metadata.
type ActionDescriptor struct {
	Action       string          `json:"action"`
	ConfigSchema json.RawMessage `json:"configSchema"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
}

// TriggerDescriptor declares configuration and emitted event data for one
// TriggerProvider source capability.
type TriggerDescriptor struct {
	Source       string          `json:"source"`
	ConfigSchema json.RawMessage `json:"configSchema"`
	EventSchema  json.RawMessage `json:"eventSchema"`
}

// Contract is the canonical immutable schema projection persisted by a host.
type Contract struct {
	Actions  []ActionDescriptor  `json:"actions"`
	Triggers []TriggerDescriptor `json:"triggers,omitempty"`
}

// Config describes the services exposed by one plugin binary.
type Config struct {
	Metadata Metadata
	Task     TaskHandler
	Trigger  pluginv1alpha1.TriggerProviderServer
	Agent    pluginv1alpha1.AgentRuntimeServer
}

// ValidationIssue is an author-facing configuration diagnostic.
type ValidationIssue struct {
	Path    string
	Code    string
	Message string
}

// TaskRequest is independent of gRPC and workflow-engine implementation types.
type TaskRequest struct {
	Action         string
	Input          json.RawMessage
	Config         json.RawMessage
	Secrets        map[string][]byte
	RequestID      string
	RunUID         string
	NodeID         string
	Attempt        uint32
	IdempotencyKey string
	Deadline       time.Time
}

// EventSink accepts non-terminal structured events and raw logs. The SDK owns
// sequence numbers, timestamps, and the single terminal task event.
type EventSink interface {
	Emit(eventType string, payload any) error
	Log(eventType string, raw []byte) error
}

// TaskHandler implements validation and execution for declared task actions.
type TaskHandler interface {
	ValidateAction(context.Context, string, json.RawMessage) []ValidationIssue
	Execute(context.Context, TaskRequest, EventSink) (any, error)
}

// TaskHandlerFuncs adapts functions into a TaskHandler.
type TaskHandlerFuncs struct {
	Validate func(context.Context, string, json.RawMessage) []ValidationIssue
	Run      func(context.Context, TaskRequest, EventSink) (any, error)
}

// ValidateAction delegates action validation to Validate when configured.
func (f TaskHandlerFuncs) ValidateAction(ctx context.Context, action string, config json.RawMessage) []ValidationIssue {
	if f.Validate == nil {
		return nil
	}
	return f.Validate(ctx, action, config)
}

// Execute delegates task execution to Run when configured.
func (f TaskHandlerFuncs) Execute(ctx context.Context, request TaskRequest, sink EventSink) (any, error) {
	if f.Run == nil {
		return nil, errors.New("task execute function is not configured")
	}
	return f.Run(ctx, request, sink)
}

// Runtime owns control, task lifecycle, cancellation, health, and shutdown.
type Runtime struct {
	pluginv1alpha1.UnimplementedPluginControlServer
	pluginv1alpha1.UnimplementedTaskProviderServer

	metadata  Metadata
	handler   TaskHandler
	actions   map[string]struct{}
	contracts map[string]compiledActionContract

	mu         sync.Mutex
	accepting  bool
	active     map[string]activeCall
	wg         sync.WaitGroup
	nextStream uint64
}

type activeCall struct {
	meta   callIdentity
	cancel context.CancelFunc
}

type callIdentity struct {
	requestID, runUID, nodeID, idempotencyKey string
	attempt                                   uint32
}

// New validates a public plugin configuration and constructs its service set.
func New(config Config) (*Runtime, Servers, error) {
	metadata, actions, contracts, err := validateMetadata(config.Metadata, config.Task != nil, config.Trigger != nil, config.Agent != nil)
	if err != nil {
		return nil, Servers{}, err
	}
	for _, capability := range metadata.Capabilities {
		switch {
		case strings.HasPrefix(capability, "task.") && config.Task == nil:
			return nil, Servers{}, fmt.Errorf("capability %q requires a task handler", capability)
		case strings.HasPrefix(capability, "trigger.") && config.Trigger == nil:
			return nil, Servers{}, fmt.Errorf("capability %q requires a trigger server", capability)
		case strings.HasPrefix(capability, "agent.") && config.Agent == nil:
			return nil, Servers{}, fmt.Errorf("capability %q requires an agent server", capability)
		}
	}
	runtime := &Runtime{metadata: metadata, handler: config.Task, actions: actions, contracts: contracts, accepting: true, active: map[string]activeCall{}}
	servers := Servers{Control: runtime}
	if config.Trigger != nil {
		servers.Trigger = &triggerAdapter{runtime: runtime, server: config.Trigger}
	}
	if config.Agent != nil {
		servers.Agent = &agentAdapter{runtime: runtime, server: config.Agent}
	}
	if config.Task != nil {
		servers.Task = runtime
	}
	return runtime, servers, nil
}

// Serve validates config and starts an isolated plugin process. It never
// returns after successful startup.
func Serve(config Config) {
	_, servers, err := New(config)
	if err != nil {
		panic(err)
	}
	hplugin.Serve(&hplugin.ServeConfig{
		HandshakeConfig: Handshake,
		Plugins:         Set(servers),
		GRPCServer:      hplugin.DefaultGRPCServer,
	})
}

// Describe negotiates protocol v1 and returns validated immutable metadata.
func (r *Runtime) Describe(_ context.Context, request *pluginv1alpha1.DescribeRequest) (*pluginv1alpha1.DescribeResponse, error) {
	host := request.GetHostProtocol()
	if host == nil || host.GetMinimum() == 0 || host.GetMaximum() < host.GetMinimum() {
		return nil, status.Error(codes.InvalidArgument, "valid host protocol range is required")
	}
	if host.GetMaximum() < ProtocolVersion || host.GetMinimum() > ProtocolVersion {
		return nil, status.Error(codes.FailedPrecondition, "plugin protocol ranges do not overlap")
	}
	return &pluginv1alpha1.DescribeResponse{
		Name: r.metadata.Name, Version: r.metadata.Version,
		Protocol:        &pluginv1alpha1.ProtocolRange{Minimum: ProtocolVersion, Maximum: ProtocolVersion},
		Capabilities:    append([]string(nil), r.metadata.Capabilities...),
		InputSchemaJson: append([]byte(nil), r.metadata.InputSchema...), OutputSchemaJson: append([]byte(nil), r.metadata.OutputSchema...),
		Actions: actionDescriptorsPB(r.metadata.Actions), Triggers: triggerDescriptorsPB(r.metadata.Triggers),
	}, nil
}

// Health is ready only while the runtime accepts new work.
func (r *Runtime) Health(context.Context, *emptypb.Empty) (*pluginv1alpha1.HealthResponse, error) {
	r.mu.Lock()
	accepting := r.accepting
	r.mu.Unlock()
	if accepting {
		return &pluginv1alpha1.HealthResponse{Ready: true, Message: "ready"}, nil
	}
	return &pluginv1alpha1.HealthResponse{Ready: false, Message: "draining"}, nil
}

// Shutdown atomically drains the runtime, cancels active handlers, and waits
// no longer than the supplied deadline.
func (r *Runtime) Shutdown(ctx context.Context, request *pluginv1alpha1.ShutdownRequest) (*emptypb.Empty, error) {
	if request == nil || request.GetDeadline() == nil || !request.GetDeadline().IsValid() {
		return nil, status.Error(codes.InvalidArgument, "valid shutdown deadline is required")
	}
	deadline := request.GetDeadline().AsTime()
	r.mu.Lock()
	r.accepting = false
	cancels := make([]context.CancelFunc, 0, len(r.active))
	for _, call := range r.active {
		cancels = append(cancels, call.cancel)
	}
	r.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	wait := time.Until(deadline)
	if wait <= 0 {
		return &emptypb.Empty{}, nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	case <-ctx.Done():
	}
	return &emptypb.Empty{}, nil
}

// ValidateAction rejects undeclared actions and delegates configuration checks.
func (r *Runtime) ValidateAction(ctx context.Context, request *pluginv1alpha1.ValidateActionRequest) (*pluginv1alpha1.ValidateActionResponse, error) {
	if err := r.canAccept(); err != nil {
		return nil, err
	}
	if _, exists := r.actions[request.GetAction()]; !exists {
		return nil, status.Errorf(codes.InvalidArgument, "task action %q is not declared", request.GetAction())
	}
	config := normalizedJSON(request.GetConfigJson())
	if !json.Valid(config) {
		return nil, status.Error(codes.InvalidArgument, "config_json must be valid JSON")
	}
	if issue := r.contracts[request.GetAction()].validateConfig(config); issue != nil {
		return &pluginv1alpha1.ValidateActionResponse{Issues: []*pluginv1alpha1.ValidationIssue{issue}}, nil
	}
	issues := r.handler.ValidateAction(ctx, request.GetAction(), config)
	response := &pluginv1alpha1.ValidateActionResponse{Issues: make([]*pluginv1alpha1.ValidationIssue, 0, len(issues))}
	for _, issue := range issues {
		if strings.TrimSpace(issue.Code) == "" || strings.TrimSpace(issue.Message) == "" {
			return nil, status.Error(codes.Internal, "task handler returned an invalid validation issue")
		}
		response.Issues = append(response.Issues, &pluginv1alpha1.ValidationIssue{Path: issue.Path, Code: issue.Code, Message: issue.Message})
	}
	return response, nil
}

// Execute delegates to the high-level handler and emits exactly one terminal event.
func (r *Runtime) Execute(request *pluginv1alpha1.ExecuteRequest, stream pluginv1alpha1.TaskProvider_ExecuteServer) error {
	meta, err := validateCallMeta(request.GetMeta(), true)
	if err != nil {
		return err
	}
	if _, exists := r.actions[request.GetAction()]; !exists {
		return status.Errorf(codes.InvalidArgument, "task action %q is not declared", request.GetAction())
	}
	input, config := normalizedJSON(request.GetInputJson()), normalizedJSON(request.GetConfigJson())
	if !json.Valid(input) || !json.Valid(config) {
		return status.Error(codes.InvalidArgument, "input_json and config_json must be valid JSON")
	}
	contract := r.contracts[request.GetAction()]
	if issue := contract.validateConfig(config); issue != nil {
		return status.Error(codes.InvalidArgument, validationIssueMessage(issue))
	}
	if issue := contract.validateInput(input); issue != nil {
		return status.Error(codes.InvalidArgument, validationIssueMessage(issue))
	}
	ctx, cancel := context.WithDeadline(stream.Context(), request.GetMeta().GetDeadline().AsTime())
	if err := r.register(meta, cancel); err != nil {
		cancel()
		return err
	}
	defer func() { cancel(); r.unregister(meta.requestID) }()
	sink := &eventSink{stream: stream}
	result, handlerErr := r.handler.Execute(ctx, TaskRequest{
		Action: request.GetAction(), Input: append(json.RawMessage(nil), input...), Config: append(json.RawMessage(nil), config...),
		Secrets: cloneSecrets(request.GetSecrets()), RequestID: meta.requestID, RunUID: meta.runUID, NodeID: meta.nodeID,
		Attempt: meta.attempt, IdempotencyKey: meta.idempotencyKey, Deadline: request.GetMeta().GetDeadline().AsTime(),
	}, sink)
	if handlerErr != nil {
		return sink.finish("task.failed", map[string]any{"error": handlerErr.Error()})
	}
	if issue := contract.validateOutputValue(result); issue != nil {
		return sink.finish("task.failed", map[string]any{"error": validationIssueMessage(issue)})
	}
	return sink.finish("task.completed", result)
}

func validationIssueMessage(issue *pluginv1alpha1.ValidationIssue) string {
	return fmt.Sprintf("%s (%s): %s", issue.GetPath(), issue.GetCode(), issue.GetMessage())
}

// Cancel cancels the matching active handler context.
func (r *Runtime) Cancel(_ context.Context, request *pluginv1alpha1.CancelRequest) (*pluginv1alpha1.CancelResponse, error) {
	meta, err := validateCallMeta(request.GetMeta(), false)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	call, exists := r.active[meta.requestID]
	r.mu.Unlock()
	if !exists || call.meta != meta {
		return &pluginv1alpha1.CancelResponse{Accepted: false, Outcome: "not-running"}, nil
	}
	call.cancel()
	return &pluginv1alpha1.CancelResponse{Accepted: true, Outcome: "context-cancel"}, nil
}

func (r *Runtime) register(meta callIdentity, cancel context.CancelFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return status.Error(codes.Unavailable, "plugin is draining")
	}
	if _, exists := r.active[meta.requestID]; exists {
		return status.Error(codes.AlreadyExists, "request is already active")
	}
	r.active[meta.requestID] = activeCall{meta: meta, cancel: cancel}
	r.wg.Add(1)
	return nil
}

func (r *Runtime) unregister(requestID string) {
	r.mu.Lock()
	delete(r.active, requestID)
	r.mu.Unlock()
	r.wg.Done()
}

func (r *Runtime) registerStream(cancel context.CancelFunc) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return "", status.Error(codes.Unavailable, "plugin is draining")
	}
	r.nextStream++
	requestID := fmt.Sprintf("trigger-watch-%d", r.nextStream)
	r.active[requestID] = activeCall{cancel: cancel}
	r.wg.Add(1)
	return requestID, nil
}

func (r *Runtime) canAccept() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.accepting {
		return status.Error(codes.Unavailable, "plugin is draining")
	}
	return nil
}

type eventStream interface {
	Send(*pluginv1alpha1.ExecuteEvent) error
}

type eventSink struct {
	mu       sync.Mutex
	stream   eventStream
	sequence uint64
	closed   bool
}

func (s *eventSink) Emit(eventType string, payload any) error {
	return s.send(eventType, payload, nil, false)
}
func (s *eventSink) Log(eventType string, raw []byte) error {
	return s.send(eventType, nil, raw, false)
}

func (s *eventSink) finish(eventType string, payload any) error {
	return s.send(eventType, payload, nil, true)
}

func (s *eventSink) send(eventType string, payload any, raw []byte, terminal bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrEventSinkClosed
	}
	if strings.TrimSpace(eventType) == "" {
		return errors.New("plugin event type is required")
	}
	if !terminal && (strings.HasSuffix(eventType, ".completed") || strings.HasSuffix(eventType, ".failed")) {
		return errors.New("terminal events are owned by the plugin SDK")
	}
	var data []byte
	var err error
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("encode plugin event payload: %w", err)
		}
	}
	if len(data) == 0 {
		data = []byte(`{}`)
	}
	s.sequence++
	if err := s.stream.Send(&pluginv1alpha1.ExecuteEvent{Sequence: s.sequence, Type: eventType, PayloadJson: data, RawLog: append([]byte(nil), raw...), OccurredAt: timestamppb.Now()}); err != nil {
		return err
	}
	if terminal {
		s.closed = true
	}
	return nil
}

func validateMetadata(metadata Metadata, hasTask, hasTrigger, hasAgent bool) (Metadata, map[string]struct{}, map[string]compiledActionContract, error) {
	if !pluginName.MatchString(metadata.Name) {
		return Metadata{}, nil, nil, fmt.Errorf("invalid plugin name %q", metadata.Name)
	}
	if _, err := semver.StrictNewVersion(metadata.Version); err != nil {
		return Metadata{}, nil, nil, fmt.Errorf("plugin version must be semantic: %w", err)
	}
	if len(metadata.Capabilities) == 0 {
		return Metadata{}, nil, nil, errors.New("plugin capabilities must not be empty")
	}
	seen, actions := map[string]struct{}{}, map[string]struct{}{}
	for _, capability := range metadata.Capabilities {
		if !capabilityName.MatchString(capability) {
			return Metadata{}, nil, nil, fmt.Errorf("invalid plugin capability %q", capability)
		}
		if _, duplicate := seen[capability]; duplicate {
			return Metadata{}, nil, nil, fmt.Errorf("duplicate plugin capability %q", capability)
		}
		seen[capability] = struct{}{}
		namespace := strings.SplitN(capability, ".", 2)[0]
		if namespace != "task" && namespace != "trigger" && namespace != "agent" {
			return Metadata{}, nil, nil, fmt.Errorf("unsupported plugin capability namespace %q", namespace)
		}
		if strings.HasPrefix(capability, "task.") {
			prefix := "task." + metadata.Name + "."
			if !strings.HasPrefix(capability, prefix) {
				return Metadata{}, nil, nil, fmt.Errorf("task capability %q must be rooted at plugin name %q", capability, metadata.Name)
			}
			actions[strings.TrimPrefix(capability, "task.")] = struct{}{}
		}
	}
	if hasTask && len(actions) == 0 {
		return Metadata{}, nil, nil, errors.New("task handler requires at least one task.<action> capability")
	}
	for label, schema := range map[string]json.RawMessage{"input": metadata.InputSchema, "output": metadata.OutputSchema} {
		if len(schema) > 0 && !json.Valid(schema) {
			return Metadata{}, nil, nil, fmt.Errorf("%s schema must be valid JSON", label)
		}
	}
	validatedActions, contracts, err := validateActionDescriptors(metadata, actions, hasAgent)
	if err != nil {
		return Metadata{}, nil, nil, err
	}
	validatedTriggers, err := validateTriggerDescriptors(metadata, hasTrigger)
	if err != nil {
		return Metadata{}, nil, nil, err
	}
	metadata.Capabilities = append([]string(nil), metadata.Capabilities...)
	sort.Strings(metadata.Capabilities)
	metadata.InputSchema = append(json.RawMessage(nil), metadata.InputSchema...)
	metadata.OutputSchema = append(json.RawMessage(nil), metadata.OutputSchema...)
	metadata.Actions = validatedActions
	metadata.Triggers = validatedTriggers
	return metadata, actions, contracts, nil
}

type triggerAdapter struct {
	pluginv1alpha1.UnimplementedTriggerProviderServer
	runtime *Runtime
	server  pluginv1alpha1.TriggerProviderServer
}

func (a *triggerAdapter) Watch(stream pluginv1alpha1.TriggerProvider_WatchServer) error {
	ctx, cancel := context.WithCancel(stream.Context())
	requestID, err := a.runtime.registerStream(cancel)
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		cancel()
		a.runtime.unregister(requestID)
	}()
	return a.server.Watch(&triggerWatchStream{TriggerProvider_WatchServer: stream, ctx: ctx})
}

type triggerWatchStream struct {
	pluginv1alpha1.TriggerProvider_WatchServer
	ctx context.Context
}

func (s *triggerWatchStream) Context() context.Context { return s.ctx }

func validateCallMeta(meta *pluginv1alpha1.CallMeta, requireFuture bool) (callIdentity, error) {
	if meta == nil || strings.TrimSpace(meta.GetRequestId()) == "" || strings.TrimSpace(meta.GetRunUid()) == "" || strings.TrimSpace(meta.GetNodeId()) == "" || meta.GetAttempt() == 0 || strings.TrimSpace(meta.GetIdempotencyKey()) == "" {
		return callIdentity{}, status.Error(codes.InvalidArgument, "complete call metadata is required")
	}
	if meta.GetDeadline() == nil || !meta.GetDeadline().IsValid() || (requireFuture && !meta.GetDeadline().AsTime().After(time.Now())) {
		return callIdentity{}, status.Error(codes.InvalidArgument, "a valid call deadline is required")
	}
	return callIdentity{requestID: meta.GetRequestId(), runUID: meta.GetRunUid(), nodeID: meta.GetNodeId(), attempt: meta.GetAttempt(), idempotencyKey: meta.GetIdempotencyKey()}, nil
}

func normalizedJSON(value []byte) []byte {
	if len(value) == 0 {
		return []byte(`{}`)
	}
	return value
}

func cloneSecrets(values map[string][]byte) map[string][]byte {
	result := make(map[string][]byte, len(values))
	for key, value := range values {
		result[key] = append([]byte(nil), value...)
	}
	return result
}

type agentAdapter struct {
	pluginv1alpha1.UnimplementedAgentRuntimeServer
	runtime *Runtime
	server  pluginv1alpha1.AgentRuntimeServer
}

func (a *agentAdapter) Execute(request *pluginv1alpha1.AgentRequest, stream pluginv1alpha1.AgentRuntime_ExecuteServer) error {
	meta, err := validateCallMeta(request.GetMeta(), true)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithDeadline(stream.Context(), request.GetMeta().GetDeadline().AsTime())
	if err := a.runtime.register(meta, cancel); err != nil {
		cancel()
		return err
	}
	defer func() { cancel(); a.runtime.unregister(meta.requestID) }()
	return a.server.Execute(request, &agentExecuteStream{AgentRuntime_ExecuteServer: stream, ctx: ctx})
}

func (a *agentAdapter) Input(ctx context.Context, request *pluginv1alpha1.AgentInput) (*emptypb.Empty, error) {
	return a.server.Input(ctx, request)
}

func (a *agentAdapter) Cancel(ctx context.Context, request *pluginv1alpha1.CancelRequest) (*pluginv1alpha1.CancelResponse, error) {
	meta, err := validateCallMeta(request.GetMeta(), false)
	if err != nil {
		return nil, err
	}
	a.runtime.mu.Lock()
	call, exists := a.runtime.active[meta.requestID]
	a.runtime.mu.Unlock()
	if !exists || call.meta != meta {
		return &pluginv1alpha1.CancelResponse{Accepted: false, Outcome: "not-running"}, nil
	}
	response, delegateErr := a.server.Cancel(ctx, request)
	call.cancel()
	return response, delegateErr
}

type agentExecuteStream struct {
	pluginv1alpha1.AgentRuntime_ExecuteServer
	ctx context.Context
}

func (s *agentExecuteStream) Context() context.Context { return s.ctx }

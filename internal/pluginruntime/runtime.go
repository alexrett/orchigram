// Package pluginruntime contains reusable first-party plugin service implementations.
package pluginruntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/resource"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxHTTPResponse = 1 << 20

type executeStream interface {
	Send(*pluginv1alpha1.ExecuteEvent) error
}

type eventWriter struct {
	mu       sync.Mutex
	sequence uint64
	stream   executeStream
}

func (w *eventWriter) send(eventType string, payload any, raw []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sequence++
	var data []byte
	var err error
	if payload != nil {
		data, err = json.Marshal(payload)
		if err != nil {
			return err
		}
	}
	return w.stream.Send(&pluginv1alpha1.ExecuteEvent{
		Sequence: w.sequence, Type: eventType, PayloadJson: data,
		RawLog: append([]byte(nil), raw...), OccurredAt: timestamppb.Now(),
	})
}

// Exec implements deterministic argv task execution.
type Exec struct {
	Runner *process.Runner
}

type execConfig struct {
	Argv        []string          `json:"argv"`
	Directory   string            `json:"directory,omitempty"`
	Environment map[string]string `json:"environment,omitempty"`
	Stdin       string            `json:"stdin,omitempty"`
}

// ValidateAction validates exec.run without invoking it.
func (*Exec) ValidateAction(_ context.Context, action string, configJSON json.RawMessage) []pluginsdk.ValidationIssue {
	issues := []pluginsdk.ValidationIssue{}
	if action != "exec.run" {
		issues = append(issues, sdkIssue("action", "unsupported", "expected exec.run"))
	}
	var config execConfig
	if err := decodeStrictJSON(configJSON, &config); err != nil {
		issues = append(issues, sdkIssue("config", "invalid", err.Error()))
	} else if len(config.Argv) == 0 || config.Argv[0] == "" {
		issues = append(issues, sdkIssue("config.argv", "required", "argv must contain an executable"))
	}
	return issues
}

// Execute streams non-terminal raw logs and returns the completion payload.
func (e *Exec) Execute(ctx context.Context, request pluginsdk.TaskRequest, sink pluginsdk.EventSink) (any, error) {
	if e.Runner == nil {
		e.Runner = process.NewRunner()
	}
	if request.Action != "exec.run" {
		return nil, errors.New("expected action exec.run")
	}
	var config execConfig
	if err := decodeStrictJSON(request.Config, &config); err != nil {
		return nil, err
	}
	if len(config.Argv) == 0 || config.Argv[0] == "" {
		return nil, errors.New("config.argv must contain an executable")
	}
	if err := sink.Emit("task.started", map[string]any{"executable": filepath.Base(config.Argv[0])}); err != nil {
		return nil, err
	}
	environment := process.MinimalEnvironment(baseEnvironment(), config.Environment)
	result, err := e.Runner.Run(ctx, request.RequestID, process.Spec{
		Executable: config.Argv[0], Args: config.Argv[1:], Directory: config.Directory,
		Environment: environment, Stdin: []byte(config.Stdin),
	}, func(output process.Output) error {
		return sink.Log("task.log."+output.Stream, output.Data)
	})
	if err != nil {
		if emitErr := sink.Emit("task.process", map[string]any{"exitCode": result.ExitCode, "outcome": result.Outcome}); emitErr != nil {
			return nil, emitErr
		}
		return nil, fmt.Errorf("%w (outcome=%s, exit=%d)", err, result.Outcome, result.ExitCode)
	}
	return map[string]any{
		"exitCode": result.ExitCode, "outcome": result.Outcome,
		"stdout": string(result.Stdout), "stderr": string(result.Stderr),
	}, nil
}

// Agent executes Codex, Claude, or a static custom argv profile.
type Agent struct {
	pluginv1alpha1.UnimplementedAgentRuntimeServer
	Runner *process.Runner
}

// Execute constructs argv directly, normalizes JSONL, and preserves raw output.
func (a *Agent) Execute(request *pluginv1alpha1.AgentRequest, stream pluginv1alpha1.AgentRuntime_ExecuteServer) error {
	if a.Runner == nil {
		a.Runner = process.NewRunner()
	}
	if err := validateMeta(request.GetMeta()); err != nil {
		return err
	}
	var profile resource.AgentProfileSpec
	if err := decodeStrictJSON(request.GetProfileJson(), &profile); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	var invocation struct {
		Doctor bool `json:"doctor"`
	}
	_ = json.Unmarshal(request.GetInputJson(), &invocation)
	if invocation.Doctor && profile.Type == "command" {
		executable := profile.Executable
		if executable == "" {
			return status.Error(codes.InvalidArgument, "command profile requires executable")
		}
		if _, err := exec.LookPath(executable); err != nil {
			return status.Error(codes.FailedPrecondition, "configured command is not discoverable")
		}
		writer := &eventWriter{stream: stream}
		return writer.send("agent.completed", map[string]any{"doctor": "executable-found"}, nil)
	}
	executable, args, directory, err := agentArgv(profile, request.GetInputJson())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if invocation.Doctor {
		switch profile.Type {
		case "codex":
			args = []string{"login", "status"}
		case "claude":
			args = []string{"auth", "status"}
		}
	}
	injected := make(map[string]string, len(profile.Environment)+len(request.GetSecrets()))
	for key, value := range profile.Environment {
		injected[key] = value
	}
	for key, value := range request.GetSecrets() {
		injected[key] = string(value)
	}
	writer := &eventWriter{stream: stream}
	if err := writer.send("agent.started", map[string]string{"profileType": profile.Type, "executable": filepath.Base(executable)}, nil); err != nil {
		return err
	}
	stdoutLines, stderrLines := &lineAssembler{}, &lineAssembler{}
	result, runErr := a.Runner.Run(stream.Context(), request.GetMeta().GetRequestId(), process.Spec{
		Executable: executable, Args: args, Directory: directory,
		Environment: process.MinimalEnvironment(baseEnvironment(), injected),
	}, func(output process.Output) error {
		if err := writer.send("agent.raw", map[string]string{"stream": output.Stream}, output.Data); err != nil {
			return err
		}
		assembler := stdoutLines
		if output.Stream == "stderr" {
			assembler = stderrLines
		}
		for _, line := range assembler.add(output.Data) {
			eventType, payload := normalizeAgentLine(line)
			if err := writer.send(eventType, payload, nil); err != nil {
				return err
			}
		}
		return nil
	})
	for _, assembler := range []*lineAssembler{stdoutLines, stderrLines} {
		if line := assembler.flush(); len(line) > 0 {
			eventType, payload := normalizeAgentLine(line)
			if err := writer.send(eventType, payload, nil); err != nil {
				return err
			}
		}
	}
	if runErr != nil {
		_ = writer.send("agent.failed", map[string]any{"exitCode": result.ExitCode, "outcome": result.Outcome}, nil)
		return status.Errorf(codes.Internal, "%v (outcome=%s, exit=%d)", runErr, result.Outcome, result.ExitCode)
	}
	completion := map[string]any{
		"exitCode": result.ExitCode, "outcome": result.Outcome, "stdout": string(result.Stdout),
	}
	if text := finalAgentText(result.Stdout); text != "" {
		completion["text"] = text
	}
	return writer.send("agent.completed", completion, nil)
}

// Input is reserved for interactive runtimes; v0.1 command profiles are non-interactive.
func (*Agent) Input(context.Context, *pluginv1alpha1.AgentInput) (*emptypb.Empty, error) {
	return nil, status.Error(codes.Unimplemented, "command profiles do not accept interactive input")
}

// Cancel terminates the active agent and all descendants.
func (a *Agent) Cancel(_ context.Context, request *pluginv1alpha1.CancelRequest) (*pluginv1alpha1.CancelResponse, error) {
	if a.Runner == nil {
		return &pluginv1alpha1.CancelResponse{Accepted: false, Outcome: "not-running"}, nil
	}
	outcome, accepted := a.Runner.Cancel(request.GetMeta().GetRequestId())
	return &pluginv1alpha1.CancelResponse{Accepted: accepted, Outcome: outcome}, nil
}

// HTTP implements generic idempotent outbound requests.
type HTTP struct {
	Client *http.Client
	Action string
}

type httpConfig struct {
	Method        string            `json:"method,omitempty"`
	URL           string            `json:"url,omitempty"`
	URLSecret     string            `json:"urlSecret,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	SecretHeaders map[string]string `json:"secretHeaders,omitempty"`
	Body          json.RawMessage   `json:"body,omitempty"`
}

// ValidateAction validates an http.request definition.
func (h *HTTP) ValidateAction(_ context.Context, action string, configJSON json.RawMessage) []pluginsdk.ValidationIssue {
	issues := []pluginsdk.ValidationIssue{}
	expected := h.expectedAction()
	if action != expected {
		issues = append(issues, sdkIssue("action", "unsupported", "expected "+expected))
	}
	var config httpConfig
	if err := decodeStrictJSON(configJSON, &config); err != nil {
		issues = append(issues, sdkIssue("config", "invalid", err.Error()))
	} else {
		switch {
		case config.URL == "" && config.URLSecret == "":
			issues = append(issues, sdkIssue("config.url", "required", "url or urlSecret is required"))
		case config.URL != "" && config.URLSecret != "":
			issues = append(issues, sdkIssue("config.urlSecret", "conflict", "url and urlSecret are mutually exclusive"))
		case config.URL != "" && !strings.HasPrefix(config.URL, "https://") && !strings.HasPrefix(config.URL, "http://"):
			issues = append(issues, sdkIssue("config.url", "invalid", "URL must use http or https"))
		}
	}
	return issues
}

// Execute sends a request with the stable activity idempotency key.
func (h *HTTP) Execute(ctx context.Context, request pluginsdk.TaskRequest, sink pluginsdk.EventSink) (any, error) {
	if request.Action != h.expectedAction() {
		return nil, fmt.Errorf("expected action %s", h.expectedAction())
	}
	var config httpConfig
	if err := decodeStrictJSON(request.Config, &config); err != nil {
		return nil, err
	}
	requestURL := config.URL
	if config.URLSecret != "" {
		secret, exists := request.Secrets[config.URLSecret]
		if !exists {
			return nil, fmt.Errorf("URL secret %q is missing", config.URLSecret)
		}
		requestURL = string(secret)
	}
	if !strings.HasPrefix(requestURL, "https://") && !strings.HasPrefix(requestURL, "http://") {
		return nil, errors.New("resolved URL must use http or https")
	}
	method := strings.ToUpper(config.Method)
	if method == "" {
		method = http.MethodPost
	}
	body := config.Body
	if len(body) == 0 {
		body = request.Input
	}
	httpRequest, err := http.NewRequestWithContext(ctx, method, requestURL, strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	for key, value := range config.Headers {
		httpRequest.Header.Set(key, value)
	}
	for header, secretName := range config.SecretHeaders {
		secret, exists := request.Secrets[secretName]
		if !exists {
			return nil, fmt.Errorf("secret %q is missing", secretName)
		}
		httpRequest.Header.Set(header, string(secret))
	}
	httpRequest.Header.Set("Idempotency-Key", request.IdempotencyKey)
	if httpRequest.Header.Get("Content-Type") == "" {
		httpRequest.Header.Set("Content-Type", "application/json")
	}
	if err := sink.Emit("http.started", map[string]string{"method": method}); err != nil {
		return nil, err
	}
	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(httpRequest)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := readLimited(response.Body, maxHTTPResponse)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{"status": response.StatusCode, "body": string(responseBody)}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP status %d", response.StatusCode)
	}
	return payload, nil
}

func (h *HTTP) expectedAction() string {
	if h.Action == "" {
		return "http.request"
	}
	return h.Action
}

func validateMeta(meta *pluginv1alpha1.CallMeta) error {
	if meta == nil || meta.GetRequestId() == "" || meta.GetRunUid() == "" || meta.GetNodeId() == "" || meta.GetIdempotencyKey() == "" {
		return status.Error(codes.InvalidArgument, "complete call metadata is required")
	}
	return nil
}

func sdkIssue(path, code, message string) pluginsdk.ValidationIssue {
	return pluginsdk.ValidationIssue{Path: path, Code: code, Message: message}
}

func decodeStrictJSON(data []byte, target any) error {
	if len(data) == 0 {
		data = []byte(`{}`)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}
	return nil
}

func baseEnvironment() map[string]string {
	result := map[string]string{"PATH": "/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin", "LANG": "C.UTF-8"}
	for _, key := range []string{"HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if value := os.Getenv(key); value != "" {
			result[key] = value
		}
	}
	return result
}

func agentArgv(profile resource.AgentProfileSpec, input json.RawMessage) (string, []string, string, error) {
	var envelope struct {
		Prompt    string `json:"prompt"`
		Workspace string `json:"workspace"`
	}
	_ = json.Unmarshal(input, &envelope)
	prompt := envelope.Prompt
	if prompt == "" {
		prompt = string(input)
	}
	executable := profile.Executable
	args := append([]string(nil), profile.Args...)
	switch profile.Type {
	case "codex":
		if executable == "" {
			executable = "codex"
		}
		if len(args) == 0 {
			args = []string{"exec", "--json"}
		}
		if profile.Model != "" {
			args = append(args, "--model", profile.Model)
		}
		if profile.Profile != "" {
			args = append(args, "--profile", profile.Profile)
		}
		if profile.Sandbox != "" {
			args = append(args, "--sandbox", profile.Sandbox)
		}
		if profile.Effort != "" {
			args = append(args, "--config", `model_reasoning_effort="`+profile.Effort+`"`)
		}
	case "claude":
		if executable == "" {
			executable = "claude"
		}
		if len(args) == 0 {
			args = []string{"--print", "--verbose", "--output-format", "stream-json"}
		}
		if profile.Model != "" {
			args = append(args, "--model", profile.Model)
		}
		if profile.Effort != "" {
			args = append(args, "--effort", profile.Effort)
		}
		switch profile.Sandbox {
		case "read-only":
			args = append(args, "--permission-mode", "plan")
		case "workspace-write":
			args = append(args, "--permission-mode", "acceptEdits")
		}
	case "command":
		if executable == "" {
			return "", nil, "", errors.New("command profile requires executable")
		}
	default:
		return "", nil, "", fmt.Errorf("unsupported profile type %q", profile.Type)
	}
	replaced := false
	for index := range args {
		if strings.Contains(args[index], "{prompt}") || strings.Contains(args[index], "{input}") {
			replaced = true
			args[index] = strings.ReplaceAll(args[index], "{prompt}", prompt)
			args[index] = strings.ReplaceAll(args[index], "{input}", string(input))
		}
	}
	if !replaced {
		args = append(args, prompt)
	}
	return executable, args, envelope.Workspace, nil
}

type lineAssembler struct{ pending []byte }

func (a *lineAssembler) add(data []byte) [][]byte {
	a.pending = append(a.pending, data...)
	parts := strings.Split(string(a.pending), "\n")
	a.pending = append(a.pending[:0], parts[len(parts)-1]...)
	result := make([][]byte, 0, len(parts)-1)
	for _, part := range parts[:len(parts)-1] {
		if strings.TrimSpace(part) != "" {
			result = append(result, []byte(part))
		}
	}
	return result
}

func (a *lineAssembler) flush() []byte {
	result := append([]byte(nil), a.pending...)
	a.pending = nil
	return result
}

var toolType = regexp.MustCompile(`(?i)(tool|command|function)`)

func normalizeAgentLine(line []byte) (string, any) {
	var value map[string]any
	if json.Unmarshal(line, &value) != nil {
		return "agent.log", map[string]string{"text": string(line)}
	}
	eventType, _ := value["type"].(string)
	switch {
	case eventType == "result" || eventType == "turn.completed":
		return "agent.result", value
	case toolType.MatchString(eventType):
		return "agent.tool", value
	case eventType == "assistant" || eventType == "item.completed" || eventType == "message":
		return "agent.message", value
	default:
		return "agent.event", value
	}
}

func finalAgentText(output []byte) string {
	var result string
	for _, line := range strings.Split(string(output), "\n") {
		var event map[string]any
		if json.Unmarshal([]byte(line), &event) != nil {
			continue
		}
		if value, ok := event["result"].(string); ok && value != "" {
			result = value
		}
		if value, ok := event["text"].(string); ok && value != "" {
			result = value
		}
		if item, ok := event["item"].(map[string]any); ok {
			if value, ok := item["text"].(string); ok && value != "" {
				result = value
			}
		}
		if message, ok := event["message"].(map[string]any); ok {
			if value, ok := message["content"].(string); ok && value != "" {
				result = value
			}
		}
	}
	return result
}

func readLimited(reader io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, errors.New("response body exceeds 1 MiB")
	}
	return data, nil
}

// DoctorExecutable checks discovery only. Authentication remains owned by the CLI.
func DoctorExecutable(profile resource.AgentProfileSpec) error {
	executable := profile.Executable
	if executable == "" {
		executable = profile.Type
	}
	_, err := exec.LookPath(executable)
	return err
}

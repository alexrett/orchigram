// Package githubplugin implements the bundled GitHub TriggerProvider and
// TaskProvider without depending on the interactive gh CLI.
package githubplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/workspace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultAPIBase = "https://api.github.com"
	maxResponse    = 4 << 20
)

// Capabilities are declared by the first-party GitHub bundle.
var Capabilities = func() []string {
	plugin, _ := firstparty.Find("github")
	return plugin.Capabilities
}()

// Runtime serves both task actions and issue-label subscriptions.
type Runtime struct {
	pluginv1alpha1.UnimplementedTaskProviderServer
	pluginv1alpha1.UnimplementedTriggerProviderServer
	Client *http.Client
	Runner *process.Runner
}

type repositoryConfig struct {
	Owner       string `json:"owner"`
	Repository  string `json:"repository"`
	APIBase     string `json:"apiBase,omitempty"`
	TokenSecret string `json:"tokenSecret"`
}

type watchConfig struct {
	repositoryConfig
	Label        string `json:"label,omitempty"`
	PollInterval string `json:"pollInterval,omitempty"`
}

type issueConfig struct {
	repositoryConfig
	Number int `json:"number"`
}

type commentConfig struct {
	issueConfig
	Body string `json:"body"`
}

type checkoutConfig struct {
	CloneURL      string `json:"cloneURL"`
	DefaultBranch string `json:"defaultBranch,omitempty"`
	IssueNumber   int    `json:"issueNumber"`
	WorkspaceRoot string `json:"workspaceRoot"`
	TokenSecret   string `json:"tokenSecret,omitempty"`
}

type commitConfig struct {
	Workspace     string `json:"workspace"`
	WorkspaceRoot string `json:"workspaceRoot"`
	Branch        string `json:"branch"`
	Message       string `json:"message"`
	TokenSecret   string `json:"tokenSecret,omitempty"`
}

type pullRequestConfig struct {
	repositoryConfig
	Head  string `json:"head"`
	Base  string `json:"base"`
	Title string `json:"title"`
	Body  string `json:"body,omitempty"`
}

type issue struct {
	Number  int    `json:"number"`
	Title   string `json:"title"`
	Body    string `json:"body"`
	HTMLURL string `json:"html_url"`
	State   string `json:"state"`
}

type issueEvent struct {
	ID        int64     `json:"id"`
	Event     string    `json:"event"`
	IssueURL  string    `json:"issue_url"`
	CreatedAt time.Time `json:"created_at"`
	Label     struct {
		Name string `json:"name"`
	} `json:"label"`
}

// ValidateAction validates action-specific JSON without network access.
func (*Runtime) ValidateAction(_ context.Context, request *pluginv1alpha1.ValidateActionRequest) (*pluginv1alpha1.ValidateActionResponse, error) {
	var err error
	switch request.GetAction() {
	case "github.issue.get":
		var config issueConfig
		err = decodeStrict(request.GetConfigJson(), &config)
		if err == nil {
			err = validateIssue(config)
		}
	case "github.issue.comment":
		var config commentConfig
		err = decodeStrict(request.GetConfigJson(), &config)
		if err == nil {
			err = validateIssue(config.issueConfig)
		}
		if err == nil && strings.TrimSpace(config.Body) == "" {
			err = errors.New("body is required")
		}
	case "github.workspace.checkout":
		var config checkoutConfig
		err = decodeStrict(request.GetConfigJson(), &config)
		if err == nil && (config.CloneURL == "" || config.IssueNumber <= 0 || config.WorkspaceRoot == "") {
			err = errors.New("cloneURL, issueNumber, and workspaceRoot are required")
		}
	case "github.workspace.commit-push":
		var config commitConfig
		err = decodeStrict(request.GetConfigJson(), &config)
		if err == nil && (config.Workspace == "" || config.WorkspaceRoot == "" || config.Branch == "" || config.Message == "") {
			err = errors.New("workspace, workspaceRoot, branch, and message are required")
		}
	case "github.pr.ensure":
		var config pullRequestConfig
		err = decodeStrict(request.GetConfigJson(), &config)
		if err == nil {
			err = validateRepository(config.repositoryConfig)
		}
		if err == nil && (config.Head == "" || config.Base == "" || config.Title == "") {
			err = errors.New("head, base, and title are required")
		}
	default:
		err = fmt.Errorf("unsupported action %q", request.GetAction())
	}
	response := &pluginv1alpha1.ValidateActionResponse{}
	if err != nil {
		response.Issues = append(response.Issues, &pluginv1alpha1.ValidationIssue{Path: "config", Code: "invalid", Message: err.Error()})
	}
	return response, nil
}

// Execute invokes one reconciled GitHub or workspace action.
func (r *Runtime) Execute(request *pluginv1alpha1.ExecuteRequest, stream pluginv1alpha1.TaskProvider_ExecuteServer) error {
	if err := validateMeta(request.GetMeta()); err != nil {
		return err
	}
	writer := &eventWriter{stream: stream}
	if err := writer.send("github.started", map[string]any{"action": request.GetAction()}, nil); err != nil {
		return err
	}
	var output any
	var err error
	switch request.GetAction() {
	case "github.issue.get":
		output, err = r.issueGet(stream.Context(), request)
	case "github.issue.comment":
		output, err = r.issueComment(stream.Context(), request)
	case "github.workspace.checkout":
		output, err = r.checkout(stream.Context(), request, writer)
	case "github.workspace.commit-push":
		output, err = r.commitPush(stream.Context(), request, writer)
	case "github.pr.ensure":
		output, err = r.ensurePullRequest(stream.Context(), request)
	default:
		err = status.Errorf(codes.InvalidArgument, "unsupported action %q", request.GetAction())
	}
	if err != nil {
		_ = writer.send("github.failed", map[string]any{"action": request.GetAction(), "error": err.Error()}, nil)
		return err
	}
	return writer.send("github.completed", output, nil)
}

// Cancel terminates an active git process; HTTP calls observe stream context cancellation.
func (r *Runtime) Cancel(_ context.Context, request *pluginv1alpha1.CancelRequest) (*pluginv1alpha1.CancelResponse, error) {
	if r.Runner == nil {
		return &pluginv1alpha1.CancelResponse{Outcome: "context-cancel"}, nil
	}
	outcome, accepted := r.Runner.Cancel(request.GetMeta().GetRequestId())
	return &pluginv1alpha1.CancelResponse{Accepted: accepted, Outcome: outcome}, nil
}

// Watch polls stable repository issue events and waits for each durable daemon ack.
func (r *Runtime) Watch(stream pluginv1alpha1.TriggerProvider_WatchServer) error {
	command, err := stream.Recv()
	if err != nil {
		return err
	}
	start := command.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first trigger command must be start")
	}
	var config watchConfig
	if err := decodeStrict(start.GetConfigJson(), &config); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateRepository(config.repositoryConfig); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if config.Label == "" {
		config.Label = "orchigram:ready"
	}
	interval := 30 * time.Second
	if config.PollInterval != "" {
		interval, err = time.ParseDuration(config.PollInterval)
		if err != nil || interval <= 0 {
			return status.Error(codes.InvalidArgument, "pollInterval must be positive")
		}
	}
	token, err := secret(start.GetSecrets(), config.TokenSecret)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	cursor, err := parseCursor(start.GetCursor())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	for {
		events, listErr := r.listReadyEvents(stream.Context(), config, token, cursor)
		if listErr != nil {
			return listErr
		}
		for _, event := range events {
			payload, marshalErr := json.Marshal(map[string]any{"repository": map[string]any{"owner": config.Owner, "name": config.Repository}, "issue": event.Issue})
			if marshalErr != nil {
				return marshalErr
			}
			providerID := fmt.Sprintf("github:%s/%s:issue-label-event:%d", config.Owner, config.Repository, event.ID)
			eventCursor := strconv.FormatInt(event.ID, 10)
			if err := stream.Send(&pluginv1alpha1.TriggerEvent{ProviderEventId: providerID, Cursor: eventCursor, OccurredAt: timestamppb.New(event.CreatedAt), PayloadJson: payload}); err != nil {
				return err
			}
			ackCommand, receiveErr := stream.Recv()
			if receiveErr != nil {
				return receiveErr
			}
			ack := ackCommand.GetAck()
			if ack == nil || ack.GetProviderEventId() != providerID || ack.GetCursor() != eventCursor {
				return status.Error(codes.FailedPrecondition, "trigger acknowledgement does not match emitted event")
			}
			cursor = event.ID
		}
		timer := time.NewTimer(interval)
		select {
		case <-stream.Context().Done():
			timer.Stop()
			return stream.Context().Err()
		case <-timer.C:
		}
	}
}

type readyEvent struct {
	ID        int64
	CreatedAt time.Time
	Issue     issue
}

func (r *Runtime) listReadyEvents(ctx context.Context, config watchConfig, token []byte, cursor int64) ([]readyEvent, error) {
	base := apiBase(config.APIBase)
	next := fmt.Sprintf("%s/repos/%s/%s/issues/events?per_page=100", base, url.PathEscape(config.Owner), url.PathEscape(config.Repository))
	result := []readyEvent{}
	for page := 0; next != "" && page < 100; page++ {
		var events []issueEvent
		headers, err := r.getJSON(ctx, next, token, &events)
		if err != nil {
			return nil, err
		}
		next = nextLink(headers.Get("Link"))
		for _, event := range events {
			if event.ID <= cursor || event.Event != "labeled" || event.Label.Name != config.Label {
				continue
			}
			var item issue
			if _, err := r.getJSON(ctx, event.IssueURL, token, &item); err != nil {
				return nil, err
			}
			result = append(result, readyEvent{ID: event.ID, CreatedAt: event.CreatedAt, Issue: item})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (r *Runtime) issueGet(ctx context.Context, request *pluginv1alpha1.ExecuteRequest) (any, error) {
	var config issueConfig
	if err := decodeStrict(request.GetConfigJson(), &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateIssue(config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := secret(request.GetSecrets(), config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	var item issue
	endpoint := fmt.Sprintf("%s/repos/%s/%s/issues/%d", apiBase(config.APIBase), url.PathEscape(config.Owner), url.PathEscape(config.Repository), config.Number)
	if _, err := r.getJSON(ctx, endpoint, token, &item); err != nil {
		return nil, err
	}
	return map[string]any{"issue": item}, nil
}

func (r *Runtime) issueComment(ctx context.Context, request *pluginv1alpha1.ExecuteRequest) (any, error) {
	var config commentConfig
	if err := decodeStrict(request.GetConfigJson(), &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateIssue(config.issueConfig); err != nil || strings.TrimSpace(config.Body) == "" {
		return nil, status.Error(codes.InvalidArgument, "valid repository, issue number, and body are required")
	}
	token, err := secret(request.GetSecrets(), config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	marker := hiddenMarker(request.GetMeta())
	endpoint := fmt.Sprintf("%s/repos/%s/%s/issues/%d/comments", apiBase(config.APIBase), url.PathEscape(config.Owner), url.PathEscape(config.Repository), config.Number)
	type commentRecord struct {
		ID      int64  `json:"id"`
		Body    string `json:"body"`
		HTMLURL string `json:"html_url"`
	}
	comments := []commentRecord{}
	next := endpoint + "?per_page=100"
	for page := 0; next != "" && page < 100; page++ {
		var batch []commentRecord
		headers, err := r.getJSON(ctx, next, token, &batch)
		if err != nil {
			return nil, err
		}
		comments = append(comments, batch...)
		next = nextLink(headers.Get("Link"))
	}
	for _, comment := range comments {
		if strings.Contains(comment.Body, marker) {
			return map[string]any{"id": comment.ID, "url": comment.HTMLURL, "reconciled": true, "marker": marker}, nil
		}
	}
	body := map[string]string{"body": strings.TrimSpace(config.Body) + "\n\n" + marker}
	var created struct {
		ID      int64  `json:"id"`
		HTMLURL string `json:"html_url"`
	}
	if err := r.mutateJSON(ctx, http.MethodPost, endpoint, token, body, &created); err != nil {
		return nil, err
	}
	return map[string]any{"id": created.ID, "url": created.HTMLURL, "reconciled": false, "marker": marker}, nil
}

func (r *Runtime) checkout(ctx context.Context, request *pluginv1alpha1.ExecuteRequest, writer *eventWriter) (any, error) {
	var config checkoutConfig
	if err := decodeStrict(request.GetConfigJson(), &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := optionalSecret(request.GetSecrets(), config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if r.Runner == nil {
		r.Runner = process.NewRunner()
	}
	manager := workspace.Manager{Root: config.WorkspaceRoot, Runner: r.Runner}
	result, err := manager.Checkout(ctx, workspace.CheckoutRequest{RequestID: request.GetMeta().GetRequestId(), RunUID: request.GetMeta().GetRunUid(), CloneURL: config.CloneURL, DefaultBranch: config.DefaultBranch, IssueNumber: config.IssueNumber, Token: token}, func(output process.Output) error {
		return writer.send("github.git", map[string]string{"stream": output.Stream}, output.Data)
	})
	return result, err
}

func (r *Runtime) commitPush(ctx context.Context, request *pluginv1alpha1.ExecuteRequest, writer *eventWriter) (any, error) {
	var config commitConfig
	if err := decodeStrict(request.GetConfigJson(), &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := optionalSecret(request.GetSecrets(), config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if r.Runner == nil {
		r.Runner = process.NewRunner()
	}
	manager := workspace.Manager{Root: config.WorkspaceRoot, Runner: r.Runner}
	result, err := manager.CommitPush(ctx, workspace.CommitRequest{RequestID: request.GetMeta().GetRequestId(), Path: config.Workspace, Branch: config.Branch, Message: config.Message, Token: token}, func(output process.Output) error {
		return writer.send("github.git", map[string]string{"stream": output.Stream}, output.Data)
	})
	return result, err
}

func (r *Runtime) ensurePullRequest(ctx context.Context, request *pluginv1alpha1.ExecuteRequest) (any, error) {
	var config pullRequestConfig
	if err := decodeStrict(request.GetConfigJson(), &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateRepository(config.repositoryConfig); err != nil || config.Head == "" || config.Base == "" || config.Title == "" {
		return nil, status.Error(codes.InvalidArgument, "valid repository, head, base, and title are required")
	}
	token, err := secret(request.GetSecrets(), config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	marker := hiddenMarker(request.GetMeta())
	query := url.Values{"state": {"all"}, "head": {config.Owner + ":" + config.Head}, "per_page": {"100"}}
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls", apiBase(config.APIBase), url.PathEscape(config.Owner), url.PathEscape(config.Repository))
	type pullRecord struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
		Body    string `json:"body"`
		Head    struct {
			Ref string `json:"ref"`
		} `json:"head"`
	}
	pulls := []pullRecord{}
	next := endpoint + "?" + query.Encode()
	for page := 0; next != "" && page < 100; page++ {
		var batch []pullRecord
		headers, err := r.getJSON(ctx, next, token, &batch)
		if err != nil {
			return nil, err
		}
		pulls = append(pulls, batch...)
		next = nextLink(headers.Get("Link"))
	}
	for _, pull := range pulls {
		if pull.Head.Ref == config.Head || strings.Contains(pull.Body, marker) {
			return map[string]any{"number": pull.Number, "url": pull.HTMLURL, "reconciled": true, "marker": marker}, nil
		}
	}
	body := map[string]any{"head": config.Head, "base": config.Base, "title": config.Title, "body": strings.TrimSpace(config.Body) + "\n\n" + marker}
	var created struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := r.mutateJSON(ctx, http.MethodPost, endpoint, token, body, &created); err != nil {
		return nil, err
	}
	return map[string]any{"number": created.Number, "url": created.HTMLURL, "reconciled": false, "marker": marker}, nil
}

func (r *Runtime) getJSON(ctx context.Context, endpoint string, token []byte, target any) (http.Header, error) {
	var response *http.Response
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		response, err = r.do(ctx, http.MethodGet, endpoint, token, nil)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusTooManyRequests && (response.StatusCode != http.StatusForbidden || response.Header.Get("X-RateLimit-Remaining") != "0") {
			break
		}
		_ = response.Body.Close()
		if attempt == 2 {
			return nil, status.Error(codes.ResourceExhausted, "GitHub API rate limit persisted after retries")
		}
		delay := retryDelay(response.Header)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, githubStatus(response)
	}
	if err := decodeLimited(response.Body, target); err != nil {
		return nil, err
	}
	return response.Header.Clone(), nil
}

func (r *Runtime) mutateJSON(ctx context.Context, method, endpoint string, token []byte, payload, target any) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	response, err := r.do(ctx, method, endpoint, token, encoded)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return githubStatus(response)
	}
	return decodeLimited(response.Body, target)
}

func (r *Runtime) do(ctx context.Context, method, endpoint string, token, body []byte) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	request.Header.Set("User-Agent", "orchigram-plugin-github")
	if len(body) > 0 {
		request.Header.Set("Content-Type", "application/json")
	}
	if len(token) > 0 {
		request.Header.Set("Authorization", "Bearer "+string(token))
	}
	client := r.Client
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return response, nil
}

func decodeLimited(reader io.Reader, target any) error {
	data, err := io.ReadAll(io.LimitReader(reader, maxResponse+1))
	if err != nil {
		return err
	}
	if len(data) > maxResponse {
		return status.Error(codes.ResourceExhausted, "GitHub response exceeds 4 MiB")
	}
	if err := json.Unmarshal(data, target); err != nil {
		return status.Error(codes.Internal, "GitHub returned malformed JSON")
	}
	return nil
}

func githubStatus(response *http.Response) error {
	data, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	var payload struct {
		Message string `json:"message"`
	}
	_ = json.Unmarshal(data, &payload)
	if payload.Message == "" {
		payload.Message = http.StatusText(response.StatusCode)
	}
	code := codes.FailedPrecondition
	if response.StatusCode >= 500 {
		code = codes.Unavailable
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		code = codes.PermissionDenied
	}
	return status.Errorf(code, "GitHub API status %d: %s", response.StatusCode, payload.Message)
}

func retryDelay(headers http.Header) time.Duration {
	if seconds, err := strconv.Atoi(headers.Get("Retry-After")); err == nil && seconds > 0 {
		return min(time.Duration(seconds)*time.Second, time.Second)
	}
	if reset, err := strconv.ParseInt(headers.Get("X-RateLimit-Reset"), 10, 64); err == nil && reset > 0 {
		return min(max(time.Until(time.Unix(reset, 0)), 10*time.Millisecond), time.Second)
	}
	return 10 * time.Millisecond
}

func nextLink(header string) string {
	for _, part := range strings.Split(header, ",") {
		sections := strings.Split(part, ";")
		if len(sections) < 2 || !strings.Contains(sections[1], `rel="next"`) {
			continue
		}
		return strings.Trim(strings.TrimSpace(sections[0]), "<>")
	}
	return ""
}

func validateRepository(config repositoryConfig) error {
	if config.Owner == "" || config.Repository == "" || config.TokenSecret == "" {
		return errors.New("owner, repository, and tokenSecret are required")
	}
	base := apiBase(config.APIBase)
	parsed, err := url.Parse(base)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return errors.New("apiBase must use http or https")
	}
	return nil
}

func validateIssue(config issueConfig) error {
	if err := validateRepository(config.repositoryConfig); err != nil {
		return err
	}
	if config.Number <= 0 {
		return errors.New("positive issue number is required")
	}
	return nil
}

func apiBase(value string) string {
	if value == "" {
		return defaultAPIBase
	}
	return strings.TrimRight(value, "/")
}

func parseCursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	cursor, err := strconv.ParseInt(value, 10, 64)
	if err != nil || cursor < 0 {
		return 0, errors.New("GitHub cursor must be a non-negative event ID")
	}
	return cursor, nil
}

func secret(values map[string][]byte, name string) ([]byte, error) {
	if name == "" {
		return nil, errors.New("tokenSecret is required")
	}
	value, exists := values[name]
	if !exists || len(value) == 0 {
		return nil, fmt.Errorf("secret %q is missing", name)
	}
	return value, nil
}

func optionalSecret(values map[string][]byte, name string) ([]byte, error) {
	if name == "" {
		return nil, nil
	}
	return secret(values, name)
}

func hiddenMarker(meta *pluginv1alpha1.CallMeta) string {
	return fmt.Sprintf("<!-- orchigram:run=%s;node=%s -->", meta.GetRunUid(), meta.GetNodeId())
}

func validateMeta(meta *pluginv1alpha1.CallMeta) error {
	if meta == nil || meta.GetRequestId() == "" || meta.GetRunUid() == "" || meta.GetNodeId() == "" || meta.GetIdempotencyKey() == "" {
		return status.Error(codes.InvalidArgument, "complete call metadata is required")
	}
	return nil
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}
	return nil
}

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
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return w.stream.Send(&pluginv1alpha1.ExecuteEvent{Sequence: w.sequence, Type: eventType, PayloadJson: data, RawLog: append([]byte(nil), raw...), OccurredAt: timestamppb.Now()})
}

// Package githubplugin implements the bundled GitHub TriggerProvider and
// TaskProvider without depending on the interactive gh CLI.
package githubplugin

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/process"
	"github.com/alexrett/orchigram/internal/workspace"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultAPIBase    = "https://api.github.com"
	githubAPIVersion  = "2026-03-10"
	maxResponse       = 4 << 20
	activationOverlap = time.Minute
)

var orchigramRunMarker = regexp.MustCompile(`<!-- orchigram:run=([A-Za-z0-9][A-Za-z0-9._:-]{0,127});node=[A-Za-z0-9][A-Za-z0-9._:-]{0,127};idempotency=[0-9a-f]{64} -->`)
var gitObjectID = regexp.MustCompile(`^[0-9a-fA-F]{40}([0-9a-fA-F]{24})?$`)

// Capabilities are declared by the first-party GitHub bundle.
var Capabilities = func() []string {
	plugin, _ := firstparty.Find("github")
	return plugin.Capabilities
}()

// Runtime serves both task actions and issue-label subscriptions.
type Runtime struct {
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
	Label          string `json:"label,omitempty"`
	PollInterval   string `json:"pollInterval,omitempty"`
	ReplayExisting bool   `json:"replayExisting,omitempty"`
}

type reviewWatchConfig struct {
	repositoryConfig
	PollInterval   string `json:"pollInterval,omitempty"`
	ReplayExisting bool   `json:"replayExisting,omitempty"`
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

type checkWaitConfig struct {
	repositoryConfig
	Ref          string   `json:"ref"`
	PullNumber   int      `json:"pullNumber"`
	Required     []string `json:"required"`
	PollInterval string   `json:"pollInterval,omitempty"`
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
	Issue     *issue    `json:"issue"`
	CreatedAt time.Time `json:"created_at"`
	Label     struct {
		Name string `json:"name"`
	} `json:"label"`
}

type pullRecord struct {
	Number  int    `json:"number"`
	HTMLURL string `json:"html_url"`
	Body    string `json:"body"`
	State   string `json:"state"`
	Head    struct {
		SHA string `json:"sha"`
	} `json:"head"`
}

type reviewRecord struct {
	ID          int64     `json:"id"`
	State       string    `json:"state"`
	Body        string    `json:"body"`
	SubmittedAt time.Time `json:"submitted_at"`
	CommitID    string    `json:"commit_id"`
	User        struct {
		Login string `json:"login"`
	} `json:"user"`
}

type reviewComment struct {
	ID           int64  `json:"id"`
	Body         string `json:"body"`
	Path         string `json:"path"`
	Line         *int   `json:"line"`
	OriginalLine *int   `json:"original_line"`
	Side         string `json:"side"`
	SubjectType  string `json:"subject_type"`
	HTMLURL      string `json:"html_url"`
}

type reviewCommentPayload struct {
	ID          int64  `json:"id"`
	Body        string `json:"body"`
	Path        string `json:"path"`
	Line        *int   `json:"line"`
	Side        string `json:"side"`
	SubjectType string `json:"subject_type"`
	HTMLURL     string `json:"html_url"`
}

type reviewCursor struct {
	SubmittedAt time.Time `json:"submittedAt"`
	ReviewID    int64     `json:"reviewID"`
}

type submittedReview struct {
	Cursor   reviewCursor
	RunUID   string
	Pull     pullRecord
	Review   reviewRecord
	Comments []reviewCommentPayload
}

type checkRunRecord struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	DetailsURL string `json:"details_url"`
	HeadSHA    string `json:"head_sha"`
}

type commitStatusRecord struct {
	Context     string `json:"context"`
	State       string `json:"state"`
	Description string `json:"description"`
	TargetURL   string `json:"target_url"`
}

type checkSummary struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	Conclusion string `json:"conclusion"`
	URL        string `json:"url"`
}

type statusSummary struct {
	Context     string `json:"context"`
	State       string `json:"state"`
	Description string `json:"description"`
	URL         string `json:"url"`
}

type checkSnapshot struct {
	HeadSHA  string          `json:"headSha"`
	State    string          `json:"state"`
	Checks   []checkSummary  `json:"checks"`
	Statuses []statusSummary `json:"statuses"`
}

// ValidateAction validates action-specific JSON without network access.
func (*Runtime) ValidateAction(_ context.Context, action string, configJSON json.RawMessage) []pluginsdk.ValidationIssue {
	var err error
	switch action {
	case "github.issue.get":
		var config issueConfig
		err = decodeStrict(configJSON, &config)
		if err == nil {
			err = validateIssue(config)
		}
	case "github.issue.comment":
		var config commentConfig
		err = decodeStrict(configJSON, &config)
		if err == nil {
			err = validateIssue(config.issueConfig)
		}
		if err == nil && strings.TrimSpace(config.Body) == "" {
			err = errors.New("body is required")
		}
	case "github.workspace.checkout":
		var config checkoutConfig
		err = decodeStrict(configJSON, &config)
		if err == nil && (config.CloneURL == "" || config.IssueNumber <= 0 || config.WorkspaceRoot == "") {
			err = errors.New("cloneURL, issueNumber, and workspaceRoot are required")
		}
	case "github.workspace.commit-push":
		var config commitConfig
		err = decodeStrict(configJSON, &config)
		if err == nil && (config.Workspace == "" || config.WorkspaceRoot == "" || config.Branch == "" || config.Message == "") {
			err = errors.New("workspace, workspaceRoot, branch, and message are required")
		}
	case "github.pr.ensure":
		var config pullRequestConfig
		err = decodeStrict(configJSON, &config)
		if err == nil {
			err = validateRepository(config.repositoryConfig)
		}
		if err == nil && (config.Head == "" || config.Base == "" || config.Title == "") {
			err = errors.New("head, base, and title are required")
		}
	case "github.commit.checks.wait":
		var config checkWaitConfig
		err = decodeStrict(configJSON, &config)
		if err == nil {
			err = validateCheckWait(config)
		}
	default:
		err = fmt.Errorf("unsupported action %q", action)
	}
	if err != nil {
		return []pluginsdk.ValidationIssue{{Path: "config", Code: "invalid", Message: err.Error()}}
	}
	return nil
}

// Execute invokes one reconciled GitHub or workspace action.
func (r *Runtime) Execute(ctx context.Context, request pluginsdk.TaskRequest, sink pluginsdk.EventSink) (any, error) {
	if err := sink.Emit("github.started", map[string]any{"action": request.Action}); err != nil {
		return nil, err
	}
	var output any
	var err error
	switch request.Action {
	case "github.issue.get":
		output, err = r.issueGet(ctx, request)
	case "github.issue.comment":
		output, err = r.issueComment(ctx, request)
	case "github.workspace.checkout":
		output, err = r.checkout(ctx, request, sink)
	case "github.workspace.commit-push":
		output, err = r.commitPush(ctx, request, sink)
	case "github.pr.ensure":
		output, err = r.ensurePullRequest(ctx, request)
	case "github.commit.checks.wait":
		output, err = r.waitForCommitChecks(ctx, request, sink)
	default:
		err = fmt.Errorf("unsupported action %q", request.Action)
	}
	return output, err
}

// Watch selects one declared source and waits for each durable daemon ack.
func (r *Runtime) Watch(stream pluginv1alpha1.TriggerProvider_WatchServer) error {
	command, err := stream.Recv()
	if err != nil {
		return err
	}
	start := command.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first trigger command must be start")
	}
	switch start.GetSource() {
	case "", "github.issues":
		return r.watchIssues(stream, start)
	case "github.reviews":
		return r.watchReviews(stream, start)
	default:
		return status.Errorf(codes.InvalidArgument, "unsupported trigger source %q", start.GetSource())
	}
}

func (r *Runtime) watchIssues(stream pluginv1alpha1.TriggerProvider_WatchServer, start *pluginv1alpha1.WatchStart) error {
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
	interval, err := providerPollInterval(config.PollInterval)
	if err != nil {
		return err
	}
	token, err := secret(start.GetSecrets(), config.TokenSecret)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	cursor, err := parseCursor(start.GetCursor())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	activatedAt, err := providerActivation(start, config.ReplayExisting, cursor > 0)
	if err != nil {
		return err
	}
	for {
		events, listErr := r.listReadyEvents(stream.Context(), config, token, cursor, activatedAt)
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
			if err := sendAndAwaitAck(stream, &pluginv1alpha1.TriggerEvent{ProviderEventId: providerID, Cursor: eventCursor, OccurredAt: timestamppb.New(event.CreatedAt), PayloadJson: payload}); err != nil {
				return err
			}
			cursor = event.ID
		}
		if err := waitForProviderPoll(stream.Context(), interval); err != nil {
			return err
		}
	}
}

func providerActivation(start *pluginv1alpha1.WatchStart, replayExisting, hasCursor bool) (time.Time, error) {
	activatedAt := start.GetActivatedAt()
	if activatedAt != nil && !activatedAt.IsValid() {
		return time.Time{}, status.Error(codes.InvalidArgument, "activated_at must be a valid timestamp")
	}
	if replayExisting || hasCursor {
		return time.Time{}, nil
	}
	if activatedAt == nil {
		return time.Time{}, status.Error(codes.FailedPrecondition, "activated_at is required for an empty-cursor non-replay subscription")
	}
	return activatedAt.AsTime(), nil
}

func providerPollInterval(value string) (time.Duration, error) {
	if value == "" {
		return 30 * time.Second, nil
	}
	interval, err := time.ParseDuration(value)
	if err != nil || interval <= 0 {
		return 0, status.Error(codes.InvalidArgument, "pollInterval must be positive")
	}
	return interval, nil
}

func sendAndAwaitAck(stream pluginv1alpha1.TriggerProvider_WatchServer, event *pluginv1alpha1.TriggerEvent) error {
	if err := stream.Send(event); err != nil {
		return err
	}
	command, err := stream.Recv()
	if err != nil {
		return err
	}
	ack := command.GetAck()
	if ack == nil || ack.GetProviderEventId() != event.GetProviderEventId() || ack.GetCursor() != event.GetCursor() {
		return status.Error(codes.FailedPrecondition, "trigger acknowledgement does not match emitted event")
	}
	return nil
}

func waitForProviderPoll(ctx context.Context, interval time.Duration) error {
	timer := time.NewTimer(interval)
	select {
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runtime) watchReviews(stream pluginv1alpha1.TriggerProvider_WatchServer, start *pluginv1alpha1.WatchStart) error {
	var config reviewWatchConfig
	if err := decodeStrict(start.GetConfigJson(), &config); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateRepository(config.repositoryConfig); err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	interval, err := providerPollInterval(config.PollInterval)
	if err != nil {
		return err
	}
	token, err := secret(start.GetSecrets(), config.TokenSecret)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	cursor, err := parseReviewCursor(start.GetCursor())
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	activatedAt, err := providerActivation(start, config.ReplayExisting, !cursor.SubmittedAt.IsZero())
	if err != nil {
		return err
	}
	for {
		events, listErr := r.listSubmittedReviews(stream.Context(), config, token, cursor, activatedAt)
		if listErr != nil {
			return listErr
		}
		for _, event := range events {
			payload, marshalErr := json.Marshal(map[string]any{
				"repository": map[string]any{"owner": config.Owner, "name": config.Repository},
				"pull":       map[string]any{"number": event.Pull.Number, "html_url": event.Pull.HTMLURL, "head_sha": event.Pull.Head.SHA},
				"review": map[string]any{
					"id": event.Review.ID, "state": strings.ToUpper(event.Review.State), "body": event.Review.Body,
					"author": event.Review.User.Login, "submitted_at": event.Review.SubmittedAt.UTC().Format(time.RFC3339Nano), "commit_id": event.Review.CommitID,
					"comments": event.Comments,
				},
			})
			if marshalErr != nil {
				return marshalErr
			}
			encodedCursor, marshalErr := encodeReviewCursor(event.Cursor)
			if marshalErr != nil {
				return marshalErr
			}
			providerID := fmt.Sprintf("github:%s/%s:pull-review:%d", config.Owner, config.Repository, event.Review.ID)
			triggerEvent := &pluginv1alpha1.TriggerEvent{
				ProviderEventId: providerID, Cursor: encodedCursor, OccurredAt: timestamppb.New(event.Review.SubmittedAt),
				PayloadJson: payload, TargetRunUid: event.RunUID,
			}
			if err := sendAndAwaitAck(stream, triggerEvent); err != nil {
				return err
			}
			cursor = event.Cursor
		}
		if err := waitForProviderPoll(stream.Context(), interval); err != nil {
			return err
		}
	}
}

func (r *Runtime) listSubmittedReviews(ctx context.Context, config reviewWatchConfig, token []byte, cursor reviewCursor, activatedAt time.Time) ([]submittedReview, error) {
	if !activatedAt.IsZero() {
		activatedAt = activatedAt.UTC().Truncate(time.Second).Add(-activationOverlap)
	}
	base := apiBase(config.APIBase)
	next := fmt.Sprintf("%s/repos/%s/%s/pulls?state=all&sort=updated&direction=desc&per_page=100", base, url.PathEscape(config.Owner), url.PathEscape(config.Repository))
	result := []submittedReview{}
	pullPages := 0
	for ; next != "" && pullPages < 100; pullPages++ {
		var pulls []pullRecord
		headers, err := r.getJSON(ctx, next, token, &pulls)
		if err != nil {
			return nil, err
		}
		next = nextLink(headers.Get("Link"))
		for _, pull := range pulls {
			runUID, ok := targetRunFromMarker(pull.Body)
			if !ok {
				continue
			}
			if pull.Number <= 0 || pull.Head.SHA == "" {
				return nil, status.Error(codes.FailedPrecondition, "GitHub returned an invalid managed pull request")
			}
			reviewsNext := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews?per_page=100", base, url.PathEscape(config.Owner), url.PathEscape(config.Repository), pull.Number)
			reviewPages := 0
			for ; reviewsNext != "" && reviewPages < 100; reviewPages++ {
				var reviews []reviewRecord
				reviewHeaders, reviewErr := r.getJSON(ctx, reviewsNext, token, &reviews)
				if reviewErr != nil {
					return nil, reviewErr
				}
				reviewsNext = nextLink(reviewHeaders.Get("Link"))
				for _, review := range reviews {
					state := strings.ToUpper(review.State)
					if review.ID <= 0 || review.SubmittedAt.IsZero() || (state != "APPROVED" && state != "CHANGES_REQUESTED") {
						continue
					}
					if review.CommitID == "" || review.User.Login == "" {
						return nil, status.Errorf(codes.FailedPrecondition, "GitHub review %d is missing required identity fields", review.ID)
					}
					review.State = state
					eventCursor := reviewCursor{SubmittedAt: review.SubmittedAt.UTC(), ReviewID: review.ID}
					if !reviewCursorAfter(eventCursor, cursor) || (!activatedAt.IsZero() && review.SubmittedAt.Before(activatedAt)) {
						continue
					}
					comments, commentErr := r.listReviewComments(ctx, config, token, pull.Number, review.ID)
					if commentErr != nil {
						return nil, commentErr
					}
					result = append(result, submittedReview{Cursor: eventCursor, RunUID: runUID, Pull: pull, Review: review, Comments: comments})
				}
			}
			if reviewsNext != "" {
				return nil, status.Errorf(codes.ResourceExhausted, "GitHub review pagination for pull request %d exceeds 100 pages", pull.Number)
			}
		}
	}
	if next != "" {
		return nil, status.Error(codes.ResourceExhausted, "GitHub pull-request pagination exceeds 100 pages")
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Cursor.SubmittedAt.Equal(result[j].Cursor.SubmittedAt) {
			return result[i].Cursor.ReviewID < result[j].Cursor.ReviewID
		}
		return result[i].Cursor.SubmittedAt.Before(result[j].Cursor.SubmittedAt)
	})
	return result, nil
}

func (r *Runtime) listReviewComments(ctx context.Context, config reviewWatchConfig, token []byte, pullNumber int, reviewID int64) ([]reviewCommentPayload, error) {
	base := apiBase(config.APIBase)
	next := fmt.Sprintf("%s/repos/%s/%s/pulls/%d/reviews/%d/comments?per_page=100", base, url.PathEscape(config.Owner), url.PathEscape(config.Repository), pullNumber, reviewID)
	result := []reviewCommentPayload{}
	for page := 0; next != "" && page < 100; page++ {
		var comments []reviewComment
		headers, err := r.getJSON(ctx, next, token, &comments)
		if err != nil {
			return nil, err
		}
		next = nextLink(headers.Get("Link"))
		for _, comment := range comments {
			if comment.ID <= 0 || strings.TrimSpace(comment.Path) == "" {
				return nil, status.Errorf(codes.FailedPrecondition, "GitHub review comment for review %d is missing required identity fields", reviewID)
			}
			line := comment.Line
			if line == nil && comment.OriginalLine != nil {
				line = comment.OriginalLine
			}
			subjectType := strings.ToLower(comment.SubjectType)
			if subjectType == "" {
				subjectType = "line"
				if line == nil {
					subjectType = "file"
				}
			}
			if subjectType != "line" && subjectType != "file" {
				return nil, status.Errorf(codes.FailedPrecondition, "GitHub review comment %d has unsupported subject_type %q", comment.ID, comment.SubjectType)
			}
			if subjectType == "line" && line == nil {
				return nil, status.Errorf(codes.FailedPrecondition, "GitHub line review comment %d has no line location", comment.ID)
			}
			result = append(result, reviewCommentPayload{ID: comment.ID, Body: comment.Body, Path: comment.Path, Line: line, Side: comment.Side, SubjectType: subjectType, HTMLURL: comment.HTMLURL})
		}
	}
	if next != "" {
		return nil, status.Errorf(codes.ResourceExhausted, "GitHub comment pagination for review %d exceeds 100 pages", reviewID)
	}
	return result, nil
}

func targetRunFromMarker(body string) (string, bool) {
	match := orchigramRunMarker.FindStringSubmatch(body)
	if len(match) != 2 {
		return "", false
	}
	return match[1], true
}

func parseReviewCursor(value string) (reviewCursor, error) {
	if value == "" {
		return reviewCursor{}, nil
	}
	var cursor reviewCursor
	if err := json.Unmarshal([]byte(value), &cursor); err != nil || cursor.SubmittedAt.IsZero() || cursor.ReviewID <= 0 {
		return reviewCursor{}, errors.New("GitHub review cursor must contain a submitted timestamp and positive review ID")
	}
	cursor.SubmittedAt = cursor.SubmittedAt.UTC()
	return cursor, nil
}

func encodeReviewCursor(cursor reviewCursor) (string, error) {
	data, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func reviewCursorAfter(candidate, cursor reviewCursor) bool {
	if cursor.SubmittedAt.IsZero() {
		return true
	}
	return candidate.SubmittedAt.After(cursor.SubmittedAt) || (candidate.SubmittedAt.Equal(cursor.SubmittedAt) && candidate.ReviewID > cursor.ReviewID)
}

type readyEvent struct {
	ID        int64
	CreatedAt time.Time
	Issue     issue
}

func (r *Runtime) listReadyEvents(ctx context.Context, config watchConfig, token []byte, cursor int64, activatedAt time.Time) ([]readyEvent, error) {
	if !activatedAt.IsZero() {
		// GitHub issue-event timestamps are second-precision and originate on a
		// different clock. A bounded overlap prevents same-second or ordinary
		// clock-skew loss within the one-minute bound; stable event IDs keep
		// restart replay deduplicated.
		activatedAt = activatedAt.UTC().Truncate(time.Second).Add(-activationOverlap)
	}
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
			if event.ID <= cursor || event.Event != "labeled" || event.Label.Name != config.Label || (!activatedAt.IsZero() && event.CreatedAt.Before(activatedAt)) {
				continue
			}
			var item issue
			switch {
			case event.Issue != nil && event.Issue.Number > 0:
				item = *event.Issue
			case strings.TrimSpace(event.IssueURL) != "":
				if _, err := r.getJSON(ctx, event.IssueURL, token, &item); err != nil {
					return nil, err
				}
			default:
				return nil, status.Errorf(codes.FailedPrecondition, "GitHub issue event %d has neither an embedded issue nor issue_url", event.ID)
			}
			result = append(result, readyEvent{ID: event.ID, CreatedAt: event.CreatedAt, Issue: item})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (r *Runtime) issueGet(ctx context.Context, request pluginsdk.TaskRequest) (any, error) {
	var config issueConfig
	if err := decodeStrict(request.Config, &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateIssue(config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := secret(request.Secrets, config.TokenSecret)
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

func (r *Runtime) issueComment(ctx context.Context, request pluginsdk.TaskRequest) (any, error) {
	var config commentConfig
	if err := decodeStrict(request.Config, &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateIssue(config.issueConfig); err != nil || strings.TrimSpace(config.Body) == "" {
		return nil, status.Error(codes.InvalidArgument, "valid repository, issue number, and body are required")
	}
	token, err := secret(request.Secrets, config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	marker := hiddenMarker(request)
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

func (r *Runtime) checkout(ctx context.Context, request pluginsdk.TaskRequest, sink pluginsdk.EventSink) (any, error) {
	var config checkoutConfig
	if err := decodeStrict(request.Config, &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := optionalSecret(request.Secrets, config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if r.Runner == nil {
		r.Runner = process.NewRunner()
	}
	manager := workspace.Manager{Root: config.WorkspaceRoot, Runner: r.Runner}
	result, err := manager.Checkout(ctx, workspace.CheckoutRequest{RequestID: request.RequestID, RunUID: request.RunUID, CloneURL: config.CloneURL, DefaultBranch: config.DefaultBranch, IssueNumber: config.IssueNumber, Token: token}, func(output process.Output) error {
		return sink.Log("github.git."+output.Stream, output.Data)
	})
	return result, err
}

func (r *Runtime) commitPush(ctx context.Context, request pluginsdk.TaskRequest, sink pluginsdk.EventSink) (any, error) {
	var config commitConfig
	if err := decodeStrict(request.Config, &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := optionalSecret(request.Secrets, config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if r.Runner == nil {
		r.Runner = process.NewRunner()
	}
	manager := workspace.Manager{Root: config.WorkspaceRoot, Runner: r.Runner}
	result, err := manager.CommitPush(ctx, workspace.CommitRequest{RequestID: request.RequestID, Path: config.Workspace, Branch: config.Branch, Message: config.Message, Token: token}, func(output process.Output) error {
		return sink.Log("github.git."+output.Stream, output.Data)
	})
	return result, err
}

func (r *Runtime) ensurePullRequest(ctx context.Context, request pluginsdk.TaskRequest) (any, error) {
	var config pullRequestConfig
	if err := decodeStrict(request.Config, &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateRepository(config.repositoryConfig); err != nil || config.Head == "" || config.Base == "" || config.Title == "" {
		return nil, status.Error(codes.InvalidArgument, "valid repository, head, base, and title are required")
	}
	token, err := secret(request.Secrets, config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	marker := hiddenMarker(request)
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
		if strings.Contains(pull.Body, marker) || pull.Head.Ref == config.Head {
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

func validateCheckWait(config checkWaitConfig) error {
	if err := validateRepository(config.repositoryConfig); err != nil {
		return err
	}
	if !gitObjectID.MatchString(config.Ref) {
		return errors.New("ref must be an exact 40- or 64-character Git object ID")
	}
	if config.PullNumber <= 0 {
		return errors.New("positive pullNumber is required")
	}
	if len(config.Required) == 0 {
		return errors.New("at least one required check name is required")
	}
	seen := map[string]bool{}
	for _, name := range config.Required {
		if strings.TrimSpace(name) == "" || name != strings.TrimSpace(name) {
			return errors.New("required check names must be non-empty and must not have surrounding whitespace")
		}
		if seen[name] {
			return fmt.Errorf("required check name %q is duplicated", name)
		}
		seen[name] = true
	}
	if config.PollInterval != "" {
		interval, err := time.ParseDuration(config.PollInterval)
		if err != nil || interval <= 0 {
			return errors.New("pollInterval must be positive")
		}
	}
	return nil
}

func (r *Runtime) waitForCommitChecks(ctx context.Context, request pluginsdk.TaskRequest, sink pluginsdk.EventSink) (any, error) {
	var config checkWaitConfig
	if err := decodeStrict(request.Config, &config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := validateCheckWait(config); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	token, err := secret(request.Secrets, config.TokenSecret)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	interval := 5 * time.Second
	if config.PollInterval != "" {
		interval, _ = time.ParseDuration(config.PollInterval)
	}
	for {
		currentHead, headErr := r.pullRequestHead(ctx, config, token)
		if headErr != nil {
			return nil, headErr
		}
		if !strings.EqualFold(currentHead, config.Ref) {
			snapshot := checkSnapshot{HeadSHA: strings.ToLower(config.Ref), State: "stale", Checks: []checkSummary{}, Statuses: []statusSummary{}}
			if err := sink.Emit("github.checks.stale", snapshot); err != nil {
				return nil, err
			}
			return snapshot, nil
		}
		snapshot, snapshotErr := r.commitCheckSnapshot(ctx, config, token)
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		snapshot.State = evaluateCheckSnapshot(snapshot, config.Required)
		if snapshot.State == "success" {
			finalHead, finalHeadErr := r.pullRequestHead(ctx, config, token)
			if finalHeadErr != nil {
				return nil, finalHeadErr
			}
			if !strings.EqualFold(finalHead, config.Ref) {
				snapshot.State = "stale"
				if err := sink.Emit("github.checks.stale", snapshot); err != nil {
					return nil, err
				}
				return snapshot, nil
			}
			if err := sink.Emit("github.checks.succeeded", snapshot); err != nil {
				return nil, err
			}
			return snapshot, nil
		}
		if snapshot.State == "failure" {
			if err := sink.Emit("github.checks.failed", snapshot); err != nil {
				return nil, err
			}
			return nil, status.Error(codes.FailedPrecondition, "required GitHub checks or commit statuses failed")
		}
		if err := sink.Emit("github.checks.pending", snapshot); err != nil {
			return nil, err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Runtime) pullRequestHead(ctx context.Context, config checkWaitConfig, token []byte) (string, error) {
	endpoint := fmt.Sprintf("%s/repos/%s/%s/pulls/%d", apiBase(config.APIBase), url.PathEscape(config.Owner), url.PathEscape(config.Repository), config.PullNumber)
	var pull struct {
		State string `json:"state"`
		Head  struct {
			SHA string `json:"sha"`
		} `json:"head"`
	}
	if _, err := r.getJSON(ctx, endpoint, token, &pull); err != nil {
		return "", err
	}
	if pull.State != "open" || !gitObjectID.MatchString(pull.Head.SHA) {
		return "", status.Error(codes.FailedPrecondition, "GitHub pull request is not open with a valid head SHA")
	}
	return pull.Head.SHA, nil
}

func (r *Runtime) commitCheckSnapshot(ctx context.Context, config checkWaitConfig, token []byte) (checkSnapshot, error) {
	base := apiBase(config.APIBase)
	checksNext := fmt.Sprintf("%s/repos/%s/%s/commits/%s/check-runs?filter=latest&per_page=100", base, url.PathEscape(config.Owner), url.PathEscape(config.Repository), url.PathEscape(config.Ref))
	checks := []checkSummary{}
	resolvedSHA := ""
	for page := 0; checksNext != "" && page < 100; page++ {
		var payload struct {
			CheckRuns []checkRunRecord `json:"check_runs"`
		}
		headers, err := r.getJSON(ctx, checksNext, token, &payload)
		if err != nil {
			return checkSnapshot{}, err
		}
		checksNext = nextLink(headers.Get("Link"))
		for _, run := range payload.CheckRuns {
			if strings.TrimSpace(run.Name) == "" || strings.TrimSpace(run.Status) == "" {
				return checkSnapshot{}, status.Error(codes.FailedPrecondition, "GitHub returned a check run without a name or status")
			}
			if run.HeadSHA != "" {
				if resolvedSHA != "" && !strings.EqualFold(resolvedSHA, run.HeadSHA) {
					return checkSnapshot{}, status.Error(codes.FailedPrecondition, "GitHub returned check runs for different head commits")
				}
				resolvedSHA = run.HeadSHA
			}
			checks = append(checks, checkSummary{Name: run.Name, Status: strings.ToLower(run.Status), Conclusion: strings.ToLower(run.Conclusion), URL: run.DetailsURL})
		}
	}
	if checksNext != "" {
		return checkSnapshot{}, status.Error(codes.ResourceExhausted, "GitHub check-run pagination exceeds 100 pages")
	}

	statusesNext := fmt.Sprintf("%s/repos/%s/%s/commits/%s/status?per_page=100", base, url.PathEscape(config.Owner), url.PathEscape(config.Repository), url.PathEscape(config.Ref))
	statuses := []statusSummary{}
	combinedState := ""
	for page := 0; statusesNext != "" && page < 100; page++ {
		var payload struct {
			State    string               `json:"state"`
			SHA      string               `json:"sha"`
			Statuses []commitStatusRecord `json:"statuses"`
		}
		headers, err := r.getJSON(ctx, statusesNext, token, &payload)
		if err != nil {
			return checkSnapshot{}, err
		}
		statusesNext = nextLink(headers.Get("Link"))
		if combinedState == "" {
			combinedState = strings.ToLower(payload.State)
		}
		if payload.SHA != "" {
			if resolvedSHA != "" && !strings.EqualFold(resolvedSHA, payload.SHA) {
				return checkSnapshot{}, status.Error(codes.FailedPrecondition, "GitHub checks and commit statuses resolved to different head commits")
			}
			resolvedSHA = payload.SHA
		}
		for _, item := range payload.Statuses {
			if strings.TrimSpace(item.Context) == "" || strings.TrimSpace(item.State) == "" {
				return checkSnapshot{}, status.Error(codes.FailedPrecondition, "GitHub returned a commit status without a context or state")
			}
			statuses = append(statuses, statusSummary{Context: item.Context, State: strings.ToLower(item.State), Description: item.Description, URL: item.TargetURL})
		}
	}
	if statusesNext != "" {
		return checkSnapshot{}, status.Error(codes.ResourceExhausted, "GitHub commit-status pagination exceeds 100 pages")
	}
	if resolvedSHA == "" {
		resolvedSHA = strings.ToLower(config.Ref)
	}
	if !strings.EqualFold(resolvedSHA, config.Ref) {
		return checkSnapshot{}, status.Error(codes.FailedPrecondition, "GitHub resolved checks for a different commit than the requested head SHA")
	}
	if len(statuses) > 0 && combinedState != "" {
		statuses = append(statuses, statusSummary{Context: "github/combined-status", State: combinedState})
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Context < statuses[j].Context })
	return checkSnapshot{HeadSHA: strings.ToLower(resolvedSHA), State: "pending", Checks: checks, Statuses: statuses}, nil
}

func evaluateCheckSnapshot(snapshot checkSnapshot, required []string) string {
	byName := make(map[string][]checkSummary, len(snapshot.Checks))
	for _, check := range snapshot.Checks {
		byName[check.Name] = append(byName[check.Name], check)
	}
	pending := false
	for _, name := range required {
		checks := byName[name]
		if len(checks) == 0 {
			pending = true
			continue
		}
		for _, check := range checks {
			if check.Status != "completed" {
				pending = true
				continue
			}
			switch check.Conclusion {
			case "success", "neutral", "skipped":
			default:
				return "failure"
			}
		}
	}
	for _, commitStatus := range snapshot.Statuses {
		switch commitStatus.State {
		case "success":
		case "failure", "error":
			return "failure"
		case "pending":
			pending = true
		default:
			return "failure"
		}
	}
	if pending {
		return "pending"
	}
	return "success"
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
	request.Header.Set("X-GitHub-Api-Version", githubAPIVersion)
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

func hiddenMarker(request pluginsdk.TaskRequest) string {
	digest := sha256.Sum256([]byte(request.IdempotencyKey))
	return fmt.Sprintf("<!-- orchigram:run=%s;node=%s;idempotency=%x -->", request.RunUID, request.NodeID, digest)
}

func decodeStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode configuration: %w", err)
	}
	return nil
}

package githubplugin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIssueEventWithoutEmbeddedIssueOrURLIsRejected(t *testing.T) {
	t.Parallel()
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `[{"id":15,"event":"labeled","created_at":"2026-08-08T10:00:00Z","label":{"name":"orchigram:ready"}}]`
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}
	runtime := &Runtime{Client: client}
	_, err := runtime.listReadyEvents(context.Background(), watchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: "https://api.example.invalid", TokenSecret: "token"}, Label: "orchigram:ready"}, []byte("fixture-token"), 0, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "neither an embedded issue nor issue_url") {
		t.Fatalf("provider error=%v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func TestPollingFixturesCoverPaginationRateLimitAndStableOrder(t *testing.T) {
	t.Parallel()
	var server *httptest.Server
	var mu sync.Mutex
	eventRequests := 0
	issueRequests := 0
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer fixture-token" {
			http.Error(writer, `{"message":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		if request.Header.Get("X-GitHub-Api-Version") != githubAPIVersion {
			http.Error(writer, `{"message":"wrong API version"}`, http.StatusBadRequest)
			return
		}
		switch request.URL.Path {
		case "/repos/acme/widget/issues/events":
			mu.Lock()
			eventRequests++
			attempt := eventRequests
			mu.Unlock()
			if attempt == 1 {
				writer.Header().Set("Retry-After", "0")
				writer.WriteHeader(http.StatusTooManyRequests)
				_, _ = writer.Write([]byte(`{"message":"slow down"}`))
				return
			}
			writer.Header().Set("Content-Type", "application/json")
			if request.URL.Query().Get("page") == "2" {
				_, _ = fmt.Fprintf(writer, `[{"id":11,"event":"labeled","issue_url":%q,"created_at":"2026-08-08T10:00:00Z","label":{"name":"orchigram:ready"}}]`, server.URL+"/repos/acme/widget/issues/42")
				return
			}
			writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/issues/events?page=2>; rel=\"next\"")
			_, _ = fmt.Fprintf(writer, `[{"id":12,"event":"labeled","issue":{"number":43,"title":"second","body":"body","html_url":"https://example.invalid/43","state":"open"},"issue_url":%q,"created_at":"2026-08-08T11:00:00Z","label":{"name":"orchigram:ready"}},{"id":9,"event":"labeled","issue_url":%q,"created_at":"2026-08-08T09:00:00Z","label":{"name":"orchigram:ready"}}]`, server.URL+"/repos/acme/widget/issues/43", server.URL+"/repos/acme/widget/issues/40")
		case "/repos/acme/widget/issues/42":
			mu.Lock()
			issueRequests++
			mu.Unlock()
			_, _ = writer.Write([]byte(`{"number":42,"title":"first","body":"body","html_url":"https://example.invalid/42","state":"open"}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	runtime := &Runtime{Client: server.Client()}
	events, err := runtime.listReadyEvents(context.Background(), watchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: server.URL, TokenSecret: "token"}, Label: "orchigram:ready"}, []byte("fixture-token"), 10, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].ID != 11 || events[0].Issue.Number != 42 || events[1].ID != 12 || events[1].Issue.Number != 43 {
		t.Fatalf("events=%+v", events)
	}
	if eventRequests != 3 {
		t.Fatalf("event requests=%d, expected rate-limit retry plus two pages", eventRequests)
	}
	if issueRequests != 1 {
		t.Fatalf("issue detail requests=%d, expected only issue_url fallback", issueRequests)
	}
}

func TestInitialSubscriptionFiltersEventsBeforeDurableActivation(t *testing.T) {
	t.Parallel()
	activation := time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`[
          {"id":101,"event":"labeled","created_at":"2026-08-08T10:00:00Z","label":{"name":"orchigram:ready"},"issue":{"number":1,"title":"historical","body":"old","state":"closed"}},
          {"id":102,"event":"labeled","created_at":"2026-08-08T11:00:00Z","label":{"name":"orchigram:ready"},"issue":{"number":7,"title":"new","body":"release","state":"open"}}
        ]`))
	}))
	defer server.Close()
	runtime := &Runtime{Client: server.Client()}
	config := watchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: server.URL, TokenSecret: "token"}, Label: "orchigram:ready"}
	events, err := runtime.listReadyEvents(context.Background(), config, []byte("fixture-token"), 0, activation)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].ID != 102 || events[0].Issue.Number != 7 {
		t.Fatalf("initial events=%+v", events)
	}
	replayed, err := runtime.listReadyEvents(context.Background(), config, []byte("fixture-token"), 0, time.Time{})
	if err != nil || len(replayed) != 2 {
		t.Fatalf("explicit replay events=%+v err=%v", replayed, err)
	}
}

func TestInitialSubscriptionUsesBoundedOverlapForSecondPrecisionAndClockSkew(t *testing.T) {
	t.Parallel()
	activation := time.Date(2026, 8, 8, 10, 30, 0, 900_000_000, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`[
          {"id":201,"event":"labeled","created_at":"2026-08-08T10:28:59Z","label":{"name":"orchigram:ready"},"issue":{"number":1,"title":"outside overlap","state":"open"}},
          {"id":202,"event":"labeled","created_at":"2026-08-08T10:29:30Z","label":{"name":"orchigram:ready"},"issue":{"number":2,"title":"clock skew","state":"open"}},
          {"id":203,"event":"labeled","created_at":"2026-08-08T10:30:00Z","label":{"name":"orchigram:ready"},"issue":{"number":3,"title":"same second","state":"open"}}
        ]`))
	}))
	defer server.Close()
	runtime := &Runtime{Client: server.Client()}
	config := watchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: server.URL, TokenSecret: "token"}, Label: "orchigram:ready"}
	events, err := runtime.listReadyEvents(context.Background(), config, []byte("fixture-token"), 0, activation)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].ID != 202 || events[1].ID != 203 {
		t.Fatalf("overlap events=%+v", events)
	}
}

func TestProviderActivationFailsClosedForOlderHost(t *testing.T) {
	t.Parallel()
	if _, err := providerActivation(&pluginv1alpha1.WatchStart{}, false, false); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing activation error=%v", err)
	}
	activation := time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC)
	actual, err := providerActivation(&pluginv1alpha1.WatchStart{ActivatedAt: timestamppb.New(activation)}, false, false)
	if err != nil || !actual.Equal(activation) {
		t.Fatalf("activation=%s err=%v", actual, err)
	}
	for name, test := range map[string]struct {
		replay    bool
		hasCursor bool
	}{
		"explicit replay": {replay: true},
		"durable cursor":  {hasCursor: true},
	} {
		t.Run(name, func(t *testing.T) {
			actual, err := providerActivation(&pluginv1alpha1.WatchStart{}, test.replay, test.hasCursor)
			if err != nil || !actual.IsZero() {
				t.Fatalf("activation=%s err=%v", actual, err)
			}
		})
	}
}

func TestReviewPollingSelectsManagedPullsAndOrdersStableSubmittedReviews(t *testing.T) {
	t.Parallel()
	marker := func(run string) string {
		return "<!-- orchigram:run=" + run + ";node=create-pr;idempotency=" + strings.Repeat("a", 64) + " -->"
	}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/repos/acme/widget/pulls":
			if request.URL.Query().Get("state") != "all" {
				t.Errorf("pull state=%q", request.URL.Query().Get("state"))
			}
			if request.URL.Query().Get("page") == "2" {
				_, _ = fmt.Fprintf(writer, `[{"number":8,"html_url":"https://example.invalid/pull/8","body":%q,"head":{"sha":"head-8"}}]`, marker("run-eight"))
				return
			}
			writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/pulls?page=2&state=all>; rel=\"next\"")
			_, _ = fmt.Fprintf(writer, `[
                  {"number":6,"html_url":"https://example.invalid/pull/6","body":"ordinary pull request","head":{"sha":"head-6"}},
                  {"number":7,"html_url":"https://example.invalid/pull/7","state":"closed","body":%q,"head":{"sha":"head-7"}},
                  {"number":9,"html_url":"https://example.invalid/pull/9","body":"<!-- orchigram:run=broken -->","head":{"sha":"head-9"}}
                ]`, marker("run-seven"))
		case "/repos/acme/widget/pulls/7/reviews":
			if request.URL.Query().Get("page") == "2" {
				_, _ = writer.Write([]byte(`[{"id":22,"state":"CHANGES_REQUESTED","body":"rework","submitted_at":"2026-08-08T11:00:00Z","commit_id":"commit-7","user":{"login":"reviewer"}}]`))
				return
			}
			writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/pulls/7/reviews?page=2>; rel=\"next\"")
			_, _ = writer.Write([]byte(`[
                  {"id":10,"state":"APPROVED","body":"historical","submitted_at":"2026-08-08T10:00:00Z","commit_id":"old","user":{"login":"reviewer"}},
                  {"id":23,"state":"COMMENTED","body":"note","submitted_at":"2026-08-08T11:01:00Z","commit_id":"commit-7","user":{"login":"reviewer"}},
                  {"id":24,"state":"PENDING","body":"draft","submitted_at":null,"commit_id":"commit-7","user":{"login":"reviewer"}}
                ]`))
		case "/repos/acme/widget/pulls/8/reviews":
			_, _ = writer.Write([]byte(`[{"id":21,"state":"APPROVED","body":"ship","submitted_at":"2026-08-08T11:00:00Z","commit_id":"commit-8","user":{"login":"approver"}}]`))
		case "/repos/acme/widget/pulls/8/reviews/21/comments":
			_, _ = writer.Write([]byte(`[]`))
		case "/repos/acme/widget/pulls/7/reviews/22/comments":
			if request.URL.Query().Get("page") == "2" {
				_, _ = writer.Write([]byte(`[
                  {"id":502,"body":"keep this deterministic","path":"internal/run.go","line":null,"original_line":18,"side":"RIGHT","subject_type":"line","html_url":"https://example.invalid/comment/502"},
                  {"id":503,"body":"document this file","path":"internal/run.go","line":null,"original_line":null,"side":"","subject_type":"file","html_url":"https://example.invalid/comment/503"}
                ]`))
				return
			}
			writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/pulls/7/reviews/22/comments?page=2>; rel=\"next\"")
			_, _ = writer.Write([]byte(`[{"id":501,"body":"handle the retry","path":"internal/run.go","line":12,"side":"RIGHT","html_url":"https://example.invalid/comment/501"}]`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	runtime := &Runtime{Client: server.Client()}
	config := reviewWatchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: server.URL, TokenSecret: "token"}}
	events, err := runtime.listSubmittedReviews(context.Background(), config, []byte("fixture-token"), reviewCursor{}, time.Date(2026, 8, 8, 10, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Review.ID != 21 || events[0].RunUID != "run-eight" || events[1].Review.ID != 22 || events[1].RunUID != "run-seven" {
		t.Fatalf("review events=%+v", events)
	}
	if len(events[1].Comments) != 3 || events[1].Comments[0].ID != 501 || events[1].Comments[1].Line == nil || *events[1].Comments[1].Line != 18 || events[1].Comments[2].Line != nil || events[1].Comments[2].SubjectType != "file" {
		t.Fatalf("review comments=%+v", events[1].Comments)
	}
	encodedComments, err := json.Marshal(events[1].Comments)
	if err != nil || !strings.Contains(string(encodedComments), `"line":null`) || !strings.Contains(string(encodedComments), `"subject_type":"file"`) {
		t.Fatalf("encoded comments=%s err=%v", encodedComments, err)
	}
	resumed, err := runtime.listSubmittedReviews(context.Background(), config, []byte("fixture-token"), events[0].Cursor, time.Time{})
	if err != nil || len(resumed) != 1 || resumed[0].Review.ID != 22 {
		t.Fatalf("resumed reviews=%+v err=%v", resumed, err)
	}
	completed, err := runtime.listSubmittedReviews(context.Background(), config, []byte("fixture-token"), events[1].Cursor, time.Time{})
	if err != nil || len(completed) != 0 {
		t.Fatalf("completed cursor replay=%+v err=%v", completed, err)
	}
	encoded, err := encodeReviewCursor(events[1].Cursor)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := parseReviewCursor(encoded)
	if err != nil || decoded.ReviewID != 22 || !decoded.SubmittedAt.Equal(events[1].Cursor.SubmittedAt) {
		t.Fatalf("decoded cursor=%+v err=%v", decoded, err)
	}
}

func TestReviewMarkerAndCursorValidationFailClosed(t *testing.T) {
	t.Parallel()
	valid := "prefix <!-- orchigram:run=run-123;node=create-pr;idempotency=" + strings.Repeat("f", 64) + " --> suffix"
	if runUID, ok := targetRunFromMarker(valid); !ok || runUID != "run-123" {
		t.Fatalf("marker run=%q ok=%t", runUID, ok)
	}
	for _, value := range []string{"", "<!-- orchigram:run=run-123;node=create-pr;idempotency=short -->", "<!-- orchigram:run=<bad>;node=create-pr;idempotency=" + strings.Repeat("f", 64) + " -->"} {
		if runUID, ok := targetRunFromMarker(value); ok || runUID != "" {
			t.Fatalf("invalid marker %q resolved run=%q", value, runUID)
		}
	}
	for _, value := range []string{`{}`, `{"submittedAt":"2026-08-08T11:00:00Z","reviewID":0}`, `not-json`} {
		if _, err := parseReviewCursor(value); err == nil {
			t.Fatalf("invalid cursor %q was accepted", value)
		}
	}
}

func TestReviewPollingFailsClosedAtPaginationBound(t *testing.T) {
	t.Parallel()
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests++
		header := http.Header{"Content-Type": []string{"application/json"}, "Link": []string{"<https://api.example.invalid/repos/acme/widget/pulls?page=next>; rel=\"next\""}}
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
	})}
	runtime := &Runtime{Client: client}
	config := reviewWatchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: "https://api.example.invalid", TokenSecret: "token"}}
	_, err := runtime.listSubmittedReviews(context.Background(), config, []byte("fixture-token"), reviewCursor{}, time.Time{})
	if status.Code(err) != codes.ResourceExhausted || requests != 100 {
		t.Fatalf("pagination requests=%d error=%v", requests, err)
	}
}

func TestReviewWatchEmitsTargetRunAndAcknowledgesStableReview(t *testing.T) {
	t.Parallel()
	marker := "<!-- orchigram:run=run-review;node=create-pr;idempotency=" + strings.Repeat("b", 64) + " -->"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/repos/acme/widget/pulls":
			_, _ = fmt.Fprintf(writer, `[{"number":12,"html_url":"https://example.invalid/pull/12","body":%q,"head":{"sha":"head-12"}}]`, marker)
		case "/repos/acme/widget/pulls/12/reviews":
			_, _ = writer.Write([]byte(`[{"id":301,"state":"CHANGES_REQUESTED","body":"fix it","submitted_at":"2026-08-08T12:00:00Z","commit_id":"commit-12","user":{"login":"reviewer"}}]`))
		case "/repos/acme/widget/pulls/12/reviews/301/comments":
			_, _ = writer.Write([]byte(`[{"id":401,"body":"fix this line","path":"internal/task.go","line":44,"side":"RIGHT","html_url":"https://example.invalid/comment/401"}]`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	config, err := json.Marshal(reviewWatchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: server.URL, TokenSecret: "token"}, PollInterval: "1h", ReplayExisting: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stream := &reviewWatchStream{ctx: ctx, cancel: cancel, start: &pluginv1alpha1.WatchStart{
		Source: "github.reviews", ConfigJson: config, Secrets: map[string][]byte{"token": []byte("fixture-token")},
	}}
	err = (&Runtime{Client: server.Client()}).Watch(stream)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("watch error=%v", err)
	}
	if len(stream.events) != 1 {
		t.Fatalf("events=%+v", stream.events)
	}
	event := stream.events[0]
	if event.GetProviderEventId() != "github:acme/widget:pull-review:301" || event.GetTargetRunUid() != "run-review" || event.GetCursor() == "" || !strings.Contains(string(event.GetPayloadJson()), `"state":"CHANGES_REQUESTED"`) || !strings.Contains(string(event.GetPayloadJson()), `"fix this line"`) {
		t.Fatalf("review event=%+v payload=%s", event, event.GetPayloadJson())
	}
}

func TestCommitChecksWaitForExactHeadAcrossChecksAndStatuses(t *testing.T) {
	t.Parallel()
	headSHA := strings.Repeat("a", 40)
	var mu sync.Mutex
	checkPolls := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer fixture-token" {
			http.Error(writer, `{"message":"unauthorized"}`, http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/repos/acme/widget/pulls/17":
			_, _ = fmt.Fprintf(writer, `{"state":"open","head":{"sha":%q}}`, headSHA)
		case "/repos/acme/widget/commits/" + headSHA + "/check-runs":
			if request.URL.Query().Get("page") == "2" {
				_, _ = fmt.Fprintf(writer, `{"check_runs":[{"name":"lint","status":"completed","conclusion":"success","details_url":"https://example.invalid/lint","head_sha":%q}]}`, headSHA)
				return
			}
			mu.Lock()
			checkPolls++
			poll := checkPolls
			mu.Unlock()
			writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/commits/"+headSHA+"/check-runs?filter=latest&per_page=100&page=2>; rel=\"next\"")
			statusValue, conclusion := "queued", ""
			if poll >= 2 {
				statusValue, conclusion = "completed", "success"
			}
			_, _ = fmt.Fprintf(writer, `{"check_runs":[{"name":"test","status":%q,"conclusion":%q,"details_url":"https://example.invalid/test","head_sha":%q}]}`, statusValue, conclusion, headSHA)
		case "/repos/acme/widget/commits/" + headSHA + "/status":
			mu.Lock()
			poll := checkPolls
			mu.Unlock()
			state := "pending"
			if poll >= 2 {
				state = "success"
			}
			_, _ = fmt.Fprintf(writer, `{"state":%q,"sha":%q,"statuses":[{"context":"legacy/build","state":%q,"description":"fixture","target_url":"https://example.invalid/status"}]}`, state, headSHA, state)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	runtime := &Runtime{Client: server.Client()}
	request := executeRequest(t, "run-checks", "wait-checks", "github.commit.checks.wait", map[string]any{
		"owner": "acme", "repository": "widget", "apiBase": server.URL, "tokenSecret": "token", "ref": headSHA,
		"pullNumber": 17, "required": []string{"test", "lint"}, "pollInterval": "1ms",
	})
	sink := &recordingSink{}
	output, err := runtime.Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := output.(checkSnapshot)
	if !ok || snapshot.State != "success" || snapshot.HeadSHA != headSHA || len(snapshot.Checks) != 2 || len(snapshot.Statuses) != 2 {
		t.Fatalf("check snapshot=%#v", output)
	}
	if checkPolls != 2 || !sink.has("github.checks.pending") || !sink.has("github.checks.succeeded") {
		t.Fatalf("polls=%d events=%v", checkPolls, sink.events)
	}
}

func TestCommitCheckPolicyFailsClosed(t *testing.T) {
	t.Parallel()
	if state := evaluateCheckSnapshot(checkSnapshot{Checks: []checkSummary{{Name: "test", Status: "completed", Conclusion: "failure"}}}, []string{"test"}); state != "failure" {
		t.Fatalf("failed check state=%q", state)
	}
	if state := evaluateCheckSnapshot(checkSnapshot{Checks: []checkSummary{{Name: "test", Status: "completed", Conclusion: "success"}}, Statuses: []statusSummary{{Context: "legacy", State: "error"}}}, []string{"test"}); state != "failure" {
		t.Fatalf("failed status state=%q", state)
	}
	if state := evaluateCheckSnapshot(checkSnapshot{}, []string{"missing"}); state != "pending" {
		t.Fatalf("missing check state=%q", state)
	}
	runtime := &Runtime{}
	for _, config := range []string{
		`{"owner":"acme","repository":"widget","tokenSecret":"token","ref":"main","pullNumber":1,"required":["test"]}`,
		`{"owner":"acme","repository":"widget","tokenSecret":"token","ref":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pullNumber":1,"required":["test","test"]}`,
	} {
		issues := runtime.ValidateAction(context.Background(), "github.commit.checks.wait", json.RawMessage(config))
		if len(issues) != 1 {
			t.Fatalf("config=%s issues=%+v", config, issues)
		}
	}
}

func TestCommitChecksReturnStaleBeforePollingOldHead(t *testing.T) {
	t.Parallel()
	approvedSHA := strings.Repeat("a", 40)
	currentSHA := strings.Repeat("b", 40)
	checkRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/repos/acme/widget/pulls/17" {
			_, _ = fmt.Fprintf(writer, `{"state":"open","head":{"sha":%q}}`, currentSHA)
			return
		}
		checkRequests++
		http.NotFound(writer, request)
	}))
	defer server.Close()
	request := executeRequest(t, "run-stale", "wait-checks", "github.commit.checks.wait", map[string]any{
		"owner": "acme", "repository": "widget", "apiBase": server.URL, "tokenSecret": "token", "ref": approvedSHA,
		"pullNumber": 17, "required": []string{"test"},
	})
	sink := &recordingSink{}
	output, err := (&Runtime{Client: server.Client()}).Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := output.(checkSnapshot)
	if !ok || snapshot.State != "stale" || snapshot.HeadSHA != approvedSHA || checkRequests != 0 || !sink.has("github.checks.stale") {
		t.Fatalf("output=%#v checkRequests=%d events=%v", output, checkRequests, sink.events)
	}
}

func TestCommitChecksRecheckPullHeadAfterSuccessfulSnapshot(t *testing.T) {
	t.Parallel()
	approvedSHA := strings.Repeat("a", 40)
	newSHA := strings.Repeat("b", 40)
	var mu sync.Mutex
	pullRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/repos/acme/widget/pulls/17":
			mu.Lock()
			pullRequests++
			currentRequest := pullRequests
			mu.Unlock()
			head := approvedSHA
			if currentRequest >= 2 {
				head = newSHA
			}
			_, _ = fmt.Fprintf(writer, `{"state":"open","head":{"sha":%q}}`, head)
		case "/repos/acme/widget/commits/" + approvedSHA + "/check-runs":
			_, _ = fmt.Fprintf(writer, `{"check_runs":[{"name":"test","status":"completed","conclusion":"success","head_sha":%q}]}`, approvedSHA)
		case "/repos/acme/widget/commits/" + approvedSHA + "/status":
			_, _ = fmt.Fprintf(writer, `{"state":"success","sha":%q,"statuses":[]}`, approvedSHA)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	request := executeRequest(t, "run-race", "wait-checks", "github.commit.checks.wait", map[string]any{
		"owner": "acme", "repository": "widget", "apiBase": server.URL, "tokenSecret": "token", "ref": approvedSHA,
		"pullNumber": 17, "required": []string{"test"},
	})
	sink := &recordingSink{}
	output, err := (&Runtime{Client: server.Client()}).Execute(context.Background(), request, sink)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, ok := output.(checkSnapshot)
	mu.Lock()
	observedPullRequests := pullRequests
	mu.Unlock()
	if !ok || snapshot.State != "stale" || observedPullRequests != 2 || !sink.has("github.checks.stale") || sink.has("github.checks.succeeded") {
		t.Fatalf("output=%#v pullRequests=%d events=%v", output, observedPullRequests, sink.events)
	}
}

type recordingSink struct {
	events []string
}

func (s *recordingSink) Emit(eventType string, _ any) error {
	s.events = append(s.events, eventType)
	return nil
}

func (*recordingSink) Log(string, []byte) error { return nil }

func (s *recordingSink) has(eventType string) bool {
	for _, current := range s.events {
		if current == eventType {
			return true
		}
	}
	return false
}

type reviewWatchStream struct {
	ctx    context.Context
	cancel context.CancelFunc
	start  *pluginv1alpha1.WatchStart
	events []*pluginv1alpha1.TriggerEvent
	recvs  int
}

func (s *reviewWatchStream) Send(event *pluginv1alpha1.TriggerEvent) error {
	s.events = append(s.events, event)
	return nil
}

func (s *reviewWatchStream) Recv() (*pluginv1alpha1.TriggerCommand, error) {
	if s.recvs == 0 {
		s.recvs++
		return &pluginv1alpha1.TriggerCommand{Value: &pluginv1alpha1.TriggerCommand_Start{Start: s.start}}, nil
	}
	if len(s.events) == 0 {
		return nil, errors.New("acknowledgement requested before an event was sent")
	}
	event := s.events[len(s.events)-1]
	s.cancel()
	return &pluginv1alpha1.TriggerCommand{Value: &pluginv1alpha1.TriggerCommand_Ack{Ack: &pluginv1alpha1.TriggerAck{ProviderEventId: event.GetProviderEventId(), Cursor: event.GetCursor()}}}, nil
}

func (*reviewWatchStream) SetHeader(metadata.MD) error  { return nil }
func (*reviewWatchStream) SendHeader(metadata.MD) error { return nil }
func (*reviewWatchStream) SetTrailer(metadata.MD)       {}
func (s *reviewWatchStream) Context() context.Context   { return s.ctx }
func (*reviewWatchStream) SendMsg(any) error            { return nil }
func (*reviewWatchStream) RecvMsg(any) error            { return nil }

func TestCommentAndPullRequestReconcileByIdempotencyMarker(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	comments := []map[string]any{}
	pulls := []map[string]any{}
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/repos/acme/widget/issues/42/comments" && request.Method == http.MethodGet:
			if request.URL.Query().Get("page") == "2" {
				_ = json.NewEncoder(writer).Encode(comments)
			} else {
				writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/issues/42/comments?page=2>; rel=\"next\"")
				_, _ = writer.Write([]byte(`[]`))
			}
		case request.URL.Path == "/repos/acme/widget/issues/42/comments" && request.Method == http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(request.Body).Decode(&body)
			created := map[string]any{"id": 1001, "html_url": "https://example.invalid/comment/1001", "body": body["body"]}
			comments = append(comments, created)
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(created)
		case request.URL.Path == "/repos/acme/widget/pulls" && request.Method == http.MethodGet:
			if request.URL.Query().Get("page") == "2" {
				_ = json.NewEncoder(writer).Encode(pulls)
			} else {
				writer.Header().Set("Link", "<"+server.URL+"/repos/acme/widget/pulls?page=2>; rel=\"next\"")
				_, _ = writer.Write([]byte(`[]`))
			}
		case request.URL.Path == "/repos/acme/widget/pulls" && request.Method == http.MethodPost:
			var body map[string]any
			_ = json.NewDecoder(request.Body).Decode(&body)
			number := len(pulls) + 7
			created := map[string]any{"number": number, "html_url": fmt.Sprintf("https://example.invalid/pull/%d", number), "body": body["body"], "head": map[string]any{"ref": body["head"]}}
			pulls = append(pulls, created)
			writer.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(writer).Encode(created)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	runtime := &Runtime{Client: server.Client()}
	commentRequest := executeRequest(t, "run-123", "publish-plan", "github.issue.comment", map[string]any{"owner": "acme", "repository": "widget", "apiBase": server.URL, "tokenSecret": "token", "number": 42, "body": "Plan"})
	firstComment, err := runtime.issueComment(context.Background(), commentRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondComment, err := runtime.issueComment(context.Background(), commentRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(comments) != 1 || firstComment.(map[string]any)["reconciled"] != false || secondComment.(map[string]any)["reconciled"] != true || !strings.Contains(comments[0]["body"].(string), "idempotency=") {
		t.Fatalf("comments=%+v first=%+v second=%+v", comments, firstComment, secondComment)
	}
	distinctComment := commentRequest
	distinctComment.IdempotencyKey = "stable/iteration-2"
	thirdComment, err := runtime.issueComment(context.Background(), distinctComment)
	if err != nil {
		t.Fatal(err)
	}
	if len(comments) != 2 || thirdComment.(map[string]any)["reconciled"] != false {
		t.Fatalf("distinct-key comments=%+v third=%+v", comments, thirdComment)
	}

	pullRequest := executeRequest(t, "run-123", "create-pr", "github.pr.ensure", map[string]any{"owner": "acme", "repository": "widget", "apiBase": server.URL, "tokenSecret": "token", "head": "orchigram/issue-42-run123", "base": "main", "title": "Implement #42", "body": "Automated change"})
	firstPull, err := runtime.ensurePullRequest(context.Background(), pullRequest)
	if err != nil {
		t.Fatal(err)
	}
	secondPull, err := runtime.ensurePullRequest(context.Background(), pullRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 1 || firstPull.(map[string]any)["reconciled"] != false || secondPull.(map[string]any)["reconciled"] != true {
		t.Fatalf("pulls=%+v first=%+v second=%+v", pulls, firstPull, secondPull)
	}
	distinctPull := pullRequest
	distinctPull.IdempotencyKey = "stable/iteration-2"
	var distinctConfig map[string]any
	if err := json.Unmarshal(distinctPull.Config, &distinctConfig); err != nil {
		t.Fatal(err)
	}
	distinctConfig["head"] = "orchigram/issue-42-iteration-2"
	distinctPull.Config, err = json.Marshal(distinctConfig)
	if err != nil {
		t.Fatal(err)
	}
	thirdPull, err := runtime.ensurePullRequest(context.Background(), distinctPull)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 2 || thirdPull.(map[string]any)["reconciled"] != false {
		t.Fatalf("distinct-key pulls=%+v third=%+v", pulls, thirdPull)
	}
	sameBranch := distinctPull
	sameBranch.IdempotencyKey = "stable/iteration-3"
	fourthPull, err := runtime.ensurePullRequest(context.Background(), sameBranch)
	if err != nil {
		t.Fatal(err)
	}
	if len(pulls) != 2 || fourthPull.(map[string]any)["reconciled"] != true || fourthPull.(map[string]any)["number"] != 8 {
		t.Fatalf("same-branch pulls=%+v fourth=%+v", pulls, fourthPull)
	}
}

func TestHiddenMarkerUsesLogicalIdempotencyKey(t *testing.T) {
	t.Parallel()
	request := pluginsdk.TaskRequest{RunUID: "run", NodeID: "repeat", IdempotencyKey: "run/repeat/iteration-1"}
	first := hiddenMarker(request)
	if retry := hiddenMarker(request); retry != first {
		t.Fatalf("equal-key retry marker changed: %q != %q", retry, first)
	}
	request.IdempotencyKey = "run/repeat/iteration-2"
	if next := hiddenMarker(request); next == first {
		t.Fatalf("distinct iteration keys shared marker %q", next)
	}
}

func executeRequest(t *testing.T, runUID, nodeID, action string, config map[string]any) pluginsdk.TaskRequest {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return pluginsdk.TaskRequest{RequestID: "request", RunUID: runUID, NodeID: nodeID, IdempotencyKey: "stable", Action: action, Config: encoded, Secrets: map[string][]byte{"token": []byte("fixture-token")}}
}

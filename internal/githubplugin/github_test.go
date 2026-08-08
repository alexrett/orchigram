package githubplugin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pluginsdk "github.com/alexrett/orchigram/sdk/plugin"
)

func TestIssueEventWithoutEmbeddedIssueOrURLIsRejected(t *testing.T) {
	t.Parallel()
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		body := `[{"id":15,"event":"labeled","created_at":"2026-08-08T10:00:00Z","label":{"name":"orchigram:ready"}}]`
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})}
	runtime := &Runtime{Client: client}
	_, err := runtime.listReadyEvents(context.Background(), watchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: "https://api.example.invalid", TokenSecret: "token"}, Label: "orchigram:ready"}, []byte("fixture-token"), 0)
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
	events, err := runtime.listReadyEvents(context.Background(), watchConfig{repositoryConfig: repositoryConfig{Owner: "acme", Repository: "widget", APIBase: server.URL, TokenSecret: "token"}, Label: "orchigram:ready"}, []byte("fixture-token"), 10)
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

func TestCommentAndPullRequestReconcileByMarkerAndBranch(t *testing.T) {
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
			created := map[string]any{"number": 7, "html_url": "https://example.invalid/pull/7", "body": body["body"], "head": map[string]any{"ref": body["head"]}}
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
	if len(comments) != 1 || firstComment.(map[string]any)["reconciled"] != false || secondComment.(map[string]any)["reconciled"] != true || !strings.Contains(comments[0]["body"].(string), "orchigram:run=run-123") {
		t.Fatalf("comments=%+v first=%+v second=%+v", comments, firstComment, secondComment)
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
}

func executeRequest(t *testing.T, runUID, nodeID, action string, config map[string]any) pluginsdk.TaskRequest {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	return pluginsdk.TaskRequest{RequestID: "request", RunUID: runUID, NodeID: nodeID, IdempotencyKey: "stable", Action: action, Config: encoded, Secrets: map[string][]byte{"token": []byte("fixture-token")}}
}

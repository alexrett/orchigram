package examples

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
)

var slackCredentialPattern = regexp.MustCompile(`(?i)(?:https://)?hooks[.]slack[.]com/services/|T[A-Z0-9]{8,}/B[A-Z0-9]{8,}/[A-Za-z0-9_-]{20,}`)

func TestShippedResourcesAreStrictAndFlowsCompile(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "**", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	rootPaths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	paths = append(paths, rootPaths...)
	if len(paths) == 0 {
		t.Fatal("no example resources found")
	}
	for _, path := range paths {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, readErr := os.ReadFile(path) //nolint:gosec // Test reads repository-owned example paths.
			if readErr != nil {
				t.Fatal(readErr)
			}
			if strings.Contains(string(data), "https://") && strings.Contains(path, "teams") {
				t.Fatal("Teams example must not contain a webhook URL")
			}
			document, decodeErr := resource.DecodeStrict(data)
			if decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if document.Kind == "Flow" {
				flowResource, decodeFlowErr := resource.DecodeFlow(document.JSON)
				if decodeFlowErr != nil {
					t.Fatal(decodeFlowErr)
				}
				_, diagnostics := flow.NewCompiler(nil).Compile(flowResource)
				if len(diagnostics) != 0 {
					t.Fatalf("compile diagnostics: %+v", diagnostics)
				}
			}
		})
	}
}

func TestSlackExampleUsesOnlySecretRefAndValidMessageShape(t *testing.T) {
	examplesRoot := filepath.Join("..", "..", "examples")
	err := filepath.WalkDir(examplesRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(path) //nolint:gosec // Test scans repository-owned examples.
		if readErr != nil {
			return readErr
		}
		if slackCredentialPattern.Match(data) {
			t.Errorf("example %s contains a Slack webhook or credential-shaped value", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	flowPath := filepath.Join(examplesRoot, "slack", "weekday-flow.yaml")
	flowYAML, err := os.ReadFile(flowPath) //nolint:gosec // Test reads a repository-owned fixture.
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(flowYAML), "urlSecret: webhook") {
		t.Fatal("Slack Flow must resolve its URL through urlSecret")
	}
	if regexp.MustCompile(`(?m)^\s+url:`).Match(flowYAML) {
		t.Fatal("Slack Flow must never contain a literal url field")
	}
	document, err := resource.DecodeStrict(flowYAML)
	if err != nil {
		t.Fatal(err)
	}
	flowResource, err := resource.DecodeFlow(document.JSON)
	if err != nil {
		t.Fatal(err)
	}
	if flowResource.Spec.Policies.MaxParallel != 1 {
		t.Fatalf("maxParallel=%d", flowResource.Spec.Policies.MaxParallel)
	}
	var notify resource.FlowNode
	for _, node := range flowResource.Spec.Nodes {
		if node.ID == "notify" {
			notify = node
			break
		}
	}
	if notify.Uses != "http.request" || notify.Retry == nil || notify.Retry.Limit != 3 {
		t.Fatalf("notify node does not define three HTTP retries: %+v", notify)
	}
	backoff, err := time.ParseDuration(notify.Retry.Backoff)
	if err != nil || backoff < time.Second {
		t.Fatalf("notify retry backoff=%q err=%v", notify.Retry.Backoff, err)
	}
	if _, exists := notify.With["url"]; exists {
		t.Fatal("Slack HTTP configuration exposes url")
	}
	if notify.With["urlSecret"] != "webhook" {
		t.Fatalf("urlSecret=%v", notify.With["urlSecret"])
	}
	body, err := json.Marshal(notify.With["body"])
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	validateSlackExamplePayload(t, payload)
	mappings, ok := notify.With["mappings"].([]any)
	if !ok || len(mappings) != 2 {
		t.Fatalf("mappings=%#v", notify.With["mappings"])
	}
	wanted := map[string]bool{"/body/text": false, "/body/blocks/0/text/text": false}
	for _, item := range mappings {
		mapping, ok := item.(map[string]any)
		if !ok || mapping["from"] != "nodes.compose.text" {
			t.Fatalf("mapping=%#v", item)
		}
		path, ok := mapping["to"].(string)
		if !ok {
			t.Fatalf("mapping target=%#v", mapping["to"])
		}
		if _, exists := wanted[path]; !exists {
			t.Fatalf("unexpected mapping target %q", path)
		}
		wanted[path] = true
	}
	for path, found := range wanted {
		if !found {
			t.Errorf("missing mapping to %s", path)
		}
	}
}

func validateSlackExamplePayload(t *testing.T, payload map[string]any) {
	t.Helper()
	fallback, ok := payload["text"].(string)
	if !ok || strings.TrimSpace(fallback) == "" || utf8.RuneCountInString(fallback) > 4000 {
		t.Fatalf("invalid Slack fallback text %#v", payload["text"])
	}
	blocks, ok := payload["blocks"].([]any)
	if !ok || len(blocks) != 1 {
		t.Fatalf("invalid Slack blocks %#v", payload["blocks"])
	}
	section, ok := blocks[0].(map[string]any)
	if !ok || section["type"] != "section" {
		t.Fatalf("invalid Slack section %#v", blocks[0])
	}
	textObject, ok := section["text"].(map[string]any)
	if !ok || textObject["type"] != "plain_text" || textObject["emoji"] != true {
		t.Fatalf("invalid Slack plain-text object %#v", section["text"])
	}
	text, ok := textObject["text"].(string)
	if !ok || strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > 3000 {
		t.Fatalf("invalid Slack section text %#v", textObject["text"])
	}
}

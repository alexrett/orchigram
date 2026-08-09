package references

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

type fixtureLookup map[string]resource.Document

func (f fixtureLookup) Get(_ context.Context, kind, namespace, name string) (resource.Document, error) {
	if document, exists := f[kind+"/"+namespace+"/"+name]; exists {
		return document, nil
	}
	return resource.Document{}, store.ErrNotFound
}

type fixtureProvider struct {
	namespace   string
	diagnostics []flow.Diagnostic
}

type failingLookup struct{}

func (failingLookup) Get(context.Context, string, string, string) (resource.Document, error) {
	return resource.Document{}, errors.New("storage offline")
}

func (f *fixtureProvider) ValidateTriggerProvider(_ context.Context, namespace, _ string, _ map[string]any) []flow.Diagnostic {
	f.namespace = namespace
	return append([]flow.Diagnostic(nil), f.diagnostics...)
}

func TestResolverUsesResourceNamespaceAndProviderVocabulary(t *testing.T) {
	t.Parallel()
	flowDocument := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: target, namespace: team-a}
spec: {nodes: [{id: done, uses: core.noop}]}
`)
	secretDocument := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: token, namespace: team-a}
spec: {backend: env, key: ORCHIGRAM_TEST_TOKEN}
`)
	trigger := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: ready, namespace: team-a}
spec:
  flow: target
  provider:
    plugin: github
    config: {secretRefs: {token: token}}
`)
	provider := &fixtureProvider{diagnostics: []flow.Diagnostic{{Path: "config.owner", Code: "required", Message: "config does not satisfy the installed provider schema"}}}
	resolver := New(fixtureLookup{
		"Flow/team-a/target": flowDocument, "SecretRef/team-a/token": secretDocument,
	}, provider)
	diagnostics := resolver.Diagnostics(context.Background(), trigger)
	if provider.namespace != "team-a" {
		t.Fatalf("provider namespace=%q", provider.namespace)
	}
	if len(diagnostics) != 1 || diagnostics[0].Path != "spec.provider.config.owner" || diagnostics[0].Code != "required" {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
}

func TestResolverRejectsCrossNamespaceAndAliasReferences(t *testing.T) {
	t.Parallel()
	profile := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: worker, namespace: team-a}
spec:
  type: command
  executable: fake-agent
  secretRefs: [GITHUB_TOKEN=token]
`)
	defaultSecret := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: SecretRef
metadata: {name: token}
spec: {backend: env, key: ORCHIGRAM_TEST_TOKEN}
`)
	resolver := New(fixtureLookup{"SecretRef/default/token": defaultSecret}, nil)
	diagnostics := resolver.Diagnostics(context.Background(), profile)
	if len(diagnostics) != 1 || diagnostics[0].Path != "spec.secretRefs[0]" || diagnostics[0].Code != "reference_not_found" {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
	status := Status(profile, diagnostics)
	encoded, err := json.Marshal(status)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || !json.Valid(encoded) {
		t.Fatalf("status=%s", encoded)
	}
	conditions := status["conditions"].([]any)
	condition := conditions[0].(map[string]any)
	if condition["status"] != "False" || condition["reason"] != "MissingReference" {
		t.Fatalf("status=%+v", status)
	}
}

func TestResolverDoesNotMisreportOperationalFailureAsMissing(t *testing.T) {
	t.Parallel()
	profile := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: AgentProfile
metadata: {name: worker}
spec: {type: command, executable: fake-agent, secretRefs: [token]}
`)
	diagnostics := New(failingLookup{}, nil).Diagnostics(context.Background(), profile)
	if len(diagnostics) != 1 || diagnostics[0].Code != "reference_unavailable" || diagnostics[0].Message != "SecretRef reference state is temporarily unavailable" {
		t.Fatalf("diagnostics=%+v", diagnostics)
	}
}

func TestResolverValidatesSignalDeliveryAgainstCoreEventNode(t *testing.T) {
	t.Parallel()
	flowDocument := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: target}
spec:
  nodes:
    - {id: review, uses: core.event}
    - {id: implement, uses: core.noop}
`)
	lookup := fixtureLookup{"Flow/default/target": flowDocument}
	for name, node := range map[string]string{"valid": "review", "wrong action": "implement", "missing": "absent"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			trigger := decodeReferenceDocument(t, `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: reviews}
spec:
  flow: target
  provider: {plugin: github}
  delivery: {mode: signal, node: `+node+`}
`)
			diagnostics := New(lookup, &fixtureProvider{}).Diagnostics(context.Background(), trigger)
			if name == "valid" {
				if len(diagnostics) != 0 {
					t.Fatalf("valid diagnostics=%+v", diagnostics)
				}
				return
			}
			if len(diagnostics) != 1 || diagnostics[0].Path != "spec.delivery.node" {
				t.Fatalf("diagnostics=%+v", diagnostics)
			}
		})
	}
}

func decodeReferenceDocument(t *testing.T, source string) resource.Document {
	t.Helper()
	document, err := resource.DecodeStrict([]byte(source))
	if err != nil {
		t.Fatal(err)
	}
	return document
}

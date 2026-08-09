package resource

import (
	"encoding/json"
	"strings"
	"testing"
)

const validFlow = `apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata:
  name: approval-demo
spec:
  nodes:
    - id: prepare
      uses: core.noop
    - id: approve
      uses: core.approval
    - id: finish
      uses: core.noop
  edges:
    - from: prepare
      to: approve
    - from: approve
      to: finish
`

func TestDecodeStrictFlowDefaultsNamespace(t *testing.T) {
	t.Parallel()
	doc, err := DecodeStrict([]byte(validFlow))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Metadata.Namespace != DefaultNamespace {
		t.Fatalf("namespace = %q", doc.Metadata.Namespace)
	}
}

func TestDecodeStrictRejectsUnknownField(t *testing.T) {
	t.Parallel()
	_, err := DecodeStrict([]byte(strings.Replace(validFlow, "spec:\n", "spec:\n  surprise: true\n", 1)))
	if err == nil || !strings.Contains(err.Error(), "field surprise not found") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTriggerRequiresExactlyOneSource(t *testing.T) {
	t.Parallel()
	_, err := DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: bad}
spec:
  flow: x
  schedule: {cron: "0 9 * * 1-5"}
  webhook: {bearerSecretRef: hook}
`))
	if err == nil || !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTriggerSignalDeliveryRequiresProviderAndValidNode(t *testing.T) {
	t.Parallel()
	for name, document := range map[string]string{
		"schedule": `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: bad}
spec:
  flow: demo
  schedule: {cron: "0 9 * * 1-5"}
  delivery: {mode: signal, node: review}
`,
		"missing node": `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: bad}
spec:
  flow: demo
  provider: {plugin: github}
  delivery: {mode: signal}
`,
		"unknown mode": `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: bad}
spec:
  flow: demo
  provider: {plugin: github}
  delivery: {mode: resume, node: review}
`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := DecodeStrict([]byte(document)); err == nil {
				t.Fatal("invalid signal delivery was accepted")
			}
		})
	}
	valid, err := DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: reviews}
spec:
  flow: demo
  provider: {plugin: github}
  delivery: {mode: signal, node: wait_review}
`))
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := DecodeTrigger(valid.JSON)
	if err != nil || trigger.Spec.Delivery == nil || trigger.Spec.Delivery.Mode != "signal" || trigger.Spec.Delivery.Node != "wait_review" {
		t.Fatalf("trigger=%+v err=%v", trigger, err)
	}
}

func TestTriggerProviderSourceIsStrictAndProjected(t *testing.T) {
	t.Parallel()
	document, err := DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: reviews}
spec:
  flow: demo
  provider: {plugin: github, source: github.reviews}
  delivery: {mode: signal, node: review}
`))
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := DecodeTrigger(document.JSON)
	if err != nil || trigger.Spec.Provider == nil || trigger.Spec.Provider.Source != "github.reviews" {
		t.Fatalf("trigger=%+v err=%v", trigger, err)
	}
	if _, err := DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: bad}
spec:
  flow: demo
  provider: {plugin: github, source: " github.reviews "}
`)); err == nil || !strings.Contains(err.Error(), "surrounding whitespace") {
		t.Fatalf("source whitespace error=%v", err)
	}
}

func TestWithServerStatusRemovesClientProjection(t *testing.T) {
	t.Parallel()
	doc, err := DecodeStrict([]byte(strings.Replace(validFlow, "spec:\n", "status: {state: client-controlled}\nspec:\n", 1)))
	if err != nil {
		t.Fatal(err)
	}
	doc, err = doc.WithServerStatus(nil)
	if err != nil {
		t.Fatal(err)
	}
	var projection map[string]any
	if err := json.Unmarshal(doc.JSON, &projection); err != nil {
		t.Fatal(err)
	}
	if _, exists := projection["status"]; exists {
		t.Fatalf("client status survived normalization: %s", doc.JSON)
	}
}

func TestWithServerStatusPreservesUint64MetadataExactly(t *testing.T) {
	t.Parallel()
	doc, err := DecodeStrict([]byte(strings.Replace(validFlow, "metadata:\n  name: approval-demo", "metadata:\n  name: approval-demo\n  resourceVersion: 18446744073709551615", 1)))
	if err != nil {
		t.Fatal(err)
	}
	doc, err = doc.WithServerStatus(map[string]any{"phase": "Ready"})
	if err != nil {
		t.Fatal(err)
	}
	var projection struct {
		Metadata ObjectMeta `json:"metadata"`
	}
	if err := json.Unmarshal(doc.JSON, &projection); err != nil {
		t.Fatal(err)
	}
	if projection.Metadata.ResourceVersion != ^uint64(0) {
		t.Fatalf("resourceVersion=%d json=%s", projection.Metadata.ResourceVersion, doc.JSON)
	}
}

package resource

import (
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

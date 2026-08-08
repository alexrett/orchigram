package releasepack

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeSPDXIsDeterministicAndArtifactBound(t *testing.T) {
	first := []byte(`{"spdxVersion":"SPDX-2.3","dataLicense":"CC0-1.0","SPDXID":"SPDXRef-DOCUMENT","name":"release.tar.gz","documentNamespace":"https://anchore.example/random-one","creationInfo":{"created":"2026-01-01T00:00:00Z","creators":["Tool: syft"]},"packages":[{"name":"orchigram"}]}`)
	second := []byte(`{"packages":[{"name":"orchigram"}],"creationInfo":{"creators":["Tool: syft"],"created":"2026-08-08T14:00:00Z"},"documentNamespace":"https://anchore.example/random-two","name":"release.tar.gz","SPDXID":"SPDXRef-DOCUMENT","dataLicense":"CC0-1.0","spdxVersion":"SPDX-2.3"}`)
	artifact := []byte("immutable release archive")

	firstNormalized, err := NormalizeSPDX(first, artifact, "2026-08-08T14:25:49+02:00")
	if err != nil {
		t.Fatal(err)
	}
	secondNormalized, err := NormalizeSPDX(second, artifact, "2026-08-08T12:25:49Z")
	if err != nil {
		t.Fatal(err)
	}
	if string(firstNormalized) != string(secondNormalized) {
		t.Fatalf("equivalent documents normalized differently:\n%s\n%s", firstNormalized, secondNormalized)
	}

	var document map[string]any
	if err := json.Unmarshal(firstNormalized, &document); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact)
	wantNamespace := spdxNamespacePrefix + hex.EncodeToString(digest[:])
	if document["documentNamespace"] != wantNamespace {
		t.Fatalf("namespace=%v want=%s", document["documentNamespace"], wantNamespace)
	}
	creationInfo := document["creationInfo"].(map[string]any)
	if creationInfo["created"] != "2026-08-08T12:25:49Z" {
		t.Fatalf("created=%v", creationInfo["created"])
	}
	if !strings.Contains(string(firstNormalized), `"packages":[{"name":"orchigram"}]`) {
		t.Fatalf("package inventory was not retained: %s", firstNormalized)
	}

	different, err := NormalizeSPDX(first, []byte("different release archive"), "2026-08-08T12:25:49Z")
	if err != nil {
		t.Fatal(err)
	}
	if string(different) == string(firstNormalized) {
		t.Fatal("different artifact bytes produced the same normalized document")
	}
}

func TestNormalizeSPDXRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name     string
		document string
		date     string
	}{
		{name: "json", document: `{`, date: "2026-08-08T12:25:49Z"},
		{name: "trailing data", document: `{"spdxVersion":"SPDX-2.3","creationInfo":{}} true`, date: "2026-08-08T12:25:49Z"},
		{name: "version", document: `{"spdxVersion":"SPDX-2.2","creationInfo":{}}`, date: "2026-08-08T12:25:49Z"},
		{name: "creation info", document: `{"spdxVersion":"SPDX-2.3"}`, date: "2026-08-08T12:25:49Z"},
		{name: "date", document: `{"spdxVersion":"SPDX-2.3","creationInfo":{}}`, date: "not-a-date"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NormalizeSPDX([]byte(test.document), []byte("artifact"), test.date); err == nil {
				t.Fatal("expected normalization error")
			}
		})
	}
}

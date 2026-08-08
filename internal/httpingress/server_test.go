package httpingress

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

type staticSecrets map[string][]byte

func (s staticSecrets) ResolveSecret(_ context.Context, _ string, name string) ([]byte, error) {
	return s[name], nil
}

func TestWebhookAuthorizationLimitAndDurableDeduplication(t *testing.T) {
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = state.Close() }()
	document, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: generic-hook}
spec:
  flow: target
  webhook: {bearerSecretRef: hook-token}
`))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := state.Apply(context.Background(), document, store.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server, err := Listen(ctx, "127.0.0.1:0", state, staticSecrets{"hook-token": []byte("correct-token")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	url := "http://" + server.Address() + "/v1/hooks/" + applied.Metadata.UID

	for _, token := range []string{"", "wrong-token"} {
		response := webhookRequest(t, url, token, "", []byte(`{"event":1}`))
		if response.StatusCode != http.StatusUnauthorized {
			t.Fatalf("token %q status=%d", token, response.StatusCode)
		}
		_ = response.Body.Close()
	}
	oversize := append(bytes.Repeat([]byte(" "), maxBody), []byte(`{}`)...)
	response := webhookRequest(t, url, "correct-token", "", oversize)
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize status=%d", response.StatusCode)
	}
	_ = response.Body.Close()

	first := webhookRequest(t, url, "correct-token", "same-event", []byte(`{"event":1}`))
	defer func() { _ = first.Body.Close() }()
	firstPayload := decodeResponse(t, first)
	if first.StatusCode != http.StatusAccepted || firstPayload["duplicate"] != false {
		t.Fatalf("first status=%d payload=%+v", first.StatusCode, firstPayload)
	}
	second := webhookRequest(t, url, "correct-token", "same-event", []byte(`{"event":2}`))
	defer func() { _ = second.Body.Close() }()
	secondPayload := decodeResponse(t, second)
	if second.StatusCode != http.StatusAccepted || secondPayload["duplicate"] != true || firstPayload["runUID"] != secondPayload["runUID"] {
		t.Fatalf("duplicate status=%d first=%+v second=%+v", second.StatusCode, firstPayload, secondPayload)
	}
	command, err := state.ClaimStart(context.Background(), time.Hour)
	if err != nil || command.Payload.RunUID != firstPayload["runUID"] {
		t.Fatalf("durable command=%+v err=%v", command, err)
	}
	if _, err := state.ClaimStart(context.Background(), time.Hour); err == nil {
		t.Fatal("duplicate webhook created a second outbox command")
	}

	withoutKeyAResponse := webhookRequest(t, url, "correct-token", "", []byte(`{"event":"a"}`))
	defer func() { _ = withoutKeyAResponse.Body.Close() }()
	withoutKeyA := decodeResponse(t, withoutKeyAResponse)
	withoutKeyBResponse := webhookRequest(t, url, "correct-token", "", []byte(`{"event":"a"}`))
	defer func() { _ = withoutKeyBResponse.Body.Close() }()
	withoutKeyB := decodeResponse(t, withoutKeyBResponse)
	if withoutKeyA["runUID"] == withoutKeyB["runUID"] || withoutKeyA["deduplicated"] != false {
		t.Fatalf("generated occurrence identities: %+v %+v", withoutKeyA, withoutKeyB)
	}
	if err := state.SetTriggerEnabled(context.Background(), applied.Metadata.UID, false); err != nil {
		t.Fatal(err)
	}
	disabled := webhookRequest(t, url, "correct-token", "disabled-event", []byte(`{"event":3}`))
	if disabled.StatusCode != http.StatusNotFound {
		t.Fatalf("disabled trigger status=%d", disabled.StatusCode)
	}
	_ = disabled.Body.Close()
}

func webhookRequest(t *testing.T, url, token, key string, body []byte) *http.Response {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func decodeResponse(t *testing.T, response *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode %q: %v", strings.TrimSpace(string(body)), err)
	}
	return payload
}

package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/alexrett/orchigram/internal/resource"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func testFlowDocument(t *testing.T) resource.Document {
	t.Helper()
	doc, err := resource.DecodeStrict([]byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: demo}
spec:
  nodes:
    - {id: begin, uses: core.noop}
`))
	if err != nil {
		t.Fatal(err)
	}
	return doc
}

func TestApplyCASGenerationAndEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	created, err := s.Apply(ctx, testFlowDocument(t), ApplyOptions{RequestID: "create"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Metadata.ResourceVersion != 1 || created.Metadata.Generation != 1 || created.Metadata.UID == "" {
		t.Fatalf("metadata: %+v", created.Metadata)
	}
	_, err = s.Apply(ctx, testFlowDocument(t), ApplyOptions{ExpectedResourceVersion: 99})
	var conflict *ConflictError
	if !errors.As(err, &conflict) || conflict.Current != 1 {
		t.Fatalf("expected conflict, got %v", err)
	}
	updatedDoc := testFlowDocument(t)
	updatedDoc.Metadata.ResourceVersion = 1
	updated, err := s.Apply(ctx, updatedDoc, ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Metadata.Generation != 1 {
		t.Fatalf("no-op apply incremented generation: %d", updated.Metadata.Generation)
	}
	events, err := s.EventsAfter(ctx, "Flow", "default", 0, 10)
	if err != nil || len(events) != 2 {
		t.Fatalf("events=%d err=%v", len(events), err)
	}
}

func TestTriggerReceiptAndOutboxAreDeduplicated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openTestStore(t)
	first, err := s.AcceptTrigger(ctx, "manual", "key-1", "demo", "default", json.RawMessage(`{"x":1}`), true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.AcceptTrigger(ctx, "manual", "key-1", "demo", "default", json.RawMessage(`{"x":2}`), true)
	if err != nil {
		t.Fatal(err)
	}
	if first.UID != second.UID || first.RunUID != second.RunUID {
		t.Fatalf("duplicate created new identity: %+v %+v", first, second)
	}
	command, err := s.ClaimStart(ctx, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if command.Payload.RunUID != first.RunUID || command.Attempts != 1 {
		t.Fatalf("command: %+v", command)
	}
	if _, err := s.ClaimStart(ctx, time.Hour); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second command: %v", err)
	}
}

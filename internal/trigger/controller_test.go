package trigger

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNextOccurrencesHandleBerlinDSTTransitions(t *testing.T) {
	t.Parallel()
	location, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Fatal(err)
	}
	controller := &Controller{}
	trigger := resource.Trigger{Metadata: resource.ObjectMeta{UID: "dst", Generation: 1}, Spec: resource.TriggerSpec{Schedule: &resource.ScheduleTrigger{Cron: "30 2 * * *", Timezone: "Europe/Berlin"}}}
	spring, err := controller.NextOccurrences(trigger, time.Date(2026, 3, 28, 2, 31, 0, 0, location), 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, occurrence := range spring {
		local := occurrence.In(location)
		if local.Year() == 2026 && local.Month() == time.March && local.Day() == 29 && local.Hour() == 2 {
			t.Fatalf("nonexistent local time was scheduled: %s", local)
		}
	}
	fall, err := controller.NextOccurrences(trigger, time.Date(2026, 10, 24, 2, 31, 0, 0, location), 4)
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index < len(fall); index++ {
		if !fall[index].After(fall[index-1]) {
			t.Fatalf("DST fall occurrences are not strictly increasing: %+v", fall)
		}
	}
}

func TestScheduleRestartDedupMisfireAndConcurrencyForbid(t *testing.T) {
	t.Parallel()
	state := openTriggerStore(t)
	trigger := applyTrigger(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: every-minute}
spec:
  flow: target
  schedule:
    cron: "* * * * *"
    timezone: UTC
    startingDeadline: 3h
    misfirePolicy: fireOnce
    concurrencyPolicy: forbid
`)
	controller := NewController(state, nil)
	now := time.Date(2026, 8, 8, 10, 0, 30, 0, time.UTC)
	if err := state.AdvanceTriggerCursor(context.Background(), trigger.Metadata.UID, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileSchedules(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	receipts, err := state.TriggerReceipts(context.Background(), trigger.Metadata.UID, 100)
	if err != nil || len(receipts) != 1 {
		t.Fatalf("misfire receipts=%+v err=%v", receipts, err)
	}
	if err := state.AdvanceTriggerCursor(context.Background(), trigger.Metadata.UID, now.Add(-2*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileSchedules(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	receipts, _ = state.TriggerReceipts(context.Background(), trigger.Metadata.UID, 100)
	if len(receipts) != 1 {
		t.Fatalf("restart created duplicate receipt: %+v", receipts)
	}
	if err := controller.ReconcileSchedules(context.Background(), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	skips, err := state.TriggerSkips(context.Background(), trigger.Metadata.UID, 100)
	if err != nil || len(skips) != 1 || skips[0].Reason != "concurrency-forbid" {
		t.Fatalf("forbid skips=%+v err=%v", skips, err)
	}
	receipts, _ = state.TriggerReceipts(context.Background(), trigger.Metadata.UID, 100)
	if len(receipts) != 1 {
		t.Fatalf("forbidden occurrence created a run: %+v", receipts)
	}
}

func TestScheduleStartingDeadlineRecordsMiss(t *testing.T) {
	t.Parallel()
	state := openTriggerStore(t)
	trigger := applyTrigger(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: daily}
spec:
  flow: target
  schedule: {cron: "0 9 * * *", timezone: UTC, startingDeadline: 1h}
`)
	controller := NewController(state, nil)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	if err := state.AdvanceTriggerCursor(context.Background(), trigger.Metadata.UID, now.Add(-24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := controller.ReconcileSchedules(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	receipts, _ := state.TriggerReceipts(context.Background(), trigger.Metadata.UID, 10)
	skips, _ := state.TriggerSkips(context.Background(), trigger.Metadata.UID, 10)
	if len(receipts) != 0 || len(skips) != 1 || skips[0].Reason != "misfire-deadline" {
		t.Fatalf("receipts=%+v skips=%+v", receipts, skips)
	}
}

type fakeProvider struct {
	accepted  chan string
	activated chan time.Time
	cancel    context.CancelFunc
}

func (f *fakeProvider) WatchTrigger(ctx context.Context, _, _ string, _ map[string]any, cursor string, activatedAt time.Time, accept func(*pluginv1alpha1.TriggerEvent) error) error {
	if cursor != "" {
		return ctx.Err()
	}
	f.activated <- activatedAt
	event := &pluginv1alpha1.TriggerEvent{ProviderEventId: "provider-event-1", Cursor: "cursor-1", OccurredAt: timestamppb.Now(), PayloadJson: []byte(`{"issue":42}`)}
	if err := accept(event); err != nil {
		return err
	}
	f.accepted <- event.GetCursor()
	f.cancel()
	return ctx.Err()
}

func TestProviderAcknowledgesOnlyAfterReceiptAndCursorPersistence(t *testing.T) {
	t.Parallel()
	state := openTriggerStore(t)
	trigger := applyTrigger(t, state, `apiVersion: orchigram.dev/v1alpha1
kind: Trigger
metadata: {name: provider}
spec:
  flow: target
  provider: {plugin: fake, config: {repository: example}}
`)
	ctx, cancel := context.WithCancel(context.Background())
	persistedState, err := state.TriggerState(context.Background(), trigger.Metadata.UID)
	if err != nil {
		t.Fatal(err)
	}
	activation := persistedState.CursorAt
	provider := &fakeProvider{accepted: make(chan string, 1), activated: make(chan time.Time, 1), cancel: cancel}
	controller := NewController(state, provider)
	controller.now = func() time.Time { return activation.Add(time.Hour) }
	go controller.watchProvider(ctx, trigger)
	select {
	case activatedAt := <-provider.activated:
		if !activatedAt.Equal(activation) {
			t.Fatalf("activatedAt=%s want=%s", activatedAt, activation)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("provider did not receive durable activation time")
	}
	select {
	case cursor := <-provider.accepted:
		if cursor != "cursor-1" {
			t.Fatalf("cursor=%q", cursor)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("provider event was not durably accepted")
	}
	persistedCursor, eventID, err := state.ProviderCursor(context.Background(), trigger.Metadata.UID)
	if err != nil || persistedCursor != "cursor-1" || eventID != "provider-event-1" {
		t.Fatalf("cursor=%q event=%q err=%v", persistedCursor, eventID, err)
	}
	receipts, err := state.TriggerReceipts(context.Background(), trigger.Metadata.UID, 10)
	if err != nil || len(receipts) != 1 || receipts[0].OccurrenceID != "provider-event-1" {
		t.Fatalf("receipts=%+v err=%v", receipts, err)
	}
}

func openTriggerStore(t *testing.T) *store.Store {
	t.Helper()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	return state
}

func applyTrigger(t *testing.T, state *store.Store, yaml string) resource.Trigger {
	t.Helper()
	document, err := resource.DecodeStrict([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	applied, err := state.Apply(context.Background(), document, store.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trigger, err := resource.DecodeTrigger(applied.JSON)
	if err != nil {
		t.Fatal(err)
	}
	return trigger
}

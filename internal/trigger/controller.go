// Package trigger implements native schedules and provider subscriptions over
// the same durable receipt/outbox boundary.
package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"github.com/alexrett/orchigram/internal/health"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"github.com/robfig/cron/v3"
)

const maxCatchupScan = 10000

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// Provider watches one active TriggerProvider and acknowledges callback success.
type Provider interface {
	WatchTrigger(context.Context, string, string, string, string, map[string]any, string, time.Time, func(*pluginv1alpha1.TriggerEvent) error) error
}

// Acceptor owns compilation and the durable receipt/plan/outbox boundary.
type Acceptor interface {
	AcceptTrigger(context.Context, string, uint64, string, string, string, json.RawMessage, bool) (store.Receipt, error)
	AcceptProviderTrigger(context.Context, string, uint64, string, string, string, json.RawMessage, string) (store.Receipt, error)
	AcceptProviderSignal(context.Context, string, uint64, string, string, string, string, string, json.RawMessage, string) (store.Receipt, error)
}

type subscription struct {
	generation uint64
	cancel     context.CancelFunc
}

// Controller reconciles schedules and provider streams into durable receipts.
type Controller struct {
	store         *store.Store
	acceptor      Acceptor
	provider      Provider
	now           func() time.Time
	mu            sync.Mutex
	scheduleMu    sync.Mutex
	subscriptions map[string]subscription
	health        *health.Tracker
}

// NewController creates a controller with the real wall clock.
func NewController(state *store.Store, provider Provider, acceptors ...Acceptor) *Controller {
	var acceptor Acceptor = state
	if len(acceptors) > 0 && acceptors[0] != nil {
		acceptor = acceptors[0]
	}
	tracker := health.NewTracker()
	tracker.Set("providers", health.Diagnostic{Path: "controllers/providers", Code: "starting", Message: "provider reconciliation has not completed"})
	tracker.Set("schedules", health.Diagnostic{Path: "controllers/schedules", Code: "starting", Message: "schedule reconciliation has not completed"})
	return &Controller{store: state, acceptor: acceptor, provider: provider, now: time.Now, subscriptions: map[string]subscription{}, health: tracker}
}

// HealthDiagnostics returns unresolved controller failures in stable order.
func (c *Controller) HealthDiagnostics() []health.Diagnostic { return c.health.Snapshot() }

// Start runs bounded schedule and provider reconciliation loops.
func (c *Controller) Start(ctx context.Context) {
	go func() {
		scheduleTicker := time.NewTicker(time.Second)
		providerTicker := time.NewTicker(5 * time.Second)
		defer scheduleTicker.Stop()
		defer providerTicker.Stop()
		c.observeSchedule(c.ReconcileSchedules(ctx, c.now()))
		c.observeProviders(c.syncProviders(ctx))
		for {
			select {
			case <-ctx.Done():
				c.stopProviders()
				return
			case now := <-scheduleTicker.C:
				c.observeSchedule(c.ReconcileSchedules(ctx, now))
			case <-providerTicker.C:
				c.observeProviders(c.syncProviders(ctx))
			}
		}
	}()
}

func (c *Controller) observeSchedule(err error) {
	if err == nil {
		c.health.Clear("schedules")
		return
	}
	if !errors.Is(err, context.Canceled) {
		c.health.Set("schedules", health.Diagnostic{Path: "controllers/schedules", Code: "reconcile_failed", Message: "schedule reconciliation failed; inspect daemon logs"})
	}
}

func (c *Controller) observeProviders(err error) {
	if err == nil {
		c.health.Clear("providers")
		return
	}
	if !errors.Is(err, context.Canceled) {
		c.health.Set("providers", health.Diagnostic{Path: "controllers/providers", Code: "reconcile_failed", Message: "provider reconciliation failed; inspect daemon logs"})
	}
}

// ReconcileSchedules evaluates every schedule with at-most-one misfire catch-up.
func (c *Controller) ReconcileSchedules(ctx context.Context, now time.Time) error {
	c.scheduleMu.Lock()
	defer c.scheduleMu.Unlock()

	documents, _, err := c.store.List(ctx, "Trigger", resource.DefaultNamespace, 1000)
	if err != nil {
		return err
	}
	for _, document := range documents {
		trigger, err := resource.DecodeTrigger(document.JSON)
		if err != nil {
			return err
		}
		if trigger.Spec.Schedule == nil {
			continue
		}
		if err := c.reconcileSchedule(ctx, trigger, now); err != nil {
			return fmt.Errorf("reconcile Trigger %s: %w", trigger.Metadata.Name, err)
		}
	}
	return nil
}

// ReconcileSchedulesNow runs one synchronous schedule pass and records its
// outcome in the public controller health projection.
func (c *Controller) ReconcileSchedulesNow(ctx context.Context, now time.Time) error {
	err := c.ReconcileSchedules(ctx, now)
	c.observeSchedule(err)
	return err
}

// NextOccurrences previews future schedule identities without changing state.
func (c *Controller) NextOccurrences(trigger resource.Trigger, from time.Time, count int) ([]time.Time, error) {
	if trigger.Spec.Schedule == nil {
		return nil, errors.New("trigger is not a schedule")
	}
	if count <= 0 || count > 100 {
		count = 5
	}
	schedule, location, err := parseSchedule(*trigger.Spec.Schedule)
	if err != nil {
		return nil, err
	}
	result := make([]time.Time, 0, count)
	cursor := from.In(location)
	for range count {
		cursor = schedule.Next(cursor)
		result = append(result, cursor)
	}
	return result, nil
}

func (c *Controller) reconcileSchedule(ctx context.Context, trigger resource.Trigger, now time.Time) error {
	scheduleSpec := trigger.Spec.Schedule
	schedule, location, err := parseSchedule(*scheduleSpec)
	if err != nil {
		return err
	}
	enabled := trigger.Spec.Enabled == nil || *trigger.Spec.Enabled
	state, err := c.store.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, enabled, now)
	if err != nil {
		return err
	}
	if !state.Enabled {
		return nil
	}
	cursor := state.CursorAt.In(location)
	nowLocal := now.In(location)
	due := make([]time.Time, 0, 1)
	last := state.CursorAt
	for range maxCatchupScan {
		next := schedule.Next(cursor)
		if next.After(nowLocal) {
			break
		}
		due = append(due, next)
		last = next
		cursor = next
	}
	if len(due) == maxCatchupScan && !schedule.Next(cursor).After(nowLocal) {
		return errors.New("schedule catch-up scan exceeded 10000 occurrences")
	}
	if len(due) == 0 {
		return nil
	}
	deadline, err := resource.ParseDuration(scheduleSpec.StartingDeadline, time.Hour)
	if err != nil {
		return err
	}
	selected := due[len(due)-1]
	if now.Sub(selected) > deadline {
		identity := OccurrenceIdentity(trigger, selected)
		if err := c.store.RecordTriggerSkip(ctx, trigger.Metadata.UID, identity, "misfire-deadline", selected); err != nil {
			return err
		}
		return c.store.AdvanceTriggerCursor(ctx, trigger.Metadata.UID, last)
	}
	identity := OccurrenceIdentity(trigger, selected)
	if _, err := c.store.ReceiptByOccurrence(ctx, trigger.Metadata.UID, identity); err == nil {
		return c.store.AdvanceTriggerCursor(ctx, trigger.Metadata.UID, last)
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	if scheduleSpec.ConcurrencyPolicy == "" || scheduleSpec.ConcurrencyPolicy == "forbid" {
		active, err := c.store.HasActiveRunForTrigger(ctx, trigger.Metadata.UID)
		if err != nil {
			return err
		}
		if active {
			if err := c.store.RecordTriggerSkip(ctx, trigger.Metadata.UID, identity, "concurrency-forbid", selected); err != nil {
				return err
			}
			return c.store.AdvanceTriggerCursor(ctx, trigger.Metadata.UID, last)
		}
	}
	payload, _ := json.Marshal(map[string]any{"trigger": map[string]any{"type": "schedule", "scheduledAt": selected.UTC().Format(time.RFC3339Nano), "occurrenceId": identity}})
	if _, err := c.acceptor.AcceptTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, identity, trigger.Spec.Flow, trigger.Metadata.Namespace, payload, true); err != nil {
		return err
	}
	return c.store.AdvanceTriggerCursor(ctx, trigger.Metadata.UID, last)
}

func (c *Controller) syncProviders(ctx context.Context) error {
	if c.provider == nil {
		return nil
	}
	documents, _, err := c.store.List(ctx, "Trigger", resource.DefaultNamespace, 1000)
	if err != nil {
		return err
	}
	wanted := map[string]resource.Trigger{}
	for _, document := range documents {
		trigger, decodeErr := resource.DecodeTrigger(document.JSON)
		if decodeErr != nil {
			return decodeErr
		}
		if trigger.Spec.Provider == nil {
			continue
		}
		enabled := trigger.Spec.Enabled == nil || *trigger.Spec.Enabled
		state, stateErr := c.store.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, enabled, c.now())
		if stateErr != nil {
			return stateErr
		}
		if state.Enabled {
			wanted[trigger.Metadata.UID] = trigger
		}
	}
	c.mu.Lock()
	for uid, running := range c.subscriptions {
		trigger, exists := wanted[uid]
		if !exists || trigger.Metadata.Generation != running.generation {
			running.cancel()
			delete(c.subscriptions, uid)
			c.health.Clear(providerHealthKey(uid))
		}
	}
	for uid, trigger := range wanted {
		if _, exists := c.subscriptions[uid]; exists {
			continue
		}
		watchContext, cancel := context.WithCancel(ctx)
		c.subscriptions[uid] = subscription{generation: trigger.Metadata.Generation, cancel: cancel}
		go c.watchProvider(watchContext, trigger)
	}
	c.mu.Unlock()
	return nil
}

func (c *Controller) watchProvider(ctx context.Context, trigger resource.Trigger) {
	backoff := time.Second
	for ctx.Err() == nil {
		state, stateErr := c.store.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, true, c.now())
		cursor, _, err := c.store.ProviderCursor(ctx, trigger.Metadata.UID)
		watchCode := "watch_failed"
		watchMessage := "provider subscription failed and is waiting to retry"
		if stateErr == nil && err == nil {
			c.health.Clear(providerHealthKey(trigger.Metadata.UID))
			watchErr := c.provider.WatchTrigger(ctx, trigger.Spec.Provider.Plugin, trigger.Spec.Provider.Source, trigger.Metadata.UID, trigger.Metadata.Namespace, trigger.Spec.Provider.Config, cursor, state.CursorAt, func(event *pluginv1alpha1.TriggerEvent) error {
				if event.GetProviderEventId() == "" || event.GetCursor() == "" || !json.Valid(event.GetPayloadJson()) {
					return errors.New("provider event identity, cursor, and JSON payload are required")
				}
				if trigger.Spec.Delivery != nil && trigger.Spec.Delivery.Mode == "signal" {
					if event.GetTargetRunUid() == "" {
						return errors.New("signal delivery requires a provider target Run UID")
					}
					_, acceptErr := c.acceptor.AcceptProviderSignal(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, event.GetProviderEventId(), trigger.Spec.Flow, trigger.Metadata.Namespace, event.GetTargetRunUid(), trigger.Spec.Delivery.Node, event.GetPayloadJson(), event.GetCursor())
					return acceptErr
				}
				_, acceptErr := c.acceptor.AcceptProviderTrigger(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, event.GetProviderEventId(), trigger.Spec.Flow, trigger.Metadata.Namespace, event.GetPayloadJson(), event.GetCursor())
				return acceptErr
			})
			if watchErr == nil {
				watchCode = "watch_ended"
				watchMessage = "provider subscription ended unexpectedly and is waiting to restart"
			}
		}
		if ctx.Err() != nil {
			return
		}
		c.health.Set(providerHealthKey(trigger.Metadata.UID), health.Diagnostic{Path: "controllers/providers/" + trigger.Metadata.UID, Code: watchCode, Message: watchMessage})
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *Controller) stopProviders() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for uid, running := range c.subscriptions {
		running.cancel()
		delete(c.subscriptions, uid)
		c.health.Clear(providerHealthKey(uid))
	}
}

func providerHealthKey(triggerUID string) string { return "provider/" + triggerUID }

func parseSchedule(spec resource.ScheduleTrigger) (cron.Schedule, *time.Location, error) {
	if len(strings.Fields(spec.Cron)) != 5 {
		return nil, nil, errors.New("schedule cron must contain exactly five fields")
	}
	timezone := spec.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	location, err := time.LoadLocation(timezone)
	if err != nil {
		return nil, nil, fmt.Errorf("load timezone %q: %w", timezone, err)
	}
	schedule, err := cronParser.Parse(spec.Cron)
	if err != nil {
		return nil, nil, fmt.Errorf("parse cron: %w", err)
	}
	if spec.MisfirePolicy != "" && spec.MisfirePolicy != "fireOnce" {
		return nil, nil, fmt.Errorf("unsupported misfirePolicy %q", spec.MisfirePolicy)
	}
	if spec.ConcurrencyPolicy != "" && spec.ConcurrencyPolicy != "forbid" {
		return nil, nil, fmt.Errorf("unsupported concurrencyPolicy %q", spec.ConcurrencyPolicy)
	}
	return schedule, location, nil
}

// OccurrenceIdentity is stable across restarts and timezone representations.
func OccurrenceIdentity(trigger resource.Trigger, scheduled time.Time) string {
	return fmt.Sprintf("%s:%d:%s", trigger.Metadata.UID, trigger.Metadata.Generation, scheduled.UTC().Format(time.RFC3339Nano))
}

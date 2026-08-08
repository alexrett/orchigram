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
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"github.com/robfig/cron/v3"
)

const maxCatchupScan = 10000

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// Provider watches one active TriggerProvider and acknowledges callback success.
type Provider interface {
	WatchTrigger(context.Context, string, string, map[string]any, string, time.Time, func(*pluginv1alpha1.TriggerEvent) error) error
}

type subscription struct {
	generation uint64
	cancel     context.CancelFunc
}

// Controller reconciles schedules and provider streams into durable receipts.
type Controller struct {
	store         *store.Store
	provider      Provider
	now           func() time.Time
	mu            sync.Mutex
	subscriptions map[string]subscription
}

// NewController creates a controller with the real wall clock.
func NewController(state *store.Store, provider Provider) *Controller {
	return &Controller{store: state, provider: provider, now: time.Now, subscriptions: map[string]subscription{}}
}

// Start runs bounded schedule and provider reconciliation loops.
func (c *Controller) Start(ctx context.Context) {
	go func() {
		scheduleTicker := time.NewTicker(time.Second)
		providerTicker := time.NewTicker(5 * time.Second)
		defer scheduleTicker.Stop()
		defer providerTicker.Stop()
		_ = c.ReconcileSchedules(ctx, c.now())
		c.syncProviders(ctx)
		for {
			select {
			case <-ctx.Done():
				c.stopProviders()
				return
			case now := <-scheduleTicker.C:
				_ = c.ReconcileSchedules(ctx, now)
			case <-providerTicker.C:
				c.syncProviders(ctx)
			}
		}
	}()
}

// ReconcileSchedules evaluates every schedule with at-most-one misfire catch-up.
func (c *Controller) ReconcileSchedules(ctx context.Context, now time.Time) error {
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
	if _, err := c.store.AcceptTrigger(ctx, trigger.Metadata.UID, identity, trigger.Spec.Flow, trigger.Metadata.Namespace, payload, true); err != nil {
		return err
	}
	return c.store.AdvanceTriggerCursor(ctx, trigger.Metadata.UID, last)
}

func (c *Controller) syncProviders(ctx context.Context) {
	if c.provider == nil {
		return
	}
	documents, _, err := c.store.List(ctx, "Trigger", resource.DefaultNamespace, 1000)
	if err != nil {
		return
	}
	wanted := map[string]resource.Trigger{}
	for _, document := range documents {
		trigger, decodeErr := resource.DecodeTrigger(document.JSON)
		if decodeErr != nil || trigger.Spec.Provider == nil {
			continue
		}
		enabled := trigger.Spec.Enabled == nil || *trigger.Spec.Enabled
		state, stateErr := c.store.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, enabled, c.now())
		if stateErr == nil && state.Enabled {
			wanted[trigger.Metadata.UID] = trigger
		}
	}
	c.mu.Lock()
	for uid, running := range c.subscriptions {
		trigger, exists := wanted[uid]
		if !exists || trigger.Metadata.Generation != running.generation {
			running.cancel()
			delete(c.subscriptions, uid)
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
}

func (c *Controller) watchProvider(ctx context.Context, trigger resource.Trigger) {
	backoff := time.Second
	for ctx.Err() == nil {
		state, stateErr := c.store.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, true, c.now())
		cursor, _, err := c.store.ProviderCursor(ctx, trigger.Metadata.UID)
		if stateErr == nil && err == nil {
			_ = c.provider.WatchTrigger(ctx, trigger.Spec.Provider.Plugin, trigger.Metadata.UID, trigger.Spec.Provider.Config, cursor, state.CursorAt, func(event *pluginv1alpha1.TriggerEvent) error {
				if event.GetProviderEventId() == "" || event.GetCursor() == "" || !json.Valid(event.GetPayloadJson()) {
					return errors.New("provider event identity, cursor, and JSON payload are required")
				}
				_, acceptErr := c.store.AcceptProviderTrigger(ctx, trigger.Metadata.UID, event.GetProviderEventId(), trigger.Spec.Flow, trigger.Metadata.Namespace, event.GetPayloadJson(), event.GetCursor())
				return acceptErr
			})
		}
		if ctx.Err() != nil {
			return
		}
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
	}
}

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

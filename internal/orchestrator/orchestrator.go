// Package orchestrator coordinates receipts, compilation, outbox, and durable execution.
package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	"github.com/google/uuid"
)

// Boundary identifies a crash-test injection point.
type Boundary string

const (
	// BoundaryAfterReceipt occurs after receipt/outbox commit and before claim.
	BoundaryAfterReceipt Boundary = "after-receipt"
	// BoundaryAfterRun occurs after local Run creation and before engine start.
	BoundaryAfterRun Boundary = "after-run"
	// BoundaryAfterEngine occurs after engine start and before outbox completion.
	BoundaryAfterEngine Boundary = "after-engine"
)

// FaultHook is used only by deterministic crash-boundary tests.
type FaultHook func(Boundary) error

// Orchestrator is the single-node control loop.
type Orchestrator struct {
	store           *store.Store
	compiler        *flow.Compiler
	engine          engine.DurableEngine
	fault           FaultHook
	wake            chan struct{}
	done            chan struct{}
	claimStaleAfter time.Duration
}

// New constructs a control loop from product-owned interfaces.
func New(state *store.Store, compiler *flow.Compiler, durable engine.DurableEngine) *Orchestrator {
	return &Orchestrator{store: state, compiler: compiler, engine: durable, wake: make(chan struct{}, 1), done: make(chan struct{}), claimStaleAfter: 5 * time.Second}
}

// SetFaultHook installs a deterministic test-only boundary hook.
func (o *Orchestrator) SetFaultHook(hook FaultHook) { o.fault = hook }

// Start runs outbox and approval reconciliation until ctx is canceled.
func (o *Orchestrator) Start(ctx context.Context) {
	go o.loop(ctx)
}

// Wait blocks until the reconciliation loop stops.
func (o *Orchestrator) Wait() { <-o.done }

// StartManual durably accepts a manual trigger before returning its Run UID.
func (o *Orchestrator) StartManual(ctx context.Context, flowName, namespace string, input json.RawMessage, idempotencyKey string) (store.Receipt, error) {
	deduplicated := idempotencyKey != ""
	if idempotencyKey == "" {
		idempotencyKey = uuid.NewString()
	}
	receipt, err := o.AcceptTrigger(ctx, "manual:"+namespace+":"+flowName, idempotencyKey, flowName, namespace, input, deduplicated)
	if err != nil {
		return store.Receipt{}, err
	}
	if err := o.hit(BoundaryAfterReceipt); err != nil {
		return receipt, err
	}
	o.notify()
	return receipt, nil
}

// AcceptTrigger compiles the referenced Flow before acknowledging the
// occurrence and atomically stores the resulting immutable plan with the
// receipt and outbox command.
func (o *Orchestrator) AcceptTrigger(ctx context.Context, triggerUID, occurrenceID, flowName, namespace string, input json.RawMessage, deduplicated bool) (store.Receipt, error) {
	if existing, err := o.store.ReceiptByOccurrence(ctx, triggerUID, occurrenceID); err == nil {
		existing.Existing = true
		return existing, nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return store.Receipt{}, err
	}
	plan, err := o.compileCurrentFlow(ctx, flowName, namespace)
	if err != nil {
		return store.Receipt{}, err
	}
	return o.store.AcceptTriggerWithPlan(ctx, triggerUID, occurrenceID, flowName, namespace, input, deduplicated, plan)
}

// AcceptProviderTrigger applies the same immutable acceptance boundary while
// also persisting the provider cursor under Trigger generation validation.
func (o *Orchestrator) AcceptProviderTrigger(ctx context.Context, triggerUID string, triggerGeneration uint64, occurrenceID, flowName, namespace string, input json.RawMessage, cursor string) (store.Receipt, error) {
	if _, err := o.store.ReceiptByOccurrence(ctx, triggerUID, occurrenceID); err == nil {
		// Re-enter the store transaction so Trigger generation/enabled state and
		// the replayed provider cursor are validated and committed together. The
		// existing receipt guarantees this compatibility method cannot create an
		// unpinned command, even if the accepted Flow has since been deleted.
		return o.store.AcceptProviderTrigger(ctx, triggerUID, triggerGeneration, occurrenceID, flowName, namespace, input, cursor)
	} else if !errors.Is(err, store.ErrNotFound) {
		return store.Receipt{}, err
	}
	plan, err := o.compileCurrentFlow(ctx, flowName, namespace)
	if err != nil {
		return store.Receipt{}, err
	}
	return o.store.AcceptProviderTriggerWithPlan(ctx, triggerUID, triggerGeneration, occurrenceID, flowName, namespace, input, cursor, plan)
}

// ReconcileOne dispatches at most one durable outbox command.
func (o *Orchestrator) ReconcileOne(ctx context.Context) error {
	command, err := o.store.ClaimStart(ctx, o.claimStaleAfter)
	if err != nil {
		return err
	}
	var plan flow.ExecutionPlan
	if command.Payload.PlanHash != "" {
		plan, err = o.store.GetPlan(ctx, command.Payload.PlanHash)
	} else {
		// Compatibility path for prototype databases that contain an accepted
		// pre-v0.1 outbox command. All new runtime acceptors persist PlanHash.
		plan, err = o.compileCurrentFlow(ctx, command.Payload.FlowName, command.Payload.Namespace)
	}
	if err != nil {
		_ = o.store.RetryOutbox(ctx, command.ID, err, time.Second)
		return err
	}
	created, err := o.store.EnsureRun(ctx, command.Payload, plan)
	if err != nil {
		_ = o.store.RetryOutbox(ctx, command.ID, err, time.Second)
		return err
	}
	if !created {
		existingRun, runErr := o.store.GetRun(ctx, command.Payload.RunUID)
		if runErr != nil {
			_ = o.store.RetryOutbox(ctx, command.ID, runErr, time.Second)
			return runErr
		}
		plan, err = o.store.GetPlan(ctx, existingRun.PlanHash)
		if err != nil {
			_ = o.store.RetryOutbox(ctx, command.ID, err, time.Second)
			return err
		}
	}
	if err := o.hit(BoundaryAfterRun); err != nil {
		return err
	}
	cancelled, err := o.store.CompleteStartIfRunCancelled(ctx, command.ID, command.Payload.RunUID)
	if err != nil {
		_ = o.store.RetryOutbox(ctx, command.ID, err, time.Second)
		return err
	}
	if cancelled {
		return nil
	}
	if err := o.engine.Start(ctx, command.Payload.RunUID, plan, command.Payload.Input); err != nil {
		_ = o.store.RetryOutbox(ctx, command.ID, err, time.Second)
		return err
	}
	if err := o.hit(BoundaryAfterEngine); err != nil {
		return err
	}
	return o.store.CompleteOutbox(ctx, command.ID)
}

func (o *Orchestrator) compileCurrentFlow(ctx context.Context, flowName, namespace string) (flow.ExecutionPlan, error) {
	doc, err := o.store.Get(ctx, "Flow", namespace, flowName)
	if err != nil {
		return flow.ExecutionPlan{}, fmt.Errorf("resolve flow: %w", err)
	}
	flowResource, err := resource.DecodeFlow(doc.JSON)
	if err != nil {
		return flow.ExecutionPlan{}, err
	}
	plan, diagnostics := o.compiler.Compile(flowResource)
	if len(diagnostics) > 0 {
		return flow.ExecutionPlan{}, fmt.Errorf("flow compile failed: %s at %s", diagnostics[0].Message, diagnostics[0].Path)
	}
	return plan, nil
}

func (o *Orchestrator) loop(ctx context.Context) {
	defer close(o.done)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := o.ReconcileOne(ctx); err != nil && !errors.Is(err, store.ErrNotFound) && !errors.Is(err, context.Canceled) {
			slog.Debug("outbox reconciliation deferred", "error", err)
		}
		_ = o.engine.Reconcile(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-o.wake:
		}
	}
}

func (o *Orchestrator) notify() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Orchestrator) hit(boundary Boundary) error {
	if o.fault == nil {
		return nil
	}
	return o.fault(boundary)
}

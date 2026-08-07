// Package engine isolates the durable workflow framework behind product contracts.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/store"
	"github.com/cschleiden/go-workflows/backend"
	workflowSqlite "github.com/cschleiden/go-workflows/backend/sqlite"
	workflowClient "github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/diag"
	"github.com/cschleiden/go-workflows/registry"
	"github.com/cschleiden/go-workflows/worker"
	workflowRuntime "github.com/cschleiden/go-workflows/workflow"
)

const (
	workflowName          = "orchigram.interpreter.v1"
	activityExecuteNode   = "orchigram.execute-node.v1"
	activityBeginApproval = "orchigram.begin-approval.v1"
	activityEndApproval   = "orchigram.end-approval.v1"
	activitySkipNode      = "orchigram.skip-node.v1"
	activityCompleteRun   = "orchigram.complete-run.v1"
	activityFailRun       = "orchigram.fail-run.v1"
)

// DurableEngine is the only execution contract visible to the daemon.
type DurableEngine interface {
	Start(context.Context, string, flow.ExecutionPlan, json.RawMessage) error
	Signal(context.Context, string, string, ApprovalSignal) error
	Cancel(context.Context, string, string) error
	Reconcile(context.Context) error
	Describe(context.Context, string) (store.Run, error)
	Close() error
}

// TaskExecutor invokes non-core actions at an at-least-once activity boundary.
type TaskExecutor interface {
	Execute(context.Context, string, flow.PlanNode, json.RawMessage, map[string]any, string) (json.RawMessage, error)
}

// ApprovalSignal is the framework-independent durable decision payload.
type ApprovalSignal struct {
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// Adapter runs one stable data-driven interpreter on go-workflows.
type Adapter struct {
	backend backend.Backend
	client  *workflowClient.Client
	worker  *worker.Worker
	store   *store.Store
	cancel  context.CancelFunc
	done    chan error
	once    sync.Once
}

// Open starts the go-workflows worker against its private SQLite history database.
func Open(ctx context.Context, workflowDBPath string, state *store.Store, executor TaskExecutor) (*Adapter, error) {
	b := workflowSqlite.NewSqliteBackend(workflowDBPath, workflowSqlite.WithBackendOptions(
		backend.WithWorkflowLockTimeout(2*time.Second),
		backend.WithActivityLockTimeout(2*time.Second),
		backend.WithStickyTimeout(0),
	))
	w := worker.New(b, nil)
	if err := w.RegisterWorkflow(InterpreterWorkflow, registry.WithName(workflowName)); err != nil {
		return nil, fmt.Errorf("register interpreter: %w", err)
	}
	activities := &Activities{store: state, executor: executor}
	registrations := []struct {
		name string
		fn   any
	}{
		{activityExecuteNode, activities.ExecuteNode},
		{activityBeginApproval, activities.BeginApproval},
		{activityEndApproval, activities.EndApproval},
		{activitySkipNode, activities.SkipNode},
		{activityCompleteRun, activities.CompleteRun},
		{activityFailRun, activities.FailRun},
	}
	for _, registration := range registrations {
		if err := w.RegisterActivity(registration.fn, registry.WithName(registration.name)); err != nil {
			return nil, fmt.Errorf("register activity %s: %w", registration.name, err)
		}
	}
	workerCtx, cancel := context.WithCancel(ctx)
	if err := w.Start(workerCtx); err != nil {
		cancel()
		return nil, err
	}
	adapter := &Adapter{backend: b, client: workflowClient.New(b), worker: w, store: state, cancel: cancel, done: make(chan error, 1)}
	go func() { adapter.done <- w.WaitForCompletion() }()
	return adapter, nil
}

// Start idempotently starts a pinned interpreter instance.
func (a *Adapter) Start(ctx context.Context, runUID string, plan flow.ExecutionPlan, input json.RawMessage) error {
	if _, err := a.findInstance(ctx, runUID); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	_, err := a.client.CreateWorkflowInstance(ctx, workflowClient.WorkflowInstanceOptions{InstanceID: runUID}, workflowName, runUID, plan, input)
	if err == nil {
		return nil
	}
	// A crash/retry can race after durable workflow creation. Reconcile by identity.
	if _, findErr := a.findInstance(ctx, runUID); findErr == nil {
		return nil
	}
	return fmt.Errorf("create interpreter instance: %w", err)
}

// Signal delivers a durable approval decision.
func (a *Adapter) Signal(ctx context.Context, runUID, nodeID string, signal ApprovalSignal) error {
	return a.client.SignalWorkflow(ctx, runUID, approvalSignalName(nodeID), signal)
}

// Cancel cancels a workflow instance after the daemon records operator intent.
func (a *Adapter) Cancel(ctx context.Context, runUID, _ string) error {
	instance, err := a.findInstance(ctx, runUID)
	if err != nil {
		return err
	}
	return a.client.CancelWorkflowInstance(ctx, instance)
}

// Reconcile redelivers durable decisions not yet acknowledged by the engine boundary.
func (a *Adapter) Reconcile(ctx context.Context) error {
	signals, err := a.store.UndeliveredApprovalSignals(ctx, 100)
	if err != nil {
		return err
	}
	for _, signal := range signals {
		err := a.Signal(ctx, signal.RunUID, signal.NodeID, ApprovalSignal{State: signal.State, Reason: signal.Reason})
		if err != nil {
			return fmt.Errorf("redeliver approval %s/%s: %w", signal.RunUID, signal.NodeID, err)
		}
		if err := a.store.MarkApprovalSignaled(ctx, signal.RunUID, signal.NodeID); err != nil {
			return err
		}
	}
	return nil
}

// Describe returns the framework-independent run projection.
func (a *Adapter) Describe(ctx context.Context, runUID string) (store.Run, error) {
	return a.store.GetRun(ctx, runUID)
}

// Close stops workers and waits for activity completion.
func (a *Adapter) Close() error {
	a.once.Do(a.cancel)
	select {
	case err := <-a.done:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-time.After(10 * time.Second):
		return errors.New("timed out stopping durable worker")
	}
}

func (a *Adapter) findInstance(ctx context.Context, runUID string) (*core.WorkflowInstance, error) {
	diagnostics, ok := a.backend.(diag.Backend)
	if !ok {
		return nil, errors.New("workflow backend does not support reconciliation diagnostics")
	}
	instances, err := diagnostics.GetWorkflowInstances(ctx, "", "", 1000)
	if err != nil {
		return nil, err
	}
	for _, candidate := range instances {
		if candidate.Instance.InstanceID == runUID {
			return candidate.Instance, nil
		}
	}
	return nil, store.ErrNotFound
}

// NodeRequest is a stable activity input.
type NodeRequest struct {
	RunUID   string          `json:"runUID"`
	Node     flow.PlanNode   `json:"node"`
	Outgoing []flow.PlanEdge `json:"outgoing"`
	Input    json.RawMessage `json:"input"`
	Nodes    map[string]any  `json:"nodes"`
}

// NodeResult is a stable recorded activity result.
type NodeResult struct {
	Output    json.RawMessage `json:"output"`
	Activated []bool          `json:"activated"`
	Rejected  bool            `json:"rejected,omitempty"`
}

// InterpreterWorkflow executes an immutable plan as deterministic data.
func InterpreterWorkflow(ctx workflowRuntime.Context, runUID string, plan flow.ExecutionPlan, input json.RawMessage) error {
	nodeIndex := make(map[string]int, len(plan.Nodes))
	incoming := make(map[string][]int, len(plan.Nodes))
	outgoing := make(map[string][]int, len(plan.Nodes))
	for i, node := range plan.Nodes {
		nodeIndex[node.ID] = i
	}
	for i, edge := range plan.Edges {
		incoming[edge.To] = append(incoming[edge.To], i)
		outgoing[edge.From] = append(outgoing[edge.From], i)
	}
	states := make([]string, len(plan.Nodes))
	edgeResolved := make([]bool, len(plan.Edges))
	edgeActive := make([]bool, len(plan.Edges))
	nodeOutputs := map[string]any{}
	remaining := len(plan.Nodes)
	for remaining > 0 {
		progressed := false
		for index, node := range plan.Nodes {
			if states[index] != "" {
				continue
			}
			allResolved, anyActive := true, len(incoming[node.ID]) == 0
			for _, edgeIndex := range incoming[node.ID] {
				if !edgeResolved[edgeIndex] {
					allResolved = false
				}
				if edgeActive[edgeIndex] {
					anyActive = true
				}
			}
			if !allResolved {
				continue
			}
			if !anyActive {
				if _, err := workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activitySkipNode, runUID, node.ID).Get(ctx); err != nil {
					return err
				}
				states[index] = "skipped"
				remaining--
				for _, edgeIndex := range outgoing[node.ID] {
					edgeResolved[edgeIndex] = true
				}
				progressed = true
				continue
			}

			edges := make([]flow.PlanEdge, len(outgoing[node.ID]))
			for i, edgeIndex := range outgoing[node.ID] {
				edges[i] = plan.Edges[edgeIndex]
			}
			request := NodeRequest{RunUID: runUID, Node: node, Outgoing: edges, Input: input, Nodes: nodeOutputs}
			var result NodeResult
			var err error
			if node.Uses == "core.approval" {
				if _, err = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityBeginApproval, request).Get(ctx); err != nil {
					return err
				}
				signalChannel := workflowRuntime.NewSignalChannel[ApprovalSignal](ctx, approvalSignalName(node.ID))
				timeout, parseErr := time.ParseDuration(node.Timeout)
				if parseErr != nil {
					return parseErr
				}
				timerContext, cancelTimer := workflowRuntime.WithCancel(ctx)
				timer := workflowRuntime.ScheduleTimer(timerContext, timeout, workflowRuntime.WithTimerName("approval-timeout-"+node.ID))
				signal := ApprovalSignal{State: "rejected", Reason: "approval timed out"}
				workflowRuntime.Select(ctx,
					workflowRuntime.Receive(signalChannel, func(_ workflowRuntime.Context, value ApprovalSignal, ok bool) {
						if ok {
							signal = value
						}
						cancelTimer()
					}),
					workflowRuntime.Await(timer, func(workflowRuntime.Context, workflowRuntime.Future[any]) {}),
				)
				result, err = workflowRuntime.ExecuteActivity[NodeResult](ctx, workflowRuntime.DefaultActivityOptions, activityEndApproval, request, signal).Get(ctx)
			} else {
				backoff, _ := time.ParseDuration(node.RetryBackoff)
				timeout, _ := time.ParseDuration(node.Timeout)
				options := workflowRuntime.ActivityOptions{RetryOptions: workflowRuntime.RetryOptions{MaxAttempts: node.RetryLimit + 1, FirstRetryInterval: backoff, BackoffCoefficient: 2, RetryTimeout: timeout}}
				result, err = workflowRuntime.ExecuteActivity[NodeResult](ctx, options, activityExecuteNode, request).Get(ctx)
			}
			if err != nil {
				_, _ = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityFailRun, runUID, err.Error()).Get(ctx)
				return err
			}
			states[index] = "completed"
			remaining--
			var output any
			_ = json.Unmarshal(result.Output, &output)
			nodeOutputs[node.ID] = output
			for i, edgeIndex := range outgoing[node.ID] {
				edgeResolved[edgeIndex] = true
				if i < len(result.Activated) {
					edgeActive[edgeIndex] = result.Activated[i]
				}
			}
			progressed = true
			if result.Rejected {
				_, err = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "rejected").Get(ctx)
				return err
			}
		}
		if !progressed {
			pending := make([]string, 0)
			for id, index := range nodeIndex {
				if states[index] == "" {
					pending = append(pending, id)
				}
			}
			sort.Strings(pending)
			message := "interpreter cannot advance cyclic component: " + strings.Join(pending, ",")
			_, _ = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityFailRun, runUID, message).Get(ctx)
			return errors.New(message)
		}
	}
	_, err := workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "succeeded").Get(ctx)
	return err
}

// Activities are framework activity boundaries with access to product state.
type Activities struct {
	store    *store.Store
	executor TaskExecutor
}

// ExecuteNode records and invokes a non-approval action at least once.
func (a *Activities) ExecuteNode(ctx context.Context, request NodeRequest) (NodeResult, error) {
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "node.started", "running", 1, nil); err != nil {
		return NodeResult{}, err
	}
	var output json.RawMessage
	var err error
	switch request.Node.Uses {
	case "core.noop":
		output = json.RawMessage(`{"ok":true}`)
		if configured, exists := request.Node.With["result"]; exists {
			output, err = json.Marshal(configured)
		}
	case "core.fail":
		err = errors.New("core.fail requested")
	default:
		if a.executor == nil {
			err = fmt.Errorf("no task executor for %q", request.Node.Uses)
		} else {
			key := fmt.Sprintf("run/%s/node/%s/iteration/0/operation/execute", request.RunUID, request.Node.ID)
			output, err = a.executor.Execute(ctx, request.RunUID, request.Node, request.Input, request.Nodes, key)
		}
	}
	if err != nil {
		_ = a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "node.failed", "running", 1, mustJSON(map[string]any{"error": err.Error()}))
		return NodeResult{}, err
	}
	activated, err := evaluate(request, output)
	if err != nil {
		return NodeResult{}, err
	}
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "node.completed", "running", 1, output); err != nil {
		return NodeResult{}, err
	}
	return NodeResult{Output: output, Activated: activated}, nil
}

// BeginApproval creates a durable pending approval before workflow suspension.
func (a *Activities) BeginApproval(ctx context.Context, request NodeRequest) (bool, error) {
	timeout, err := time.ParseDuration(request.Node.Timeout)
	if err != nil {
		return false, err
	}
	if err := a.store.EnsureApproval(ctx, request.RunUID, request.Node.ID, time.Now().Add(timeout)); err != nil {
		return false, err
	}
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "approval.waiting", "waiting", 1, nil); err != nil {
		return false, err
	}
	return true, nil
}

// EndApproval records a delivered decision and evaluates its outgoing conditions.
func (a *Activities) EndApproval(ctx context.Context, request NodeRequest, signal ApprovalSignal) (NodeResult, error) {
	state, err := a.store.ApprovalState(ctx, request.RunUID, request.Node.ID)
	if errors.Is(err, store.ErrNotFound) {
		return NodeResult{}, err
	}
	if err != nil {
		return NodeResult{}, err
	}
	if state == "pending" {
		if err := a.store.DecideApproval(ctx, request.RunUID, request.Node.ID, signal.State, "system", signal.Reason); err != nil {
			return NodeResult{}, err
		}
		state = signal.State
	}
	output := mustJSON(map[string]any{"approved": state == "approved", "state": state, "reason": signal.Reason})
	activated, err := evaluate(request, output)
	if err != nil {
		return NodeResult{}, err
	}
	eventType := "approval.approved"
	if state != "approved" {
		eventType = "approval.rejected"
	}
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, eventType, "running", 1, output); err != nil {
		return NodeResult{}, err
	}
	return NodeResult{Output: output, Activated: activated, Rejected: state != "approved"}, nil
}

// SkipNode records a conditional skip.
func (a *Activities) SkipNode(ctx context.Context, runUID, nodeID string) (bool, error) {
	return true, a.store.AppendRunEvent(ctx, runUID, nodeID, "node.skipped", "running", 0, nil)
}

// CompleteRun records a terminal successful or rejected run.
func (a *Activities) CompleteRun(ctx context.Context, runUID, phase string) (bool, error) {
	return true, a.store.AppendRunEvent(ctx, runUID, "", "run."+phase, phase, 0, nil)
}

// FailRun records a terminal interpreter failure.
func (a *Activities) FailRun(ctx context.Context, runUID, message string) (bool, error) {
	return true, a.store.AppendRunEvent(ctx, runUID, "", "run.failed", "failed", 0, mustJSON(map[string]any{"error": message}))
}

func evaluate(request NodeRequest, output json.RawMessage) ([]bool, error) {
	inputMap := map[string]any{}
	resultMap := map[string]any{}
	if len(request.Input) > 0 {
		if err := json.Unmarshal(request.Input, &inputMap); err != nil {
			return nil, fmt.Errorf("decode run input: %w", err)
		}
	}
	if len(output) > 0 {
		if err := json.Unmarshal(output, &resultMap); err != nil {
			return nil, fmt.Errorf("decode node output: %w", err)
		}
	}
	return flow.EvaluateEdges(request.Outgoing, inputMap, resultMap, request.Nodes)
}

func mustJSON(value any) json.RawMessage {
	data, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return data
}

func approvalSignalName(nodeID string) string { return "approval:" + nodeID }

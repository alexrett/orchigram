// Package engine isolates the durable workflow framework behind product contracts.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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

type runCanceler interface {
	CancelRun(context.Context, string, string) error
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
	runs    runCanceler
	once    sync.Once
}

// Open starts the go-workflows worker against its private SQLite history database.
func Open(ctx context.Context, workflowDBPath string, state *store.Store, executor TaskExecutor) (*Adapter, error) {
	b := workflowSqlite.NewSqliteBackend(workflowDBPath, workflowSqlite.WithBackendOptions(
		backend.WithWorkflowLockTimeout(2*time.Second),
		backend.WithActivityLockTimeout(2*time.Second),
		backend.WithStickyTimeout(0),
	))
	workerOptions := worker.DefaultOptions
	workerOptions.WorkflowHeartbeatInterval = 500 * time.Millisecond
	workerOptions.ActivityHeartbeatInterval = 500 * time.Millisecond
	w := worker.New(b, &workerOptions)
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
	adapter.runs, _ = executor.(runCanceler)
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
func (a *Adapter) Cancel(ctx context.Context, runUID, reason string) error {
	var providerErr error
	if a.runs != nil {
		providerErr = a.runs.CancelRun(ctx, runUID, reason)
	}
	instance, err := a.findInstance(ctx, runUID)
	if err != nil {
		if providerErr != nil {
			return fmt.Errorf("cancel provider calls: %w", providerErr)
		}
		return err
	}
	return errors.Join(providerErr, a.client.CancelWorkflowInstance(ctx, instance))
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
	cancellations, err := a.store.UndeliveredRunCancellations(ctx, 100)
	if err != nil {
		return err
	}
	for _, cancellation := range cancellations {
		if err := a.Cancel(ctx, cancellation.RunUID, cancellation.Reason); err != nil && !errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("redeliver cancellation %s: %w", cancellation.RunUID, err)
		}
		if err := a.store.MarkRunCancellationDelivered(ctx, cancellation.RunUID); err != nil {
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
	RunUID    string          `json:"runUID"`
	Node      flow.PlanNode   `json:"node"`
	Iteration int             `json:"iteration"`
	Outgoing  []flow.PlanEdge `json:"outgoing"`
	Input     json.RawMessage `json:"input"`
	Nodes     map[string]any  `json:"nodes"`
}

// NodeResult is a stable recorded activity result.
type NodeResult struct {
	Output    json.RawMessage `json:"output"`
	Activated []bool          `json:"activated"`
	Rejected  bool            `json:"rejected,omitempty"`
}

// InterpreterWorkflow executes an immutable plan as deterministic data.
func InterpreterWorkflow(ctx workflowRuntime.Context, runUID string, plan flow.ExecutionPlan, input json.RawMessage) error {
	model, err := buildComponentModel(plan)
	if err != nil {
		_, _ = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityFailRun, runUID, err.Error()).Get(ctx)
		return err
	}
	edgeActive := make([]bool, len(plan.Edges))
	nodeOutputs := map[string]any{}
	for _, componentIndex := range model.order {
		component := model.components[componentIndex]
		active := len(component.incoming) == 0
		for _, edgeIndex := range component.incoming {
			if edgeActive[edgeIndex] {
				active = true
			}
		}
		if !active {
			for _, nodeID := range component.nodes {
				if err := skipPlanNode(ctx, runUID, nodeID); err != nil {
					return err
				}
			}
			continue
		}
		if !component.cyclic {
			node := model.nodes[component.nodes[0]]
			result, executeErr := executePlanNode(ctx, NodeRequest{RunUID: runUID, Node: node, Iteration: 0, Outgoing: model.outgoingEdges(node.ID, plan), Input: input, Nodes: nodeOutputs})
			if executeErr != nil {
				return failInterpreterRun(ctx, runUID, executeErr)
			}
			model.recordResult(node.ID, result, edgeActive, nodeOutputs)
			if result.Rejected {
				_, executeErr = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "rejected").Get(ctx)
				return executeErr
			}
			continue
		}
		queue := []string{}
		queued := map[string]bool{}
		for _, edgeIndex := range component.incoming {
			if edgeActive[edgeIndex] {
				target := plan.Edges[edgeIndex].To
				if !queued[target] {
					queue, queued[target] = append(queue, target), true
				}
			}
		}
		if len(queue) == 0 && len(component.incoming) == 0 {
			queue, queued[component.nodes[0]] = append(queue, component.nodes[0]), true
		}
		counts := map[string]int{}
		for len(queue) > 0 {
			nodeID := queue[0]
			queue = queue[1:]
			queued[nodeID] = false
			if counts[nodeID] >= component.maxIterations {
				continue
			}
			node := model.nodes[nodeID]
			iteration := counts[nodeID]
			counts[nodeID]++
			result, executeErr := executePlanNode(ctx, NodeRequest{RunUID: runUID, Node: node, Iteration: iteration, Outgoing: model.outgoingEdges(node.ID, plan), Input: input, Nodes: nodeOutputs})
			if executeErr != nil {
				return failInterpreterRun(ctx, runUID, executeErr)
			}
			model.recordResult(node.ID, result, edgeActive, nodeOutputs)
			if result.Rejected {
				_, executeErr = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "rejected").Get(ctx)
				return executeErr
			}
			for position, edgeIndex := range model.outgoing[nodeID] {
				if position >= len(result.Activated) || !result.Activated[position] {
					continue
				}
				target := plan.Edges[edgeIndex].To
				if model.componentByNode[target] == componentIndex && counts[target] < component.maxIterations && !queued[target] {
					queue, queued[target] = append(queue, target), true
				}
			}
		}
		for _, nodeID := range component.nodes {
			if counts[nodeID] == 0 {
				if err := skipPlanNode(ctx, runUID, nodeID); err != nil {
					return err
				}
			}
		}
	}
	_, err = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "succeeded").Get(ctx)
	return err
}

type planComponent struct {
	nodes         []string
	incoming      []int
	cyclic        bool
	maxIterations int
}

type componentModel struct {
	nodes           map[string]flow.PlanNode
	outgoing        map[string][]int
	componentByNode map[string]int
	components      []planComponent
	order           []int
}

func buildComponentModel(plan flow.ExecutionPlan) (componentModel, error) {
	model := componentModel{nodes: map[string]flow.PlanNode{}, outgoing: map[string][]int{}, componentByNode: map[string]int{}}
	for _, node := range plan.Nodes {
		model.nodes[node.ID] = node
	}
	components := make([][]string, len(plan.Components))
	for index, component := range plan.Components {
		components[index] = append([]string(nil), component...)
	}
	covered := map[string]bool{}
	for _, component := range components {
		for _, nodeID := range component {
			covered[nodeID] = true
		}
	}
	for _, node := range plan.Nodes {
		if !covered[node.ID] {
			components = append(components, []string{node.ID})
		}
	}
	for index, nodeIDs := range components {
		sort.Strings(nodeIDs)
		component := planComponent{nodes: nodeIDs, maxIterations: 1}
		for _, nodeID := range nodeIDs {
			node, exists := model.nodes[nodeID]
			if !exists {
				return componentModel{}, fmt.Errorf("plan component references unknown node %s", nodeID)
			}
			model.componentByNode[nodeID] = index
			if node.LoopMaxIterations > component.maxIterations {
				component.maxIterations = node.LoopMaxIterations
			}
		}
		model.components = append(model.components, component)
	}
	indegree := make([]int, len(model.components))
	adjacency := make(map[int][]int)
	seenComponentEdge := map[[2]int]bool{}
	for edgeIndex, edge := range plan.Edges {
		fromComponent, fromOK := model.componentByNode[edge.From]
		toComponent, toOK := model.componentByNode[edge.To]
		if !fromOK || !toOK {
			return componentModel{}, fmt.Errorf("plan edge references unknown node %s -> %s", edge.From, edge.To)
		}
		model.outgoing[edge.From] = append(model.outgoing[edge.From], edgeIndex)
		if fromComponent == toComponent {
			model.components[fromComponent].cyclic = model.components[fromComponent].cyclic || len(model.components[fromComponent].nodes) > 1 || edge.From == edge.To
			continue
		}
		model.components[toComponent].incoming = append(model.components[toComponent].incoming, edgeIndex)
		key := [2]int{fromComponent, toComponent}
		if !seenComponentEdge[key] {
			seenComponentEdge[key] = true
			adjacency[fromComponent] = append(adjacency[fromComponent], toComponent)
			indegree[toComponent]++
		}
	}
	for nodeID := range model.outgoing {
		sort.Slice(model.outgoing[nodeID], func(i, j int) bool {
			left, right := plan.Edges[model.outgoing[nodeID][i]], plan.Edges[model.outgoing[nodeID][j]]
			if left.To == right.To {
				return left.Condition < right.Condition
			}
			return left.To < right.To
		})
	}
	ready := []int{}
	for index, degree := range indegree {
		if degree == 0 {
			ready = append(ready, index)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return model.components[ready[i]].nodes[0] < model.components[ready[j]].nodes[0] })
	for len(ready) > 0 {
		current := ready[0]
		ready = ready[1:]
		model.order = append(model.order, current)
		for _, next := range adjacency[current] {
			indegree[next]--
			if indegree[next] == 0 {
				ready = append(ready, next)
				sort.Slice(ready, func(i, j int) bool { return model.components[ready[i]].nodes[0] < model.components[ready[j]].nodes[0] })
			}
		}
	}
	if len(model.order) != len(model.components) {
		return componentModel{}, errors.New("plan component graph is cyclic")
	}
	return model, nil
}

func (m componentModel) outgoingEdges(nodeID string, plan flow.ExecutionPlan) []flow.PlanEdge {
	result := make([]flow.PlanEdge, len(m.outgoing[nodeID]))
	for index, edgeIndex := range m.outgoing[nodeID] {
		result[index] = plan.Edges[edgeIndex]
	}
	return result
}

func (m componentModel) recordResult(nodeID string, result NodeResult, edgeActive []bool, nodeOutputs map[string]any) {
	var output any
	_ = json.Unmarshal(result.Output, &output)
	nodeOutputs[nodeID] = output
	for position, edgeIndex := range m.outgoing[nodeID] {
		if position < len(result.Activated) && result.Activated[position] {
			edgeActive[edgeIndex] = true
		}
	}
}

func executePlanNode(ctx workflowRuntime.Context, request NodeRequest) (NodeResult, error) {
	if request.Node.Uses != "core.approval" {
		backoff, _ := time.ParseDuration(request.Node.RetryBackoff)
		timeout, _ := time.ParseDuration(request.Node.Timeout)
		options := workflowRuntime.ActivityOptions{RetryOptions: workflowRuntime.RetryOptions{MaxAttempts: request.Node.RetryLimit + 1, FirstRetryInterval: backoff, BackoffCoefficient: 2, RetryTimeout: timeout}}
		return workflowRuntime.ExecuteActivity[NodeResult](ctx, options, activityExecuteNode, request).Get(ctx)
	}
	if _, err := workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityBeginApproval, request).Get(ctx); err != nil {
		return NodeResult{}, err
	}
	signalChannel := workflowRuntime.NewSignalChannel[ApprovalSignal](ctx, approvalSignalName(request.Node.ID))
	timeout, err := time.ParseDuration(request.Node.Timeout)
	if err != nil {
		return NodeResult{}, err
	}
	timerContext, cancelTimer := workflowRuntime.WithCancel(ctx)
	timer := workflowRuntime.ScheduleTimer(timerContext, timeout, workflowRuntime.WithTimerName(fmt.Sprintf("approval-timeout-%s-%d", request.Node.ID, request.Iteration)))
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
	return workflowRuntime.ExecuteActivity[NodeResult](ctx, workflowRuntime.DefaultActivityOptions, activityEndApproval, request, signal).Get(ctx)
}

func skipPlanNode(ctx workflowRuntime.Context, runUID, nodeID string) error {
	_, err := workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activitySkipNode, runUID, nodeID).Get(ctx)
	return err
}

func failInterpreterRun(ctx workflowRuntime.Context, runUID string, cause error) error {
	_, _ = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityFailRun, runUID, cause.Error()).Get(ctx)
	return cause
}

// Activities are framework activity boundaries with access to product state.
type Activities struct {
	store    *store.Store
	executor TaskExecutor
}

// ExecuteNode records and invokes a non-approval action at least once.
func (a *Activities) ExecuteNode(ctx context.Context, request NodeRequest) (NodeResult, error) {
	if request.Iteration < 0 || request.Iteration >= 1000 {
		return NodeResult{}, errors.New("node iteration is outside the compiled limit")
	}
	attempt := uint32(request.Iteration) + 1 //nolint:gosec // Iteration was bounded to [0, 999] above.
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "node.started", "running", attempt, nil); err != nil {
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
			key := fmt.Sprintf("run/%s/node/%s/iteration/%d/operation/execute", request.RunUID, request.Node.ID, request.Iteration)
			output, err = a.executor.Execute(ctx, request.RunUID, request.Node, request.Input, request.Nodes, key)
		}
	}
	if err != nil {
		_ = a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "node.failed", "running", attempt, mustJSON(map[string]any{"error": err.Error()}))
		return NodeResult{}, err
	}
	activated, err := evaluate(request, output)
	if err != nil {
		return NodeResult{}, err
	}
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "node.completed", "running", attempt, output); err != nil {
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

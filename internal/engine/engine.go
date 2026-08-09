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
	workflowActivity "github.com/cschleiden/go-workflows/activity"
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
	activityBeginEvent    = "orchigram.begin-event.v1"
	activityEndEvent      = "orchigram.end-event.v1"
	activityIgnoreEvent   = "orchigram.ignore-event.v1"
	activitySkipNode      = "orchigram.skip-node.v1"
	activityCancelCalls   = "orchigram.cancel-active-calls.v1"
	activityCompleteRun   = "orchigram.complete-run.v1"
	activityFailRun       = "orchigram.fail-run.v1"
)

// DurableEngine is the only execution contract visible to the daemon.
type DurableEngine interface {
	Start(context.Context, string, flow.ExecutionPlan, json.RawMessage) error
	Signal(context.Context, string, string, ApprovalSignal) error
	SignalEvent(context.Context, string, string, EventSignal) error
	Cancel(context.Context, string, string) error
	Reconcile(context.Context) error
	Describe(context.Context, string) (store.Run, error)
	Close() error
}

// TaskExecutor invokes non-core actions at an at-least-once activity boundary.
type TaskExecutor interface {
	Execute(context.Context, string, flow.PlanNode, json.RawMessage, map[string]any, string) (json.RawMessage, error)
}

type taskIdentityKey struct{}

// TaskIdentity identifies one physical delivery and its workflow-engine retry
// ordinal within a logical node iteration. Executors use it for CallMeta,
// artifacts, and structured events.
type TaskIdentity struct {
	LogicalIteration int
	Attempt          uint32
	FrameworkAttempt uint32
}

// TaskIdentityFromContext returns the durable execution identity. Direct
// executor conformance calls default to the first attempt of iteration zero.
func TaskIdentityFromContext(ctx context.Context) TaskIdentity {
	identity, _ := ctx.Value(taskIdentityKey{}).(TaskIdentity)
	if identity.Attempt == 0 {
		identity.Attempt = 1
	}
	if identity.FrameworkAttempt == 0 {
		identity.FrameworkAttempt = 1
	}
	return identity
}

func withTaskIdentity(ctx context.Context, identity TaskIdentity) context.Context {
	return context.WithValue(ctx, taskIdentityKey{}, identity)
}

type runCanceler interface {
	CancelRun(context.Context, string, string) error
}

// ApprovalSignal is the framework-independent durable decision payload.
type ApprovalSignal struct {
	State  string `json:"state"`
	Reason string `json:"reason,omitempty"`
}

// EventSignal is the framework-independent external event envelope. Stable
// provider IDs let the interpreter ignore at-least-once signal redelivery.
type EventSignal struct {
	ProviderEventID string          `json:"providerEventID"`
	Payload         json.RawMessage `json:"payload"`
}

// Adapter runs one stable data-driven interpreter on go-workflows.
type Adapter struct {
	backend   backend.Backend
	client    *workflowClient.Client
	worker    *worker.Worker
	store     *store.Store
	cancel    context.CancelFunc
	done      chan error
	runs      runCanceler
	lifecycle sync.Mutex
	once      sync.Once
}

// OpenOptions bounds durable worker concurrency for one daemon process.
type OpenOptions struct {
	MaxConcurrentActivities int
}

// Open starts the go-workflows worker against its private SQLite history database.
func Open(ctx context.Context, workflowDBPath string, state *store.Store, executor TaskExecutor, options ...OpenOptions) (*Adapter, error) {
	b, err := openWorkflowBackend(workflowDBPath)
	if err != nil {
		return nil, err
	}
	workerOptions := worker.DefaultOptions
	if len(options) > 0 && options[0].MaxConcurrentActivities > 0 {
		workerOptions.MaxParallelActivityTasks = options[0].MaxConcurrentActivities
	}
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
		{activityBeginEvent, activities.BeginEvent},
		{activityEndEvent, activities.EndEvent},
		{activityIgnoreEvent, activities.IgnoreEvent},
		{activitySkipNode, activities.SkipNode},
		{activityCancelCalls, activities.CancelActiveCalls},
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

func openWorkflowBackend(workflowDBPath string) (result backend.Backend, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				err = fmt.Errorf("initialize workflow SQLite backend: %w", recoveredErr)
			} else {
				err = fmt.Errorf("initialize workflow SQLite backend: %v", recovered)
			}
			result = nil
		}
	}()
	result = workflowSqlite.NewSqliteBackend(workflowDBPath, workflowSqlite.WithBackendOptions(
		backend.WithWorkflowLockTimeout(2*time.Second),
		backend.WithActivityLockTimeout(2*time.Second),
		backend.WithStickyTimeout(0),
	))
	return result, nil
}

// Start idempotently starts a pinned interpreter instance.
func (a *Adapter) Start(ctx context.Context, runUID string, plan flow.ExecutionPlan, input json.RawMessage) error {
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	run, err := a.store.GetRun(ctx, runUID)
	if err != nil {
		return err
	}
	if run.Phase == "cancelled" {
		return nil
	}
	if _, err := a.findInstance(ctx, runUID); err == nil {
		return nil
	} else if !errors.Is(err, store.ErrNotFound) {
		return err
	}
	_, err = a.client.CreateWorkflowInstance(ctx, workflowClient.WorkflowInstanceOptions{InstanceID: runUID}, workflowName, runUID, plan, input)
	if err == nil {
		return a.cancelCreatedRunIfRequested(ctx, runUID)
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

// SignalEvent durably delivers one external occurrence to a core.event node.
func (a *Adapter) SignalEvent(ctx context.Context, runUID, nodeID string, signal EventSignal) error {
	return a.client.SignalWorkflow(ctx, runUID, eventSignalName(nodeID), signal)
}

// Cancel cancels a workflow instance after the daemon records operator intent.
func (a *Adapter) Cancel(ctx context.Context, runUID, reason string) error {
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
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

func (a *Adapter) cancelCreatedRunIfRequested(ctx context.Context, runUID string) error {
	run, err := a.store.GetRun(ctx, runUID)
	if err != nil {
		return err
	}
	if run.Phase != "cancelled" {
		return nil
	}
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
	cancellations, err := a.store.UndeliveredRunCancellations(ctx, 100)
	if err != nil {
		return err
	}
	for _, cancellation := range cancellations {
		if err := a.Cancel(ctx, cancellation.RunUID, cancellation.Reason); err != nil {
			if !errors.Is(err, store.ErrNotFound) {
				return fmt.Errorf("redeliver cancellation %s: %w", cancellation.RunUID, err)
			}
			_, markErr := a.store.MarkRunCancellationDeliveredIfStartImpossible(ctx, cancellation.RunUID)
			if markErr != nil {
				return markErr
			}
			continue
		}
		if err := a.store.MarkRunCancellationDelivered(ctx, cancellation.RunUID); err != nil {
			return err
		}
	}
	return nil
}

// RemoveFinishedRun deletes one terminal workflow history after product
// retention has selected the corresponding immutable Run evidence.
func (a *Adapter) RemoveFinishedRun(ctx context.Context, runUID string) error {
	a.lifecycle.Lock()
	defer a.lifecycle.Unlock()
	instance, err := a.findInstance(ctx, runUID)
	if errors.Is(err, store.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return a.client.RemoveWorkflowInstance(ctx, instance)
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
	maxParallel := plan.MaxParallel
	if maxParallel <= 0 {
		maxParallel = 1
	}
	pending := append([]int(nil), model.order...)
	completed := make([]bool, len(model.components))
	completions := workflowRuntime.NewBufferedChannel[componentCompletion](maxParallel)
	schedulerContext, cancelScheduler := workflowRuntime.WithCancel(ctx)
	defer cancelScheduler()
	active := 0
	stopAdmission := false
	firstError := ""
	rejected := false
	for len(pending) > 0 || active > 0 {
		if !stopAdmission {
			for active < maxParallel {
				position := model.nextReady(pending, completed)
				if position < 0 {
					break
				}
				componentIndex := pending[position]
				pending = append(pending[:position], pending[position+1:]...)
				componentActive := model.componentActive(componentIndex, edgeActive)
				inputEdges := append([]bool(nil), edgeActive...)
				inputNodes := cloneNodeOutputs(nodeOutputs)
				active++
				workflowRuntime.Go(schedulerContext, func(componentContext workflowRuntime.Context) {
					result := executePlanComponent(componentContext, runUID, plan, input, model, componentIndex, componentActive, inputEdges, inputNodes)
					if !completions.SendNonblocking(componentCompletion{index: componentIndex, result: result}) {
						panic("component completion buffer is exhausted")
					}
				})
			}
		}
		if active == 0 {
			if stopAdmission {
				break
			}
			return failInterpreterRun(ctx, runUID, errors.New("component scheduler reached a dependency deadlock"))
		}
		completion, ok := completions.Receive(ctx)
		if !ok {
			return ctx.Err()
		}
		active--
		completed[completion.index] = true
		if completion.result.errorText != "" {
			if firstError == "" {
				firstError = completion.result.errorText
				stopAdmission = true
				_, _ = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.ActivityOptions{RetryOptions: workflowRuntime.RetryOptions{MaxAttempts: 1}}, activityCancelCalls, runUID, "parallel component failed").Get(ctx)
				cancelScheduler()
			}
			continue
		}
		for edgeIndex, activated := range completion.result.edgeActive {
			if activated {
				edgeActive[edgeIndex] = true
			}
		}
		for nodeID, output := range completion.result.outputs {
			nodeOutputs[nodeID] = output
		}
		if completion.result.rejected && !rejected {
			rejected = true
			stopAdmission = true
			_, _ = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.ActivityOptions{RetryOptions: workflowRuntime.RetryOptions{MaxAttempts: 1}}, activityCancelCalls, runUID, "parallel approval rejected").Get(ctx)
			cancelScheduler()
		}
	}
	if firstError != "" {
		return failInterpreterRun(ctx, runUID, errors.New(firstError))
	}
	if rejected {
		_, err = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "rejected").Get(ctx)
		return err
	}
	_, err = workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityCompleteRun, runUID, "succeeded").Get(ctx)
	return err
}

type componentCompletion struct {
	index  int
	result componentResult
}

type componentResult struct {
	edgeActive []bool
	outputs    map[string]any
	rejected   bool
	errorText  string
}

func executePlanComponent(ctx workflowRuntime.Context, runUID string, plan flow.ExecutionPlan, input json.RawMessage, model componentModel, componentIndex int, active bool, inputEdges []bool, inputNodes map[string]any) componentResult {
	component := model.components[componentIndex]
	result := componentResult{edgeActive: make([]bool, len(plan.Edges)), outputs: map[string]any{}}
	if !active {
		for _, nodeID := range component.nodes {
			if err := skipPlanNode(ctx, runUID, nodeID); err != nil {
				result.errorText = err.Error()
				return result
			}
		}
		return result
	}
	localNodes := cloneNodeOutputs(inputNodes)
	seenEventIDs := map[string]bool{}
	record := func(nodeID string, nodeResult NodeResult) {
		model.recordResult(nodeID, nodeResult, result.edgeActive, localNodes)
		result.outputs[nodeID] = localNodes[nodeID]
	}
	if !component.cyclic {
		node := model.nodes[component.nodes[0]]
		nodeResult, err := executePlanNode(ctx, NodeRequest{RunUID: runUID, Node: node, Iteration: 0, Outgoing: model.outgoingEdges(node.ID, plan), Input: input, Nodes: localNodes}, seenEventIDs)
		if err != nil {
			result.errorText = err.Error()
			return result
		}
		record(node.ID, nodeResult)
		result.rejected = nodeResult.Rejected
		return result
	}
	queue := []string{}
	queued := map[string]bool{}
	for _, edgeIndex := range component.incoming {
		if inputEdges[edgeIndex] {
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
		nodeResult, err := executePlanNode(ctx, NodeRequest{RunUID: runUID, Node: node, Iteration: iteration, Outgoing: model.outgoingEdges(node.ID, plan), Input: input, Nodes: localNodes}, seenEventIDs)
		if err != nil {
			result.errorText = err.Error()
			return result
		}
		record(nodeID, nodeResult)
		if nodeResult.Rejected {
			result.rejected = true
			return result
		}
		for position, edgeIndex := range model.outgoing[nodeID] {
			if position >= len(nodeResult.Activated) || !nodeResult.Activated[position] {
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
				result.errorText = err.Error()
				return result
			}
		}
	}
	return result
}

func cloneNodeOutputs(source map[string]any) map[string]any {
	result := make(map[string]any, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

type planComponent struct {
	nodes         []string
	incoming      []int
	predecessors  []int
	cyclic        bool
	maxIterations int
}

func (m componentModel) nextReady(pending []int, completed []bool) int {
	for position, componentIndex := range pending {
		ready := true
		for _, predecessor := range m.components[componentIndex].predecessors {
			if !completed[predecessor] {
				ready = false
				break
			}
		}
		if ready {
			return position
		}
	}
	return -1
}

func (m componentModel) componentActive(componentIndex int, edgeActive []bool) bool {
	component := m.components[componentIndex]
	if len(component.incoming) == 0 {
		return true
	}
	for _, edgeIndex := range component.incoming {
		if edgeActive[edgeIndex] {
			return true
		}
	}
	return false
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
			model.components[toComponent].predecessors = append(model.components[toComponent].predecessors, fromComponent)
			indegree[toComponent]++
		}
	}
	for index := range model.components {
		sort.Slice(model.components[index].predecessors, func(i, j int) bool {
			left := model.components[model.components[index].predecessors[i]].nodes[0]
			right := model.components[model.components[index].predecessors[j]].nodes[0]
			return left < right
		})
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

func executePlanNode(ctx workflowRuntime.Context, request NodeRequest, seenEventIDs map[string]bool) (NodeResult, error) {
	if request.Node.Uses != "core.approval" && request.Node.Uses != "core.event" {
		backoff, _ := time.ParseDuration(request.Node.RetryBackoff)
		timeout, _ := time.ParseDuration(request.Node.Timeout)
		options := workflowRuntime.ActivityOptions{RetryOptions: workflowRuntime.RetryOptions{MaxAttempts: request.Node.RetryLimit + 1, FirstRetryInterval: backoff, BackoffCoefficient: 2, RetryTimeout: timeout}}
		return workflowRuntime.ExecuteActivity[NodeResult](ctx, options, activityExecuteNode, request).Get(ctx)
	}
	if request.Node.Uses == "core.event" {
		return executeEventNode(ctx, request, seenEventIDs)
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

func executeEventNode(ctx workflowRuntime.Context, request NodeRequest, seenEventIDs map[string]bool) (NodeResult, error) {
	if _, err := workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityBeginEvent, request).Get(ctx); err != nil {
		return NodeResult{}, err
	}
	signalChannel := workflowRuntime.NewSignalChannel[EventSignal](ctx, eventSignalName(request.Node.ID))
	timeout, err := time.ParseDuration(request.Node.Timeout)
	if err != nil {
		return NodeResult{}, err
	}
	timerContext, cancelTimer := workflowRuntime.WithCancel(ctx)
	defer cancelTimer()
	timer := workflowRuntime.ScheduleTimer(timerContext, timeout, workflowRuntime.WithTimerName(fmt.Sprintf("event-timeout-%s-%d", request.Node.ID, request.Iteration)))
	for {
		var signal EventSignal
		received, expired := false, false
		workflowRuntime.Select(ctx,
			workflowRuntime.Receive(signalChannel, func(_ workflowRuntime.Context, value EventSignal, ok bool) {
				if ok {
					signal, received = value, true
				}
			}),
			workflowRuntime.Await(timer, func(workflowRuntime.Context, workflowRuntime.Future[any]) { expired = true }),
		)
		if expired {
			return NodeResult{}, fmt.Errorf("external event node %s timed out", request.Node.ID)
		}
		if !received || signal.ProviderEventID == "" || !json.Valid(signal.Payload) {
			return NodeResult{}, fmt.Errorf("external event node %s received an invalid signal", request.Node.ID)
		}
		var object map[string]any
		if err := json.Unmarshal(signal.Payload, &object); err != nil || object == nil {
			return NodeResult{}, fmt.Errorf("external event node %s requires a JSON object payload", request.Node.ID)
		}
		if seenEventIDs[signal.ProviderEventID] {
			if _, err := workflowRuntime.ExecuteActivity[bool](ctx, workflowRuntime.DefaultActivityOptions, activityIgnoreEvent, request, signal).Get(ctx); err != nil {
				return NodeResult{}, err
			}
			continue
		}
		seenEventIDs[signal.ProviderEventID] = true
		cancelTimer()
		return workflowRuntime.ExecuteActivity[NodeResult](ctx, workflowRuntime.DefaultActivityOptions, activityEndEvent, request, signal).Get(ctx)
	}
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
	frameworkAttempt := workflowActivity.Attempt(ctx) + 1
	if frameworkAttempt < 1 || frameworkAttempt > request.Node.RetryLimit+1 {
		return NodeResult{}, fmt.Errorf("framework attempt %d is outside retry policy", frameworkAttempt)
	}
	frameworkAttemptNumber := uint32(frameworkAttempt) //nolint:gosec // Framework attempt is bounded by the compiled retry limit.
	key := fmt.Sprintf("run/%s/node/%s/iteration/%d/operation/execute", request.RunUID, request.Node.ID, request.Iteration)
	record, created, err := a.store.BeginNodeAttempt(ctx, request.RunUID, request.Node.ID, request.Iteration, frameworkAttemptNumber, key, request.Input)
	if err != nil {
		return NodeResult{}, err
	}
	if !created && !record.CompletedAt.IsZero() {
		if record.Phase != "succeeded" {
			return NodeResult{}, errors.New(record.ErrorText)
		}
		activated, evaluateErr := evaluate(request, record.Output)
		if evaluateErr != nil {
			return NodeResult{}, evaluateErr
		}
		return NodeResult{Output: record.Output, Activated: activated}, nil
	}
	attempt := record.Attempt
	var output json.RawMessage
	var executeErr error
	exitOutcome := "completed"
	switch request.Node.Uses {
	case "core.noop":
		output = json.RawMessage(`{"ok":true}`)
		if configured, exists := request.Node.With["result"]; exists {
			output, executeErr = json.Marshal(configured)
		}
	case "core.fail":
		executeErr = errors.New("core.fail requested")
		exitOutcome = "core-error"
	default:
		if a.executor == nil {
			executeErr = fmt.Errorf("no task executor for %q", request.Node.Uses)
			exitOutcome = "executor-unavailable"
		} else {
			executionContext := withTaskIdentity(ctx, TaskIdentity{LogicalIteration: request.Iteration, Attempt: attempt, FrameworkAttempt: frameworkAttemptNumber})
			output, executeErr = a.executor.Execute(executionContext, request.RunUID, request.Node, request.Input, request.Nodes, key)
			exitOutcome = "plugin-completed"
		}
	}
	if executeErr != nil {
		persistContext, cancelPersistence := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		resultError := executeErr
		switch {
		case errors.Is(executeErr, context.Canceled):
			exitOutcome = "cancelled"
			if ctx.Err() != nil {
				run, runErr := a.store.GetRun(persistContext, request.RunUID)
				if runErr == nil && run.Phase != "cancelled" {
					exitOutcome = "delivery-lost"
					resultError = errors.New("activity delivery was interrupted before durable completion")
				}
			} else {
				resultError = workflowRuntime.NewPermanentError(executeErr)
			}
		case errors.Is(executeErr, context.DeadlineExceeded):
			exitOutcome = "deadline-exceeded"
		case exitOutcome == "plugin-completed":
			exitOutcome = "plugin-failed"
		}
		_, _ = a.store.CompleteNodeAttempt(persistContext, request.RunUID, request.Node.ID, request.Iteration, attempt, "failed", nil, resultError.Error(), exitOutcome)
		cancelPersistence()
		return NodeResult{}, resultError
	}
	activated, evaluateErr := evaluate(request, output)
	if evaluateErr != nil {
		persistContext, cancelPersistence := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_, _ = a.store.CompleteNodeAttempt(persistContext, request.RunUID, request.Node.ID, request.Iteration, attempt, "failed", nil, evaluateErr.Error(), "evaluation-failed")
		cancelPersistence()
		return NodeResult{}, evaluateErr
	}
	persistContext, cancelPersistence := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	_, err = a.store.CompleteNodeAttempt(persistContext, request.RunUID, request.Node.ID, request.Iteration, attempt, "succeeded", output, "", exitOutcome)
	cancelPersistence()
	if err != nil {
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

// BeginEvent records the durable wait in the product projection before the
// workflow suspends on its signal channel.
func (a *Activities) BeginEvent(ctx context.Context, request NodeRequest) (bool, error) {
	return true, a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "event.waiting", "waiting", uint32(request.Iteration+1), nil) //nolint:gosec // Iterations are bounded below 1000.
}

// EndEvent projects the accepted payload and evaluates declarative edges.
func (a *Activities) EndEvent(ctx context.Context, request NodeRequest, signal EventSignal) (NodeResult, error) {
	activated, err := evaluate(request, signal.Payload)
	if err != nil {
		return NodeResult{}, err
	}
	var payload any
	if err := json.Unmarshal(signal.Payload, &payload); err != nil {
		return NodeResult{}, err
	}
	evidence := mustJSON(map[string]any{"providerEventID": signal.ProviderEventID, "payload": payload})
	if err := a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "event.received", "running", uint32(request.Iteration+1), evidence); err != nil { //nolint:gosec // Iterations are bounded below 1000.
		return NodeResult{}, err
	}
	return NodeResult{Output: signal.Payload, Activated: activated}, nil
}

// IgnoreEvent records at-least-once redelivery without advancing the graph.
func (a *Activities) IgnoreEvent(ctx context.Context, request NodeRequest, signal EventSignal) (bool, error) {
	payload := mustJSON(map[string]any{"providerEventID": signal.ProviderEventID})
	return true, a.store.AppendRunEvent(ctx, request.RunUID, request.Node.ID, "event.duplicate", "waiting", uint32(request.Iteration+1), payload) //nolint:gosec // Iterations are bounded below 1000.
}

// SkipNode records a conditional skip.
func (a *Activities) SkipNode(ctx context.Context, runUID, nodeID string) (bool, error) {
	return true, a.store.AppendRunEvent(ctx, runUID, nodeID, "node.skipped", "running", 0, nil)
}

// CancelActiveCalls propagates an interpreter branch failure to provider calls
// that were already admitted in sibling components. The scheduler still drains
// every component result before committing the terminal Run transition.
func (a *Activities) CancelActiveCalls(ctx context.Context, runUID, reason string) (bool, error) {
	canceler, ok := a.executor.(runCanceler)
	if !ok {
		return false, nil
	}
	return true, canceler.CancelRun(ctx, runUID, reason)
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
func eventSignalName(nodeID string) string    { return "event:" + nodeID }

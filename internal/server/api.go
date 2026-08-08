// Package server implements the public daemon gRPC control plane.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	"github.com/alexrett/orchigram/internal/backup"
	"github.com/alexrett/orchigram/internal/engine"
	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/health"
	"github.com/alexrett/orchigram/internal/orchestrator"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/plugincontroller"
	"github.com/alexrett/orchigram/internal/pluginmanager"
	"github.com/alexrett/orchigram/internal/references"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
	triggercontroller "github.com/alexrett/orchigram/internal/trigger"
	"github.com/alexrett/orchigram/internal/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

const maxArtifactDownload = 64 << 20

// API implements all public control-plane services over one authoritative state.
type API struct {
	store        *store.Store
	compiler     *flow.Compiler
	orchestrator *orchestrator.Orchestrator
	engine       engine.DurableEngine
	plugins      *pluginmanager.Manager
	pluginState  *plugincontroller.Controller
	references   *references.Resolver
	triggers     *triggercontroller.Controller
	stateDir     string
	startedAt    time.Time
}

// NewAPI constructs the public service implementation.
func NewAPI(state *store.Store, compiler *flow.Compiler, control *orchestrator.Orchestrator, durable engine.DurableEngine, plugins *pluginmanager.Manager, triggers *triggercontroller.Controller, stateDir string, pluginControllers ...*plugincontroller.Controller) *API {
	var pluginState *plugincontroller.Controller
	if len(pluginControllers) > 0 {
		pluginState = pluginControllers[0]
	}
	return &API{store: state, compiler: compiler, orchestrator: control, engine: durable, plugins: plugins, pluginState: pluginState, references: references.New(state, plugins), triggers: triggers, stateDir: stateDir, startedAt: time.Now()}
}

// Register binds every public service to one gRPC server.
func (a *API) Register(server *grpc.Server) {
	controlv1alpha1.RegisterResourceServiceServer(server, &resourceService{api: a})
	controlv1alpha1.RegisterFlowServiceServer(server, &flowService{api: a})
	controlv1alpha1.RegisterRunServiceServer(server, &runService{api: a})
	controlv1alpha1.RegisterTriggerServiceServer(server, &triggerService{api: a})
	controlv1alpha1.RegisterPluginServiceServer(server, &pluginService{api: a})
	controlv1alpha1.RegisterSystemServiceServer(server, &systemService{api: a})
}

// Apply validates and CAS-applies one strict resource.
func (a *API) Apply(ctx context.Context, request *controlv1alpha1.ApplyRequest) (*controlv1alpha1.ApplyResponse, error) {
	return a.apply(ctx, request, request.GetDryRun())
}

// Validate performs the exact apply validation path without persistence.
func (a *API) Validate(ctx context.Context, request *controlv1alpha1.ApplyRequest) (*controlv1alpha1.ApplyResponse, error) {
	return a.apply(ctx, request, true)
}

func (a *API) apply(ctx context.Context, request *controlv1alpha1.ApplyRequest, dryRun bool) (*controlv1alpha1.ApplyResponse, error) {
	doc, err := resource.DecodeStrict(request.GetDocument())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	// Status is a server projection, not desired state. In particular this keeps
	// SecretRef resolution state from being persisted when a GET is edited and
	// applied back through a schema-derived client form.
	doc, err = doc.WithServerStatus(nil)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resourceDiagnostics, diagnosticErr := a.resourceDiagnostics(ctx, doc)
	if diagnosticErr != nil {
		return nil, status.Error(codes.InvalidArgument, diagnosticErr.Error())
	}
	diagnostics := make([]*controlv1alpha1.Diagnostic, 0, len(resourceDiagnostics))
	for _, diagnostic := range resourceDiagnostics {
		diagnostics = append(diagnostics, diagnosticPB(diagnostic))
	}
	if flow.HasErrors(resourceDiagnostics) {
		return &controlv1alpha1.ApplyResponse{Resource: resourcePB(doc), Diagnostics: diagnostics}, nil
	}
	if dryRun {
		return &controlv1alpha1.ApplyResponse{Resource: resourcePB(doc), Diagnostics: diagnostics}, nil
	}
	meta := request.GetMeta()
	applied, err := a.store.Apply(ctx, doc, store.ApplyOptions{
		ExpectedResourceVersion: request.GetExpectedResourceVersion(),
		RequestID:               meta.GetRequestId(),
		Actor:                   "unix-peer",
		Context:                 meta.GetContext(),
	})
	if err != nil {
		return nil, rpcError(err)
	}
	return &controlv1alpha1.ApplyResponse{Resource: a.projectResource(ctx, applied), Diagnostics: diagnostics}, nil
}

// Get returns one resource.
func (a *API) Get(ctx context.Context, request *controlv1alpha1.GetRequest) (*controlv1alpha1.ResourceDocument, error) {
	key := request.GetKey()
	doc, err := a.store.Get(ctx, key.GetKind(), key.GetNamespace(), key.GetName())
	if err != nil {
		return nil, rpcError(err)
	}
	return a.projectResource(ctx, doc), nil
}

// List returns a stable resource page.
func (a *API) List(ctx context.Context, request *controlv1alpha1.ListRequest) (*controlv1alpha1.ListResponse, error) {
	if request.GetKind() != "" && !validResourceKind(request.GetKind()) {
		return nil, status.Error(codes.InvalidArgument, "kind filter is invalid")
	}
	if request.GetNamespace() != "" {
		if err := resource.ValidateMetadata(resource.ObjectMeta{Name: "list", Namespace: request.GetNamespace()}); err != nil {
			return nil, status.Error(codes.InvalidArgument, "namespace filter is invalid")
		}
	}
	if request.GetLimit() > 1000 {
		return nil, status.Error(codes.InvalidArgument, "limit must not exceed 1000")
	}
	token, err := decodeResourcePageToken(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	docs, revision, more, err := a.store.ListResourcePage(ctx, store.ResourcePageOptions{
		Kind: request.GetKind(), Namespace: request.GetNamespace(), Labels: request.GetLabels(), Limit: int(request.GetLimit()),
		AfterKind: token.Kind, AfterNamespace: token.Namespace, AfterName: token.Name, ExpectedRevision: token.Revision,
	})
	if err != nil {
		return nil, rpcError(err)
	}
	result := &controlv1alpha1.ListResponse{Revision: revision, Resources: make([]*controlv1alpha1.ResourceDocument, 0, len(docs))}
	for _, doc := range docs {
		result.Resources = append(result.Resources, a.projectResource(ctx, doc))
	}
	if more && len(docs) > 0 {
		result.ContinueToken, err = encodeResourcePageToken(request, revision, docs[len(docs)-1])
		if err != nil {
			return nil, status.Error(codes.Internal, "encode resource continue token")
		}
	}
	return result, nil
}

func (a *API) projectResource(ctx context.Context, document resource.Document) *controlv1alpha1.ResourceDocument {
	if document.Kind == "SecretRef" {
		state, backend := "Missing", "unknown"
		if a.plugins != nil {
			state, backend = a.plugins.SecretStatus(ctx, document.Metadata.Namespace, document.Metadata.Name)
		}
		projected, err := document.WithServerStatus(map[string]any{"state": state, "backend": backend})
		if err == nil {
			document = projected
		}
		return resourcePB(document)
	}
	if document.Kind == "Flow" || references.Supports(document.Kind) {
		diagnostics, err := a.resourceDiagnostics(ctx, document)
		if err == nil {
			if projected, projectionErr := document.WithServerStatus(references.Status(document, diagnostics)); projectionErr == nil {
				document = projected
			}
		}
	}
	return resourcePB(document)
}

func (a *API) resourceDiagnostics(ctx context.Context, document resource.Document) ([]flow.Diagnostic, error) {
	diagnostics := make([]flow.Diagnostic, 0)
	if document.Kind == "Flow" {
		flowResource, err := resource.DecodeFlow(document.JSON)
		if err != nil {
			return nil, err
		}
		_, diagnostics = a.compiler.Compile(flowResource)
	}
	if references.Supports(document.Kind) {
		diagnostics = append(diagnostics, a.references.Diagnostics(ctx, document)...)
	}
	if document.Kind == "PluginInstallation" && a.pluginState != nil {
		diagnostics = append(diagnostics, a.pluginState.Diagnostics(ctx, document)...)
	}
	return diagnostics, nil
}

// Delete CAS-deletes a resource.
func (a *API) Delete(ctx context.Context, request *controlv1alpha1.DeleteRequest) (*emptypb.Empty, error) {
	key := request.GetKey()
	if key.GetKind() == "PluginInstallation" {
		document, err := a.store.Get(ctx, key.GetKind(), key.GetNamespace(), key.GetName())
		if err != nil {
			return nil, rpcError(err)
		}
		installation, err := resource.DecodePluginInstallation(document.JSON)
		if err != nil {
			return nil, rpcError(err)
		}
		if installation.Spec.Enabled != nil && *installation.Spec.Enabled {
			return nil, status.Error(codes.FailedPrecondition, "an enabled PluginInstallation must be disabled before deletion")
		}
	}
	if err := a.store.Delete(ctx, key.GetKind(), key.GetNamespace(), key.GetName(), request.GetExpectedResourceVersion(), request.GetMeta().GetRequestId()); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

// Watch streams resource events from a requested global revision.
func (a *API) Watch(request *controlv1alpha1.WatchRequest, stream grpc.ServerStreamingServer[controlv1alpha1.ResourceEvent]) error {
	revision := request.GetAfterRevision()
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := a.store.EventsAfter(stream.Context(), request.GetKind(), request.GetNamespace(), revision, 100)
		if err != nil {
			return rpcError(err)
		}
		for _, event := range events {
			var document *controlv1alpha1.ResourceDocument
			if len(event.Document) > 0 {
				doc, decodeErr := resource.DecodeStrict(event.Document)
				if decodeErr != nil {
					return status.Error(codes.Internal, "stored resource event is invalid")
				}
				document = a.projectResource(stream.Context(), doc)
			}
			if err := stream.Send(&controlv1alpha1.ResourceEvent{Revision: event.Revision, Type: event.Type, Resource: document, ObservedAt: timestamppb.New(event.ObservedAt)}); err != nil {
				return err
			}
			revision = event.Revision
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

// Export returns canonical JSON in the YAML-compatible response envelope.
func (a *API) Export(ctx context.Context, request *controlv1alpha1.ExportRequest) (*controlv1alpha1.ExportResponse, error) {
	var output bytes.Buffer
	encoder := yaml.NewEncoder(&output)
	encoder.SetIndent(2)
	for _, key := range request.GetKeys() {
		doc, err := a.store.Get(ctx, key.GetKind(), key.GetNamespace(), key.GetName())
		if err != nil {
			return nil, rpcError(err)
		}
		doc, err = doc.WithServerStatus(nil)
		if err != nil {
			return nil, status.Error(codes.Internal, "strip server-owned status for export")
		}
		var value yaml.Node
		if err := yaml.Unmarshal(doc.JSON, &value); err != nil {
			return nil, status.Error(codes.Internal, "decode stored resource for export")
		}
		clearYAMLNodeStyles(&value)
		if err := encoder.Encode(&value); err != nil {
			return nil, status.Error(codes.Internal, "encode resource export")
		}
	}
	if err := encoder.Close(); err != nil {
		return nil, status.Error(codes.Internal, "close resource export")
	}
	return &controlv1alpha1.ExportResponse{Yaml: output.Bytes()}, nil
}

func clearYAMLNodeStyles(node *yaml.Node) {
	if node == nil {
		return
	}
	node.Style = 0
	for _, child := range node.Content {
		clearYAMLNodeStyles(child)
	}
}

// Compile compiles a strict Flow without storing it.
func (a *API) Compile(_ context.Context, request *controlv1alpha1.CompileRequest) (*controlv1alpha1.CompileResponse, error) {
	flowResource, err := resource.DecodeFlow(request.GetFlow())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	plan, diagnostics := a.compiler.Compile(flowResource)
	result := &controlv1alpha1.CompileResponse{PlanHash: plan.PlanHash}
	for _, diagnostic := range diagnostics {
		result.Diagnostics = append(result.Diagnostics, diagnosticPB(diagnostic))
	}
	if !flow.HasErrors(diagnostics) {
		result.ExecutionPlanJson, err = json.Marshal(plan)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return result, nil
}

// PreviewGraph returns the compiler's graph projection.
func (a *API) PreviewGraph(ctx context.Context, request *controlv1alpha1.PreviewGraphRequest) (*controlv1alpha1.PreviewGraphResponse, error) {
	compiled, err := a.Compile(ctx, &controlv1alpha1.CompileRequest{Flow: request.GetFlowOrPlan()})
	if err != nil {
		return nil, err
	}
	result := &controlv1alpha1.PreviewGraphResponse{Diagnostics: compiled.GetDiagnostics()}
	if len(compiled.GetExecutionPlanJson()) == 0 {
		return result, nil
	}
	var plan flow.ExecutionPlan
	if err := json.Unmarshal(compiled.GetExecutionPlanJson(), &plan); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	for _, node := range plan.Nodes {
		result.Nodes = append(result.Nodes, &controlv1alpha1.GraphNode{Id: node.ID, Label: node.Name, Action: node.Uses})
	}
	for _, edge := range plan.Edges {
		result.Edges = append(result.Edges, &controlv1alpha1.GraphEdge{From: edge.From, To: edge.To, Condition: edge.Condition})
	}
	return result, nil
}

// Start durably accepts a manual start request.
func (a *API) Start(ctx context.Context, request *controlv1alpha1.StartRunRequest) (*controlv1alpha1.RunRef, error) {
	if request.GetFlow() == "" {
		return nil, status.Error(codes.InvalidArgument, "flow is required")
	}
	input := request.GetInputJson()
	if len(input) == 0 {
		input = []byte(`{}`)
	}
	if !json.Valid(input) {
		return nil, status.Error(codes.InvalidArgument, "input_json must be valid JSON")
	}
	receipt, err := a.orchestrator.StartManual(ctx, request.GetFlow(), resource.DefaultNamespace, input, request.GetIdempotencyKey())
	if err != nil {
		return nil, rpcError(err)
	}
	return &controlv1alpha1.RunRef{Uid: receipt.RunUID}, nil
}

// ListRuns returns newest runs first.
func (a *API) ListRuns(ctx context.Context, request *controlv1alpha1.ListRunsRequest) (*controlv1alpha1.ListRunsResponse, error) {
	if request.GetLimit() > 1000 {
		return nil, status.Error(codes.InvalidArgument, "limit must not exceed 1000")
	}
	if phase := request.GetPhase(); phase != "" && !validRunPhase(phase) {
		return nil, status.Error(codes.InvalidArgument, "phase filter is invalid")
	}
	flowUID := request.GetFlow()
	if flowUID != "" {
		if document, getErr := a.store.Get(ctx, "Flow", resource.DefaultNamespace, flowUID); getErr == nil {
			flowUID = document.Metadata.UID
		} else if !errors.Is(getErr, store.ErrNotFound) {
			return nil, rpcError(getErr)
		}
	}
	runs, err := a.store.ListRunsFiltered(ctx, flowUID, request.GetPhase(), int(request.GetLimit()))
	if err != nil {
		return nil, rpcError(err)
	}
	result := &controlv1alpha1.ListRunsResponse{}
	for _, run := range runs {
		result.Runs = append(result.Runs, runPB(run))
	}
	return result, nil
}

// GetRun returns one run projection without reconciling or mutating it.
func (a *API) GetRun(ctx context.Context, request *controlv1alpha1.RunRequest) (*controlv1alpha1.RunSummary, error) {
	if request.GetUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}
	run, err := a.store.GetRun(ctx, request.GetUid())
	if err != nil {
		return nil, rpcError(err)
	}
	return runPB(run), nil
}

func validRunPhase(phase string) bool {
	switch phase {
	case "pending", "running", "waiting", "succeeded", "failed", "rejected", "cancelled":
		return true
	default:
		return false
	}
}

// RunPlan returns the exact immutable plan pinned by a run.
func (a *API) RunPlan(ctx context.Context, request *controlv1alpha1.RunRequest) (*controlv1alpha1.CompileResponse, error) {
	run, err := a.store.GetRun(ctx, request.GetUid())
	if err != nil {
		return nil, rpcError(err)
	}
	plan, err := a.store.GetPlan(ctx, run.PlanHash)
	if err != nil {
		return nil, rpcError(err)
	}
	encoded, err := json.Marshal(plan)
	if err != nil {
		return nil, rpcError(err)
	}
	return &controlv1alpha1.CompileResponse{ExecutionPlanJson: encoded, PlanHash: plan.PlanHash}, nil
}

// WatchEvents streams durable run events and supports replay from a sequence.
func (a *API) WatchEvents(request *controlv1alpha1.WatchRunRequest, stream grpc.ServerStreamingServer[controlv1alpha1.RunEvent]) error {
	sequence := request.GetAfterSequence()
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		events, err := a.store.RunEventsAfter(stream.Context(), request.GetUid(), sequence, 100)
		if err != nil {
			return rpcError(err)
		}
		for _, event := range events {
			if err := stream.Send(&controlv1alpha1.RunEvent{Sequence: event.Sequence, RunUid: event.RunUID, NodeId: event.NodeID, Attempt: event.Attempt, Type: event.Type, PayloadJson: event.Payload, OccurredAt: timestamppb.New(event.OccurredAt)}); err != nil {
				return err
			}
			sequence = event.Sequence
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-ticker.C:
		}
	}
}

// ListAttempts returns durable physical retries for one run.
func (a *API) ListAttempts(ctx context.Context, request *controlv1alpha1.ListAttemptsRequest) (*controlv1alpha1.ListAttemptsResponse, error) {
	if request.GetRunUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_uid is required")
	}
	attempts, err := a.store.ListNodeAttempts(ctx, request.GetRunUid(), request.GetNodeId(), int(request.GetLimit()))
	if err != nil {
		return nil, rpcError(err)
	}
	response := &controlv1alpha1.ListAttemptsResponse{}
	for _, attempt := range attempts {
		item := &controlv1alpha1.NodeAttempt{
			RunUid: attempt.RunUID, NodeId: attempt.NodeID, LogicalIteration: uint32(attempt.LogicalIteration), //nolint:gosec // Store rejects negative iterations.
			Attempt: attempt.Attempt, FrameworkAttempt: attempt.FrameworkAttempt, Phase: attempt.Phase, IdempotencyKey: attempt.IdempotencyKey,
			InputJson: attempt.Input, OutputJson: attempt.Output, Error: attempt.ErrorText,
			ExitOutcome: attempt.ExitOutcome, StartedAt: timestamppb.New(attempt.StartedAt),
		}
		if !attempt.CompletedAt.IsZero() {
			item.CompletedAt = timestamppb.New(attempt.CompletedAt)
		}
		response.Attempts = append(response.Attempts, item)
	}
	return response, nil
}

// ListArtifacts returns metadata without exposing private server paths.
func (a *API) ListArtifacts(ctx context.Context, request *controlv1alpha1.ListArtifactsRequest) (*controlv1alpha1.ListArtifactsResponse, error) {
	if request.GetRunUid() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_uid is required")
	}
	artifacts, err := a.store.ListArtifacts(ctx, request.GetRunUid(), int(request.GetLimit()))
	if err != nil {
		return nil, rpcError(err)
	}
	response := &controlv1alpha1.ListArtifactsResponse{}
	for _, artifact := range artifacts {
		response.Artifacts = append(response.Artifacts, artifactPB(artifact))
	}
	return response, nil
}

// GetArtifact verifies registered metadata before streaming bounded content.
func (a *API) GetArtifact(request *controlv1alpha1.GetArtifactRequest, stream grpc.ServerStreamingServer[controlv1alpha1.ArtifactChunk]) error {
	if request.GetUid() == "" {
		return status.Error(codes.InvalidArgument, "uid is required")
	}
	artifact, err := a.store.ArtifactByUID(stream.Context(), request.GetUid())
	if err != nil {
		return rpcError(err)
	}
	if artifact.SizeBytes < 0 || artifact.SizeBytes > maxArtifactDownload {
		return status.Error(codes.ResourceExhausted, "artifact exceeds the 64 MiB download limit")
	}
	name, err := secureArtifactName(artifact.RelativePath)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	root, err := os.OpenRoot(a.stateDir)
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(name)
	if err != nil {
		return status.Error(codes.DataLoss, "registered artifact file is unavailable")
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() != artifact.SizeBytes {
		return status.Error(codes.DataLoss, "artifact metadata does not match the registered file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxArtifactDownload+1))
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	if int64(len(data)) != artifact.SizeBytes {
		return status.Error(codes.DataLoss, "artifact size changed while it was read")
	}
	digest := sha256.Sum256(data)
	if hex.EncodeToString(digest[:]) != artifact.SHA256 {
		return status.Error(codes.DataLoss, "artifact digest verification failed")
	}
	for offset := 0; offset < len(data); offset += 64 << 10 {
		end := min(offset+(64<<10), len(data))
		if err := stream.Send(&controlv1alpha1.ArtifactChunk{Data: data[offset:end]}); err != nil {
			return err
		}
	}
	return nil
}

func secureArtifactName(relativePath string) (string, error) {
	if relativePath == "" || filepath.IsAbs(relativePath) {
		return "", errors.New("artifact path is not relative")
	}
	name := filepath.Clean(filepath.FromSlash(relativePath))
	if name == "." || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return "", errors.New("artifact path escapes state directory")
	}
	return name, nil
}

// Approve persists then delivers an approval signal.
func (a *API) Approve(ctx context.Context, request *controlv1alpha1.ApprovalRequest) (*emptypb.Empty, error) {
	return a.decide(ctx, request, "approved")
}

// Reject persists then delivers a rejection signal.
func (a *API) Reject(ctx context.Context, request *controlv1alpha1.ApprovalRequest) (*emptypb.Empty, error) {
	return a.decide(ctx, request, "rejected")
}

func (a *API) decide(ctx context.Context, request *controlv1alpha1.ApprovalRequest, decision string) (*emptypb.Empty, error) {
	if request.GetRunUid() == "" || request.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "run_uid and node_id are required")
	}
	if err := a.store.DecideApproval(ctx, request.GetRunUid(), request.GetNodeId(), decision, "unix-peer", request.GetReason()); err != nil {
		return nil, rpcError(err)
	}
	if err := a.engine.Signal(ctx, request.GetRunUid(), request.GetNodeId(), engine.ApprovalSignal{State: decision, Reason: request.GetReason()}); err != nil {
		// Decision remains durable and engine reconciliation redelivers it.
		return &emptypb.Empty{}, nil
	}
	if err := a.store.MarkApprovalSignaled(ctx, request.GetRunUid(), request.GetNodeId()); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

// Cancel records cancellation before asking the framework to cancel.
func (a *API) Cancel(ctx context.Context, request *controlv1alpha1.CancelRunRequest) (*emptypb.Empty, error) {
	if err := a.store.RequestRunCancellation(ctx, request.GetRunUid(), request.GetReason()); err != nil {
		return nil, rpcError(err)
	}
	if err := a.engine.Cancel(ctx, request.GetRunUid(), request.GetReason()); err != nil {
		if !errors.Is(err, store.ErrNotFound) {
			// Intent remains durable and engine reconciliation redelivers it.
			return &emptypb.Empty{}, nil
		}
		_, markErr := a.store.MarkRunCancellationDeliveredIfStartImpossible(ctx, request.GetRunUid())
		if markErr != nil {
			return nil, rpcError(markErr)
		}
		return &emptypb.Empty{}, nil
	}
	if err := a.store.MarkRunCancellationDelivered(ctx, request.GetRunUid()); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

// Reconcile returns the current run after running engine reconciliation.
func (a *API) Reconcile(ctx context.Context, request *controlv1alpha1.ReconcileRequest) (*controlv1alpha1.RunSummary, error) {
	if err := a.engine.Reconcile(ctx); err != nil {
		return nil, rpcError(err)
	}
	run, err := a.store.GetRun(ctx, request.GetRunUid())
	if err != nil {
		return nil, rpcError(err)
	}
	return runPB(run), nil
}

// Info reports protocol negotiation and process identity.
func (a *API) Info(context.Context, *emptypb.Empty) (*controlv1alpha1.SystemInfo, error) {
	hostname, _ := os.Hostname()
	capabilities := []string{"resources.v1alpha1", "flows.compile", "runs.approval", "plugins.grpc.v1", "plugins.automtls", "transport.uds"}
	if a.pluginState != nil {
		capabilities = append(capabilities, "plugins.declarative.v1")
	}
	sort.Strings(capabilities)
	return &controlv1alpha1.SystemInfo{Version: version.Version, ProtocolVersion: "v1alpha1", Hostname: hostname, Os: runtime.GOOS, Architecture: runtime.GOARCH, ProcessId: int64(os.Getpid()), StartedAt: timestamppb.New(a.startedAt), Capabilities: capabilities}, nil
}

// Health aggregates required control-plane components without leaking
// dependency errors, configuration, secret material, or daemon paths.
func (a *API) Health(ctx context.Context, _ *emptypb.Empty) (*controlv1alpha1.HealthResponse, error) {
	diagnostics := make([]health.Diagnostic, 0)
	if a.orchestrator != nil {
		diagnostics = append(diagnostics, a.orchestrator.HealthDiagnostics()...)
	}
	if a.triggers != nil {
		diagnostics = append(diagnostics, a.triggers.HealthDiagnostics()...)
	}
	if a.plugins != nil {
		diagnostics = append(diagnostics, a.plugins.HealthDiagnostics(ctx)...)
	}
	if a.pluginState != nil {
		diagnostics = append(diagnostics, a.pluginState.HealthDiagnostics()...)
	}
	sort.Slice(diagnostics, func(i, j int) bool {
		if diagnostics[i].Path != diagnostics[j].Path {
			return diagnostics[i].Path < diagnostics[j].Path
		}
		return diagnostics[i].Code < diagnostics[j].Code
	})
	response := &controlv1alpha1.HealthResponse{Ready: len(diagnostics) == 0, Diagnostics: make([]*controlv1alpha1.Diagnostic, 0, len(diagnostics))}
	for _, diagnostic := range diagnostics {
		response.Diagnostics = append(response.Diagnostics, &controlv1alpha1.Diagnostic{Severity: controlv1alpha1.Diagnostic_SEVERITY_ERROR, Path: diagnostic.Path, Code: diagnostic.Code, Message: diagnostic.Message})
	}
	return response, nil
}

// Backup creates an online state snapshot under the daemon state directory.
func (a *API) Backup(ctx context.Context, request *controlv1alpha1.BackupRequest) (*controlv1alpha1.BackupResponse, error) {
	result, err := backup.Create(ctx, a.stateDir, request.GetDestination())
	if err != nil {
		return nil, rpcError(err)
	}
	return &controlv1alpha1.BackupResponse{Path: result.Path, Sha256: result.SHA256}, nil
}

// InstallPlugin receives one bounded bundle and closes with its verified identity.
func (a *API) InstallPlugin(stream grpc.ClientStreamingServer[controlv1alpha1.PluginUploadRequest, controlv1alpha1.PluginInstallResponse]) error {
	if a.plugins == nil {
		return status.Error(codes.Unavailable, "plugin manager is unavailable")
	}
	bundle := make([]byte, 0)
	final := false
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if final {
			return status.Error(codes.InvalidArgument, "bundle data followed final chunk")
		}
		if len(bundle)+len(chunk.GetBundleChunk()) > 128<<20 {
			return status.Error(codes.ResourceExhausted, "plugin bundle exceeds 128 MiB")
		}
		bundle = append(bundle, chunk.GetBundleChunk()...)
		final = chunk.GetFinal()
		if final {
			break
		}
	}
	if !final {
		return status.Error(codes.InvalidArgument, "plugin bundle is missing a final chunk")
	}
	record, err := a.plugins.Install(stream.Context(), bundle)
	if err != nil {
		return rpcError(err)
	}
	if a.pluginState != nil {
		if _, err := a.pluginState.EnsureProjection(stream.Context(), record); err != nil {
			return rpcError(err)
		}
		if err := a.pluginState.Reconcile(stream.Context()); err != nil {
			return rpcError(err)
		}
	}
	return stream.SendAndClose(&controlv1alpha1.PluginInstallResponse{Name: record.Name, Version: record.Version, Digest: record.Digest})
}

// EnablePlugin activates a verified immutable plugin version.
func (a *API) EnablePlugin(ctx context.Context, request *controlv1alpha1.PluginRequest) (*emptypb.Empty, error) {
	if a.pluginState == nil {
		return nil, status.Error(codes.Unavailable, "plugin installation controller is unavailable")
	}
	if err := a.pluginState.SetEnabled(ctx, request.GetName(), request.GetVersion(), true); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

// DisablePlugin removes the current activation without deleting a version.
func (a *API) DisablePlugin(ctx context.Context, request *controlv1alpha1.PluginRequest) (*emptypb.Empty, error) {
	if a.pluginState == nil {
		return nil, status.Error(codes.Unavailable, "plugin installation controller is unavailable")
	}
	if err := a.pluginState.SetEnabled(ctx, request.GetName(), "", false); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

// ListPlugins returns all immutable installed versions.
func (a *API) ListPlugins(ctx context.Context, _ *emptypb.Empty) (*controlv1alpha1.ListPluginsResponse, error) {
	records, err := a.plugins.List(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	response := &controlv1alpha1.ListPluginsResponse{}
	for _, record := range records {
		response.Plugins = append(response.Plugins, pluginInfo(record))
	}
	return response, nil
}

// DescribePlugin returns one version or the active version when omitted.
func (a *API) DescribePlugin(ctx context.Context, request *controlv1alpha1.PluginRequest) (*controlv1alpha1.PluginInfo, error) {
	record, err := a.plugins.Describe(ctx, request.GetName(), request.GetVersion())
	if err != nil {
		return nil, rpcError(err)
	}
	return pluginInfo(record), nil
}

// DoctorPlugin verifies executable integrity, protocol negotiation, and health.
func (a *API) DoctorPlugin(ctx context.Context, request *controlv1alpha1.PluginRequest) (*controlv1alpha1.DoctorResponse, error) {
	if err := a.plugins.Doctor(ctx, request.GetName(), request.GetVersion()); err != nil {
		return &controlv1alpha1.DoctorResponse{Diagnostics: []*controlv1alpha1.Diagnostic{{Severity: controlv1alpha1.Diagnostic_SEVERITY_ERROR, Path: "plugin", Code: "doctor_failed", Message: err.Error()}}}, nil
	}
	return &controlv1alpha1.DoctorResponse{}, nil
}

// NextTriggerOccurrences previews deterministic future schedule identities.
func (a *API) NextTriggerOccurrences(ctx context.Context, request *controlv1alpha1.NextOccurrencesRequest) (*controlv1alpha1.NextOccurrencesResponse, error) {
	document, err := a.store.ResourceByUID(ctx, "Trigger", request.GetUid())
	if err != nil {
		return nil, rpcError(err)
	}
	trigger, err := resource.DecodeTrigger(document.JSON)
	if err != nil {
		return nil, rpcError(err)
	}
	occurrences, err := a.triggers.NextOccurrences(trigger, time.Now(), int(request.GetCount()))
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := &controlv1alpha1.NextOccurrencesResponse{}
	for _, occurrence := range occurrences {
		response.Occurrences = append(response.Occurrences, &controlv1alpha1.Occurrence{Identity: triggercontroller.OccurrenceIdentity(trigger, occurrence), ScheduledAt: timestamppb.New(occurrence)})
	}
	return response, nil
}

// SetTriggerEnabled applies an operator enable or disable override.
func (a *API) SetTriggerEnabled(ctx context.Context, request *controlv1alpha1.TriggerRequest, enabled bool) (*emptypb.Empty, error) {
	document, err := a.store.ResourceByUID(ctx, "Trigger", request.GetUid())
	if err != nil {
		return nil, rpcError(err)
	}
	trigger, err := resource.DecodeTrigger(document.JSON)
	if err != nil {
		return nil, rpcError(err)
	}
	if _, err := a.store.EnsureTriggerState(ctx, trigger.Metadata.UID, trigger.Metadata.Generation, trigger.Spec.Enabled == nil || *trigger.Spec.Enabled, time.Now()); err != nil {
		return nil, rpcError(err)
	}
	if err := a.store.SetTriggerEnabled(ctx, trigger.Metadata.UID, enabled); err != nil {
		return nil, rpcError(err)
	}
	return &emptypb.Empty{}, nil
}

// TriggerReceipts returns recent accepted external occurrences.
func (a *API) TriggerReceipts(ctx context.Context, request *controlv1alpha1.ReceiptRequest) (*controlv1alpha1.ReceiptResponse, error) {
	receipts, err := a.store.TriggerReceipts(ctx, request.GetTriggerUid(), int(request.GetLimit()))
	if err != nil {
		return nil, rpcError(err)
	}
	response := &controlv1alpha1.ReceiptResponse{}
	for _, receipt := range receipts {
		response.Receipts = append(response.Receipts, &controlv1alpha1.TriggerReceipt{Uid: receipt.UID, TriggerUid: receipt.TriggerUID, OccurrenceId: receipt.OccurrenceID, RunUid: receipt.RunUID, Deduplicated: receipt.Deduplicated, AcceptedAt: timestamppb.New(receipt.AcceptedAt)})
	}
	skips, err := a.store.TriggerSkips(ctx, request.GetTriggerUid(), int(request.GetLimit()))
	if err != nil {
		return nil, rpcError(err)
	}
	for _, skip := range skips {
		response.Skips = append(response.Skips, &controlv1alpha1.TriggerSkip{OccurrenceId: skip.OccurrenceID, Reason: skip.Reason, ScheduledAt: timestamppb.New(skip.ScheduledAt)})
	}
	return response, nil
}

func resourcePB(doc resource.Document) *controlv1alpha1.ResourceDocument {
	return &controlv1alpha1.ResourceDocument{Key: &controlv1alpha1.ResourceKey{Kind: doc.Kind, Namespace: doc.Metadata.Namespace, Name: doc.Metadata.Name, Uid: doc.Metadata.UID}, ResourceVersion: doc.Metadata.ResourceVersion, Generation: doc.Metadata.Generation, Json: doc.JSON}
}

func diagnosticPB(diagnostic flow.Diagnostic) *controlv1alpha1.Diagnostic {
	severity := controlv1alpha1.Diagnostic_SEVERITY_ERROR
	if diagnostic.Severity == flow.SeverityWarning {
		severity = controlv1alpha1.Diagnostic_SEVERITY_WARNING
	}
	return &controlv1alpha1.Diagnostic{Severity: severity, Path: diagnostic.Path, Code: diagnostic.Code, Message: diagnostic.Message}
}

func runPB(run store.Run) *controlv1alpha1.RunSummary {
	return &controlv1alpha1.RunSummary{Uid: run.UID, Flow: run.FlowUID, Phase: run.Phase, PlanHash: run.PlanHash, InterpreterVersion: run.InterpreterVersion, CreatedAt: timestamppb.New(run.CreatedAt), UpdatedAt: timestamppb.New(run.UpdatedAt)}
}

func pluginInfo(record store.PluginRecord) *controlv1alpha1.PluginInfo {
	var manifest pluginbundle.Manifest
	_ = json.Unmarshal(record.ManifestJSON, &manifest)
	state := record.State
	if record.Active {
		state = "active"
	}
	return &controlv1alpha1.PluginInfo{Name: record.Name, Version: record.Version, Digest: record.Digest, State: state, Capabilities: manifest.Capabilities}
}

func artifactPB(artifact store.ArtifactRecord) *controlv1alpha1.ArtifactInfo {
	return &controlv1alpha1.ArtifactInfo{
		Uid: artifact.UID, RunUid: artifact.RunUID, NodeId: artifact.NodeID,
		LogicalIteration: uint32(artifact.LogicalIteration), Attempt: artifact.Attempt, //nolint:gosec // Store rejects negative iterations.
		Name: artifact.Name, MediaType: artifact.MediaType, SizeBytes: artifact.SizeBytes,
		Sha256: artifact.SHA256, CreatedAt: timestamppb.New(artifact.CreatedAt), UpdatedAt: timestamppb.New(artifact.UpdatedAt),
	}
}

func rpcError(err error) error {
	var conflict *store.ConflictError
	var staleFlow *store.StaleFlowPlanError
	var staleTrigger *store.StaleTriggerGenerationError
	var changedReference *store.TriggerReferenceChangedError
	var staleObserved *store.StaleObservedGenerationError
	var changedSnapshot *store.SnapshotRevisionError
	switch {
	case errors.As(err, &conflict):
		return status.Error(codes.Aborted, conflict.Error())
	case errors.As(err, &staleFlow), errors.As(err, &staleTrigger), errors.As(err, &changedReference), errors.As(err, &staleObserved):
		return status.Error(codes.Aborted, err.Error())
	case errors.As(err, &changedSnapshot):
		return status.Error(codes.Aborted, "resource collection changed; restart pagination")
	case errors.Is(err, store.ErrTriggerDisabled):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

type resourceService struct {
	controlv1alpha1.UnimplementedResourceServiceServer
	api *API
}

func (s *resourceService) Apply(ctx context.Context, request *controlv1alpha1.ApplyRequest) (*controlv1alpha1.ApplyResponse, error) {
	return s.api.Apply(ctx, request)
}
func (s *resourceService) Validate(ctx context.Context, request *controlv1alpha1.ApplyRequest) (*controlv1alpha1.ApplyResponse, error) {
	return s.api.Validate(ctx, request)
}
func (s *resourceService) Get(ctx context.Context, request *controlv1alpha1.GetRequest) (*controlv1alpha1.ResourceDocument, error) {
	return s.api.Get(ctx, request)
}
func (s *resourceService) List(ctx context.Context, request *controlv1alpha1.ListRequest) (*controlv1alpha1.ListResponse, error) {
	return s.api.List(ctx, request)
}
func (s *resourceService) Delete(ctx context.Context, request *controlv1alpha1.DeleteRequest) (*emptypb.Empty, error) {
	return s.api.Delete(ctx, request)
}
func (s *resourceService) Watch(request *controlv1alpha1.WatchRequest, stream grpc.ServerStreamingServer[controlv1alpha1.ResourceEvent]) error {
	return s.api.Watch(request, stream)
}
func (s *resourceService) Export(ctx context.Context, request *controlv1alpha1.ExportRequest) (*controlv1alpha1.ExportResponse, error) {
	return s.api.Export(ctx, request)
}

type flowService struct {
	controlv1alpha1.UnimplementedFlowServiceServer
	api *API
}

func (s *flowService) Compile(ctx context.Context, request *controlv1alpha1.CompileRequest) (*controlv1alpha1.CompileResponse, error) {
	return s.api.Compile(ctx, request)
}
func (s *flowService) PreviewGraph(ctx context.Context, request *controlv1alpha1.PreviewGraphRequest) (*controlv1alpha1.PreviewGraphResponse, error) {
	return s.api.PreviewGraph(ctx, request)
}

type runService struct {
	controlv1alpha1.UnimplementedRunServiceServer
	api *API
}

func (s *runService) Start(ctx context.Context, request *controlv1alpha1.StartRunRequest) (*controlv1alpha1.RunRef, error) {
	return s.api.Start(ctx, request)
}
func (s *runService) Get(ctx context.Context, request *controlv1alpha1.RunRequest) (*controlv1alpha1.RunSummary, error) {
	return s.api.GetRun(ctx, request)
}
func (s *runService) List(ctx context.Context, request *controlv1alpha1.ListRunsRequest) (*controlv1alpha1.ListRunsResponse, error) {
	return s.api.ListRuns(ctx, request)
}
func (s *runService) Plan(ctx context.Context, request *controlv1alpha1.RunRequest) (*controlv1alpha1.CompileResponse, error) {
	return s.api.RunPlan(ctx, request)
}
func (s *runService) WatchEvents(request *controlv1alpha1.WatchRunRequest, stream grpc.ServerStreamingServer[controlv1alpha1.RunEvent]) error {
	return s.api.WatchEvents(request, stream)
}
func (s *runService) ListAttempts(ctx context.Context, request *controlv1alpha1.ListAttemptsRequest) (*controlv1alpha1.ListAttemptsResponse, error) {
	return s.api.ListAttempts(ctx, request)
}
func (s *runService) ListArtifacts(ctx context.Context, request *controlv1alpha1.ListArtifactsRequest) (*controlv1alpha1.ListArtifactsResponse, error) {
	return s.api.ListArtifacts(ctx, request)
}
func (s *runService) GetArtifact(request *controlv1alpha1.GetArtifactRequest, stream grpc.ServerStreamingServer[controlv1alpha1.ArtifactChunk]) error {
	return s.api.GetArtifact(request, stream)
}
func (s *runService) Approve(ctx context.Context, request *controlv1alpha1.ApprovalRequest) (*emptypb.Empty, error) {
	return s.api.Approve(ctx, request)
}
func (s *runService) Reject(ctx context.Context, request *controlv1alpha1.ApprovalRequest) (*emptypb.Empty, error) {
	return s.api.Reject(ctx, request)
}
func (s *runService) Cancel(ctx context.Context, request *controlv1alpha1.CancelRunRequest) (*emptypb.Empty, error) {
	return s.api.Cancel(ctx, request)
}
func (s *runService) Reconcile(ctx context.Context, request *controlv1alpha1.ReconcileRequest) (*controlv1alpha1.RunSummary, error) {
	return s.api.Reconcile(ctx, request)
}

type triggerService struct {
	controlv1alpha1.UnimplementedTriggerServiceServer
	api *API
}

func (s *triggerService) Next(ctx context.Context, request *controlv1alpha1.NextOccurrencesRequest) (*controlv1alpha1.NextOccurrencesResponse, error) {
	return s.api.NextTriggerOccurrences(ctx, request)
}
func (s *triggerService) Enable(ctx context.Context, request *controlv1alpha1.TriggerRequest) (*emptypb.Empty, error) {
	return s.api.SetTriggerEnabled(ctx, request, true)
}
func (s *triggerService) Disable(ctx context.Context, request *controlv1alpha1.TriggerRequest) (*emptypb.Empty, error) {
	return s.api.SetTriggerEnabled(ctx, request, false)
}
func (s *triggerService) Receipts(ctx context.Context, request *controlv1alpha1.ReceiptRequest) (*controlv1alpha1.ReceiptResponse, error) {
	return s.api.TriggerReceipts(ctx, request)
}

type pluginService struct {
	controlv1alpha1.UnimplementedPluginServiceServer
	api *API
}

func (s *pluginService) Install(stream grpc.ClientStreamingServer[controlv1alpha1.PluginUploadRequest, controlv1alpha1.PluginInstallResponse]) error {
	return s.api.InstallPlugin(stream)
}
func (s *pluginService) Enable(ctx context.Context, request *controlv1alpha1.PluginRequest) (*emptypb.Empty, error) {
	return s.api.EnablePlugin(ctx, request)
}
func (s *pluginService) Disable(ctx context.Context, request *controlv1alpha1.PluginRequest) (*emptypb.Empty, error) {
	return s.api.DisablePlugin(ctx, request)
}
func (s *pluginService) List(ctx context.Context, request *emptypb.Empty) (*controlv1alpha1.ListPluginsResponse, error) {
	return s.api.ListPlugins(ctx, request)
}
func (s *pluginService) Describe(ctx context.Context, request *controlv1alpha1.PluginRequest) (*controlv1alpha1.PluginInfo, error) {
	return s.api.DescribePlugin(ctx, request)
}
func (s *pluginService) Doctor(ctx context.Context, request *controlv1alpha1.PluginRequest) (*controlv1alpha1.DoctorResponse, error) {
	return s.api.DoctorPlugin(ctx, request)
}

type systemService struct {
	controlv1alpha1.UnimplementedSystemServiceServer
	api *API
}

func (s *systemService) Info(ctx context.Context, request *emptypb.Empty) (*controlv1alpha1.SystemInfo, error) {
	return s.api.Info(ctx, request)
}
func (s *systemService) Health(ctx context.Context, request *emptypb.Empty) (*controlv1alpha1.HealthResponse, error) {
	return s.api.Health(ctx, request)
}
func (s *systemService) Backup(ctx context.Context, request *controlv1alpha1.BackupRequest) (*controlv1alpha1.BackupResponse, error) {
	return s.api.Backup(ctx, request)
}

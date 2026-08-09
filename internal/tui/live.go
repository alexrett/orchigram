package tui

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

const (
	liveResourcePageSize = 500
	liveRunLimit         = 500
	liveEventLimit       = 2000
)

type liveSnapshot struct {
	Revision    uint64
	Resources   map[string]*controlv1alpha1.ResourceDocument
	Runs        map[string]*controlv1alpha1.RunSummary
	RunEvents   map[string][]*controlv1alpha1.RunEvent
	RunSequence map[string]uint64
	Plugins     map[string]*controlv1alpha1.PluginInfo
	System      *controlv1alpha1.SystemInfo
	Health      *controlv1alpha1.HealthResponse
	Connections map[string]string
}

type liveModel struct {
	mu    sync.RWMutex
	state liveSnapshot
}

func newLiveModel() *liveModel {
	return &liveModel{state: liveSnapshot{
		Resources: map[string]*controlv1alpha1.ResourceDocument{}, Runs: map[string]*controlv1alpha1.RunSummary{},
		RunEvents: map[string][]*controlv1alpha1.RunEvent{}, RunSequence: map[string]uint64{}, Plugins: map[string]*controlv1alpha1.PluginInfo{}, Connections: map[string]string{},
	}}
}

func (m *liveModel) snapshot() liveSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := liveSnapshot{
		Revision: m.state.Revision, Resources: make(map[string]*controlv1alpha1.ResourceDocument, len(m.state.Resources)),
		Runs: make(map[string]*controlv1alpha1.RunSummary, len(m.state.Runs)), RunEvents: make(map[string][]*controlv1alpha1.RunEvent, len(m.state.RunEvents)),
		RunSequence: make(map[string]uint64, len(m.state.RunSequence)), Plugins: make(map[string]*controlv1alpha1.PluginInfo, len(m.state.Plugins)),
		System: cloneMessage(m.state.System), Health: cloneMessage(m.state.Health), Connections: make(map[string]string, len(m.state.Connections)),
	}
	for key, document := range m.state.Resources {
		result.Resources[key] = cloneMessage(document)
	}
	for key, run := range m.state.Runs {
		result.Runs[key] = cloneMessage(run)
	}
	for key, events := range m.state.RunEvents {
		result.RunEvents[key] = make([]*controlv1alpha1.RunEvent, 0, len(events))
		for _, event := range events {
			result.RunEvents[key] = append(result.RunEvents[key], cloneMessage(event))
		}
	}
	for key, sequence := range m.state.RunSequence {
		result.RunSequence[key] = sequence
	}
	for key, plugin := range m.state.Plugins {
		result.Plugins[key] = cloneMessage(plugin)
	}
	for component, connection := range m.state.Connections {
		result.Connections[component] = connection
	}
	return result
}

func cloneMessage[T proto.Message](value T) T {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || (reflected.Kind() == reflect.Pointer && reflected.IsNil()) {
		return value
	}
	return proto.Clone(value).(T) //nolint:forcetypeassert // Clone preserves the concrete generated message type.
}

func (m *liveModel) replaceResources(resources []*controlv1alpha1.ResourceDocument, revision uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Resources = make(map[string]*controlv1alpha1.ResourceDocument, len(resources))
	for _, document := range resources {
		m.state.Resources[resourceLiveKey(document.GetKey())] = cloneMessage(document)
	}
	m.state.Revision = revision
}

func (m *liveModel) applyResourceEvent(event *controlv1alpha1.ResourceEvent) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if event.GetRevision() <= m.state.Revision {
		return false
	}
	document := event.GetResource()
	if document != nil {
		key := resourceLiveKey(document.GetKey())
		if event.GetType() == "DELETED" {
			delete(m.state.Resources, key)
		} else {
			m.state.Resources[key] = cloneMessage(document)
		}
	}
	m.state.Revision = event.GetRevision()
	return true
}

func (m *liveModel) replaceRuns(runs []*controlv1alpha1.RunSummary) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]struct{}, len(runs))
	active := make([]string, 0)
	for _, run := range runs {
		uid := run.GetUid()
		seen[uid] = struct{}{}
		existing := m.state.Runs[uid]
		if existing == nil || !existing.GetUpdatedAt().AsTime().After(run.GetUpdatedAt().AsTime()) {
			m.state.Runs[uid] = cloneMessage(run)
		}
		if !terminalRunPhase(m.state.Runs[uid].GetPhase()) {
			active = append(active, uid)
		}
	}
	for uid := range m.state.Runs {
		if _, ok := seen[uid]; !ok {
			delete(m.state.Runs, uid)
			delete(m.state.RunEvents, uid)
			delete(m.state.RunSequence, uid)
		}
	}
	sort.Strings(active)
	return active
}

func (m *liveModel) applyRunEvent(event *controlv1alpha1.RunEvent) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	uid := event.GetRunUid()
	if uid == "" || event.GetSequence() <= m.state.RunSequence[uid] {
		return false
	}
	m.state.RunSequence[uid] = event.GetSequence()
	m.state.RunEvents[uid] = append(m.state.RunEvents[uid], cloneMessage(event))
	events := m.state.RunEvents[uid]
	if len(events) > liveEventLimit {
		events = append([]*controlv1alpha1.RunEvent(nil), events[len(events)-liveEventLimit:]...)
	}
	m.state.RunEvents[uid] = events
	if run := m.state.Runs[uid]; run != nil {
		if phase := phaseForRunEvent(event.GetType()); phase != "" {
			run.Phase = phase
		}
		if event.GetOccurredAt() != nil {
			run.UpdatedAt = cloneMessage(event.GetOccurredAt())
		}
	}
	return true
}

func (m *liveModel) replacePlugins(plugins []*controlv1alpha1.PluginInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Plugins = make(map[string]*controlv1alpha1.PluginInfo, len(plugins))
	for _, plugin := range plugins {
		m.state.Plugins[pluginLiveKey(plugin)] = cloneMessage(plugin)
	}
}

func (m *liveModel) setSystem(info *controlv1alpha1.SystemInfo, health *controlv1alpha1.HealthResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if info != nil {
		m.state.System = cloneMessage(info)
	}
	if health != nil {
		m.state.Health = cloneMessage(health)
	}
}

func (m *liveModel) setConnection(component, state string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Connections[component] = state
}

func (m *liveModel) revision() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.Revision
}

func (m *liveModel) runSequence(uid string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state.RunSequence[uid]
}

func resourceLiveKey(key *controlv1alpha1.ResourceKey) string {
	return key.GetKind() + "\x00" + key.GetNamespace() + "\x00" + key.GetName()
}

func pluginLiveKey(plugin *controlv1alpha1.PluginInfo) string {
	return plugin.GetName() + "\x00" + plugin.GetVersion()
}

func terminalRunPhase(phase string) bool {
	switch phase {
	case "succeeded", "failed", "rejected", "cancelled":
		return true
	default:
		return false
	}
}

func terminalRunEvent(eventType string) bool {
	switch eventType {
	case "run.succeeded", "run.failed", "run.rejected", "run.cancelled":
		return true
	default:
		return false
	}
}

func phaseForRunEvent(eventType string) string {
	switch eventType {
	case "run.accepted":
		return "pending"
	case "node.started", "node.completed", "node.failed", "node.skipped", "approval.approved", "event.received":
		return "running"
	case "approval.waiting", "event.waiting", "event.duplicate":
		return "waiting"
	case "approval.rejected", "run.rejected":
		return "rejected"
	case "run.succeeded":
		return "succeeded"
	case "run.failed":
		return "failed"
	case "run.cancelled":
		return "cancelled"
	default:
		return ""
	}
}

type liveController struct {
	client          *clientpkg.Client
	model           *liveModel
	emitMu          sync.Mutex
	watchMu         sync.Mutex
	watchedRuns     map[string]struct{}
	runPollInterval time.Duration
	statusInterval  time.Duration
}

func newLiveController(client *clientpkg.Client) *liveController {
	return &liveController{client: client, model: newLiveModel(), watchedRuns: map[string]struct{}{}, runPollInterval: time.Second, statusInterval: 2 * time.Second}
}

func (c *liveController) bootstrap(ctx context.Context) (liveSnapshot, error) {
	if err := c.resyncResources(ctx); err != nil {
		return liveSnapshot{}, err
	}
	if err := c.refreshRuns(ctx); err != nil {
		return liveSnapshot{}, err
	}
	if err := c.refreshStatus(ctx); err != nil {
		return liveSnapshot{}, err
	}
	return c.model.snapshot(), nil
}

func (c *liveController) run(ctx context.Context, onUpdate func(liveSnapshot)) {
	go c.watchResources(ctx, onUpdate)
	go c.pollRuns(ctx, onUpdate)
	go c.pollStatus(ctx, onUpdate)
	for uid, run := range c.model.snapshot().Runs {
		if !terminalRunPhase(run.GetPhase()) {
			c.ensureRunWatch(ctx, uid, onUpdate)
		}
	}
}

func (c *liveController) resyncResources(ctx context.Context) error {
	resources, revision, err := retryResourceSnapshot(ctx, c.listResources)
	if err != nil {
		return err
	}
	c.model.replaceResources(resources, revision)
	c.model.setConnection("resources", "connected")
	return nil
}

func retryResourceSnapshot(ctx context.Context, load func(context.Context) ([]*controlv1alpha1.ResourceDocument, uint64, error)) ([]*controlv1alpha1.ResourceDocument, uint64, error) {
	backoff := 25 * time.Millisecond
	for {
		resources, revision, err := load(ctx)
		if err == nil {
			return resources, revision, nil
		}
		if status.Code(err) != codes.Aborted || ctx.Err() != nil {
			return nil, 0, err
		}
		if !waitLiveBackoff(ctx, &backoff) {
			return nil, 0, ctx.Err()
		}
		if backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
}

func (c *liveController) listResources(ctx context.Context) ([]*controlv1alpha1.ResourceDocument, uint64, error) {
	var resources []*controlv1alpha1.ResourceDocument
	var revision uint64
	continueToken := ""
	for {
		response, err := c.client.Resources.List(ctx, &controlv1alpha1.ListRequest{Limit: liveResourcePageSize, ContinueToken: continueToken})
		if err != nil {
			return nil, 0, err
		}
		if revision == 0 {
			revision = response.GetRevision()
		}
		resources = append(resources, response.GetResources()...)
		continueToken = response.GetContinueToken()
		if continueToken == "" {
			return resources, revision, nil
		}
	}
}

func (c *liveController) watchResources(ctx context.Context, onUpdate func(liveSnapshot)) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := c.client.Resources.Watch(ctx, &controlv1alpha1.WatchRequest{AfterRevision: c.model.revision()})
		if err != nil {
			c.model.setConnection("resources", "reconnecting")
			c.emit(onUpdate)
			if !waitLiveBackoff(ctx, &backoff) {
				return
			}
			continue
		}
		for ctx.Err() == nil {
			event, receiveErr := stream.Recv()
			if receiveErr != nil {
				if ctx.Err() != nil {
					return
				}
				if code := status.Code(receiveErr); code == codes.Aborted || code == codes.FailedPrecondition || code == codes.OutOfRange {
					if err := c.resyncResources(ctx); err == nil {
						c.emit(onUpdate)
						backoff = 250 * time.Millisecond
						break
					}
				}
				c.model.setConnection("resources", "reconnecting")
				c.emit(onUpdate)
				if !waitLiveBackoff(ctx, &backoff) {
					return
				}
				break
			}
			backoff = 250 * time.Millisecond
			c.model.setConnection("resources", "connected")
			if c.model.applyResourceEvent(event) {
				c.emit(onUpdate)
			}
		}
	}
}

func (c *liveController) refreshRuns(ctx context.Context) error {
	response, err := c.client.Runs.List(ctx, &controlv1alpha1.ListRunsRequest{Limit: liveRunLimit})
	if err != nil {
		return err
	}
	c.model.replaceRuns(response.GetRuns())
	c.model.setConnection("runs", "connected")
	return nil
}

func (c *liveController) pollRuns(ctx context.Context, onUpdate func(liveSnapshot)) {
	ticker := time.NewTicker(c.runPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			response, err := c.client.Runs.List(ctx, &controlv1alpha1.ListRunsRequest{Limit: liveRunLimit})
			if err != nil {
				c.model.setConnection("runs", "reconnecting")
				c.emit(onUpdate)
				continue
			}
			active := c.model.replaceRuns(response.GetRuns())
			c.model.setConnection("runs", "connected")
			for _, uid := range active {
				c.ensureRunWatch(ctx, uid, onUpdate)
			}
			c.emit(onUpdate)
		}
	}
}

func (c *liveController) ensureRunWatch(ctx context.Context, uid string, onUpdate func(liveSnapshot)) {
	c.watchMu.Lock()
	if _, exists := c.watchedRuns[uid]; exists {
		c.watchMu.Unlock()
		return
	}
	c.watchedRuns[uid] = struct{}{}
	c.watchMu.Unlock()
	go c.watchRun(ctx, uid, onUpdate)
}

func (c *liveController) ensureRunHistory(ctx context.Context, uid string, onUpdate func(liveSnapshot)) {
	c.ensureRunWatch(ctx, uid, onUpdate)
}

func (c *liveController) watchRun(ctx context.Context, uid string, onUpdate func(liveSnapshot)) {
	backoff := 250 * time.Millisecond
	for ctx.Err() == nil {
		stream, err := c.client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: uid, AfterSequence: c.model.runSequence(uid)})
		if err != nil {
			c.model.setConnection("run/"+uid, "reconnecting")
			c.emit(onUpdate)
			if !waitLiveBackoff(ctx, &backoff) {
				return
			}
			continue
		}
		for ctx.Err() == nil {
			event, receiveErr := stream.Recv()
			if receiveErr != nil {
				if ctx.Err() != nil {
					return
				}
				c.model.setConnection("run/"+uid, "reconnecting")
				c.emit(onUpdate)
				if !waitLiveBackoff(ctx, &backoff) {
					return
				}
				break
			}
			backoff = 250 * time.Millisecond
			c.model.setConnection("run/"+uid, "connected")
			if c.model.applyRunEvent(event) {
				c.emit(onUpdate)
			}
			if terminalRunEvent(event.GetType()) {
				return
			}
		}
	}
}

func (c *liveController) refreshStatus(ctx context.Context) error {
	plugins, err := c.client.Plugins.List(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	info, err := c.client.System.Info(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	health, err := c.client.System.Health(ctx, &emptypb.Empty{})
	if err != nil {
		return err
	}
	c.model.replacePlugins(plugins.GetPlugins())
	c.model.setSystem(info, health)
	c.model.setConnection("status", "connected")
	return nil
}

func (c *liveController) pollStatus(ctx context.Context, onUpdate func(liveSnapshot)) {
	ticker := time.NewTicker(c.statusInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.refreshStatus(ctx); err != nil {
				c.model.setConnection("status", "reconnecting")
			}
			c.emit(onUpdate)
		}
	}
}

func (c *liveController) emit(onUpdate func(liveSnapshot)) {
	if onUpdate == nil {
		return
	}
	c.emitMu.Lock()
	defer c.emitMu.Unlock()
	onUpdate(c.model.snapshot())
}

func waitLiveBackoff(ctx context.Context, backoff *time.Duration) bool {
	timer := time.NewTimer(*backoff)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
	}
	if *backoff < 10*time.Second {
		*backoff *= 2
	}
	return true
}

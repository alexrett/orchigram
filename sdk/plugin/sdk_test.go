package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	pluginv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/plugin/v1alpha1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewRejectsInvalidMetadata(t *testing.T) {
	t.Parallel()
	valid := Config{Metadata: Metadata{Name: "echo", Version: "0.1.0", Capabilities: []string{"task.echo.echo"}}, Task: TaskHandlerFuncs{}}
	for name, mutate := range map[string]func(*Config){
		"name":        func(c *Config) { c.Metadata.Name = "Echo!" },
		"version":     func(c *Config) { c.Metadata.Version = "latest" },
		"capability":  func(c *Config) { c.Metadata.Capabilities = []string{"echo"} },
		"schema":      func(c *Config) { c.Metadata.InputSchema = json.RawMessage(`{`) },
		"service":     func(c *Config) { c.Task = nil },
		"namespace":   func(c *Config) { c.Metadata.Capabilities = []string{"storage.echo.read"} },
		"task prefix": func(c *Config) { c.Metadata.Capabilities = []string{"task.other.run"} },
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			mutate(&config)
			if _, _, err := New(config); err == nil {
				t.Fatal("invalid metadata was accepted")
			}
		})
	}
}

func TestShutdownCancelsAndDrainsActiveTriggerWatch(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	provider := &blockingTriggerProvider{started: started}
	runtime, servers, err := New(Config{
		Metadata: Metadata{Name: "echo", Version: "0.1.0", Capabilities: []string{"trigger.events.watch"}},
		Trigger:  provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	stream := &triggerTestStream{ctx: context.Background()}
	done := make(chan error, 1)
	go func() { done <- servers.Trigger.Watch(stream) }()
	<-started
	deadline := time.Now().Add(time.Second)
	if _, err := runtime.Shutdown(context.Background(), &pluginv1alpha1.ShutdownRequest{Deadline: timestamppb.New(deadline)}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("watch error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not drain trigger watch")
	}
	if err := servers.Trigger.Watch(&triggerTestStream{ctx: context.Background()}); err == nil {
		t.Fatal("trigger watch started after shutdown")
	}
}

func TestExecuteAssignsSequencesAndOwnsExactlyOneTerminal(t *testing.T) {
	t.Parallel()
	var retained EventSink
	runtime := newTestRuntime(t, TaskHandlerFuncs{Run: func(_ context.Context, _ TaskRequest, sink EventSink) (any, error) {
		retained = sink
		if err := sink.Emit("echo.progress", map[string]int{"percent": 50}); err != nil {
			return nil, err
		}
		if err := sink.Emit("echo.completed", nil); err == nil {
			return nil, errors.New("author terminal event was accepted")
		}
		if err := sink.Log("echo.log", []byte("working")); err != nil {
			return nil, err
		}
		return map[string]string{"message": "hello"}, nil
	}})
	stream := &captureStream{ctx: context.Background()}
	if err := runtime.Execute(testExecuteRequest(), stream); err != nil {
		t.Fatal(err)
	}
	events := stream.snapshot()
	if len(events) != 3 || events[0].GetSequence() != 1 || events[1].GetSequence() != 2 || events[2].GetSequence() != 3 || events[2].GetType() != "task.completed" {
		t.Fatalf("events=%+v", events)
	}
	if err := retained.Emit("echo.late", nil); !errors.Is(err, ErrEventSinkClosed) {
		t.Fatalf("late emission error=%v", err)
	}
}

func TestExecuteRejectsInvalidCallMetadata(t *testing.T) {
	t.Parallel()
	runtime := newTestRuntime(t, TaskHandlerFuncs{Run: func(context.Context, TaskRequest, EventSink) (any, error) { return nil, nil }})
	request := testExecuteRequest()
	request.Meta.IdempotencyKey = ""
	if err := runtime.Execute(request, &captureStream{ctx: context.Background()}); err == nil {
		t.Fatal("invalid call metadata was accepted")
	}
}

func TestCancelAndShutdownLifecycle(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	runtime := newTestRuntime(t, TaskHandlerFuncs{Run: func(ctx context.Context, _ TaskRequest, _ EventSink) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	stream := &captureStream{ctx: context.Background()}
	done := make(chan error, 1)
	go func() { done <- runtime.Execute(testExecuteRequest(), stream) }()
	<-started
	response, err := runtime.Cancel(context.Background(), &pluginv1alpha1.CancelRequest{Meta: testMeta()})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("cancel response=%+v err=%v", response, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	events := stream.snapshot()
	if len(events) != 1 || events[0].GetType() != "task.failed" {
		t.Fatalf("events=%+v", events)
	}

	health, err := runtime.Health(context.Background(), &emptypb.Empty{})
	if err != nil || !health.GetReady() {
		t.Fatalf("health before shutdown=%+v err=%v", health, err)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	if _, err := runtime.Shutdown(context.Background(), &pluginv1alpha1.ShutdownRequest{Deadline: timestamppb.New(deadline)}); err != nil {
		t.Fatal(err)
	}
	health, _ = runtime.Health(context.Background(), &emptypb.Empty{})
	if health.GetReady() {
		t.Fatal("runtime remained ready after shutdown")
	}
	if err := runtime.Execute(testExecuteRequest(), &captureStream{ctx: context.Background()}); err == nil {
		t.Fatal("runtime accepted work after shutdown")
	}
}

func TestShutdownIsBoundedWhenHandlerIgnoresCancellation(t *testing.T) {
	t.Parallel()
	started, release := make(chan struct{}), make(chan struct{})
	runtime := newTestRuntime(t, TaskHandlerFuncs{Run: func(context.Context, TaskRequest, EventSink) (any, error) {
		close(started)
		<-release
		return nil, nil
	}})
	go func() { _ = runtime.Execute(testExecuteRequest(), &captureStream{ctx: context.Background()}) }()
	<-started
	start := time.Now()
	deadline := start.Add(30 * time.Millisecond)
	_, _ = runtime.Shutdown(context.Background(), &pluginv1alpha1.ShutdownRequest{Deadline: timestamppb.New(deadline)})
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("shutdown exceeded deadline: %v", elapsed)
	}
	close(release)
}

func newTestRuntime(t *testing.T, handler TaskHandler) *Runtime {
	t.Helper()
	runtime, _, err := New(Config{Metadata: Metadata{Name: "echo", Version: "0.1.0", Capabilities: []string{"task.echo.echo"}}, Task: handler})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

func testMeta() *pluginv1alpha1.CallMeta {
	return &pluginv1alpha1.CallMeta{RequestId: "request-1", RunUid: "run-1", NodeId: "echo", Attempt: 1, IdempotencyKey: "run-1/echo/1", Deadline: timestamppb.New(time.Now().Add(time.Minute))}
}

func testExecuteRequest() *pluginv1alpha1.ExecuteRequest {
	return &pluginv1alpha1.ExecuteRequest{Meta: testMeta(), Action: "echo.echo", InputJson: []byte(`{"message":"hello"}`), ConfigJson: []byte(`{}`)}
}

type captureStream struct {
	ctx    context.Context
	mu     sync.Mutex
	events []*pluginv1alpha1.ExecuteEvent
}

type blockingTriggerProvider struct {
	pluginv1alpha1.UnimplementedTriggerProviderServer
	started chan struct{}
}

func (p *blockingTriggerProvider) Watch(stream pluginv1alpha1.TriggerProvider_WatchServer) error {
	close(p.started)
	<-stream.Context().Done()
	return stream.Context().Err()
}

type triggerTestStream struct {
	ctx context.Context
}

func (*triggerTestStream) Send(*pluginv1alpha1.TriggerEvent) error { return nil }
func (*triggerTestStream) Recv() (*pluginv1alpha1.TriggerCommand, error) {
	return nil, context.Canceled
}
func (*triggerTestStream) SetHeader(metadata.MD) error  { return nil }
func (*triggerTestStream) SendHeader(metadata.MD) error { return nil }
func (*triggerTestStream) SetTrailer(metadata.MD)       {}
func (s *triggerTestStream) Context() context.Context   { return s.ctx }
func (*triggerTestStream) SendMsg(any) error            { return nil }
func (*triggerTestStream) RecvMsg(any) error            { return nil }

func (s *captureStream) Send(event *pluginv1alpha1.ExecuteEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *captureStream) snapshot() []*pluginv1alpha1.ExecuteEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pluginv1alpha1.ExecuteEvent(nil), s.events...)
}

func (s *captureStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureStream) SendHeader(metadata.MD) error { return nil }
func (s *captureStream) SetTrailer(metadata.MD)       {}
func (s *captureStream) Context() context.Context     { return s.ctx }
func (s *captureStream) SendMsg(any) error            { return nil }
func (s *captureStream) RecvMsg(any) error            { return nil }

package tui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
	clientpkg "github.com/alexrett/orchigram/internal/client"
	"github.com/alexrett/orchigram/internal/config"
	"github.com/alexrett/orchigram/internal/contextcfg"
	"github.com/alexrett/orchigram/internal/daemon"
	"github.com/alexrett/orchigram/internal/firstparty"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestTUILiveNavigationReflectsResourceAddAndDelete(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-live-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(80, 24)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	waitForScreenText(t, application, screen, "Contexts", true)

	flow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: live-visible}
spec: {nodes: [{id: done, uses: core.noop}]}
`)
	applied, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flow})
	if err != nil {
		t.Fatal(err)
	}
	waitForScreenText(t, application, screen, "live-visible", true)
	for range 3 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "done", true)
	updatedFlow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: live-visible}
spec: {nodes: [{id: changed-live, uses: core.noop}]}
`)
	updated, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: updatedFlow, ExpectedResourceVersion: applied.GetResource().GetResourceVersion()})
	if err != nil {
		t.Fatal(err)
	}
	waitForScreenText(t, application, screen, "changed-live", true)
	if _, err := client.Resources.Delete(context.Background(), &controlv1alpha1.DeleteRequest{Key: updated.GetResource().GetKey(), ExpectedResourceVersion: updated.GetResource().GetResourceVersion()}); err != nil {
		t.Fatal(err)
	}
	waitForScreenText(t, application, screen, "resources=0", true)

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	stopTUI()
	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not stop")
	}
}

func TestTUIRequestsContextSwitchThroughKeyboard(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-context-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(80, 24)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	contexts := contextcfg.File{Current: "alpha", Contexts: map[string]contextcfg.Context{"alpha": {Socket: cfg.SocketPath}, "beta": {Socket: cfg.SocketPath}}}
	runResult := make(chan error, 1)
	go func() {
		runResult <- runWithApplicationContext(context.Background(), client, application, "alpha", &contexts)
	}()
	waitForScreenText(t, application, screen, "alpha  [connected]", true)
	for range 2 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Switch from alpha to beta?", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	select {
	case runErr := <-runResult:
		var request *ContextSwitchError
		if !errors.As(runErr, &request) || request.Name != "beta" {
			t.Fatalf("context switch error=%v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not return a context switch request")
	}
}

func TestTUIKeyboardCreatesEditsStartsAndDeletesFlow(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-mutations-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(80, 24)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	waitForScreenText(t, application, screen, "Contexts", true)

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'n', tcell.ModNone))
	waitForScreenText(t, application, screen, "Create resource", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "New Flow (strict YAML)", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyCtrlS, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Created Flow/new-flow", true)
	waitForScreenText(t, application, screen, "resources=1", true)

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, '/', tcell.ModNone))
	waitForScreenText(t, application, screen, "Resource filter", true)
	postTUIText(t, screen, "new-flow")
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Resource filter", false)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Opened Flow/new-flow", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Edit node start", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIText(t, screen, "Edited start")
	for range 7 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Edited start", true)

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Edit edge start -> finish", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIText(t, screen, "true")
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Applied Flow generation 3", true)

	current, err := client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "new-flow"}})
	if err != nil {
		t.Fatal(err)
	}
	definition, err := resource.DecodeFlow(current.GetJson())
	if err != nil {
		t.Fatal(err)
	}
	if definition.Spec.Nodes[0].Name != "Edited start" || len(definition.Spec.Edges) != 1 || definition.Spec.Edges[0].When != "true" {
		t.Fatalf("edited Flow=%+v", definition.Spec)
	}

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Edit node start", true)
	externalDefinition := definition
	externalDefinition.Status = nil
	externalDefinition.Spec.Nodes[0].Name = "Externally updated"
	externalJSON, err := json.Marshal(externalDefinition)
	if err != nil {
		t.Fatal(err)
	}
	external, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: externalJSON, ExpectedResourceVersion: current.GetResourceVersion()})
	if err != nil {
		t.Fatal(err)
	}
	for range 8 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "CAS conflict", true)
	preserved, err := client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: external.GetResource().GetKey()})
	if err != nil {
		t.Fatal(err)
	}
	preservedDefinition, err := resource.DecodeFlow(preserved.GetJson())
	if err != nil {
		t.Fatal(err)
	}
	if preserved.GetResourceVersion() != external.GetResource().GetResourceVersion() || preservedDefinition.Spec.Nodes[0].Name != "Externally updated" {
		t.Fatalf("stale edit changed server state: resource=%+v Flow=%+v", preserved, preservedDefinition.Spec)
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Externally upda", true)

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Edit node start", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyCtrlU, 0, tcell.ModNone))
	postTUIText(t, screen, "INVALID")
	for range 8 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "must match", true)
	afterInvalid, err := client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: external.GetResource().GetKey()})
	if err != nil {
		t.Fatal(err)
	}
	if afterInvalid.GetResourceVersion() != external.GetResource().GetResourceVersion() {
		t.Fatalf("invalid edit advanced resource version from %d to %d", external.GetResource().GetResourceVersion(), afterInvalid.GetResourceVersion())
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone))

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'S', tcell.ModNone))
	waitForScreenText(t, application, screen, "Start Flow/new-flow", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Accepted run", true)

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'x', tcell.ModNone))
	waitForScreenText(t, application, screen, "Delete Flow/new-flow?", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Deleted Flow/new-flow", true)
	if _, err := client.Resources.Get(context.Background(), &controlv1alpha1.GetRequest{Key: &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "new-flow"}}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted Flow get error=%v", err)
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	stopTUI()
	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not stop")
	}
}

func TestFlowCASRetryRequiresSameUIDAndGeneration(t *testing.T) {
	t.Parallel()
	document := func(uid string, generation, version uint64) *controlv1alpha1.ResourceDocument {
		return &controlv1alpha1.ResourceDocument{
			Key:        &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "demo", Uid: uid},
			Generation: generation, ResourceVersion: version,
		}
	}
	selected := document("flow-uid", 2, 4)
	if !sameFlowGeneration(selected, document("flow-uid", 2, 5)) {
		t.Fatal("status-only revision was not retryable")
	}
	if sameFlowGeneration(selected, document("flow-uid", 3, 5)) {
		t.Fatal("spec generation change was retryable")
	}
	if sameFlowGeneration(selected, document("replacement-uid", 2, 5)) {
		t.Fatal("replacement resource was retryable")
	}
}

func TestFlowCASRetriesRepeatedStatusOnlyRevisions(t *testing.T) {
	t.Parallel()
	documentJSON := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: demo, namespace: default, uid: flow-uid, resourceVersion: 10, generation: 2}
spec:
  nodes: [{id: start, uses: core.noop}]
`)
	document := &controlv1alpha1.ResourceDocument{
		Key:             &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "demo", Uid: "flow-uid"},
		ResourceVersion: 10, Generation: 2, Json: documentJSON,
	}
	definition, err := resource.DecodeFlow(documentJSON)
	if err != nil {
		t.Fatal(err)
	}
	definition.Spec.Nodes[0].Name = "edited"
	resources := &statusBumpResourceClient{documentJSON: documentJSON, currentVersion: 10, conflicts: 5}
	client := &clientpkg.Client{Resources: resources}
	notifications := tview.NewTextView()
	if !applyFlowDefinition(context.Background(), client, document, definition, notifications) {
		t.Fatalf("Flow edit failed after status-only revisions: %s", notifications.GetText(false))
	}
	if resources.applyAttempts != 6 || resources.getAttempts != 5 || document.GetResourceVersion() != 16 || document.GetGeneration() != 3 {
		t.Fatalf("applyAttempts=%d getAttempts=%d document=%+v", resources.applyAttempts, resources.getAttempts, document)
	}
}

type statusBumpResourceClient struct {
	controlv1alpha1.ResourceServiceClient
	documentJSON   []byte
	currentVersion uint64
	conflicts      int
	applyAttempts  int
	getAttempts    int
}

func (c *statusBumpResourceClient) Validate(context.Context, *controlv1alpha1.ApplyRequest, ...grpc.CallOption) (*controlv1alpha1.ApplyResponse, error) {
	return &controlv1alpha1.ApplyResponse{}, nil
}

func (c *statusBumpResourceClient) Apply(_ context.Context, request *controlv1alpha1.ApplyRequest, _ ...grpc.CallOption) (*controlv1alpha1.ApplyResponse, error) {
	c.applyAttempts++
	if request.GetExpectedResourceVersion() != c.currentVersion {
		return nil, status.Errorf(codes.Internal, "expected request revision %d, got %d", c.currentVersion, request.GetExpectedResourceVersion())
	}
	if c.applyAttempts <= c.conflicts {
		return nil, status.Error(codes.Aborted, "status revision advanced")
	}
	c.currentVersion++
	return &controlv1alpha1.ApplyResponse{Resource: &controlv1alpha1.ResourceDocument{
		Key:             &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "demo", Uid: "flow-uid"},
		ResourceVersion: c.currentVersion, Generation: 3, Json: request.GetDocument(),
	}}, nil
}

func (c *statusBumpResourceClient) Get(context.Context, *controlv1alpha1.GetRequest, ...grpc.CallOption) (*controlv1alpha1.ResourceDocument, error) {
	c.getAttempts++
	c.currentVersion++
	return &controlv1alpha1.ResourceDocument{
		Key:             &controlv1alpha1.ResourceKey{Kind: "Flow", Namespace: "default", Name: "demo", Uid: "flow-uid"},
		ResourceVersion: c.currentVersion, Generation: 2, Json: c.documentJSON,
	}, nil
}

func TestTUIKeyboardInstallsActivatesRollsBackAndDisablesPlugin(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-plugins-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)

	bundleDirectory := filepath.Join(root, "bundles")
	if err := os.MkdirAll(bundleDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	versions := []string{"0.1.0", "0.2.0"}
	paths := make(map[string]string, len(versions))
	for _, version := range versions {
		path := filepath.Join(bundleDirectory, "exec-"+version+".tar.gz")
		if err := os.WriteFile(path, tuiPluginBundle(t, version), 0o600); err != nil {
			t.Fatal(err)
		}
		paths[version] = path
	}

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(120, 40)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	waitForScreenText(t, application, screen, "Contexts", true)

	for _, version := range versions {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'i', tcell.ModNone))
		waitForScreenText(t, application, screen, "Install plugin bundle", true)
		postTUIText(t, screen, paths[version])
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
		waitForScreenText(t, application, screen, "Installed exec:"+version, true)
	}
	waitForScreenText(t, application, screen, "exec:0.2.0 [instal", true)

	openFilteredTUIEntry(t, application, screen, "exec:0.2.0", "Plugin exec")
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Plugin operation completed", true)
	waitForScreenText(t, application, screen, "exec:0.2.0 [activ", true)
	pluginFlow := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: plugin-flow}
spec:
  nodes:
    - id: execute
      uses: exec.run
      with: {argv: [/usr/bin/true]}
`)
	pluginFlowResponse, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: pluginFlow})
	if err != nil || len(pluginFlowResponse.GetDiagnostics()) != 0 {
		t.Fatalf("apply plugin Flow: response=%+v err=%v", pluginFlowResponse, err)
	}
	compiledPluginFlow, err := compileFlowPlan(context.Background(), client, pluginFlowResponse.GetResource())
	if err != nil || len(compiledPluginFlow.Nodes) != 1 || compiledPluginFlow.Nodes[0].Contract == nil || len(compiledPluginFlow.Nodes[0].Contract.ConfigSchema) == 0 {
		t.Fatalf("compiled plugin Flow did not pin its action schema: plan=%+v err=%v", compiledPluginFlow, err)
	}
	waitForScreenText(t, application, screen, "resources=3", true)
	openFilteredTUIEntry(t, application, screen, "plugin-flow", "Opened Flow/plugin-flow")
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	for range 7 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	waitForScreenText(t, application, screen, "Config JSON", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone))

	openFilteredTUIEntry(t, application, screen, "exec:0.2.0", "Plugin exec")
	for range 3 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Activate previous exec version", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Activated exec:0.1.0", true)

	openFilteredTUIEntry(t, application, screen, "exec:0.1.0", "Plugin exec")
	for range 2 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Plugin operation completed", true)
	waitForScreenText(t, application, screen, "exec:0.1.0 [instal", true)

	plugins, err := client.Plugins.List(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if len(plugins.GetPlugins()) != 2 {
		t.Fatalf("plugins=%+v", plugins.GetPlugins())
	}
	for _, plugin := range plugins.GetPlugins() {
		if plugin.GetState() != "installed" {
			t.Fatalf("plugin remained active: %+v", plugin)
		}
	}

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	stopTUI()
	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not stop")
	}
}

func TestOperatorApprovesWaitingRunThroughTUI(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()

	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: tui-approval}
spec:
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 30s}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	response, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetDiagnostics()) != 0 {
		t.Fatalf("apply diagnostics: %+v", response.GetDiagnostics())
	}
	run, err := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "tui-approval", InputJson: []byte(`{}`), IdempotencyKey: "tui-approval-gate"})
	if err != nil {
		t.Fatal(err)
	}
	waitForTUIRunEvent(t, client, run.GetUid(), "approval.waiting")

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(120, 40)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	time.Sleep(100 * time.Millisecond)
	// Contexts/current, Flows/flow, Triggers, Repositories, AgentProfiles,
	// PluginInstallations, and the Runs heading precede the run item.
	for range 9 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyDown, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	for _, view := range []struct {
		key   rune
		title string
	}{{key: 'e', title: "Events "}, {key: 't', title: "Attempts "}, {key: 'f', title: "Artifacts "}, {key: 'l', title: "Logs "}} {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, view.key, tcell.ModNone))
		waitForScreenText(t, application, screen, view.title, true)
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEsc, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'a', tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForTUIRunEvent(t, client, run.GetUid(), "run.succeeded")
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	stopTUI()
	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not stop")
	}
}

func TestTUIRejectsAndCancelsRunsKeyboardOnlyAt80x24(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "orchigram-tui-decisions-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	cfg := config.Development(filepath.Join(root, "state"))
	daemonContext, stopDaemon := context.WithCancel(context.Background())
	instance, err := daemon.Open(daemonContext, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- instance.Serve(daemonContext) }()
	defer func() {
		stopDaemon()
		select {
		case serveErr := <-served:
			if serveErr != nil {
				t.Errorf("serve daemon: %v", serveErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("daemon did not stop")
		}
	}()
	client, err := clientpkg.DialUnix(context.Background(), cfg.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	waitForTUIHealth(t, client)
	flowDocument := []byte(`apiVersion: orchigram.dev/v1alpha1
kind: Flow
metadata: {name: tui-decisions}
spec:
  nodes:
    - {id: prepare, uses: core.noop}
    - {id: approval, uses: core.approval, timeout: 1h}
    - {id: finish, uses: core.noop}
  edges:
    - {from: prepare, to: approval}
    - {from: approval, to: finish, when: result.approved}
`)
	response, err := client.Resources.Apply(context.Background(), &controlv1alpha1.ApplyRequest{Document: flowDocument})
	if err != nil || len(response.GetDiagnostics()) != 0 {
		t.Fatalf("apply Flow: response=%+v err=%v", response, err)
	}
	start := func(key string) *controlv1alpha1.RunRef {
		run, startErr := client.Runs.Start(context.Background(), &controlv1alpha1.StartRunRequest{Flow: "tui-decisions", InputJson: []byte(`{}`), IdempotencyKey: key})
		if startErr != nil {
			t.Fatal(startErr)
		}
		waitForTUIRunEvent(t, client, run.GetUid(), "approval.waiting")
		return run
	}
	rejected := start("tui-reject")

	screen := tcell.NewSimulationScreen("UTF-8")
	if err := screen.Init(); err != nil {
		t.Fatal(err)
	}
	screen.SetSize(80, 24)
	application := tview.NewApplication().SetScreen(screen).EnableMouse(true)
	tuiContext, stopTUI := context.WithCancel(context.Background())
	runResult := make(chan error, 1)
	go func() { runResult <- runWithApplication(tuiContext, client, application) }()
	waitForScreenText(t, application, screen, short(rejected.GetUid()), true)

	openFilteredTUIEntry(t, application, screen, short(rejected.GetUid()), short(rejected.GetUid()))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'r', tcell.ModNone))
	waitForScreenText(t, application, screen, "Reject run", true)
	for range 2 {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	}
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForTUIRunEvent(t, client, rejected.GetUid(), "run.rejected")

	cancelled := start("tui-cancel")
	waitForScreenText(t, application, screen, "runs=2", true)
	openFilteredTUIEntry(t, application, screen, short(cancelled.GetUid()), short(cancelled.GetUid()))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'c', tcell.ModNone))
	waitForScreenText(t, application, screen, "Cancel run", true)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyTab, 0, tcell.ModNone))
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForTUIRunEvent(t, client, cancelled.GetUid(), "run.cancelled")

	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, 'q', tcell.ModNone))
	stopTUI()
	select {
	case runErr := <-runResult:
		if runErr != nil {
			t.Fatal(runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TUI did not stop")
	}
}

func TestRunInspectorEscapesDynamicColorMarkup(t *testing.T) {
	t.Parallel()
	text := runInspectorText(&controlv1alpha1.RunSummary{Uid: "[red]", Flow: "[red]", Phase: "[red]", PlanHash: "[red]", InterpreterVersion: "[red]"})
	if strings.Count(text, "[red[]") != 5 {
		t.Fatalf("run fields were not escaped for a dynamic-color view: %q", text)
	}
}

func postTUIEvent(t *testing.T, screen tcell.SimulationScreen, event tcell.Event) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		err := screen.PostEvent(event)
		if err == nil {
			return
		}
		if !errors.Is(err, tcell.ErrEventQFull) || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
	}
}

func postTUIText(t *testing.T, screen tcell.SimulationScreen, value string) {
	t.Helper()
	for _, character := range value {
		postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, character, tcell.ModNone))
	}
}

func openFilteredTUIEntry(t *testing.T, application *tview.Application, screen tcell.SimulationScreen, filter, detail string) {
	t.Helper()
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyRune, '/', tcell.ModNone))
	waitForScreenText(t, application, screen, "Resource filter", true)
	postTUIText(t, screen, filter)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, "Resource filter", false)
	postTUIEvent(t, screen, tcell.NewEventKey(tcell.KeyEnter, 0, tcell.ModNone))
	waitForScreenText(t, application, screen, detail, true)
}

func tuiPluginBundle(t *testing.T, version string) []byte {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable) //nolint:gosec // The test intentionally bundles its own TestMain plugin mode.
	if err != nil {
		t.Fatal(err)
	}
	catalog, ok := firstparty.Find("exec")
	if !ok {
		t.Fatal("exec first-party plugin is missing")
	}
	digest := sha256.Sum256(binary)
	manifest := pluginbundle.Manifest{
		APIVersion:   pluginbundle.APIVersion,
		Name:         "exec",
		Version:      version,
		Protocol:     pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1},
		Capabilities: catalog.Capabilities,
		Platforms: []pluginbundle.Platform{{
			OS: runtime.GOOS, Arch: runtime.GOARCH, Path: "bin/plugin", SHA256: hex.EncodeToString(digest[:]),
		}},
	}
	bundle, err := pluginbundle.Build(manifest, map[string][]byte{"bin/plugin": binary})
	if err != nil {
		t.Fatal(err)
	}
	return bundle
}

func waitForScreenText(t *testing.T, application *tview.Application, screen tcell.SimulationScreen, expected string, present bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		content := snapshotScreenText(application, screen)
		if strings.Contains(content, expected) == present {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("screen presence of %q did not become %t:\n%s", expected, present, content)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func snapshotScreenText(application *tview.Application, screen tcell.SimulationScreen) string {
	var content strings.Builder
	application.QueueUpdate(func() {
		cells, width, _ := screen.GetContents()
		for index, cell := range cells {
			content.Write(cell.Bytes)
			if width > 0 && (index+1)%width == 0 {
				content.WriteByte('\n')
			}
		}
	})
	return content.String()
}

func waitForTUIHealth(t *testing.T, client *clientpkg.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		health, err := client.System.Health(ctx, &emptypb.Empty{})
		if err == nil && health.GetReady() {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("daemon was not ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func waitForTUIRunEvent(t *testing.T, client *clientpkg.Client, runUID, eventType string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.Runs.WatchEvents(ctx, &controlv1alpha1.WatchRunRequest{Uid: runUID})
	if err != nil {
		t.Fatal(err)
	}
	for {
		event, receiveErr := stream.Recv()
		if receiveErr != nil {
			t.Fatalf("wait for %s: %v", eventType, receiveErr)
		}
		if event.GetType() == eventType {
			return
		}
	}
}

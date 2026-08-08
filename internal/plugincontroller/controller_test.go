package plugincontroller

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/health"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

type fakeRuntime struct {
	records    []store.PluginRecord
	health     []health.Diagnostic
	enableErr  error
	disableErr error
	enables    []string
	disables   []string
}

func (f *fakeRuntime) List(context.Context) ([]store.PluginRecord, error) {
	return append([]store.PluginRecord(nil), f.records...), nil
}

func (f *fakeRuntime) Enable(_ context.Context, name, version string) error {
	f.enables = append(f.enables, name+"@"+version)
	if f.enableErr != nil {
		return f.enableErr
	}
	for index := range f.records {
		if f.records[index].Name == name {
			f.records[index].Active = f.records[index].Version == version
		}
	}
	return nil
}

func (f *fakeRuntime) Disable(_ context.Context, name string) error {
	f.disables = append(f.disables, name)
	if f.disableErr != nil {
		return f.disableErr
	}
	for index := range f.records {
		if f.records[index].Name == name {
			f.records[index].Active = false
		}
	}
	return nil
}

func (f *fakeRuntime) HealthDiagnostics(context.Context) []health.Diagnostic {
	return append([]health.Diagnostic(nil), f.health...)
}

func TestControllerAdoptsActivatesRollsBackAndReportsRuntimeFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := openControllerStore(t)
	runtime := &fakeRuntime{records: []store.PluginRecord{
		pluginRecord("exec", "0.1.0", strings.Repeat("1", 64)),
		pluginRecord("exec", "0.2.0", strings.Repeat("2", 64)),
	}}
	controller := New(state, runtime)
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	installations := listInstallations(t, state)
	if len(installations) != 2 {
		t.Fatalf("adopted installations=%+v", installations)
	}
	for _, installation := range installations {
		if installation.Spec.Enabled == nil || *installation.Spec.Enabled || installation.Status["phase"] != "Installed" || installation.Status["observedGeneration"] != float64(installation.Metadata.Generation) {
			t.Fatalf("adopted installation=%+v", installation)
		}
	}
	if err := controller.SetEnabled(ctx, "exec", "0.2.0", true); err != nil {
		t.Fatal(err)
	}
	if len(runtime.enables) != 1 || runtime.enables[0] != "exec@0.2.0" {
		t.Fatalf("enable calls=%v", runtime.enables)
	}
	active := installationByVersion(t, state, "0.2.0")
	if active.Status["phase"] != "Active" || active.Status["active"] != true {
		t.Fatalf("active status=%+v", active.Status)
	}
	if err := controller.SetEnabled(ctx, "exec", "0.1.0", true); err != nil {
		t.Fatal(err)
	}
	if len(runtime.enables) != 2 || runtime.enables[1] != "exec@0.1.0" {
		t.Fatalf("rollback calls=%v", runtime.enables)
	}
	rolledBack := installationByVersion(t, state, "0.1.0")
	previous := installationByVersion(t, state, "0.2.0")
	if rolledBack.Status["phase"] != "Active" || rolledBack.Spec.Enabled == nil || !*rolledBack.Spec.Enabled || previous.Spec.Enabled == nil || *previous.Spec.Enabled {
		t.Fatalf("rollback=%+v previous=%+v", rolledBack, previous)
	}
	runtime.health = []health.Diagnostic{{Path: "plugins/exec@0.1.0", Code: "process_exited", Message: "active plugin process exited; the next health probe will attempt restart"}}
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	failed := installationByVersion(t, state, "0.1.0")
	if failed.Status["phase"] != "Error" || failed.Status["active"] != true || len(controller.HealthDiagnostics()) == 0 {
		t.Fatalf("failed status=%+v health=%+v", failed.Status, controller.HealthDiagnostics())
	}
	runtime.health = nil
	restarted := New(state, runtime)
	if err := restarted.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	recovered := installationByVersion(t, state, "0.1.0")
	if recovered.Status["phase"] != "Active" || len(restarted.HealthDiagnostics()) != 0 || len(runtime.enables) != 2 {
		t.Fatalf("recovered status=%+v health=%+v enables=%v", recovered.Status, restarted.HealthDiagnostics(), runtime.enables)
	}
}

func TestConflictingEnabledVersionsDoNotSwitchActivation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := openControllerStore(t)
	first := pluginRecord("exec", "0.1.0", strings.Repeat("1", 64))
	runtime := &fakeRuntime{records: []store.PluginRecord{first, pluginRecord("exec", "0.2.0", strings.Repeat("2", 64))}}
	controller := New(state, runtime)
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	// Simulate an activation that existed before two concurrent desired updates
	// became visible. Conflict reconciliation must retain it, not pick a winner.
	runtime.records[0].Active = true
	runtime.disables = nil
	for _, installation := range listInstallations(t, state) {
		setInstallationEnabled(t, state, installation, true)
	}
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if len(runtime.enables) != 0 || len(runtime.disables) != 0 || !runtime.records[0].Active || runtime.records[1].Active {
		t.Fatalf("activation changed: enables=%v disables=%v records=%+v", runtime.enables, runtime.disables, runtime.records)
	}
	for _, installation := range listInstallations(t, state) {
		if installation.Status["phase"] != "Conflict" {
			t.Fatalf("conflict status=%+v", installation.Status)
		}
	}
	if diagnostics := controller.HealthDiagnostics(); len(diagnostics) != 2 || diagnostics[0].Code != "multipleenabledversions" {
		t.Fatalf("health=%+v", diagnostics)
	}
}

func TestDiagnosticsRejectIdentityMutationAndPersistMissingBundleWarning(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := openControllerStore(t)
	runtime := &fakeRuntime{}
	controller := New(state, runtime)
	document := installationDocument(t, "missing", "ghost", "1.0.0", strings.Repeat("a", 64), false)
	diagnostics := controller.Diagnostics(ctx, document)
	if len(diagnostics) != 1 || diagnostics[0].Code != "bundle_missing" || diagnostics[0].Severity != flow.SeverityWarning {
		t.Fatalf("missing diagnostics=%+v", diagnostics)
	}
	applied, err := state.Apply(ctx, document, store.ApplyOptions{RequestID: "declare-missing"})
	if err != nil {
		t.Fatal(err)
	}
	mutated := installationDocument(t, applied.Metadata.Name, "other", "1.0.0", strings.Repeat("b", 64), false)
	mutated.Metadata = applied.Metadata
	mutated, err = mutated.WithServerMetadata(applied.Metadata)
	if err != nil {
		t.Fatal(err)
	}
	diagnostics = controller.Diagnostics(ctx, mutated)
	if len(diagnostics) == 0 || diagnostics[0].Code != "immutable_identity" {
		t.Fatalf("mutation diagnostics=%+v", diagnostics)
	}
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	missing := installationByVersion(t, state, "1.0.0")
	if missing.Status["phase"] != "Error" || missing.Status["installed"] != false {
		t.Fatalf("missing status=%+v", missing.Status)
	}
}

func TestControllerReportsDigestAndProtocolMismatchWithoutDuplicateProjection(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	state := openControllerStore(t)
	correctDigest := strings.Repeat("c", 64)
	wrongDigest := strings.Repeat("d", 64)
	compatible := pluginRecord("exec", "1.0.0", correctDigest)
	incompatible := pluginRecord("github", "2.0.0", strings.Repeat("e", 64))
	var manifest pluginbundle.Manifest
	if err := json.Unmarshal(incompatible.ManifestJSON, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Protocol = pluginbundle.ProtocolRange{Minimum: 2, Maximum: 2}
	incompatible.ManifestJSON, _ = json.Marshal(manifest)
	runtime := &fakeRuntime{records: []store.PluginRecord{compatible, incompatible}}
	controller := New(state, runtime)
	if _, err := state.Apply(ctx, installationDocument(t, "expected-exec", "exec", "1.0.0", wrongDigest, false), store.ApplyOptions{RequestID: "declare-wrong-digest"}); err != nil {
		t.Fatal(err)
	}
	if err := controller.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	installations := listInstallations(t, state)
	if len(installations) != 2 {
		t.Fatalf("installations=%+v", installations)
	}
	for _, installation := range installations {
		switch installation.Spec.Plugin {
		case "exec":
			if installation.Status["phase"] != "Error" || firstStatusDiagnosticCode(installation.Status) != "digestmismatch" {
				t.Fatalf("digest status=%+v", installation.Status)
			}
		case "github":
			if installation.Status["phase"] != "Error" || firstStatusDiagnosticCode(installation.Status) != "protocolincompatible" {
				t.Fatalf("protocol status=%+v", installation.Status)
			}
		}
	}
}

func openControllerStore(t *testing.T) *store.Store {
	t.Helper()
	state, err := store.Open(filepath.Join(t.TempDir(), "state.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = state.Close() })
	return state
}

func pluginRecord(name, version, digest string) store.PluginRecord {
	manifest, _ := json.Marshal(pluginbundle.Manifest{
		APIVersion: pluginbundle.APIVersion, Name: name, Version: version,
		Protocol: pluginbundle.ProtocolRange{Minimum: 1, Maximum: 1}, Capabilities: []string{"task." + name + ".run"},
	})
	return store.PluginRecord{Name: name, Version: version, Digest: digest, ManifestJSON: manifest, State: "installed"}
}

func installationDocument(t *testing.T, name, plugin, version, digest string, enabled bool) resource.Document {
	t.Helper()
	value := enabled
	installation := resource.PluginInstallation{
		TypeMeta: resource.TypeMeta{APIVersion: resource.APIVersion, Kind: "PluginInstallation"},
		Metadata: resource.ObjectMeta{Name: name, Namespace: resource.DefaultNamespace},
		Spec:     resource.PluginInstallationSpec{Plugin: plugin, Version: version, Digest: digest, Enabled: &value},
	}
	encoded, err := json.Marshal(installation)
	if err != nil {
		t.Fatal(err)
	}
	document, err := resource.DecodeStrict(encoded)
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func listInstallations(t *testing.T, state *store.Store) []resource.PluginInstallation {
	t.Helper()
	documents, _, err := state.List(context.Background(), "PluginInstallation", "", 100)
	if err != nil {
		t.Fatal(err)
	}
	result := make([]resource.PluginInstallation, 0, len(documents))
	for _, document := range documents {
		installation, err := resource.DecodePluginInstallation(document.JSON)
		if err != nil {
			t.Fatal(err)
		}
		result = append(result, installation)
	}
	return result
}

func installationByVersion(t *testing.T, state *store.Store, version string) resource.PluginInstallation {
	t.Helper()
	for _, installation := range listInstallations(t, state) {
		if installation.Spec.Version == version {
			return installation
		}
	}
	t.Fatalf("PluginInstallation version %s not found", version)
	return resource.PluginInstallation{}
}

func setInstallationEnabled(t *testing.T, state *store.Store, installation resource.PluginInstallation, enabled bool) {
	t.Helper()
	installation.Spec.Enabled = &enabled
	encoded, err := json.Marshal(installation)
	if err != nil {
		t.Fatal(err)
	}
	document, err := resource.DecodeStrict(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Apply(context.Background(), document, store.ApplyOptions{ExpectedResourceVersion: installation.Metadata.ResourceVersion, RequestID: "test-desired-state"}); err != nil {
		t.Fatal(err)
	}
}

func firstStatusDiagnosticCode(status map[string]any) string {
	diagnostics, _ := status["diagnostics"].([]any)
	if len(diagnostics) == 0 {
		return ""
	}
	diagnostic, _ := diagnostics[0].(map[string]any)
	code, _ := diagnostic["code"].(string)
	return code
}

func TestSetEnabledReturnsNotFoundForUnknownPlugin(t *testing.T) {
	t.Parallel()
	state := openControllerStore(t)
	runtime := &fakeRuntime{records: []store.PluginRecord{pluginRecord("exec", "1.0.0", strings.Repeat("f", 64))}}
	controller := New(state, runtime)
	if err := controller.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := controller.SetEnabled(context.Background(), "missing", "1.0.0", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("error=%v", err)
	}
	if err := controller.SetEnabled(context.Background(), "exec", "9.9.9", true); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("unknown version error=%v", err)
	}
	installation := installationByVersion(t, state, "1.0.0")
	if installation.Spec.Enabled == nil || *installation.Spec.Enabled || len(runtime.enables) != 0 || len(runtime.disables) != 0 {
		t.Fatalf("unknown version mutated state: installation=%+v runtime=%+v", installation, runtime)
	}
}

func TestResourceNameIsDeterministicDNSLabelForLongSemanticVersion(t *testing.T) {
	t.Parallel()
	record := pluginRecord("community-provider", "1.0.0-"+strings.Repeat("preview", 20), strings.Repeat("a", 64))
	name := ResourceName(record)
	if len(name) > 63 || !strings.HasSuffix(name, "-aaaaaaaaaa") {
		t.Fatalf("resource name=%q length=%d", name, len(name))
	}
	document := installationDocument(t, name, record.Name, record.Version, record.Digest, false)
	if document.Metadata.Name != name || ResourceName(record) != name {
		t.Fatalf("resource name is not stable: %q", name)
	}
}

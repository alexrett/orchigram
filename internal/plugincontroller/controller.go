// Package plugincontroller reconciles declarative PluginInstallation resources
// with immutable plugin bundles and the single-node activation store.
package plugincontroller

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/alexrett/orchigram/internal/flow"
	"github.com/alexrett/orchigram/internal/health"
	"github.com/alexrett/orchigram/internal/pluginbundle"
	"github.com/alexrett/orchigram/internal/resource"
	"github.com/alexrett/orchigram/internal/store"
)

const hostProtocolVersion = 1

// ResourceStore is the durable desired/status boundary used by the controller.
type ResourceStore interface {
	Apply(context.Context, resource.Document, store.ApplyOptions) (resource.Document, error)
	Get(context.Context, string, string, string) (resource.Document, error)
	List(context.Context, string, string, int) ([]resource.Document, uint64, error)
	UpdateResourceStatus(context.Context, string, string, string, uint64, map[string]any) (resource.Document, error)
}

// Runtime owns immutable bundle records and process activation.
type Runtime interface {
	List(context.Context) ([]store.PluginRecord, error)
	Enable(context.Context, string, string) error
	Disable(context.Context, string) error
	HealthDiagnostics(context.Context) []health.Diagnostic
}

// Controller makes PluginInstallation desired state authoritative.
type Controller struct {
	store     ResourceStore
	runtime   Runtime
	interval  time.Duration
	reconcile sync.Mutex
	health    *health.Tracker
	issueKeys map[string]struct{}
}

// New constructs a controller. Start performs the first reconciliation.
func New(state ResourceStore, runtime Runtime) *Controller {
	tracker := health.NewTracker()
	tracker.Set("starting", health.Diagnostic{Path: "controllers/plugins", Code: "starting", Message: "plugin installation reconciliation has not completed"})
	return &Controller{store: state, runtime: runtime, interval: time.Second, health: tracker, issueKeys: map[string]struct{}{}}
}

// Start continuously converges immutable installations and activation state.
func (c *Controller) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		_ = c.Reconcile(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.Reconcile(ctx)
			}
		}
	}()
}

// HealthDiagnostics returns stable, secret-safe controller failures.
func (c *Controller) HealthDiagnostics() []health.Diagnostic { return c.health.Snapshot() }

func (c *Controller) observe(err error) {
	if err == nil {
		c.health.Clear("starting")
		c.health.Clear("reconcile")
		return
	}
	if !errors.Is(err, context.Canceled) {
		c.health.Set("reconcile", health.Diagnostic{Path: "controllers/plugins", Code: "reconcile_failed", Message: "plugin installation reconciliation failed; inspect daemon logs"})
	}
}

// Reconcile adopts legacy bundle records, converges activation, and persists
// status-only watch revisions. Calls are serialized with CLI mutations.
func (c *Controller) Reconcile(ctx context.Context) error {
	c.reconcile.Lock()
	defer c.reconcile.Unlock()
	err := c.reconcileLocked(ctx)
	c.observe(err)
	return err
}

func (c *Controller) reconcileLocked(ctx context.Context) error {
	records, err := c.runtime.List(ctx)
	if err != nil {
		return err
	}
	if err := c.ensureProjections(ctx, records); err != nil {
		return err
	}
	installations, err := c.installations(ctx)
	if err != nil {
		return err
	}
	groups := groupInstallations(installations)
	recordByVersion, activeByPlugin := indexRecords(records)
	failures := map[string]statusFailure{}
	for plugin, group := range groups {
		enabled := enabledInstallations(group)
		if len(enabled) > 1 {
			failures[plugin] = statusFailure{code: "multiple_enabled_versions", message: "multiple plugin versions are enabled"}
			continue
		}
		if len(enabled) == 0 {
			if _, active := activeByPlugin[plugin]; active {
				if err := c.runtime.Disable(ctx, plugin); err != nil && !errors.Is(err, store.ErrNotFound) {
					failures[plugin] = statusFailure{code: "disable_failed", message: "plugin deactivation failed; inspect daemon logs"}
				}
			}
			continue
		}
		target := enabled[0]
		record, valid := exactRecord(target, recordByVersion)
		if !valid {
			continue
		}
		if incompatible(record) {
			continue
		}
		active, alreadyActive := activeByPlugin[plugin]
		if !alreadyActive || active.Version != record.Version || active.Digest != record.Digest {
			if err := c.runtime.Enable(ctx, record.Name, record.Version); err != nil {
				failures[plugin] = statusFailure{code: "activation_failed", message: "plugin activation failed; inspect daemon logs"}
			}
		}
	}
	records, err = c.runtime.List(ctx)
	if err != nil {
		return err
	}
	recordByVersion, activeByPlugin = indexRecords(records)
	for _, diagnostic := range c.runtime.HealthDiagnostics(ctx) {
		plugin := pluginFromHealthPath(diagnostic.Path)
		if plugin != "" {
			failures[plugin] = statusFailure{code: diagnostic.Code, message: diagnostic.Message}
		}
	}
	issues := map[string]health.Diagnostic{}
	for _, installation := range installations {
		status, issue := installationStatus(installation, groups[installation.Spec.Plugin], recordByVersion, activeByPlugin, failures)
		if _, err := c.store.UpdateResourceStatus(ctx, "PluginInstallation", installation.Metadata.Namespace, installation.Metadata.Name, installation.Metadata.Generation, status); err != nil {
			return err
		}
		if issue != nil {
			issues[installationKey(installation.Spec.Plugin, installation.Spec.Version)+"/"+installation.Metadata.Namespace+"/"+installation.Metadata.Name] = *issue
		}
	}
	c.replaceIssues(issues)
	return nil
}

// Diagnostics validates immutable identity and reports bundle observations.
// Missing or mismatched bundles remain persistable desired state and therefore
// use warnings; mutation of an existing identity and duplicate projections are
// rejected.
func (c *Controller) Diagnostics(ctx context.Context, document resource.Document) []flow.Diagnostic {
	if document.Kind != "PluginInstallation" {
		return nil
	}
	installation, err := resource.DecodePluginInstallation(document.JSON)
	if err != nil {
		return []flow.Diagnostic{{Path: "spec", Code: "invalid", Message: "PluginInstallation cannot be decoded"}}
	}
	diagnostics := make([]flow.Diagnostic, 0)
	current, getErr := c.store.Get(ctx, document.Kind, document.Metadata.Namespace, document.Metadata.Name)
	if getErr == nil {
		stored, decodeErr := resource.DecodePluginInstallation(current.JSON)
		if decodeErr != nil {
			return []flow.Diagnostic{{Path: "spec", Code: "invalid", Message: "stored PluginInstallation cannot be decoded"}}
		}
		if stored.Spec.Plugin != installation.Spec.Plugin || stored.Spec.Version != installation.Spec.Version || stored.Spec.Digest != installation.Spec.Digest {
			diagnostics = append(diagnostics, flow.Diagnostic{Path: "spec", Code: "immutable_identity", Message: "PluginInstallation plugin, version, and digest are immutable"})
		}
	} else if !errors.Is(getErr, store.ErrNotFound) {
		diagnostics = append(diagnostics, flow.Diagnostic{Path: "spec", Code: "reference_unavailable", Message: "plugin installation state is temporarily unavailable"})
	}
	documents, _, listErr := c.store.List(ctx, "PluginInstallation", "", 1000)
	if listErr != nil {
		diagnostics = append(diagnostics, flow.Diagnostic{Path: "spec", Code: "reference_unavailable", Message: "plugin installation state is temporarily unavailable"})
		return diagnostics
	}
	for _, candidate := range documents {
		if candidate.Metadata.UID == document.Metadata.UID || (candidate.Metadata.Namespace == document.Metadata.Namespace && candidate.Metadata.Name == document.Metadata.Name) {
			continue
		}
		other, decodeErr := resource.DecodePluginInstallation(candidate.JSON)
		if decodeErr == nil && other.Spec.Plugin == installation.Spec.Plugin && other.Spec.Version == installation.Spec.Version {
			diagnostics = append(diagnostics, flow.Diagnostic{Path: "spec.version", Code: "duplicate_installation", Message: "plugin name and version already have a PluginInstallation resource"})
			break
		}
	}
	records, runtimeErr := c.runtime.List(ctx)
	if runtimeErr != nil {
		diagnostics = append(diagnostics, flow.Diagnostic{Path: "spec", Code: "reference_unavailable", Message: "installed plugin state is temporarily unavailable"})
		return diagnostics
	}
	recordByVersion, _ := indexRecords(records)
	record, found := recordByVersion[installationKey(installation.Spec.Plugin, installation.Spec.Version)]
	switch {
	case !found:
		diagnostics = append(diagnostics, flow.Diagnostic{Severity: flow.SeverityWarning, Path: "spec.version", Code: "bundle_missing", Message: "the immutable plugin bundle is not installed"})
	case record.Digest != installation.Spec.Digest:
		diagnostics = append(diagnostics, flow.Diagnostic{Severity: flow.SeverityWarning, Path: "spec.digest", Code: "digest_mismatch", Message: "the installed plugin bundle has a different digest"})
	case incompatible(record):
		diagnostics = append(diagnostics, flow.Diagnostic{Severity: flow.SeverityWarning, Path: "spec.version", Code: "protocol_incompatible", Message: "the installed plugin protocol is incompatible with this daemon"})
	}
	return diagnostics
}

// EnsureProjection creates the one deterministic disabled resource owned by an
// uploaded immutable bundle, or returns the existing matching projection.
func (c *Controller) EnsureProjection(ctx context.Context, record store.PluginRecord) (resource.Document, error) {
	c.reconcile.Lock()
	defer c.reconcile.Unlock()
	return c.ensureProjection(ctx, record)
}

// SetEnabled implements compatibility CLI mutations through resource desired
// state, then synchronously reconciles the same controller path used by Apply.
func (c *Controller) SetEnabled(ctx context.Context, plugin, version string, enabled bool) error {
	c.reconcile.Lock()
	defer c.reconcile.Unlock()
	installations, err := c.installations(ctx)
	if err != nil {
		return err
	}
	found := false
	targetFound := !enabled
	ordered := make([]resource.PluginInstallation, 0)
	if enabled {
		for _, installation := range installations {
			if installation.Spec.Plugin == plugin && installation.Spec.Version == version {
				ordered = append(ordered, installation)
				targetFound = true
			}
		}
		if !targetFound {
			return store.ErrNotFound
		}
		found = true
	}
	for _, installation := range installations {
		if installation.Spec.Plugin != plugin || (enabled && installation.Spec.Version == version) {
			continue
		}
		ordered = append(ordered, installation)
		found = true
	}
	if !found {
		return store.ErrNotFound
	}
	for _, installation := range ordered {
		desired := enabled && installation.Spec.Version == version
		current := installation.Spec.Enabled != nil && *installation.Spec.Enabled
		if current == desired {
			continue
		}
		value := desired
		installation.Spec.Enabled = &value
		encoded, err := json.Marshal(installation)
		if err != nil {
			return err
		}
		document, err := resource.DecodeStrict(encoded)
		if err != nil {
			return err
		}
		if _, err := c.store.Apply(ctx, document, store.ApplyOptions{ExpectedResourceVersion: installation.Metadata.ResourceVersion, RequestID: "plugin-cli-desired-state", Actor: "unix-peer"}); err != nil {
			return err
		}
	}
	err = c.reconcileLocked(ctx)
	c.observe(err)
	return err
}

func (c *Controller) ensureProjections(ctx context.Context, records []store.PluginRecord) error {
	for _, record := range records {
		if _, err := c.ensureProjection(ctx, record); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) ensureProjection(ctx context.Context, record store.PluginRecord) (resource.Document, error) {
	documents, _, err := c.store.List(ctx, "PluginInstallation", "", 1000)
	if err != nil {
		return resource.Document{}, err
	}
	for _, document := range documents {
		installation, decodeErr := resource.DecodePluginInstallation(document.JSON)
		if decodeErr != nil {
			return resource.Document{}, decodeErr
		}
		if installation.Spec.Plugin == record.Name && installation.Spec.Version == record.Version {
			// A predeclared digest mismatch is an observable desired-state
			// error, not permission to create a second projection for the same
			// immutable plugin version.
			return document, nil
		}
	}
	name := ResourceName(record)
	// Upgrade compatibility: an activation from the pre-controller store is
	// adopted as enabled desired state. Newly uploaded records are inactive and
	// therefore create disabled resources.
	value := record.Active
	installation := resource.PluginInstallation{
		TypeMeta: resource.TypeMeta{APIVersion: resource.APIVersion, Kind: "PluginInstallation"},
		Metadata: resource.ObjectMeta{Name: name, Namespace: resource.DefaultNamespace},
		Spec:     resource.PluginInstallationSpec{Plugin: record.Name, Version: record.Version, Digest: record.Digest, Enabled: &value},
	}
	encoded, err := json.Marshal(installation)
	if err != nil {
		return resource.Document{}, err
	}
	document, err := resource.DecodeStrict(encoded)
	if err != nil {
		return resource.Document{}, err
	}
	return c.store.Apply(ctx, document, store.ApplyOptions{RequestID: "adopt-plugin-" + record.Name + "-" + record.Version, Actor: "plugin-controller"})
}

func (c *Controller) installations(ctx context.Context) ([]resource.PluginInstallation, error) {
	documents, _, err := c.store.List(ctx, "PluginInstallation", "", 1000)
	if err != nil {
		return nil, err
	}
	result := make([]resource.PluginInstallation, 0, len(documents))
	for _, document := range documents {
		installation, err := resource.DecodePluginInstallation(document.JSON)
		if err != nil {
			return nil, err
		}
		result = append(result, installation)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Spec.Plugin != result[j].Spec.Plugin {
			return result[i].Spec.Plugin < result[j].Spec.Plugin
		}
		if result[i].Spec.Version != result[j].Spec.Version {
			return result[i].Spec.Version < result[j].Spec.Version
		}
		if result[i].Metadata.Namespace != result[j].Metadata.Namespace {
			return result[i].Metadata.Namespace < result[j].Metadata.Namespace
		}
		return result[i].Metadata.Name < result[j].Metadata.Name
	})
	return result, nil
}

type statusFailure struct {
	code    string
	message string
}

func installationStatus(installation resource.PluginInstallation, group []resource.PluginInstallation, records map[string]store.PluginRecord, active map[string]store.PluginRecord, failures map[string]statusFailure) (map[string]any, *health.Diagnostic) {
	phase, ready, reason, message := "Installed", true, "Reconciled", "immutable plugin installation is reconciled"
	record, found := records[installationKey(installation.Spec.Plugin, installation.Spec.Version)]
	activeRecord, isActive := active[installation.Spec.Plugin]
	isActive = isActive && activeRecord.Version == installation.Spec.Version && activeRecord.Digest == installation.Spec.Digest
	enabled := installation.Spec.Enabled != nil && *installation.Spec.Enabled
	capabilities := []string{}
	if found {
		var manifest pluginbundle.Manifest
		if json.Unmarshal(record.ManifestJSON, &manifest) == nil {
			capabilities = append(capabilities, manifest.Capabilities...)
			sort.Strings(capabilities)
		}
	}
	var diagnostic *health.Diagnostic
	switch {
	case len(enabledInstallations(group)) > 1 && enabled:
		phase, ready, reason, message = "Conflict", false, "MultipleEnabledVersions", "multiple plugin versions are enabled"
	case !found:
		phase, ready, reason, message = "Error", false, "BundleMissing", "the immutable plugin bundle is not installed"
	case record.Digest != installation.Spec.Digest:
		phase, ready, reason, message = "Error", false, "DigestMismatch", "the installed plugin bundle has a different digest"
	case incompatible(record):
		phase, ready, reason, message = "Error", false, "ProtocolIncompatible", "the installed plugin protocol is incompatible with this daemon"
	case failures[installation.Spec.Plugin].code != "" && (enabled || isActive):
		failure := failures[installation.Spec.Plugin]
		phase, ready, reason, message = "Error", false, reasonTitle(failure.code), failure.message
	case enabled && isActive:
		phase, reason, message = "Active", "Activated", "the selected immutable plugin version is active"
	case enabled:
		phase, ready, reason, message = "Error", false, "ActivationPending", "the selected immutable plugin version is not active"
	}
	if !ready {
		diagnostic = &health.Diagnostic{Path: "controllers/plugins/" + installation.Spec.Plugin + "@" + installation.Spec.Version, Code: strings.ToLower(reason), Message: message}
	}
	diagnostics := []map[string]any{}
	if !ready {
		diagnostics = append(diagnostics, map[string]any{"code": strings.ToLower(reason), "message": message})
	}
	return map[string]any{
		"observedGeneration": installation.Metadata.Generation,
		"phase":              phase,
		"installed":          found && record.Digest == installation.Spec.Digest,
		"active":             isActive,
		"plugin":             installation.Spec.Plugin,
		"version":            installation.Spec.Version,
		"digest":             installation.Spec.Digest,
		"capabilities":       capabilities,
		"conditions": []map[string]any{{
			"type": "Ready", "status": map[bool]string{true: "True", false: "False"}[ready], "reason": reason, "message": message,
		}},
		"diagnostics": diagnostics,
	}, diagnostic
}

func (c *Controller) replaceIssues(next map[string]health.Diagnostic) {
	for key := range c.issueKeys {
		if _, exists := next[key]; !exists {
			c.health.Clear("desired/" + key)
		}
	}
	for key, diagnostic := range next {
		c.health.Set("desired/"+key, diagnostic)
	}
	c.issueKeys = make(map[string]struct{}, len(next))
	for key := range next {
		c.issueKeys[key] = struct{}{}
	}
}

func groupInstallations(installations []resource.PluginInstallation) map[string][]resource.PluginInstallation {
	result := map[string][]resource.PluginInstallation{}
	for _, installation := range installations {
		result[installation.Spec.Plugin] = append(result[installation.Spec.Plugin], installation)
	}
	return result
}

func enabledInstallations(installations []resource.PluginInstallation) []resource.PluginInstallation {
	result := make([]resource.PluginInstallation, 0, len(installations))
	for _, installation := range installations {
		if installation.Spec.Enabled != nil && *installation.Spec.Enabled {
			result = append(result, installation)
		}
	}
	return result
}

func indexRecords(records []store.PluginRecord) (map[string]store.PluginRecord, map[string]store.PluginRecord) {
	versions := make(map[string]store.PluginRecord, len(records))
	active := map[string]store.PluginRecord{}
	for _, record := range records {
		versions[installationKey(record.Name, record.Version)] = record
		if record.Active {
			active[record.Name] = record
		}
	}
	return versions, active
}

func exactRecord(installation resource.PluginInstallation, records map[string]store.PluginRecord) (store.PluginRecord, bool) {
	record, found := records[installationKey(installation.Spec.Plugin, installation.Spec.Version)]
	return record, found && record.Digest == installation.Spec.Digest
}

func incompatible(record store.PluginRecord) bool {
	var manifest pluginbundle.Manifest
	return json.Unmarshal(record.ManifestJSON, &manifest) != nil || manifest.Protocol.Minimum > hostProtocolVersion || manifest.Protocol.Maximum < hostProtocolVersion
}

func pluginFromHealthPath(path string) string {
	const prefix = "plugins/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	identity := strings.TrimPrefix(path, prefix)
	plugin, _, _ := strings.Cut(identity, "@")
	return plugin
}

func installationKey(plugin, version string) string { return plugin + "@" + version }

func reasonTitle(code string) string {
	parts := strings.Split(code, "_")
	for index := range parts {
		if parts[index] != "" {
			parts[index] = strings.ToUpper(parts[index][:1]) + parts[index][1:]
		}
	}
	return strings.Join(parts, "")
}

// ResourceName returns a stable DNS label for one immutable bundle identity.
func ResourceName(record store.PluginRecord) string {
	digest := strings.ToLower(record.Digest)
	if len(digest) > 10 {
		digest = digest[:10]
	}
	base := sanitizeLabel(record.Name + "-" + record.Version)
	if base == "" {
		base = "plugin"
	}
	suffix := ""
	if digest != "" {
		suffix = "-" + digest
	}
	maximumBase := 63 - len(suffix)
	if maximumBase < 1 {
		maximumBase = 1
	}
	if len(base) > maximumBase {
		base = strings.Trim(base[:maximumBase], "-")
	}
	if base == "" {
		base = "p"
	}
	return base + suffix
}

func sanitizeLabel(value string) string {
	var builder strings.Builder
	lastDash := false
	for _, char := range strings.ToLower(value) {
		if unicode.IsLetter(char) || unicode.IsDigit(char) {
			builder.WriteRune(char)
			lastDash = false
		} else if !lastDash && builder.Len() > 0 {
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

// Package health provides secret-safe, deterministic control-plane health
// diagnostics without coupling runtime components to protobuf types.
package health

import (
	"sort"
	"sync"
)

// Diagnostic is one stable operator-facing health failure.
type Diagnostic struct {
	Path    string
	Code    string
	Message string
}

// Tracker retains component failures until their owner observes recovery.
type Tracker struct {
	mu          sync.RWMutex
	diagnostics map[string]Diagnostic
}

// NewTracker creates an empty runtime health tracker.
func NewTracker() *Tracker {
	return &Tracker{diagnostics: map[string]Diagnostic{}}
}

// Set records or replaces one component failure. Callers provide only stable,
// secret-safe text rather than forwarding arbitrary dependency errors.
func (t *Tracker) Set(component string, diagnostic Diagnostic) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.diagnostics[component] = diagnostic
	t.mu.Unlock()
}

// Clear records a successful reconciliation of one component.
func (t *Tracker) Clear(component string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	delete(t.diagnostics, component)
	t.mu.Unlock()
}

// Has reports whether a component still has an unresolved failure.
func (t *Tracker) Has(component string) bool {
	if t == nil {
		return false
	}
	t.mu.RLock()
	_, exists := t.diagnostics[component]
	t.mu.RUnlock()
	return exists
}

// Snapshot returns failures in stable component order.
func (t *Tracker) Snapshot() []Diagnostic {
	if t == nil {
		return nil
	}
	t.mu.RLock()
	components := make([]string, 0, len(t.diagnostics))
	for component := range t.diagnostics {
		components = append(components, component)
	}
	sort.Strings(components)
	result := make([]Diagnostic, 0, len(components))
	for _, component := range components {
		result = append(result, t.diagnostics[component])
	}
	t.mu.RUnlock()
	return result
}

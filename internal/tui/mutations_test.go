package tui

import (
	"testing"

	controlv1alpha1 "github.com/alexrett/orchigram/gen/orchigram/control/v1alpha1"
)

func TestPluginRollbackCandidatesUseSemanticVersionOrder(t *testing.T) {
	current := &controlv1alpha1.PluginInfo{Name: "exec", Version: "1.0.0"}
	plugins := []*controlv1alpha1.PluginInfo{
		{Name: "other", Version: "9.0.0"},
		{Name: "exec", Version: "0.2.0-beta.1"},
		{Name: "exec", Version: "0.2.0"},
		{Name: "exec", Version: "0.10.0"},
		current,
	}
	candidates := pluginRollbackCandidates(current, plugins)
	want := []string{"0.10.0", "0.2.0", "0.2.0-beta.1"}
	if len(candidates) != len(want) {
		t.Fatalf("candidate count=%d want=%d", len(candidates), len(want))
	}
	for index, version := range want {
		if candidates[index].GetVersion() != version {
			t.Fatalf("candidate %d=%q want=%q", index, candidates[index].GetVersion(), version)
		}
	}
}

package health

import (
	"reflect"
	"sync"
	"testing"
)

func TestTrackerSnapshotsDeterministicallyAndRecovers(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	tracker.Set("z", Diagnostic{Path: "z", Code: "failed", Message: "z failed"})
	tracker.Set("a", Diagnostic{Path: "a", Code: "failed", Message: "a failed"})
	want := []Diagnostic{{Path: "a", Code: "failed", Message: "a failed"}, {Path: "z", Code: "failed", Message: "z failed"}}
	if got := tracker.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot=%+v want=%+v", got, want)
	}
	tracker.Clear("a")
	if tracker.Has("a") || !tracker.Has("z") {
		t.Fatalf("unexpected component state: %+v", tracker.Snapshot())
	}
}

func TestTrackerSupportsConcurrentHealthUpdates(t *testing.T) {
	t.Parallel()
	tracker := NewTracker()
	var workers sync.WaitGroup
	for index := range 32 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			component := string(rune('a' + index%8))
			tracker.Set(component, Diagnostic{Path: component, Code: "failed", Message: "component failed"})
			_ = tracker.Snapshot()
			tracker.Clear(component)
		}()
	}
	workers.Wait()
}

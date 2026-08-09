package pluginmanager

import (
	"context"
	"testing"
	"time"
)

func TestCallLimiterBoundsAndCancelsWaiters(t *testing.T) {
	t.Parallel()
	limiter := newCallLimiter("test", 1)
	release, err := limiter.acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := limiter.acquire(ctx)
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for limiter.waiting.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if limiter.active.Load() != 1 || limiter.waiting.Load() != 1 {
		t.Fatalf("active=%d waiting=%d", limiter.active.Load(), limiter.waiting.Load())
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled waiter acquired capacity")
	}
	release()
	if limiter.active.Load() != 0 {
		t.Fatalf("active after release=%d", limiter.active.Load())
	}
}

package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	for i := 0; i < 100; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

func TestInProcPutGetWatch(t *testing.T) {
	s := NewInProcStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Watch(ctx, WorkersPrefix)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, WorkerStateKey("w1"), []byte(`{"v":1,"worker_id":"w1"}`), time.Minute); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-ch:
		if ev.Key != WorkerStateKey("w1") {
			t.Fatalf("watch key = %q", ev.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("no watch event")
	}

	if _, ok, _ := s.Get(ctx, WorkerStateKey("w1")); !ok {
		t.Fatal("Get after Put failed")
	}
}

func TestInProcTTLExpiry(t *testing.T) {
	s := NewInProcStore()
	ctx := context.Background()
	_ = s.Put(ctx, "k", []byte("v"), 20*time.Millisecond)
	if _, ok, _ := s.Get(ctx, "k"); !ok {
		t.Fatal("expected value before TTL")
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok, _ := s.Get(ctx, "k"); ok {
		t.Fatal("expected expiry after TTL")
	}
}

func TestPluginProviderMaterializes(t *testing.T) {
	s := NewInProcStore()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewPluginProvider(s)
	if err := p.Start(ctx); err != nil {
		t.Fatal(err)
	}

	wb, _ := json.Marshal(WorkerState{Version: 1, WorkerID: "w1", KVUsage: 0.42})
	_ = s.Put(ctx, WorkerStateKey("w1"), wb, time.Minute)
	sb, _ := json.Marshal(SessionState{Version: 1, WorkerID: "w1", SessionID: "s1", CachedTokens: 1234})
	_ = s.Put(ctx, SessionStateKey("s1"), sb, time.Minute)

	waitFor(t, func() bool { _, ok := p.WorkerState("w1"); return ok })
	waitFor(t, func() bool { _, ok := p.SessionCachedTokens("w1", "s1"); return ok })

	if ws, _ := p.WorkerState("w1"); ws.KVUsage != 0.42 {
		t.Fatalf("kv_usage = %v", ws.KVUsage)
	}
	if n, ok := p.SessionCachedTokens("w1", "s1"); !ok || n != 1234 {
		t.Fatalf("session cached = %d ok=%v", n, ok)
	}
	if _, ok := p.SessionCachedTokens("w2", "s1"); ok {
		t.Fatal("w2 must not own s1")
	}
	// Exact prefix matching is deferred (needs the tokenizer).
	if _, ok := p.CachedTokens("w1", "anything"); ok {
		t.Fatal("plugin CachedTokens should be unknown")
	}
}

func TestCompositeFallsBackToApproximation(t *testing.T) {
	approx := NewApproximateProvider(1 << 20)
	c := NewCompositeProvider(nil, approx)

	if _, ok := c.WorkerState("w"); ok {
		t.Fatal("no plugin -> no worker state")
	}
	approx.Observe("w", "a shared system prompt prefix")
	if n, ok := c.CachedTokens("w", "a shared system prompt prefix and more"); !ok || n == 0 {
		t.Fatalf("approx fallback = %d ok=%v", n, ok)
	}
}

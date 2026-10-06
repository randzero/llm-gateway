package policies

import (
	"context"
	"strings"
	"testing"
)

func llCand(id string, load, cached, thpt int) Candidate {
	// WaitingTokens: -1 = "no real backlog reported" -> the policy estimates.
	return Candidate{ID: id, Healthy: true, Load: load, CachedTokens: cached, Throughput: thpt, WaitingTokens: -1}
}

func newLL() *LeastLatency { return NewLeastLatency(1) }

// The point over cache-first: a big cache on a busy worker can lose to no cache
// on an idle one. P=2000: w1 = ((50+1)*2000-1000)/1000 = 101, w2 = 2000/1000 = 2.
func TestLeastLatencyTradesCacheForLoad(t *testing.T) {
	req := &RequestMeta{Text: strings.Repeat("x", 8000)}
	cs := []Candidate{llCand("w1", 50, 1000, 1000), llCand("w2", 0, 0, 1000)}
	if idx, err := newLL().Select(context.Background(), req, cs); err != nil || idx != 1 {
		t.Fatalf("idx=%d err=%v, want w2 (idle beats cached-but-busy)", idx, err)
	}
}

func TestLeastLatencyPrefersCache(t *testing.T) {
	req := &RequestMeta{Text: strings.Repeat("x", 8000)}
	cs := []Candidate{
		llCand("w1", 0, 1000, 1000), // (2000-1000)/1000 = 1
		llCand("w2", 0, 0, 1000),    // 2
	}
	if idx, _ := newLL().Select(context.Background(), req, cs); idx != 0 {
		t.Fatalf("idx=%d, want w1 (cache wins)", idx)
	}
}

func TestLeastLatencyPrefersThroughput(t *testing.T) {
	req := &RequestMeta{Text: strings.Repeat("x", 8000)}
	cs := []Candidate{llCand("w1", 0, 0, 2000), llCand("w2", 0, 0, 1000)}
	if idx, _ := newLL().Select(context.Background(), req, cs); idx != 0 {
		t.Fatalf("idx=%d, want w1 (faster)", idx)
	}
}

func TestLeastLatencyFallbackLeastLoaded(t *testing.T) {
	req := &RequestMeta{Text: strings.Repeat("x", 8000)}
	cs := []Candidate{ // no throughput reported
		llCand("w1", 5, 9999, 0),
		llCand("w2", 1, 0, 0),
	}
	if idx, _ := newLL().Select(context.Background(), req, cs); idx != 1 {
		t.Fatalf("idx=%d, want w2 (least-loaded fallback)", idx)
	}
}

// No signal distinguishes equal candidates: the policy must spread, not always
// return the first.
func TestLeastLatencySpreadsTies(t *testing.T) {
	req := &RequestMeta{Text: strings.Repeat("x", 8000)}
	cs := []Candidate{llCand("w1", 0, 0, 1000), llCand("w2", 0, 0, 1000), llCand("w3", 0, 0, 1000)}
	seen := map[int]bool{}
	p := newLL()
	for i := 0; i < 200; i++ {
		idx, _ := p.Select(context.Background(), req, cs)
		seen[idx] = true
	}
	if len(seen) < 2 {
		t.Fatalf("ties not spread: only saw %v", seen)
	}
}

// With the engine's real prefill backlog, a worker with a huge backlog loses
// even if it holds the cache; and one with a small backlog beats an idle one
// once its cache is big enough.
func TestLeastLatencyUsesRealBacklog(t *testing.T) {
	req := &RequestMeta{Text: strings.Repeat("x", 8000)} // P = 2000

	// w1: 1M tokens queued (real) with cache -> (1e6 + 2000 - 1000)/1000 = 1001
	// w2: empty backlog, no cache            -> 2
	busy := Candidate{ID: "w1", Healthy: true, Throughput: 1000, CachedTokens: 1000, WaitingTokens: 1_000_000}
	idle := Candidate{ID: "w2", Healthy: true, Throughput: 1000, WaitingTokens: 0}
	if idx, _ := newLL().Select(context.Background(), req, []Candidate{busy, idle}); idx != 1 {
		t.Fatalf("idx=%d, want w2 (huge backlog loses)", idx)
	}

	// Flip: w1 small backlog (100) + big cache -> 0.2 beats idle w2 -> 2.0.
	near := Candidate{ID: "w1", Healthy: true, Throughput: 1000, CachedTokens: 1900, WaitingTokens: 100}
	if idx, _ := newLL().Select(context.Background(), req, []Candidate{near, idle}); idx != 0 {
		t.Fatalf("idx=%d, want w1 (small backlog + big cache)", idx)
	}
}

func TestLeastLatencyNoCandidate(t *testing.T) {
	cs := []Candidate{{ID: "w1", Healthy: false}}
	if _, err := newLL().Select(context.Background(), &RequestMeta{Text: "x"}, cs); err != ErrNoCandidate {
		t.Fatalf("err=%v, want ErrNoCandidate", err)
	}
}

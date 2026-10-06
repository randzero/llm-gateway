package prefix

import (
	"context"
	"testing"
	"time"
)

// countingIndex tallies Lookup calls to prove cache hits bypass the backing index.
type countingIndex struct {
	Index
	lookups int
}

func (c *countingIndex) Lookup(ctx context.Context, hash string) ([]string, error) {
	c.lookups++
	return c.Index.Lookup(ctx, hash)
}

func TestCachingIndexServesFromCache(t *testing.T) {
	ctx := context.Background()
	inner := &countingIndex{Index: NewInProcIndex()}
	_ = inner.Add(ctx, "w1", "h0")
	c := NewCachingIndex(inner, 10, 0)

	if ws, _ := c.Lookup(ctx, "h0"); len(ws) != 1 {
		t.Fatalf("lookup got %v", ws)
	}
	if inner.lookups != 1 {
		t.Fatalf("inner lookups = %d, want 1", inner.lookups)
	}
	_, _ = c.Lookup(ctx, "h0")
	if inner.lookups != 1 {
		t.Fatalf("second lookup should be cached; inner lookups = %d", inner.lookups)
	}
}

func TestCachingIndexInvalidatesOnWrite(t *testing.T) {
	ctx := context.Background()
	inner := &countingIndex{Index: NewInProcIndex()}
	c := NewCachingIndex(inner, 10, 0)

	_, _ = c.Lookup(ctx, "h0") // cache empty-set (miss)
	before := inner.lookups
	_ = c.Add(ctx, "w1", "h0") // must invalidate
	if ws, _ := c.Lookup(ctx, "h0"); len(ws) != 1 {
		t.Fatalf("after Add lookup = %v, want [w1]", ws)
	}
	if inner.lookups <= before {
		t.Fatalf("Add did not invalidate: inner lookups stayed %d", inner.lookups)
	}
}

func TestCachingIndexTTL(t *testing.T) {
	ctx := context.Background()
	inner := &countingIndex{Index: NewInProcIndex()}
	_ = inner.Add(ctx, "w1", "h0")
	c := NewCachingIndex(inner, 10, time.Minute)

	clock := time.Unix(0, 0)
	c.now = func() time.Time { return clock }

	_, _ = c.Lookup(ctx, "h0") // cached at t0
	_, _ = c.Lookup(ctx, "h0") // from cache
	clock = clock.Add(2 * time.Minute)
	_, _ = c.Lookup(ctx, "h0") // expired -> refetch
	if inner.lookups != 2 {
		t.Fatalf("inner lookups = %d, want 2 (1 initial + 1 after TTL)", inner.lookups)
	}
}

func TestCachingIndexMatch(t *testing.T) {
	ctx := context.Background()
	inner := NewInProcIndex()
	for _, h := range []string{"h0", "h1"} {
		_ = inner.Add(ctx, "w1", h)
	}
	c := NewCachingIndex(inner, 10, 0)
	matched, workers, err := c.Match(ctx, []string{"h0", "h1", "h2"})
	if err != nil || matched != 2 || len(workers) != 1 || workers[0] != "w1" {
		t.Fatalf("match = %d,%v err=%v", matched, workers, err)
	}
}

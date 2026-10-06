package policies

import (
	"context"
	"strings"
	"testing"
)

func TestCacheAwarePrefersOverlap(t *testing.T) {
	p := NewCacheAware(0.5)
	prompt := strings.Repeat("a", 400) // ~100 estimated tokens
	c := []Candidate{
		{ID: "a", Healthy: true, CachedTokens: 90, Load: 5}, // 0.9 overlap
		{ID: "b", Healthy: true, CachedTokens: 0, Load: 0},
	}
	idx, err := p.Select(context.Background(), &RequestMeta{Text: prompt}, c)
	if err != nil {
		t.Fatal(err)
	}
	if c[idx].ID != "a" {
		t.Fatalf("expected highest-overlap worker 'a', got %q", c[idx].ID)
	}
}

func TestCacheAwareFallsBackToLeastLoaded(t *testing.T) {
	p := NewCacheAware(0.8)
	prompt := strings.Repeat("a", 400) // ~100 tokens
	c := []Candidate{
		{ID: "a", Healthy: true, CachedTokens: 10, Load: 9}, // 0.1 < threshold
		{ID: "b", Healthy: true, CachedTokens: 0, Load: 1},
	}
	idx, err := p.Select(context.Background(), &RequestMeta{Text: prompt}, c)
	if err != nil {
		t.Fatal(err)
	}
	if c[idx].ID != "b" {
		t.Fatalf("expected least-loaded fallback 'b', got %q", c[idx].ID)
	}
}

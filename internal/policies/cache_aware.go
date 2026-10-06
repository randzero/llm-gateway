package policies

import "context"

// CacheAware routes to the worker with the highest prefix overlap for the
// request, and falls back to the least-loaded worker when no worker clears the
// threshold. Each Candidate's CachedTokens (from the StateProvider) is the
// estimated cached tokens for this request on that worker.
type CacheAware struct{ threshold float64 }

// NewCacheAware returns a cache-aware policy; threshold is the minimum match
// rate (0..1) to prefer the highest-overlap worker (default 0.8).
func NewCacheAware(threshold float64) *CacheAware {
	if threshold <= 0 || threshold > 1 {
		threshold = 0.8
	}
	return &CacheAware{threshold: threshold}
}

// Name implements Policy.
func (*CacheAware) Name() string { return "cache_aware" }

// Select implements Policy.
func (p *CacheAware) Select(_ context.Context, req *RequestMeta, cs []Candidate) (int, error) {
	h := HealthyIndices(cs)
	if len(h) == 0 {
		return -1, ErrNoCandidate
	}

	if promptTokens := len(req.Text) / 4; promptTokens > 0 {
		best, bestRate := -1, 0.0
		for _, i := range h {
			if cs[i].CachedTokens <= 0 {
				continue
			}
			rate := float64(cs[i].CachedTokens) / float64(promptTokens)
			if rate > bestRate {
				bestRate, best = rate, i
			}
		}
		if best >= 0 && bestRate >= p.threshold {
			return best, nil
		}
	}

	// Fallback: least loaded.
	lo := h[0]
	for _, i := range h[1:] {
		if cs[i].Load < cs[lo].Load {
			lo = i
		}
	}
	return lo, nil
}

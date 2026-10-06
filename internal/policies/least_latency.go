package policies

import (
	"context"
	"math"
	"math/rand"
)

// LeastLatency routes to the worker minimizing the request's estimated
// time-to-first-token. It is a single, general objective that folds every
// signal the router has — cached-prefix saving, queue load, prefill throughput
// — onto one scale (seconds):
//
//	TTFT(w) = ((Load(w)+1) * P - Cached(w)) / Throughput(w)
//
// where P is the request's prompt token count and Load is running+waiting. A
// cache hit lowers TTFT, load raises it, and a faster worker divides it, so a
// slightly-less-cached but idler (or faster) worker can win — cache-first
// routing and least-loaded are the degenerate cases (weight one term to ~0).
//
// Ties are broken at random, so even with no distinguishing signal the policy
// spreads load rather than always picking the same worker. When throughput (or
// the prompt text) is unavailable it degrades to least-loaded; workers that
// report no throughput are skipped.
type LeastLatency struct{ r *rand.Rand }

// NewLeastLatency returns a least-latency policy (seed for tie-breaking).
func NewLeastLatency(seed int64) *LeastLatency {
	return &LeastLatency{r: rand.New(rand.NewSource(seed))}
}

// Name implements Policy.
func (*LeastLatency) Name() string { return "least_latency" }

// Select implements Policy.
func (p *LeastLatency) Select(_ context.Context, req *RequestMeta, cs []Candidate) (int, error) {
	h := HealthyIndices(cs)
	if len(h) == 0 {
		return -1, ErrNoCandidate
	}
	if len(h) == 1 {
		return h[0], nil
	}

	if promptTokens := len(req.Text) / 4; promptTokens > 0 {
		best := math.MaxFloat64
		var ties []int
		for _, i := range h {
			if cs[i].Throughput <= 0 {
				continue
			}
			// Queue depth in tokens: the engine's real prefill backlog when the
			// plugin reports it, else an estimate (Load requests each ≈ P).
			queued := cs[i].Load * promptTokens
			if cs[i].WaitingTokens >= 0 {
				queued = cs[i].WaitingTokens
			}
			ttft := (float64(queued) + float64(promptTokens) - float64(cs[i].CachedTokens)) / float64(cs[i].Throughput)
			switch {
			case ttft < best-1e-9:
				best, ties = ttft, append(ties[:0], i)
			case ttft <= best+1e-9:
				ties = append(ties, i)
			}
		}
		if len(ties) > 0 {
			return ties[p.r.Intn(len(ties))], nil
		}
	}

	return p.leastLoaded(h, cs), nil
}

// leastLoaded picks the least-loaded candidate, ties broken at random.
func (p *LeastLatency) leastLoaded(h []int, cs []Candidate) int {
	best := cs[h[0]].Load
	ties := []int{h[0]}
	for _, i := range h[1:] {
		switch {
		case cs[i].Load < best:
			best, ties = cs[i].Load, append(ties[:0], i)
		case cs[i].Load == best:
			ties = append(ties, i)
		}
	}
	return ties[p.r.Intn(len(ties))]
}

package policies

import (
	"context"
	"math/rand"
	"sync/atomic"
)

// RoundRobin cycles over eligible candidates in order.
type RoundRobin struct{ n atomic.Uint64 }

// NewRoundRobin returns a round-robin policy.
func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

// Name implements Policy.
func (*RoundRobin) Name() string { return "round_robin" }

// Select implements Policy.
func (p *RoundRobin) Select(_ context.Context, _ *RequestMeta, cs []Candidate) (int, error) {
	h := HealthyIndices(cs)
	if len(h) == 0 {
		return -1, ErrNoCandidate
	}
	i := p.n.Add(1) - 1
	return h[i%uint64(len(h))], nil
}

// Random selects a uniformly random eligible candidate.
type Random struct{ r *rand.Rand }

// NewRandom returns a random policy (seeded).
func NewRandom(seed int64) *Random { return &Random{r: rand.New(rand.NewSource(seed))} }

// Name implements Policy.
func (*Random) Name() string { return "random" }

// Select implements Policy.
func (p *Random) Select(_ context.Context, _ *RequestMeta, cs []Candidate) (int, error) {
	h := HealthyIndices(cs)
	if len(h) == 0 {
		return -1, ErrNoCandidate
	}
	return h[p.r.Intn(len(h))], nil
}

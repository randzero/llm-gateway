package policies

import (
	"context"
	"math/rand"
)

// PowerOfTwo picks two random eligible candidates and returns the less loaded
// one ("power of two choices").
type PowerOfTwo struct{ r *rand.Rand }

// NewPowerOfTwo returns a power-of-two policy (seeded).
func NewPowerOfTwo(seed int64) *PowerOfTwo {
	return &PowerOfTwo{r: rand.New(rand.NewSource(seed))}
}

// Name implements Policy.
func (*PowerOfTwo) Name() string { return "power_of_two" }

// Select implements Policy.
func (p *PowerOfTwo) Select(_ context.Context, _ *RequestMeta, cs []Candidate) (int, error) {
	h := HealthyIndices(cs)
	switch len(h) {
	case 0:
		return -1, ErrNoCandidate
	case 1:
		return h[0], nil
	}
	i := p.r.Intn(len(h))
	j := p.r.Intn(len(h) - 1)
	if j >= i {
		j++
	}
	a, b := h[i], h[j]
	if cs[a].Load <= cs[b].Load {
		return a, nil
	}
	return b, nil
}

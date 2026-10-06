// Package policies defines the load-balancing / routing policy interface and
// its registry. A policy is a pure function of a request and the candidate
// set — the candidate set is assembled elsewhere from the control plane
// (health, discovery) and the StateProvider (load, prefix affinity).
package policies

import (
	"context"
	"errors"
)

// ErrNoCandidate is returned when no worker can serve the request.
var ErrNoCandidate = errors.New("no eligible candidate")

// HealthyIndices returns the indices of eligible candidates (healthy + closed
// circuit), preserving order.
func HealthyIndices(cs []Candidate) []int {
	idx := make([]int, 0, len(cs))
	for i := range cs {
		if cs[i].Healthy {
			idx = append(idx, i)
		}
	}
	return idx
}

// RequestMeta is the routing-relevant view of an incoming request.
type RequestMeta struct {
	Model     string
	SessionID string
	UserID    string
	TenantID  string
	// Text is the raw prompt/request text, used for prefix-affinity policies.
	// Empty when the body is not inspected (pass-through streaming).
	Text   string
	DPRank int
}

// Candidate is a worker eligible for selection. Signals are pre-populated so
// a policy only has to read what it needs.
type Candidate struct {
	ID      string
	URL     string
	Model   string
	DPRank  int
	PDRole  string  // "prefill" | "decode" | "" (non-disaggregated)
	Healthy bool    // healthy + circuit-breaker closed
	Load    int     // running + waiting, best known
	KVUsage float64 // 0..1
	// CachedTokens is the best prefix overlap for this request on this worker
	// (from the StateProvider; 0 when unknown).
	CachedTokens int
	// Throughput is the worker's reported prefill throughput (tokens/s), used by
	// cost-aware policies; 0 = unknown.
	Throughput int
	// WaitingTokens is the worker's real prefill backlog (tokens); -1 = unknown.
	WaitingTokens int
}

// Policy selects one candidate by index for a request.
type Policy interface {
	Name() string
	Select(ctx context.Context, req *RequestMeta, candidates []Candidate) (int, error)
}

// Registry holds the available policies by name.
type Registry struct {
	policies map[string]Policy
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{policies: make(map[string]Policy)}
}

// Register adds a policy under its name (last registration wins).
func (r *Registry) Register(p Policy) {
	r.policies[p.Name()] = p
}

// Get looks up a policy by name.
func (r *Registry) Get(name string) (Policy, bool) {
	p, ok := r.policies[name]
	return p, ok
}

// Names lists registered policy names.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.policies))
	for n := range r.policies {
		names = append(names, n)
	}
	return names
}

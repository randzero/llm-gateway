package prefix

import (
	"context"
	"net/http"
)

// WorkerLoad is a worker's reported state used by the router's policy.
type WorkerLoad struct {
	Running       int
	Waiting       int
	Throughput    int // prefill throughput (tokens/s); 0 = unknown
	WaitingTokens int // real prefill backlog (tokens); -1 = unknown
}

// MatchInfo is the routing info for one request, produced by a matcher (the
// local one, or the state-service client). The zero value means "nothing known".
type MatchInfo struct {
	// Tokens / Workers: the longest cached prefix's token count and holders.
	Tokens  int
	Workers []string
	// SessionWorker / SessionTokens: the session's owning worker + cached tokens.
	SessionWorker string
	SessionTokens int
	// Loads: reported load per candidate worker (may be nil).
	Loads map[string]WorkerLoad
}

// PrefixMatcher ties a Hasher (request -> block-hash chain) to an Index
// (block hash -> workers) to answer, for one request, the token count of its
// longest cached prefix and the workers holding it.
type PrefixMatcher struct {
	hasher    Hasher
	index     Index
	blockSize int
	// sample, when true, reduces the per-block hash chain to the exponential
	// subset (SampleIndices) before querying, trading prefix resolution for
	// fewer index lookups. Off by default (exact, one lookup per block).
	sample bool
}

// NewPrefixMatcher builds a matcher. blockSize must match the engine's KV block
// size (0 defaults to 16).
func NewPrefixMatcher(h Hasher, idx Index, blockSize int, sample bool) *PrefixMatcher {
	if blockSize <= 0 {
		blockSize = 16
	}
	return &PrefixMatcher{hasher: h, index: idx, blockSize: blockSize, sample: sample}
}

// Match returns the longest cached prefix for one request. ok=false means no
// hashes or the index was unavailable (caller should fall back). ok=true with
// Tokens==0 means "reached the index, nothing cached". The local matcher has no
// session or load view (Loads nil, SessionWorker empty).
func (m *PrefixMatcher) Match(ctx context.Context, r *http.Request, body []byte, _ []string) (MatchInfo, bool) {
	hashes, err := m.hasher.Hashes(ctx, r, body)
	if err != nil || len(hashes) == 0 {
		return MatchInfo{}, false
	}
	idxs := m.indices(len(hashes))
	query := make([]string, len(idxs))
	for i, j := range idxs {
		query[i] = hashes[j]
	}
	matched, workers, err := m.index.Match(ctx, query)
	if err != nil {
		return MatchInfo{}, false
	}
	if matched == 0 {
		return MatchInfo{}, true
	}
	deepest := idxs[matched-1]
	return MatchInfo{Tokens: (deepest + 1) * m.blockSize, Workers: workers}, true
}

// indices maps hash positions to query positions: identity, or the exponential
// sample of block indices.
func (m *PrefixMatcher) indices(n int) []int {
	if !m.sample {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	}
	return SampleIndices(n)
}

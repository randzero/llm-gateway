package prefix

import (
	"context"
	"sync"
)

// Index is the inverted prefix index: it maps a block hash to the set of
// workers that currently hold that block's KV, and answers longest-cached-prefix
// queries. It is the authoritative view (fed by engine events), not an
// approximation inferred from routing history.
//
// The inverted shape ("hash -> workers") keeps every value small (bounded by
// the worker count), so it never grows into a big key — unlike a forward layout
// ("worker -> its hashes"), whose value grows with a worker's cache.
type Index interface {
	// Add records that worker holds the block with the given hash.
	Add(ctx context.Context, worker, hash string) error
	// Remove records that worker no longer holds the block with the given hash.
	Remove(ctx context.Context, worker, hash string) error
	// Lookup returns the workers currently holding hash (order unspecified).
	Lookup(ctx context.Context, hash string) ([]string, error)
	// Match walks hashes shallowest-first and returns the number of leading
	// hashes that are cached — the longest contiguous cached prefix — and the
	// workers holding the deepest of them (nil if none). hashes MUST be ordered
	// shallowest-first (block 0 first).
	Match(ctx context.Context, hashes []string) (matched int, workers []string, err error)
	// Close releases resources.
	Close() error
}

// InProcIndex is an in-memory Index, for single-instance use and tests.
type InProcIndex struct {
	mu sync.RWMutex
	m  map[string]map[string]struct{} // hash -> set of workers
}

// NewInProcIndex returns an empty in-memory index.
func NewInProcIndex() *InProcIndex {
	return &InProcIndex{m: make(map[string]map[string]struct{})}
}

// Add implements Index.
func (x *InProcIndex) Add(_ context.Context, worker, hash string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	s := x.m[hash]
	if s == nil {
		s = make(map[string]struct{})
		x.m[hash] = s
	}
	s[worker] = struct{}{}
	return nil
}

// Remove implements Index.
func (x *InProcIndex) Remove(_ context.Context, worker, hash string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if s := x.m[hash]; s != nil {
		delete(s, worker)
		if len(s) == 0 {
			delete(x.m, hash)
		}
	}
	return nil
}

// Lookup implements Index.
func (x *InProcIndex) Lookup(_ context.Context, hash string) ([]string, error) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return workersOf(x.m[hash]), nil
}

// Match implements Index.
func (x *InProcIndex) Match(_ context.Context, hashes []string) (int, []string, error) {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var workers []string
	matched := 0
	for _, h := range hashes {
		s := x.m[h]
		if len(s) == 0 {
			break // stop at the first miss: the prefix must be contiguous
		}
		matched++
		workers = workersOf(s)
	}
	if matched == 0 {
		return 0, nil, nil
	}
	return matched, workers, nil
}

// Close implements Index.
func (*InProcIndex) Close() error { return nil }

func workersOf(s map[string]struct{}) []string {
	if len(s) == 0 {
		return nil
	}
	out := make([]string, 0, len(s))
	for w := range s {
		out = append(out, w)
	}
	return out
}

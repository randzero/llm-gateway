package control

import (
	"context"
	"net/http"
	"sync"
	"time"
)

// Registry holds the set of workers and their per-model index.
type Registry struct {
	mu         sync.RWMutex
	workers    []*Worker
	byModel    map[string][]*Worker
	multiModel bool // true if any worker declares a model
}

// NewRegistry builds a registry from workers.
func NewRegistry(workers []*Worker) *Registry {
	r := &Registry{workers: workers, byModel: make(map[string][]*Worker)}
	for _, w := range workers {
		r.byModel[w.Model] = append(r.byModel[w.Model], w)
		if w.Model != "" {
			r.multiModel = true
		}
	}
	return r
}

// All returns a copy of all workers.
func (r *Registry) All() []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]*Worker(nil), r.workers...)
}

// MultiModel reports whether any worker declares a model (i.e. strict per-model
// routing is in effect).
func (r *Registry) MultiModel() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.multiModel
}

// ForModel returns the workers serving model.
//
//   - In single-pool mode (no worker declares a model), it returns all workers,
//     ignoring the requested model.
//   - In multi-model mode it returns only workers declaring that exact model
//     (possibly empty — the caller must not fall back to other models).
func (r *Registry) ForModel(model string) []*Worker {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.multiModel || model == "" {
		return append([]*Worker(nil), r.workers...)
	}
	return append([]*Worker(nil), r.byModel[model]...)
}

// Sync replaces the worker set, preserving existing workers (and their load /
// circuit-breaker state) when ID, URL and model are unchanged.
func (r *Registry) Sync(workers []*Worker) {
	r.mu.Lock()
	defer r.mu.Unlock()

	existing := make(map[string]*Worker, len(r.workers))
	for _, w := range r.workers {
		existing[w.ID] = w
	}

	next := make([]*Worker, 0, len(workers))
	byModel := make(map[string][]*Worker)
	seen := make(map[string]bool)
	multi := false
	for _, w := range workers {
		if seen[w.ID] {
			continue
		}
		seen[w.ID] = true
		if old, ok := existing[w.ID]; ok && old.URL == w.URL && old.Model == w.Model {
			w = old // reuse to keep in-flight load and breaker state
		}
		next = append(next, w)
		byModel[w.Model] = append(byModel[w.Model], w)
		if w.Model != "" {
			multi = true
		}
	}
	r.workers = next
	r.byModel = byModel
	r.multiModel = multi
}

// HealthLoop periodically probes GET {worker}/health until ctx is done.
func (r *Registry) HealthLoop(ctx context.Context, interval time.Duration, client *http.Client) {
	probe := func() {
		for _, w := range r.All() {
			ok := false
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.URL+"/health", nil)
			if err == nil {
				if resp, err := client.Do(req); err == nil {
					ok = resp.StatusCode == http.StatusOK
					resp.Body.Close()
				}
			}
			w.SetHealthy(ok)
		}
	}
	probe()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			probe()
		}
	}
}

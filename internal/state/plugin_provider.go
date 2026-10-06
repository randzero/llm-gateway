package state

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
)

// PluginProvider maintains a local materialized view from a StateStore's watch
// stream. State written by the engine plugin (via the router's /state ingest)
// becomes authoritative for worker and session queries.
type PluginProvider struct {
	store StateStore

	mu       sync.RWMutex
	workers  map[string]WorkerState
	prefixes map[string]PrefixState
	sessions map[string]SessionState
}

// NewPluginProvider returns a provider over store.
func NewPluginProvider(store StateStore) *PluginProvider {
	return &PluginProvider{
		store:    store,
		workers:  make(map[string]WorkerState),
		prefixes: make(map[string]PrefixState),
		sessions: make(map[string]SessionState),
	}
}

// Start begins consuming the store's watch streams. It returns once the
// watches are established; materialization happens in the background.
func (p *PluginProvider) Start(ctx context.Context) error {
	wch, err := p.store.Watch(ctx, WorkersPrefix)
	if err != nil {
		return err
	}
	sch, err := p.store.Watch(ctx, SessionsPrefix)
	if err != nil {
		return err
	}
	go p.consume(wch)
	go p.consume(sch)
	return nil
}

func (p *PluginProvider) consume(ch <-chan WatchedEntry) {
	for ev := range ch {
		switch {
		case strings.HasSuffix(ev.Key, "/state") && strings.HasPrefix(ev.Key, WorkersPrefix):
			id := between(ev.Key, WorkersPrefix, "/state")
			p.mu.Lock()
			if ev.Type == EventDelete {
				delete(p.workers, id)
			} else {
				var ws WorkerState
				if json.Unmarshal(ev.Value.Data, &ws) == nil {
					p.workers[id] = ws
				}
			}
			p.mu.Unlock()
		case strings.HasSuffix(ev.Key, "/prefix") && strings.HasPrefix(ev.Key, WorkersPrefix):
			id := between(ev.Key, WorkersPrefix, "/prefix")
			p.mu.Lock()
			if ev.Type == EventDelete {
				delete(p.prefixes, id)
			} else {
				var ps PrefixState
				if json.Unmarshal(ev.Value.Data, &ps) == nil {
					p.prefixes[id] = ps
				}
			}
			p.mu.Unlock()
		case strings.HasSuffix(ev.Key, "/state") && strings.HasPrefix(ev.Key, SessionsPrefix):
			sid := between(ev.Key, SessionsPrefix, "/state")
			p.mu.Lock()
			if ev.Type == EventDelete {
				delete(p.sessions, sid)
			} else {
				var ss SessionState
				if json.Unmarshal(ev.Value.Data, &ss) == nil {
					p.sessions[sid] = ss
				}
			}
			p.mu.Unlock()
		}
	}
}

// WorkerState implements StateProvider.
func (p *PluginProvider) WorkerState(id string) (WorkerState, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ws, ok := p.workers[id]
	return ws, ok
}

// SessionState implements StateProvider.
func (p *PluginProvider) SessionState(sessionID string) (SessionState, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ss, ok := p.sessions[sessionID]
	return ss, ok
}

// SessionCachedTokens implements StateProvider: real tokens for a session on a
// specific worker (only when that worker owns the session's cache).
func (p *PluginProvider) SessionCachedTokens(id, sessionID string) (int, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	ss, ok := p.sessions[sessionID]
	if !ok || ss.WorkerID != id {
		return 0, false
	}
	return ss.CachedTokens, true
}

// CachedTokens implements StateProvider. Exact prefix matching from vLLM block
// hashes needs the engine tokenizer, so this returns unknown and callers fall
// back to the router-side approximation.
func (*PluginProvider) CachedTokens(string, string) (int, bool) { return 0, false }

// Workers implements StateProvider.
func (p *PluginProvider) Workers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.workers))
	for id := range p.workers {
		out = append(out, id)
	}
	return out
}

// Close implements StateProvider.
func (*PluginProvider) Close() error { return nil }

func between(s, prefix, suffix string) string {
	s = strings.TrimPrefix(s, prefix)
	return strings.TrimSuffix(s, suffix)
}

// CompositeProvider prefers real plugin state and falls back to the router-side
// approximation per worker.
type CompositeProvider struct {
	plugin *PluginProvider
	approx *ApproximateProvider
}

// NewCompositeProvider combines a plugin provider (may be nil) and an
// approximation.
func NewCompositeProvider(plugin *PluginProvider, approx *ApproximateProvider) *CompositeProvider {
	return &CompositeProvider{plugin: plugin, approx: approx}
}

// WorkerState implements StateProvider.
func (c *CompositeProvider) WorkerState(id string) (WorkerState, bool) {
	if c.plugin != nil {
		return c.plugin.WorkerState(id)
	}
	return WorkerState{}, false
}

// CachedTokens implements StateProvider: prefix affinity stays approximate
// (real block-hash matching would need the tokenizer).
func (c *CompositeProvider) CachedTokens(id, prompt string) (int, bool) {
	return c.approx.CachedTokens(id, prompt)
}

// SessionCachedTokens implements StateProvider (real when the plugin reports).
func (c *CompositeProvider) SessionCachedTokens(id, sessionID string) (int, bool) {
	if c.plugin != nil {
		return c.plugin.SessionCachedTokens(id, sessionID)
	}
	return 0, false
}

// SessionState implements StateProvider.
func (c *CompositeProvider) SessionState(sessionID string) (SessionState, bool) {
	if c.plugin != nil {
		return c.plugin.SessionState(sessionID)
	}
	return SessionState{}, false
}

// Workers implements StateProvider.
func (c *CompositeProvider) Workers() []string { return c.approx.Workers() }

// Close implements StateProvider.
func (c *CompositeProvider) Close() error { return c.approx.Close() }

// Observe implements Observer, feeding the approximation.
func (c *CompositeProvider) Observe(workerID, text string) { c.approx.Observe(workerID, text) }

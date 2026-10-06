package state

import "sync"

// Observer is implemented by providers that learn from routed traffic.
type Observer interface {
	// Observe records that text was routed to worker id.
	Observe(workerID, text string)
}

// ApproximateProvider builds a bounded, per-worker prefix trie from the traffic
// the router itself routed. It needs no engine plugin and is the graceful
// fallback when no real plugin state exists.
//
// It approximates KV reuse in *characters* (to avoid tokenization); CachedTokens
// reports chars-derived tokens (chars/4).
type ApproximateProvider struct {
	mu       sync.RWMutex
	trees    map[string]*prefixTrie
	maxNodes int
}

// NewApproximateProvider creates the provider with a per-worker node cap.
func NewApproximateProvider(maxNodes int) *ApproximateProvider {
	if maxNodes <= 0 {
		maxNodes = 1 << 20
	}
	return &ApproximateProvider{trees: make(map[string]*prefixTrie), maxNodes: maxNodes}
}

// Observe implements Observer.
func (p *ApproximateProvider) Observe(workerID, text string) {
	if workerID == "" || text == "" {
		return
	}
	p.mu.Lock()
	t := p.trees[workerID]
	if t == nil {
		t = newPrefixTrie(p.maxNodes)
		p.trees[workerID] = t
	}
	t.insert(text)
	p.mu.Unlock()
}

// CachedTokens implements StateProvider.
func (p *ApproximateProvider) CachedTokens(workerID, prompt string) (int, bool) {
	if prompt == "" {
		return 0, false
	}
	p.mu.RLock()
	t := p.trees[workerID]
	n := 0
	if t != nil {
		n = t.prefixLen(prompt)
	}
	p.mu.RUnlock()
	return n / 4, true
}

// WorkerState implements StateProvider (not tracked by the approximation).
func (*ApproximateProvider) WorkerState(string) (WorkerState, bool) { return WorkerState{}, false }

// SessionCachedTokens implements StateProvider (not tracked by the approximation).
func (*ApproximateProvider) SessionCachedTokens(string, string) (int, bool) {
	return 0, false
}

// SessionState implements StateProvider (not tracked by the approximation).
func (*ApproximateProvider) SessionState(string) (SessionState, bool) {
	return SessionState{}, false
}

// Workers implements StateProvider.
func (p *ApproximateProvider) Workers() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, 0, len(p.trees))
	for id := range p.trees {
		out = append(out, id)
	}
	return out
}

// Close implements StateProvider.
func (*ApproximateProvider) Close() error { return nil }

// --- bounded byte trie ------------------------------------------------------

type trieNode struct {
	children map[byte]*trieNode
}

type prefixTrie struct {
	root     *trieNode
	nodes    int
	maxNodes int
}

func newPrefixTrie(maxNodes int) *prefixTrie {
	return &prefixTrie{root: &trieNode{}, maxNodes: maxNodes}
}

func (t *prefixTrie) insert(s string) {
	// Crude but bounded: reset when the node cap is exceeded.
	if t.nodes >= t.maxNodes {
		t.root = &trieNode{}
		t.nodes = 0
	}
	n := t.root
	for i := 0; i < len(s); i++ {
		c := s[i]
		if n.children == nil {
			n.children = make(map[byte]*trieNode)
		}
		child := n.children[c]
		if child == nil {
			child = &trieNode{}
			n.children[c] = child
			t.nodes++
		}
		n = child
	}
}

// prefixLen returns the length of the longest prefix of s that is also a
// prefix of some previously inserted string.
func (t *prefixTrie) prefixLen(s string) int {
	n := t.root
	i := 0
	for ; i < len(s); i++ {
		if n.children == nil {
			break
		}
		child := n.children[s[i]]
		if child == nil {
			break
		}
		n = child
	}
	return i
}

package prefix

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// CachingIndex is a read-through, bounded LRU (+ optional TTL) cache in front of
// another Index. Hot hashes — the shared prefixes that repeat across requests —
// are answered from local memory, so the common case never touches the backing
// store (e.g. Redis). Writes to the backing index pass through and invalidate
// the cached entry, keeping the view from going stale beyond TTL.
type CachingIndex struct {
	inner Index
	cap   int
	ttl   time.Duration
	now   func() time.Time

	mu sync.Mutex
	ll *list.List // of *cacheEntry, front = most recent
	m  map[string]*list.Element
}

type cacheEntry struct {
	hash    string
	workers []string
	exp     time.Time
}

// NewCachingIndex wraps inner. capacity is the max number of hashes cached
// (<=0 uses 4096); ttl<=0 caches for the process lifetime.
func NewCachingIndex(inner Index, capacity int, ttl time.Duration) *CachingIndex {
	if capacity <= 0 {
		capacity = 4096
	}
	return &CachingIndex{
		inner: inner,
		cap:   capacity,
		ttl:   ttl,
		now:   time.Now,
		ll:    list.New(),
		m:     make(map[string]*list.Element),
	}
}

// Add implements Index (writes through and invalidates the cache entry).
func (c *CachingIndex) Add(ctx context.Context, worker, hash string) error {
	err := c.inner.Add(ctx, worker, hash)
	c.invalidate(hash)
	return err
}

// Remove implements Index.
func (c *CachingIndex) Remove(ctx context.Context, worker, hash string) error {
	err := c.inner.Remove(ctx, worker, hash)
	c.invalidate(hash)
	return err
}

// Lookup implements Index.
func (c *CachingIndex) Lookup(ctx context.Context, hash string) ([]string, error) {
	if ws, ok := c.get(hash); ok {
		return ws, nil
	}
	ws, err := c.inner.Lookup(ctx, hash)
	if err != nil {
		return nil, err
	}
	c.put(hash, ws)
	return ws, nil
}

// Match implements Index: same contiguous-prefix scan, served through the cache.
func (c *CachingIndex) Match(ctx context.Context, hashes []string) (int, []string, error) {
	var workers []string
	matched := 0
	for _, h := range hashes {
		ws, err := c.Lookup(ctx, h)
		if err != nil {
			return matched, workers, err
		}
		if len(ws) == 0 {
			break
		}
		matched++
		workers = ws
	}
	if matched == 0 {
		return 0, nil, nil
	}
	return matched, workers, nil
}

// Close implements Index.
func (c *CachingIndex) Close() error { return c.inner.Close() }

func (c *CachingIndex) get(hash string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.m[hash]
	if !ok {
		return nil, false
	}
	e := el.Value.(*cacheEntry)
	if !e.exp.IsZero() && c.now().After(e.exp) {
		c.ll.Remove(el)
		delete(c.m, hash)
		return nil, false
	}
	c.ll.MoveToFront(el)
	return e.workers, true
}

func (c *CachingIndex) put(hash string, workers []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[hash]; ok {
		c.ll.MoveToFront(el)
		el.Value.(*cacheEntry).workers = workers
		return
	}
	e := &cacheEntry{hash: hash, workers: workers}
	if c.ttl > 0 {
		e.exp = c.now().Add(c.ttl)
	}
	c.m[hash] = c.ll.PushFront(e)
	for c.ll.Len() > c.cap {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.ll.Remove(back)
		delete(c.m, back.Value.(*cacheEntry).hash)
	}
}

func (c *CachingIndex) invalidate(hash string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[hash]; ok {
		c.ll.Remove(el)
		delete(c.m, hash)
	}
}

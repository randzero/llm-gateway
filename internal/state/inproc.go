package state

import (
	"context"
	"strings"
	"sync"
	"time"
)

type entry struct {
	val Value
	exp time.Time // zero => no expiry
}

type watchSub struct {
	prefix string
	ch     chan WatchedEntry
}

// InProcStore is a single-process StateStore: last-value KV + watch + TTL.
// Suitable for the single-instance mode; a cluster uses etcd/NATS instead.
type InProcStore struct {
	mu     sync.Mutex
	m      map[string]entry
	rev    int64
	subs   map[*watchSub]struct{}
	closed bool
}

// NewInProcStore returns an empty store.
func NewInProcStore() *InProcStore {
	return &InProcStore{m: make(map[string]entry), subs: make(map[*watchSub]struct{})}
}

// Get implements StateStore.
func (s *InProcStore) Get(_ context.Context, key string) (Value, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.m[key]
	if !ok {
		return Value{}, false, nil
	}
	if !e.exp.IsZero() && time.Now().After(e.exp) {
		return Value{}, false, nil
	}
	return e.val, true, nil
}

// Put implements StateStore.
func (s *InProcStore) Put(_ context.Context, key string, value []byte, ttl time.Duration) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.rev++
	e := entry{val: Value{Data: append([]byte(nil), value...), Rev: s.rev}}
	if ttl > 0 {
		e.exp = time.Now().Add(ttl)
	}
	s.m[key] = e
	subs := s.subsFor(key)
	s.mu.Unlock()

	for _, sub := range subs {
		select {
		case sub.ch <- WatchedEntry{Key: key, Value: e.val, Type: EventPut}:
		default: // drop for a slow watcher rather than block writers
		}
	}
	return nil
}

// Delete implements StateStore.
func (s *InProcStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	e, ok := s.m[key]
	delete(s.m, key)
	subs := s.subsFor(key)
	s.mu.Unlock()
	if ok {
		for _, sub := range subs {
			select {
			case sub.ch <- WatchedEntry{Key: key, Value: e.val, Type: EventDelete}:
			default:
			}
		}
	}
	return nil
}

// Watch implements StateStore. It replays current entries under prefix, then
// streams changes.
func (s *InProcStore) Watch(ctx context.Context, prefix string) (<-chan WatchedEntry, error) {
	ch := make(chan WatchedEntry, 256)
	sub := &watchSub{prefix: prefix, ch: ch}

	s.mu.Lock()
	for k, e := range s.m { // replay under the lock so no live event interleaves
		if strings.HasPrefix(k, prefix) && (e.exp.IsZero() || time.Now().Before(e.exp)) {
			select {
			case ch <- WatchedEntry{Key: k, Value: e.val, Type: EventPut}:
			default:
			}
		}
	}
	s.subs[sub] = struct{}{}
	s.mu.Unlock()

	go func() {
		<-ctx.Done()
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

// Close implements StateStore.
func (s *InProcStore) Close() error {
	s.mu.Lock()
	s.closed = true
	for sub := range s.subs {
		close(sub.ch)
	}
	s.subs = make(map[*watchSub]struct{})
	s.mu.Unlock()
	return nil
}

func (s *InProcStore) subsFor(key string) []*watchSub {
	var out []*watchSub
	for sub := range s.subs {
		if strings.HasPrefix(key, sub.prefix) {
			out = append(out, sub)
		}
	}
	return out
}

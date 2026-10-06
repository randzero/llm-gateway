package state

import (
	"context"
	"time"
)

// EventType enumerates watch notifications.
type EventType int

const (
	EventPut EventType = iota
	EventDelete
	EventExpired
)

// Value is a stored entry with its revision.
type Value struct {
	Data []byte
	Rev  int64
}

// WatchedEntry is one change delivered by a Watch.
type WatchedEntry struct {
	Key   string
	Value Value
	Type  EventType
}

// StateStore is the shared state layer: **last-value KV + watch + TTL lease**.
//
// It is deliberately *not* a pub/sub fire-and-forget bus: readers need the
// current value (for cold start / restarts) as well as subsequent changes.
//
// Implementations:
//   - inproc  — single-instance, in-process map + condition variable
//   - etcd    — Get/Watch/Lease, k8s-native (recommended for clusters)
//   - natskv  — NATS JetStream KV
//
// Business code (plugin and router) depends only on this interface.
type StateStore interface {
	// Get returns the current value at key.
	Get(ctx context.Context, key string) (Value, bool, error)
	// Put writes value at key with a TTL (lease). TTL <= 0 means no expiry.
	Put(ctx context.Context, key string, value []byte, ttl time.Duration) error
	// Delete removes key.
	Delete(ctx context.Context, key string) error
	// Watch streams changes under prefix until ctx is cancelled or the channel
	// is closed. It should first replay current entries, then deliver changes.
	Watch(ctx context.Context, prefix string) (<-chan WatchedEntry, error)
	// Close releases resources.
	Close() error
}

// Key layout. One key per worker/engine and per session keeps the store's size
// bounded by the number of workers/sessions, not by event volume.
const (
	WorkersPrefix  = "workers/"
	SessionsPrefix = "sessions/"
)

// WorkerStateKey is the key holding a WorkerState.
func WorkerStateKey(id string) string { return WorkersPrefix + id + "/state" }

// PrefixStateKey is the key holding a PrefixState (top-N prefixes).
func PrefixStateKey(id string) string { return WorkersPrefix + id + "/prefix" }

// SessionStateKey is the key holding a SessionState.
func SessionStateKey(sessionID string) string {
	return SessionsPrefix + sessionID + "/state"
}

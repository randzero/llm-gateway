package state

// StateProvider is the typed, read-side view the router depends on. Policies
// never touch a StateStore directly — they read through a StateProvider.
//
// Two implementations, chosen per worker with graceful degradation:
//
//   - PluginProvider      — maintains a local materialized view from a
//     StateStore's Watch stream; authoritative when the engine plugin reports.
//   - ApproximateProvider — builds a bounded radix tree from the traffic the
//     router itself routed; used when no plugin state exists (or per worker,
//     when that worker is not reporting).
//
// Reads must be cheap and in-memory (no network on the hot path); the Watch
// loop keeps the view fresh.
type StateProvider interface {
	// WorkerState returns the latest snapshot for a worker, if known.
	WorkerState(id string) (WorkerState, bool)

	// CachedTokens estimates how many leading tokens of prompt are cached on
	// worker id. ok=false means unknown (caller should fall back).
	CachedTokens(id string, prompt string) (int, bool)

	// SessionCachedTokens returns the real cached-token count for a session on
	// a worker, when the engine plugin reports it. ok=false means unknown.
	SessionCachedTokens(id string, sessionID string) (int, bool)

	// SessionState returns per-session cache state, if tracked.
	SessionState(sessionID string) (SessionState, bool)

	// Workers lists the currently known worker ids.
	Workers() []string

	// Close releases resources (watch streams, etc.).
	Close() error
}

// Package state defines the versioned wire schema shared by the engine plugin
// (which writes) and the router (which reads). Field layout mirrors what a
// stock vLLM exposes via SchedulerStats/IterationStats and KV events.
package state

// SchemaVersion is bumped on any breaking change to the structs below.
const SchemaVersion = 1

// Load is the per-worker request load.
type Load struct {
	Running     int `json:"running"`
	Waiting     int `json:"waiting"`
	Preemptions int `json:"preemptions,omitempty"`
}

// Prefill carries prefill-side capacity signals.
type Prefill struct {
	Throughput      int `json:"throughput,omitempty"`
	TokensToPrefill int `json:"tokens_to_prefill,omitempty"`
	// WaitingTokens is the engine's real prefill backlog (waiting requests' full
	// prompt tokens + running requests' un-computed remainder). -1 = unknown
	// (the engine plugin could not read it).
	WaitingTokens int `json:"waiting_tokens,omitempty"`
}

// PrefixCache is the cumulative prefix-cache accounting for a worker.
type PrefixCache struct {
	QueryTokens int64 `json:"query_tokens,omitempty"`
	HitTokens   int64 `json:"hit_tokens,omitempty"`
}

// Loras lists adapters currently resident / queued on a worker.
type Loras struct {
	Running []string `json:"running,omitempty"`
	Waiting []string `json:"waiting,omitempty"`
}

// WorkerState is a low-frequency snapshot of a single worker/engine.
type WorkerState struct {
	Version     int         `json:"v"`
	WorkerID    string      `json:"worker_id"`
	Endpoint    string      `json:"endpoint"`
	Model       string      `json:"model"`
	DPRank      int         `json:"dp_rank"`
	EngineIndex int         `json:"engine_index"`
	PDRole      string      `json:"pd_role,omitempty"`
	Load        Load        `json:"load"`
	KVUsage     float64     `json:"kv_usage"`
	Prefill     Prefill     `json:"prefill"`
	PrefixCache PrefixCache `json:"prefix_cache"`
	Loras       Loras       `json:"loras"`
	TS          float64     `json:"ts"`
}

// PrefixEntry is one hot cached prefix: a block/prefix hash and its token size.
type PrefixEntry struct {
	Hash   string `json:"hash"`
	Tokens int    `json:"tokens"`
}

// PrefixState is the bounded (top-N) set of hot prefixes on a worker, newest
// first. Bounded by construction, so its size is independent of event volume.
type PrefixState struct {
	Version        int           `json:"v"`
	WorkerID       string        `json:"worker_id"`
	Prefixes       []PrefixEntry `json:"prefixes"`
	HitTokensTotal int64         `json:"hit_tokens_total,omitempty"`
	TS             float64       `json:"ts"`
}

// SessionState is optional per-session cache accounting.
type SessionState struct {
	Version      int     `json:"v"`
	WorkerID     string  `json:"worker_id"`
	SessionID    string  `json:"session_id"`
	CachedTokens int     `json:"cached_tokens"`
	TS           float64 `json:"ts"`
}

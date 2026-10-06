// Package api holds HTTP endpoints that are not the proxy itself.
package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/randzero/llm-gateway/internal/state"
)

// StateIngestPayload is what the engine plugin POSTs to the router.
type StateIngestPayload struct {
	WorkerID    string             `json:"worker_id"`
	WorkerState *state.WorkerState `json:"worker_state,omitempty"`
	Sessions    []SessionEntry     `json:"sessions,omitempty"`
}

// SessionEntry is one session's cache accounting.
type SessionEntry struct {
	SessionID    string `json:"session_id"`
	CachedTokens int    `json:"cached_tokens"`
}

// NewStateIngest returns a handler that writes plugin-reported state into store
// with the given TTL (so a dead plugin's state expires).
func NewStateIngest(store state.StateStore, ttl time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var p StateIngestPayload
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&p); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		if p.WorkerID == "" {
			http.Error(w, "worker_id is required", http.StatusBadRequest)
			return
		}
		ctx := r.Context()
		if p.WorkerState != nil {
			if b, err := json.Marshal(p.WorkerState); err == nil {
				_ = store.Put(ctx, state.WorkerStateKey(p.WorkerID), b, ttl)
			}
		}
		for _, s := range p.Sessions {
			if s.SessionID == "" {
				continue
			}
			ss := state.SessionState{
				Version:      state.SchemaVersion,
				WorkerID:     p.WorkerID,
				SessionID:    s.SessionID,
				CachedTokens: s.CachedTokens,
				TS:           float64(time.Now().UnixNano()) / 1e9,
			}
			if b, err := json.Marshal(&ss); err == nil {
				_ = store.Put(ctx, state.SessionStateKey(s.SessionID), b, ttl)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

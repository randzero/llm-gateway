// Package stateserver is the shared state service. Routers and engine plugins
// talk to it — not to Redis directly — and it fronts Redis. It serves both the
// state plane (worker/session scalars) and the prefix index (block hash ->
// workers), doing the longest-prefix merge server-side so routers stay thin.
//
// Writes come from engine plugins (POST /state, POST /kv); reads come from
// routers (POST /match — the longest prefix, plus the session's cached tokens).
package stateserver

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/randzero/llm-gateway/internal/prefix"
	"github.com/randzero/llm-gateway/internal/state"
)

// Server fronts Redis for the state plane and the prefix index.
type Server struct {
	rdb       redis.UniversalClient
	idx       prefix.Index
	keyPrefix string
	blockSize int
	ttl       time.Duration
}

// New builds a Server. keyPrefix namespaces the state-plane keys; blockSize
// converts matched blocks to tokens; ttl expires worker/session state so dead
// workers fall out.
func New(rdb redis.UniversalClient, idx prefix.Index, keyPrefix string, blockSize int, ttl time.Duration) *Server {
	if keyPrefix == "" {
		keyPrefix = "lg:"
	}
	if blockSize <= 0 {
		blockSize = 16
	}
	return &Server{rdb: rdb, idx: idx, keyPrefix: keyPrefix, blockSize: blockSize, ttl: ttl}
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/state", s.handleState)
	mux.HandleFunc("/kv", s.handleKV)
	mux.HandleFunc("/match", s.handleMatch)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return mux
}

func (s *Server) wKey(id string) string { return s.keyPrefix + "w:" + id }
func (s *Server) sKey(id string) string { return s.keyPrefix + "s:" + id }

// statePayload mirrors the plugin's POST /state body.
type statePayload struct {
	WorkerID    string            `json:"worker_id"`
	WorkerState state.WorkerState `json:"worker_state"`
	Sessions    []struct {
		SessionID    string `json:"session_id"`
		CachedTokens int    `json:"cached_tokens"`
	} `json:"sessions"`
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var p statePayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	id := p.WorkerID
	if id == "" {
		id = p.WorkerState.WorkerID
	}
	if id == "" {
		http.Error(w, "worker_id required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	ws := p.WorkerState
	ws.WorkerID = id
	ws.Version = state.SchemaVersion
	ws.TS = now()
	if err := s.set(ctx, s.wKey(id), ws); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for _, sess := range p.Sessions {
		if sess.SessionID == "" {
			continue
		}
		ss := state.SessionState{
			Version: state.SchemaVersion, WorkerID: id,
			SessionID: sess.SessionID, CachedTokens: sess.CachedTokens, TS: ws.TS,
		}
		_ = s.set(ctx, s.sKey(sess.SessionID), ss)
	}
	w.WriteHeader(http.StatusNoContent)
}

// kvPayload mirrors the plugin's POST /kv body.
type kvPayload struct {
	WorkerID string   `json:"worker_id"`
	Add      []string `json:"add"`
	Remove   []string `json:"remove"`
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var p kvPayload
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if p.WorkerID == "" {
		http.Error(w, "worker_id required", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	for _, h := range p.Add {
		if err := s.idx.Add(ctx, p.WorkerID, h); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	for _, h := range p.Remove {
		if err := s.idx.Remove(ctx, p.WorkerID, h); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

// matchRequest asks for the longest cached prefix (and, if session_id is set,
// that session's cached tokens), plus the reported load of the given workers.
type matchRequest struct {
	Hashes    []string `json:"hashes"`
	SessionID string   `json:"session_id"`
	Workers   []string `json:"workers"`
}

func (s *Server) handleMatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req matchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	matched, workers, err := s.idx.Match(ctx, req.Hashes)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if workers == nil {
		workers = []string{}
	}
	resp := map[string]any{
		"matched": matched,
		"workers": workers,
		"tokens":  matched * s.blockSize,
	}
	if req.SessionID != "" {
		if ss, ok := s.session(ctx, req.SessionID); ok {
			resp["session_worker"] = ss.WorkerID
			resp["session_tokens"] = ss.CachedTokens
		}
	}
	if loads := s.loads(ctx, req.Workers); len(loads) > 0 {
		resp["loads"] = loads
	}
	writeJSON(w, resp)
}

// loads reads the reported state for each worker id (one pipeline).
func (s *Server) loads(ctx context.Context, ids []string) map[string]workerLoad {
	if len(ids) == 0 {
		return nil
	}
	pipe := s.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.Get(ctx, s.wKey(id))
	}
	_, _ = pipe.Exec(ctx) // missing keys surface per-command
	out := make(map[string]workerLoad, len(ids))
	for i, id := range ids {
		b, err := cmds[i].Bytes()
		if err != nil {
			continue
		}
		var ws state.WorkerState
		if json.Unmarshal(b, &ws) != nil {
			continue
		}
		out[id] = workerLoad{
			Running:       ws.Load.Running,
			Waiting:       ws.Load.Waiting,
			Throughput:    ws.Prefill.Throughput,
			WaitingTokens: ws.Prefill.WaitingTokens,
		}
	}
	return out
}

// workerLoad is the per-worker slice of state the router's policy needs.
type workerLoad struct {
	Running       int `json:"running"`
	Waiting       int `json:"waiting"`
	Throughput    int `json:"throughput,omitempty"`
	WaitingTokens int `json:"waiting_tokens,omitempty"`
}

// session reads a session's state (worker + cached tokens) from Redis.
func (s *Server) session(ctx context.Context, sid string) (state.SessionState, bool) {
	b, err := s.rdb.Get(ctx, s.sKey(sid)).Bytes()
	if err != nil {
		return state.SessionState{}, false
	}
	var ss state.SessionState
	if json.Unmarshal(b, &ss) != nil {
		return state.SessionState{}, false
	}
	return ss, true
}

// -- helpers --

func (s *Server) set(ctx context.Context, key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, key, b, s.ttl).Err()
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func now() float64 { return float64(time.Now().UnixNano()) / 1e9 }

package api

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/randzero/llm-gateway/internal/prefix"
)

// KVPayload is one worker's batch of block add/remove events, keyed by block
// hash. The engine reports blocks it now holds (add) or has evicted (remove).
type KVPayload struct {
	WorkerID string   `json:"worker_id"`
	Add      []string `json:"add"`
	Remove   []string `json:"remove"`
}

// NewKVIngest applies block add/remove events to the prefix index, keeping the
// inverted index (block hash -> workers) in sync with engine KV state.
func NewKVIngest(index prefix.Index) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var p KVPayload
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
			if err := index.Add(ctx, p.WorkerID, h); err != nil {
				log.Printf("kv ingest: add %s/%s: %v", p.WorkerID, h, err)
			}
		}
		for _, h := range p.Remove {
			if err := index.Remove(ctx, p.WorkerID, h); err != nil {
				log.Printf("kv ingest: remove %s/%s: %v", p.WorkerID, h, err)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

package stateclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

type fixedHasher []string

func (f fixedHasher) Hashes(context.Context, *http.Request, []byte) ([]string, error) {
	return []string(f), nil
}

func TestClientMatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/match" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req struct {
			Hashes    []string `json:"hashes"`
			SessionID string   `json:"session_id"`
			Workers   []string `json:"workers"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.SessionID != "s1" {
			t.Errorf("session_id = %q, want s1", req.SessionID)
		}
		if len(req.Workers) != 2 {
			t.Errorf("workers = %v, want the two candidates", req.Workers)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"matched": len(req.Hashes), "workers": []string{"w1"}, "tokens": len(req.Hashes) * 16,
			"session_worker": "w1", "session_tokens": 99,
			"loads": map[string]any{"w1": map[string]int{"running": 3, "waiting": 1}},
		})
	}))
	defer srv.Close()

	c := New(srv.URL, fixedHasher{"h0", "h1"}, srv.Client())
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Session-ID", "s1")

	info, ok := c.Match(context.Background(), r, nil, []string{"w1", "w2"})
	if !ok || info.Tokens != 32 || len(info.Workers) != 1 || info.Workers[0] != "w1" ||
		info.SessionWorker != "w1" || info.SessionTokens != 99 {
		t.Fatalf("info = %+v ok=%v", info, ok)
	}
	if l := info.Loads["w1"]; l.Running != 3 || l.Waiting != 1 {
		t.Fatalf("loads[w1] = %+v, want 3/1", l)
	}
}

func TestClientNoHasherFallsBack(t *testing.T) {
	c := New("http://unused", nil, nil)
	_, ok := c.Match(context.Background(), httptest.NewRequest(http.MethodPost, "/", nil), nil, nil)
	if ok {
		t.Fatal("ok should be false without a hasher")
	}
}

package stateserver

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/randzero/llm-gateway/internal/prefix"
)

func newTest(t *testing.T) *httptest.Server {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	idx := prefix.NewRedisIndex(rdb, time.Minute)
	ts := httptest.NewServer(New(rdb, idx, "lg:", 16, time.Minute).Handler())
	t.Cleanup(ts.Close)
	return ts
}

func post(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	resp, err := http.Post(url, "application/json", bytes.NewReader([]byte(body)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func TestKVThenMatch(t *testing.T) {
	ts := newTest(t)
	if code, _ := post(t, ts.URL+"/kv", `{"worker_id":"w1","add":["h0","h1"]}`); code != 204 {
		t.Fatalf("kv add status %d", code)
	}
	code, b := post(t, ts.URL+"/match", `{"hashes":["h0","h1","h2"]}`)
	if code != 200 {
		t.Fatalf("match status %d: %s", code, b)
	}
	var m struct {
		Matched int      `json:"matched"`
		Workers []string `json:"workers"`
		Tokens  int      `json:"tokens"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if m.Matched != 2 || m.Tokens != 32 || len(m.Workers) != 1 || m.Workers[0] != "w1" {
		t.Fatalf("match = %+v", m)
	}
	// Remove moves the boundary.
	post(t, ts.URL+"/kv", `{"worker_id":"w1","remove":["h1"]}`)
	_, b = post(t, ts.URL+"/match", `{"hashes":["h0","h1"]}`)
	_ = json.Unmarshal(b, &m)
	if m.Matched != 1 {
		t.Fatalf("after remove matched = %d, want 1", m.Matched)
	}
}

func TestMatchWithSession(t *testing.T) {
	ts := newTest(t)
	// A worker's /state report registers a session.
	body := `{"worker_id":"w1","worker_state":{"v":1,"worker_id":"w1","load":{"running":3}},` +
		`"sessions":[{"session_id":"s1","cached_tokens":99}]}`
	if code, _ := post(t, ts.URL+"/state", body); code != 204 {
		t.Fatalf("state status %d", code)
	}
	// /match returns the prefix result plus the session's worker/tokens and the
	// candidates' reported load.
	code, b := post(t, ts.URL+"/match", `{"hashes":["h0","h1"],"session_id":"s1","workers":["w1"]}`)
	if code != 200 {
		t.Fatalf("match status %d: %s", code, b)
	}
	var m struct {
		Matched       int    `json:"matched"`
		SessionWorker string `json:"session_worker"`
		SessionTokens int    `json:"session_tokens"`
		Loads         map[string]struct {
			Running int `json:"running"`
			Waiting int `json:"waiting"`
		} `json:"loads"`
	}
	_ = json.Unmarshal(b, &m)
	if m.SessionWorker != "w1" || m.SessionTokens != 99 {
		t.Fatalf("session = %q/%d, want w1/99 (body %s)", m.SessionWorker, m.SessionTokens, b)
	}
	if m.Loads["w1"].Running != 3 {
		t.Fatalf("loads = %+v, want w1.running=3 (body %s)", m.Loads, b)
	}
	// A session-less, worker-less match just omits those fields.
	_, b = post(t, ts.URL+"/match", `{"hashes":["h0"]}`)
	if bytes.Contains(b, []byte("session_worker")) || bytes.Contains(b, []byte("loads")) {
		t.Fatalf("unexpected extra fields: %s", b)
	}
}

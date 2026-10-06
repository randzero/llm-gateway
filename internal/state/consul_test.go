package state

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeConsul is a minimal in-memory Consul KV + session server: enough of the
// HTTP API to exercise the store, including blocking queries and X-Consul-Index.
type fakeConsul struct {
	mu      sync.Mutex
	kv      map[string]string // key -> base64 value
	index   uint64
	changed chan struct{}
}

func newFakeConsul() *fakeConsul {
	return &fakeConsul{kv: map[string]string{}, index: 4, changed: make(chan struct{})}
}

func (f *fakeConsul) touch() {
	f.index++
	close(f.changed)
	f.changed = make(chan struct{})
}

func (f *fakeConsul) waitChange(ctx context.Context, from uint64, d time.Duration) {
	deadline := time.Now().Add(d)
	for {
		f.mu.Lock()
		cur, ch := f.index, f.changed
		f.mu.Unlock()
		if cur > from {
			return
		}
		rem := time.Until(deadline)
		if rem <= 0 {
			return
		}
		select {
		case <-ch:
		case <-time.After(rem):
			return
		case <-ctx.Done():
			return
		}
	}
}

func (f *fakeConsul) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPut && r.URL.Path == "/v1/session/create":
		_ = json.NewEncoder(w).Encode(map[string]string{"ID": "sess-1"})
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/session/renew/"):
		_ = json.NewEncoder(w).Encode(map[string]string{"ID": strings.TrimPrefix(r.URL.Path, "/v1/session/renew/")})
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/kv/"):
		f.handleGet(w, r)
	case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/kv/"):
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.kv[strings.TrimPrefix(r.URL.Path, "/v1/kv/")] = base64.StdEncoding.EncodeToString(body)
		f.touch()
		f.mu.Unlock()
		_, _ = w.Write([]byte("true"))
	case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/v1/kv/"):
		f.mu.Lock()
		delete(f.kv, strings.TrimPrefix(r.URL.Path, "/v1/kv/"))
		f.touch()
		f.mu.Unlock()
		_, _ = w.Write([]byte("true"))
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (f *fakeConsul) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
	q := r.URL.Query()
	if idx, _ := strconv.ParseUint(q.Get("index"), 10, 64); idx > 0 {
		if d, err := time.ParseDuration(q.Get("wait")); err == nil {
			f.waitChange(r.Context(), idx, d)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	w.Header().Set("X-Consul-Index", strconv.FormatUint(f.index, 10))

	recurse := q.Get("recurse") == "true"
	var keys []string
	for k := range f.kv {
		if recurse {
			if strings.HasPrefix(k, key) {
				keys = append(keys, k)
			}
		} else if k == key {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	sort.Strings(keys)
	type entry struct {
		Key   string  `json:"Key"`
		Value *string `json:"Value"`
	}
	out := make([]entry, 0, len(keys))
	for _, k := range keys {
		v := f.kv[k]
		out = append(out, entry{Key: k, Value: &v})
	}
	_ = json.NewEncoder(w).Encode(out)
}

func TestConsulStorePutGetWatch(t *testing.T) {
	srv := httptest.NewServer(newFakeConsul())
	defer srv.Close()

	s := NewConsulStore(srv.URL, "gw/")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := s.Watch(ctx, WorkersPrefix)
	if err != nil {
		t.Fatal(err)
	}

	val := []byte(`{"v":1,"worker_id":"w1"}`)
	if err := s.Put(ctx, WorkerStateKey("w1"), val, time.Minute); err != nil {
		t.Fatal(err)
	}

	select {
	case ev := <-ch:
		if ev.Key != WorkerStateKey("w1") || ev.Type != EventPut {
			t.Fatalf("watch event = %+v", ev)
		}
		if string(ev.Value.Data) != string(val) {
			t.Fatalf("watch value = %q", ev.Value.Data)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no watch event")
	}

	got, ok, err := s.Get(ctx, WorkerStateKey("w1"))
	if err != nil || !ok {
		t.Fatalf("Get ok=%v err=%v", ok, err)
	}
	if string(got.Data) != string(val) {
		t.Fatalf("Get value = %q", got.Data)
	}
}

func TestConsulStoreDelete(t *testing.T) {
	srv := httptest.NewServer(newFakeConsul())
	defer srv.Close()

	s := NewConsulStore(srv.URL, "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Put(ctx, WorkerStateKey("w2"), []byte("x"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, WorkerStateKey("w2")); !ok {
		t.Fatal("expected value after Put")
	}
	if err := s.Delete(ctx, WorkerStateKey("w2")); err != nil {
		t.Fatal(err)
	}
	if _, ok, _ := s.Get(ctx, WorkerStateKey("w2")); ok {
		t.Fatal("expected gone after Delete")
	}
}

func TestConsulStoreSessionOnTTL(t *testing.T) {
	fake := newFakeConsul()
	srv := httptest.NewServer(fake)
	defer srv.Close()

	s := NewConsulStore(srv.URL, "")
	ctx := context.Background()

	// A ttl Put must acquire a session; the second reuses (renews) it.
	if err := s.Put(ctx, "k1", []byte("a"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "k2", []byte("b"), time.Minute); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	sess := s.session
	s.mu.Unlock()
	if sess != "sess-1" {
		t.Fatalf("expected session sess-1, got %q", sess)
	}
	// No ttl -> plain write, no session required.
	if err := s.Put(ctx, "k3", []byte("c"), 0); err != nil {
		t.Fatal(err)
	}
}

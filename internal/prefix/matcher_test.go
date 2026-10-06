package prefix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"
)

func hashesHeader(v string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	r.Header.Set("X-KV-Hashes", v)
	return r
}

func TestPrefixMatcherExact(t *testing.T) {
	ctx := context.Background()
	idx := NewInProcIndex()
	for _, h := range []string{"h0", "h1", "h2"} {
		_ = idx.Add(ctx, "w1", h)
	}
	m := NewPrefixMatcher(HeaderHasher{}, idx, 16, false)

	info, ok := m.Match(ctx, hashesHeader("h0,h1,h2,h9"), nil, nil)
	if !ok {
		t.Fatal("ok = false, want true")
	}
	if info.Tokens != 3*16 {
		t.Fatalf("tokens = %d, want %d", info.Tokens, 3*16)
	}
	if !reflect.DeepEqual(info.Workers, []string{"w1"}) {
		t.Fatalf("workers = %v, want [w1]", info.Workers)
	}
}

func TestPrefixMatcherAuthoritativeMiss(t *testing.T) {
	ctx := context.Background()
	m := NewPrefixMatcher(HeaderHasher{}, NewInProcIndex(), 16, false)
	info, ok := m.Match(ctx, hashesHeader("z0,z1"), nil, nil)
	if !ok || info.Tokens != 0 || info.Workers != nil {
		t.Fatalf("got %+v ok=%v, want zero,true", info, ok)
	}
}

func TestPrefixMatcherNoHashesFallsBack(t *testing.T) {
	ctx := context.Background()
	m := NewPrefixMatcher(HeaderHasher{}, NewInProcIndex(), 16, false)
	if _, ok := m.Match(ctx, hashesHeader(""), nil, nil); ok {
		t.Fatal("ok = true with no hashes, want false (fall back)")
	}
}

// Sampling trades resolution for fewer lookups: with 20 blocks cached 0..7, the
// sampled query only resolves down to the sampled block index 4.
func TestPrefixMatcherSampled(t *testing.T) {
	ctx := context.Background()
	idx := NewInProcIndex()
	hashes := make([]string, 20)
	for i := range hashes {
		hashes[i] = "b" + strconv.Itoa(i)
	}
	for i := 0; i < 8; i++ {
		_ = idx.Add(ctx, "w1", hashes[i])
	}
	m := NewPrefixMatcher(headerHasherFunc(func(*http.Request) []string { return hashes }), idx, 16, true)

	info, ok := m.Match(ctx, hashesHeader("x"), nil, nil)
	if !ok || len(info.Workers) != 1 || info.Workers[0] != "w1" {
		t.Fatalf("ok=%v workers=%v", ok, info.Workers)
	}
	// Sampled indices {0,1,2,4,8,16}: 0..4 present, 8 absent -> deepest block 4.
	if want := 5 * 16; info.Tokens != want {
		t.Fatalf("tokens = %d, want %d (sampled underestimate)", info.Tokens, want)
	}
}

type headerHasherFunc func(*http.Request) []string

func (f headerHasherFunc) Hashes(_ context.Context, r *http.Request, _ []byte) ([]string, error) {
	return f(r), nil
}

func TestHTTPHasher(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"hashes":["a","b"]}`))
	}))
	defer srv.Close()

	h := &HTTPHasher{URL: srv.URL, Client: srv.Client()}
	got, err := h.Hashes(context.Background(), hashesHeader(""), []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("got %v", got)
	}
}

package proxy

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/randzero/llm-gateway/internal/control"
	"github.com/randzero/llm-gateway/internal/observability"
	"github.com/randzero/llm-gateway/internal/policies"
)

func gateway(workers []*control.Worker, retries int) *Gateway {
	return New(control.NewRegistry(workers),
		policies.NewRoundRobin(), &http.Client{}, observability.New(), retries)
}

func TestForwardsRequest(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream", "yes")
		_, _ = w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	wk := control.NewWorker(upstream.URL, "", 0, "", 5, time.Second)
	g := gateway([]*control.Worker{wk}, 0)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m"}`))
	g.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if body := rec.Body.String(); body != "hello" {
		t.Fatalf("body = %q", body)
	}
	if rec.Header().Get("X-Upstream") != "yes" {
		t.Fatal("upstream header not copied")
	}
}

func TestRetriesOnDeadWorker(t *testing.T) {
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer good.Close()

	dead := control.NewWorker("http://127.0.0.1:1", "", 0, "", 5, time.Second) // nothing listening
	live := control.NewWorker(good.URL, "", 0, "", 5, time.Second)
	g := gateway([]*control.Worker{dead, live}, 1)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`))
	g.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK || rec.Body.String() != "ok" {
		t.Fatalf("expected failover to live worker, got %d %q", rec.Code, rec.Body.String())
	}
}

func TestNoWorkers(t *testing.T) {
	g := New(control.NewRegistry(nil), policies.NewRoundRobin(), &http.Client{}, observability.New(), 0)
	rec := httptest.NewRecorder()
	g.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

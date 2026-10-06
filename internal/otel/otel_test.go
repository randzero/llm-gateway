package otel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExportAndPropagation(t *testing.T) {
	type captured struct {
		body  map[string]any
		ctHdr string
	}
	got := make(chan captured, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		got <- captured{body: body, ctHdr: r.Header.Get("Content-Type")}
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(srv.URL, "llm-gateway", srv.Client())
	incoming := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	s := tr.Start("POST /v1/chat/completions", incoming)
	s.SetAttr("llm.model", "qwen")
	s.SetAttr("llm.worker", "w1")
	s.End(context.Background())

	// Downstream header carries our trace id and our span id as parent.
	if tp := s.Traceparent(); tp[:3] != "00-" || len(tp) != 55 {
		t.Fatalf("bad traceparent %q", tp)
	}

	c := <-got
	if c.ctHdr != "application/json" {
		t.Fatalf("content-type %q", c.ctHdr)
	}
	rs := c.body["resourceSpans"].([]any)[0].(map[string]any)
	sp := rs["scopeSpans"].([]any)[0].(map[string]any)["spans"].([]any)[0].(map[string]any)
	if sp["traceId"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("traceId = %v, want the incoming one", sp["traceId"])
	}
	if sp["parentSpanId"] != "00f067aa0ba902b7" {
		t.Fatalf("parentSpanId = %v", sp["parentSpanId"])
	}
	if sp["status"].(map[string]any)["code"].(float64) != 1 {
		t.Fatalf("status = %v", sp["status"])
	}
}

func TestDisabledWithoutEndpoint(t *testing.T) {
	tr := New("", "svc", nil)
	s := tr.Start("x", "")
	s.End(context.Background()) // must not panic or block
}

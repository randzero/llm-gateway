package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestConsulDiscovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/health/service/qwen") {
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{
					"Node":    map[string]any{"Address": "10.0.0.1"},
					"Service": map[string]any{"ID": "qwen-1", "Service": "qwen", "Address": "10.0.0.1", "Port": 8000},
				},
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	d := NewConsul(srv.URL, []string{"qwen"}, "http", time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := d.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case eps := <-ch:
		if len(eps) != 1 || eps[0].URL != "http://10.0.0.1:8000" || eps[0].Model != "qwen" {
			t.Fatalf("unexpected endpoints: %+v", eps)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot within timeout")
	}
}

func TestStaticDiscovery(t *testing.T) {
	d := NewStatic([]Endpoint{{URL: "http://a", Model: "m"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := d.Start(ctx)
	eps := <-ch
	if len(eps) != 1 || eps[0].URL != "http://a" {
		t.Fatalf("unexpected: %+v", eps)
	}
}

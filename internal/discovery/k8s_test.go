package discovery

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestK8sDiscovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/apis/discovery.k8s.io/v1/namespaces/ns1/endpointslices" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("Authorization") != "Bearer tok" {
			t.Errorf("missing bearer token: %q", r.Header.Get("Authorization"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []any{
				map[string]any{
					"metadata": map[string]any{"labels": map[string]string{"kubernetes.io/service-name": "qwen"}},
					"endpoints": []any{
						map[string]any{"addresses": []string{"10.0.0.1"}, "conditions": map[string]bool{"ready": true}},
						map[string]any{"addresses": []string{"10.0.0.9"}, "conditions": map[string]bool{"ready": false}},
					},
					"ports": []any{map[string]any{"name": "http", "port": 8000}},
				},
			},
		})
	}))
	defer srv.Close()

	k := NewK8s(K8sConfig{
		APIURL: srv.URL, Token: "tok", Namespace: "ns1",
		PortName: "http", Scheme: "http", Interval: time.Hour, Client: srv.Client(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := k.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case eps := <-ch:
		if len(eps) != 1 || eps[0].URL != "http://10.0.0.1:8000" || eps[0].Model != "qwen" {
			t.Fatalf("unexpected endpoints: %+v", eps)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot")
	}
}

package discovery

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestDNSDiscovery(t *testing.T) {
	d := NewDNS([]string{"qwen"}, "svc.local", "http", time.Hour)
	d.lookupSRV = func(service, proto, name string) (string, []*net.SRV, error) {
		if service != "qwen" || name != "svc.local" {
			t.Fatalf("unexpected lookup %s/%s/%s", service, proto, name)
		}
		return "", []*net.SRV{
			{Target: "10.0.0.1.", Port: 8000},
			{Target: "", Port: 8000},       // dropped (no target)
			{Target: "10.0.0.2.", Port: 0}, // dropped (no port)
		}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := d.Start(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case eps := <-ch:
		if len(eps) != 1 {
			t.Fatalf("got %d endpoints, want 1: %+v", len(eps), eps)
		}
		if eps[0].URL != "http://10.0.0.1:8000" || eps[0].Model != "qwen" {
			t.Fatalf("unexpected endpoint %+v", eps[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot")
	}
}

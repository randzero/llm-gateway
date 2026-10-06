package discovery

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"
)

// DNS discovers workers from SRV records: each configured service name is a
// model, looked up as _<model>._tcp.<base>. Targets and ports become endpoints.
// Polls on an interval (like the Consul backend).
type DNS struct {
	services  []string
	base      string
	scheme    string
	interval  time.Duration
	lookupSRV func(service, proto, name string) (string, []*net.SRV, error)
}

// NewDNS builds a DNS/SRV discovery backend. base is the domain, e.g.
// "svc.cluster.local"; services are the model names.
func NewDNS(services []string, base, scheme string, interval time.Duration) *DNS {
	if scheme == "" {
		scheme = "http"
	}
	if interval <= 0 {
		interval = 3 * time.Second
	}
	return &DNS{
		services:  services,
		base:      strings.TrimSuffix(base, "."),
		scheme:    scheme,
		interval:  interval,
		lookupSRV: net.LookupSRV,
	}
}

// Start implements Discovery.
func (d *DNS) Start(ctx context.Context) (<-chan []Endpoint, error) {
	ch := make(chan []Endpoint, 1)
	go func() {
		defer close(ch)
		d.emit(ctx, ch)
		t := time.NewTicker(d.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				d.emit(ctx, ch)
			}
		}
	}()
	return ch, nil
}

// Close implements Discovery.
func (*DNS) Close() error { return nil }

func (d *DNS) emit(ctx context.Context, ch chan<- []Endpoint) {
	var out []Endpoint
	for _, svc := range d.services {
		_, addrs, err := d.lookupSRV(svc, "tcp", d.base)
		if err != nil {
			continue
		}
		for _, a := range addrs {
			host := strings.TrimSuffix(a.Target, ".")
			if host == "" || a.Port == 0 {
				continue
			}
			out = append(out, Endpoint{
				ID:    fmt.Sprintf("%s:%d", host, a.Port),
				URL:   fmt.Sprintf("%s://%s:%d", d.scheme, host, a.Port),
				Model: svc,
			})
		}
	}
	if out == nil {
		out = []Endpoint{}
	}
	select {
	case ch <- out:
	case <-ctx.Done():
	}
}

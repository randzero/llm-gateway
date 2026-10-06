package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Consul discovers workers from a Consul catalog: each configured service name
// maps to a model, and its healthy instances become endpoints. Uses the Consul
// HTTP API (no SDK dependency) and polls on an interval.
type Consul struct {
	addr     string // e.g. http://127.0.0.1:8500
	services []string
	scheme   string // scheme for the worker URL, e.g. "http"
	interval time.Duration
	client   *http.Client
}

// NewConsul builds a Consul discovery backend. services are Consul service
// names; each doubles as the model name.
func NewConsul(addr string, services []string, scheme string, interval time.Duration) *Consul {
	if scheme == "" {
		scheme = "http"
	}
	if interval <= 0 {
		interval = 3 * time.Second
	}
	return &Consul{
		addr:     strings.TrimRight(addr, "/"),
		services: services,
		scheme:   scheme,
		interval: interval,
		client:   &http.Client{Timeout: 5 * time.Second},
	}
}

// Start implements Discovery.
func (c *Consul) Start(ctx context.Context) (<-chan []Endpoint, error) {
	ch := make(chan []Endpoint, 1)
	go func() {
		defer close(ch)
		c.emit(ctx, ch)
		t := time.NewTicker(c.interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				c.emit(ctx, ch)
			}
		}
	}()
	return ch, nil
}

// Close implements Discovery.
func (*Consul) Close() error { return nil }

func (c *Consul) emit(ctx context.Context, ch chan<- []Endpoint) {
	var out []Endpoint
	for _, svc := range c.services {
		out = append(out, c.instances(ctx, svc)...)
	}
	if out == nil {
		out = []Endpoint{}
	}
	select {
	case ch <- out:
	case <-ctx.Done():
	}
}

type consulService struct {
	Node struct {
		Address string
	}
	Service struct {
		ID      string
		Service string
		Address string
		Port    int
	}
}

func (c *Consul) instances(ctx context.Context, service string) []Endpoint {
	url := fmt.Sprintf("%s/v1/health/service/%s?passing=true", c.addr, service)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var entries []consulService
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil
	}
	out := make([]Endpoint, 0, len(entries))
	for _, e := range entries {
		addr := e.Service.Address
		if addr == "" {
			addr = e.Node.Address
		}
		if addr == "" || e.Service.Port == 0 {
			continue
		}
		out = append(out, Endpoint{
			ID:    firstNonEmpty(e.Service.ID, fmt.Sprintf("%s:%d", addr, e.Service.Port)),
			URL:   fmt.Sprintf("%s://%s:%d", c.scheme, addr, e.Service.Port),
			Model: service,
		})
	}
	return out
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

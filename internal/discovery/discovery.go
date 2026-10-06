// Package discovery resolves the set of backend workers. Backends are
// pluggable: static and Consul today, with k8s/DNS/registry as further
// adapters behind the same interface.
//
// Consul and the k8s/DNS backends are reached over plain HTTP/catalog APIs so
// the core stays dependency-free.
package discovery

import "context"

// Endpoint is one discovered worker.
type Endpoint struct {
	ID     string
	URL    string
	Model  string
	DPRank int
	PDRole string
}

// Discovery yields snapshots of the current endpoint set: the current set on
// Start, then updates as they are observed.
type Discovery interface {
	Start(ctx context.Context) (<-chan []Endpoint, error)
	Close() error
}

// Static is a fixed endpoint list; it emits the list once and then stays quiet.
type Static struct{ endpoints []Endpoint }

// NewStatic builds a static discovery backend.
func NewStatic(endpoints []Endpoint) *Static {
	return &Static{endpoints: append([]Endpoint(nil), endpoints...)}
}

// Start implements Discovery.
func (s *Static) Start(ctx context.Context) (<-chan []Endpoint, error) {
	ch := make(chan []Endpoint, 1)
	ch <- append([]Endpoint(nil), s.endpoints...)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

// Close implements Discovery.
func (*Static) Close() error { return nil }

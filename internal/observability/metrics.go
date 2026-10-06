// Package observability exposes metrics in Prometheus text format with no
// external dependencies (a client library can replace this later).
package observability

import (
	"fmt"
	"net/http"
	"sort"
	"sync"
)

// Metrics is a small thread-safe counter set.
type Metrics struct {
	mu            sync.Mutex
	requestsTotal int64
	errorsTotal   int64
	latencySumMs  float64
	selected      map[string]int64
}

// New returns empty metrics.
func New() *Metrics {
	return &Metrics{selected: make(map[string]int64)}
}

// IncRequests counts an incoming request.
func (m *Metrics) IncRequests() { m.mu.Lock(); m.requestsTotal++; m.mu.Unlock() }

// IncErrors counts a request that could not be served.
func (m *Metrics) IncErrors() { m.mu.Lock(); m.errorsTotal++; m.mu.Unlock() }

// ObserveLatency records a served request's latency in milliseconds.
func (m *Metrics) ObserveLatency(ms float64) { m.mu.Lock(); m.latencySumMs += ms; m.mu.Unlock() }

// ObserveSelection records which worker was chosen.
func (m *Metrics) ObserveSelection(worker string) {
	m.mu.Lock()
	m.selected[worker]++
	m.mu.Unlock()
}

// ServeHTTP renders metrics in Prometheus text exposition format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintf(w, "# TYPE llmgateway_requests_total counter\nllmgateway_requests_total %d\n", m.requestsTotal)
	fmt.Fprintf(w, "# TYPE llmgateway_errors_total counter\nllmgateway_errors_total %d\n", m.errorsTotal)
	fmt.Fprintf(w, "# TYPE llmgateway_request_latency_ms_sum counter\nllmgateway_request_latency_ms_sum %f\n", m.latencySumMs)
	fmt.Fprint(w, "# TYPE llmgateway_worker_selected_total counter\n")
	keys := make([]string, 0, len(m.selected))
	for k := range m.selected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "llmgateway_worker_selected_total{worker=%q} %d\n", k, m.selected[k])
	}
}

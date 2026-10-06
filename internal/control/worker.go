// Package control holds the worker registry and health tracking.
package control

import (
	"net/url"
	"sync/atomic"
	"time"

	"github.com/randzero/llm-gateway/internal/resilience"
)

// Worker is a single inference backend.
type Worker struct {
	ID     string // stable id (host by default)
	URL    string // base URL, e.g. http://10.0.0.1:8000
	Model  string
	DPRank int
	PDRole string // "prefill" | "decode" | ""

	healthy atomic.Bool
	load    atomic.Int64
	cb      *resilience.CircuitBreaker
}

// NewWorker builds a worker. It starts assumed healthy until a probe says
// otherwise.
func NewWorker(rawURL, model string, dpRank int, pdRole string, cbThreshold int, cbCooldown time.Duration) *Worker {
	id := rawURL
	if u, err := url.Parse(rawURL); err == nil && u.Host != "" {
		id = u.Host
	}
	w := &Worker{
		ID:     id,
		URL:    rawURL,
		Model:  model,
		DPRank: dpRank,
		PDRole: pdRole,
		cb:     resilience.NewCircuitBreaker(cbThreshold, cbCooldown),
	}
	w.healthy.Store(true)
	return w
}

// Healthy reports the last observed health.
func (w *Worker) Healthy() bool { return w.healthy.Load() }

// SetHealthy records health from a probe.
func (w *Worker) SetHealthy(b bool) { w.healthy.Store(b) }

// Load is the number of in-flight requests this router sent to the worker.
func (w *Worker) Load() int { return int(w.load.Load()) }

// IncLoad / DecLoad track in-flight requests.
func (w *Worker) IncLoad() { w.load.Add(1) }
func (w *Worker) DecLoad() { w.load.Add(-1) }

// Eligible reports whether the worker may be a candidate (healthy and its
// circuit is not open).
func (w *Worker) Eligible() bool {
	return w.healthy.Load() && w.cb.State() != resilience.Open
}

// CB exposes the circuit breaker for dispatch-time gating.
func (w *Worker) CB() *resilience.CircuitBreaker { return w.cb }

// Package resilience provides per-worker circuit breaking.
package resilience

import (
	"sync"
	"time"
)

// State is a circuit breaker state.
type State int

const (
	Closed State = iota
	Open
	HalfOpen
)

// CircuitBreaker trips after Threshold consecutive failures and rejects calls
// for Cooldown, then allows a single probe (half-open).
type CircuitBreaker struct {
	mu        sync.Mutex
	state     State
	failures  int
	threshold int
	cooldown  time.Duration
	openUntil time.Time
	probing   bool
}

// NewCircuitBreaker creates a breaker.
func NewCircuitBreaker(threshold int, cooldown time.Duration) *CircuitBreaker {
	if threshold < 1 {
		threshold = 1
	}
	return &CircuitBreaker{state: Closed, threshold: threshold, cooldown: cooldown}
}

// Allow reports whether a call may proceed.
func (c *CircuitBreaker) Allow() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch c.state {
	case Open:
		if time.Now().After(c.openUntil) {
			c.state = HalfOpen
			c.probing = true
			return true
		}
		return false
	case HalfOpen:
		if c.probing {
			return false // only one probe at a time
		}
		c.probing = true
		return true
	default:
		return true
	}
}

// Record feeds the outcome of a call back into the breaker.
func (c *CircuitBreaker) Record(success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if success {
		c.failures = 0
		c.state = Closed
		c.probing = false
		return
	}
	switch c.state {
	case HalfOpen:
		c.state = Open
		c.openUntil = time.Now().Add(c.cooldown)
		c.probing = false
	case Closed:
		c.failures++
		if c.failures >= c.threshold {
			c.state = Open
			c.openUntil = time.Now().Add(c.cooldown)
		}
	}
}

// State returns the current state.
func (c *CircuitBreaker) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

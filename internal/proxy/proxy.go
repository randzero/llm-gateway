// Package proxy is the OpenAI/Anthropic-compatible forwarding handler: it picks
// a worker via the configured policy, forwards the request, and streams the
// response (SSE-safe) back to the client.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/randzero/llm-gateway/internal/control"
	"github.com/randzero/llm-gateway/internal/observability"
	"github.com/randzero/llm-gateway/internal/otel"
	"github.com/randzero/llm-gateway/internal/policies"
	"github.com/randzero/llm-gateway/internal/prefix"
	"github.com/randzero/llm-gateway/internal/state"
)

const maxBodyBytes = 32 << 20 // 32 MiB

var hopHeaders = map[string]bool{
	"Connection":          true,
	"Proxy-Connection":    true,
	"Keep-Alive":          true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Upgrade":             true,
}

// PrefixMatcher answers, for one request, the cached-token count of its longest
// cached prefix and the workers holding it, the session's owning worker and
// cached tokens, and the reported load of the candidate workers. Implemented by
// the local prefix.PrefixMatcher or the state-service client.
type PrefixMatcher interface {
	Match(ctx context.Context, r *http.Request, body []byte, candidates []string) (prefix.MatchInfo, bool)
}

// Gateway is the HTTP handler.
type Gateway struct {
	registry *control.Registry
	policy   policies.Policy
	client   *http.Client
	metrics  *observability.Metrics
	retries  int
	provider state.StateProvider
	observer state.Observer
	prefix   PrefixMatcher
	tracer   *otel.Tracer
}

// New builds a gateway.
func New(reg *control.Registry, pol policies.Policy, client *http.Client, m *observability.Metrics, retries int) *Gateway {
	return &Gateway{registry: reg, policy: pol, client: client, metrics: m, retries: retries}
}

// WithProvider attaches a state provider used to compute per-candidate prefix
// overlap (nil is fine — CachedTokens stays 0).
func (g *Gateway) WithProvider(p state.StateProvider) *Gateway {
	g.provider = p
	return g
}

// WithObserver attaches an observer notified of successful routings, so the
// approximation (or any provider) can learn from traffic.
func (g *Gateway) WithObserver(o state.Observer) *Gateway {
	g.observer = o
	return g
}

// WithPrefix attaches the real block-hash prefix matcher. When present, its
// (authoritative) per-request prefix match overrides the approximate provider
// for the workers it names.
func (g *Gateway) WithPrefix(m PrefixMatcher) *Gateway {
	g.prefix = m
	return g
}

// WithTracer attaches an OpenTelemetry tracer; each request becomes a span.
func (g *Gateway) WithTracer(t *otel.Tracer) *Gateway {
	g.tracer = t
	return g
}

// ServeHTTP implements http.Handler.
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.metrics.IncRequests()
	start := time.Now()

	var span *otel.Span
	if g.tracer != nil {
		span = g.tracer.Start(r.Method+" "+r.URL.Path, r.Header.Get("traceparent"))
		r.Header.Set("traceparent", span.Traceparent()) // propagate to the worker
		defer span.End(context.Background())
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		g.metrics.IncErrors()
		if span != nil {
			span.SetError()
		}
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()

	meta := buildMeta(r, body)
	if span != nil {
		span.SetAttr("llm.model", meta.Model)
	}
	workers := g.registry.ForModel(meta.Model)
	if len(workers) == 0 {
		g.metrics.IncErrors()
		if span != nil {
			span.SetError()
		}
		http.Error(w, "no upstream workers", http.StatusServiceUnavailable)
		return
	}

	// Real routing info (authoritative), computed once per request: the longest
	// cached prefix, the session's cached tokens, and the candidates' load.
	ids := make([]string, len(workers))
	for i, w := range workers {
		ids[i] = w.ID
	}
	var info prefix.MatchInfo
	if g.prefix != nil {
		info, _ = g.prefix.Match(r.Context(), r, body, ids)
	}
	var hint map[string]int
	if info.Tokens > 0 {
		hint = make(map[string]int, len(info.Workers))
		for _, id := range info.Workers {
			hint[id] = info.Tokens
		}
	}
	if info.SessionWorker != "" && info.SessionTokens > 0 {
		if hint == nil {
			hint = map[string]int{}
		}
		if info.SessionTokens > hint[info.SessionWorker] {
			hint[info.SessionWorker] = info.SessionTokens
		}
	}

	exclude := make(map[string]bool)
	for attempt := 0; attempt <= g.retries; attempt++ {
		idx, serr := g.policy.Select(r.Context(), meta, candidates(workers, exclude, g.provider, meta.Text, meta.SessionID, hint, info.Loads))
		if serr != nil {
			break
		}
		wk := workers[idx]
		if !wk.CB().Allow() {
			exclude[wk.ID] = true
			continue
		}
		handled, _ := g.forward(w, r, body, wk)
		if handled {
			wk.CB().Record(true)
			g.metrics.ObserveSelection(wk.ID)
			g.metrics.ObserveLatency(float64(time.Since(start).Milliseconds()))
			if span != nil {
				span.SetAttr("llm.worker", wk.ID)
			}
			if g.observer != nil && meta.Text != "" {
				g.observer.Observe(wk.ID, meta.Text)
			}
			return
		}
		wk.CB().Record(false)
		exclude[wk.ID] = true
	}

	g.metrics.IncErrors()
	if span != nil {
		span.SetError()
	}
	http.Error(w, "no upstream worker available", http.StatusBadGateway)
}

// forward sends the request to one worker. handled=false means the request
// failed before any response was written (safe to retry elsewhere).
func (g *Gateway) forward(dst http.ResponseWriter, r *http.Request, body []byte, wk *control.Worker) (bool, error) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, wk.URL+r.URL.Path, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.URL.RawQuery = r.URL.RawQuery
	copyHeader(req.Header, r.Header)
	if wk.DPRank > 0 {
		req.Header.Set("X-Data-Parallel-Rank", strconv.Itoa(wk.DPRank))
	}

	wk.IncLoad()
	defer wk.DecLoad()

	resp, err := g.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	copyHeader(dst.Header(), resp.Header)
	dst.WriteHeader(resp.StatusCode)
	fw := &flushWriter{w: dst}
	if f, ok := dst.(http.Flusher); ok {
		fw.f = f
	}
	_, copyErr := io.Copy(fw, resp.Body)
	return true, copyErr
}

// buildMeta extracts the routing-relevant parts of a request.
func buildMeta(r *http.Request, body []byte) *policies.RequestMeta {
	meta := &policies.RequestMeta{
		SessionID: r.Header.Get("X-Session-ID"),
		UserID:    r.Header.Get("X-User-ID"),
		TenantID:  r.Header.Get("X-Tenant-ID"),
	}
	var m struct {
		Model string `json:"model"`
	}
	if json.Unmarshal(body, &m) == nil {
		meta.Model = m.Model
	}
	if meta.Model == "" {
		meta.Model = r.Header.Get("X-Model")
	}
	meta.Text = extractPromptText(body)
	return meta
}

// extractPromptText best-effort extracts the prompt text for prefix affinity.
// Handles OpenAI (prompt / messages[].content) and Anthropic (/v1/messages:
// system + messages[].content blocks). Returns "" when nothing is readable.
func extractPromptText(body []byte) string {
	var m struct {
		Prompt   json.RawMessage `json:"prompt"`
		System   json.RawMessage `json:"system"`
		Messages []struct {
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if json.Unmarshal(body, &m) != nil {
		return ""
	}
	var b strings.Builder
	b.WriteString(contentText(m.System)) // Anthropic system prompt
	b.WriteString(contentText(m.Prompt)) // OpenAI completion prompt
	for _, msg := range m.Messages {
		b.WriteString(contentText(msg.Content))
	}
	return b.String()
}

// contentText extracts text from a field that is a string, an array of strings
// (OpenAI prompt), or an array of content blocks {"type":"text","text":...}
// (Anthropic messages / system). Returns "" for anything else.
func contentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var arr []string
	if json.Unmarshal(raw, &arr) == nil {
		return strings.Join(arr, "")
	}
	var blocks []struct {
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) == nil {
		var b strings.Builder
		for _, blk := range blocks {
			b.WriteString(blk.Text)
		}
		return b.String()
	}
	return ""
}

// candidates builds a candidate slice parallel to workers (same indices). hint,
// when non-nil, carries the authoritative real-prefix token count per worker and
// overrides the approximation; loads, when non-nil, carries the reported
// per-worker load and overrides the router's local in-flight counter.
func candidates(workers []*control.Worker, exclude map[string]bool, provider state.StateProvider, prompt, sessionID string, hint map[string]int, loads map[string]prefix.WorkerLoad) []policies.Candidate {
	cs := make([]policies.Candidate, len(workers))
	for i, wk := range workers {
		load := wk.Load()
		throughput, waitingTokens := 0, -1
		if l, ok := loads[wk.ID]; ok {
			load = l.Running + l.Waiting
			throughput = l.Throughput
			waitingTokens = l.WaitingTokens
		}
		cs[i] = policies.Candidate{
			ID:      wk.ID,
			URL:     wk.URL,
			Model:   wk.Model,
			DPRank:  wk.DPRank,
			PDRole:  wk.PDRole,
			Healthy: wk.Eligible() && !exclude[wk.ID],
			Load:    load, Throughput: throughput, WaitingTokens: waitingTokens,
		}
		tok, authoritative := 0, false
		if v, ok := hint[wk.ID]; ok {
			tok, authoritative = v, true
		}
		if provider != nil {
			if !authoritative {
				if v, ok := provider.CachedTokens(wk.ID, prompt); ok {
					tok = v
				}
			}
			if sessionID != "" {
				// Real per-session cached tokens (from the plugin) win.
				if st, ok := provider.SessionCachedTokens(wk.ID, sessionID); ok && st > tok {
					tok = st
				}
			}
		}
		cs[i].CachedTokens = tok
	}
	return cs
}

func copyHeader(dst, src http.Header) {
	for k, vv := range src {
		if hopHeaders[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}

// flushWriter flushes after each write, keeping SSE/streaming low-latency.
type flushWriter struct {
	w io.Writer
	f http.Flusher
}

func (fw *flushWriter) Write(p []byte) (int, error) {
	n, err := fw.w.Write(p)
	if fw.f != nil {
		fw.f.Flush()
	}
	return n, err
}

// Package otel is a minimal OpenTelemetry exporter: it emits one span per
// routed request as OTLP/HTTP JSON (no OTel SDK dependency) and does W3C
// traceparent propagation. It is deliberately small — enough to light up
// tracing in a collector; swap in the OTel SDK if you need more.
package otel

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Tracer exports spans to an OTLP/HTTP traces endpoint (e.g.
// http://localhost:4318/v1/traces). An empty endpoint disables it.
type Tracer struct {
	endpoint string
	service  string
	hc       *http.Client
}

// New builds a Tracer. hc nil uses a 5s-timeout client.
func New(endpoint, service string, hc *http.Client) *Tracer {
	if hc == nil {
		hc = &http.Client{Timeout: 5 * time.Second}
	}
	return &Tracer{endpoint: endpoint, service: service, hc: hc}
}

// Attr is a string span attribute.
type Attr struct{ Key, Value string }

// Span is an in-flight span.
type Span struct {
	tracer   *Tracer
	TraceID  string
	SpanID   string
	ParentID string
	Name     string
	Start    time.Time
	EndTime  time.Time
	Attrs    []Attr
	Error    bool
}

// Start begins a span. traceparent, if non-empty, is the incoming W3C header;
// its trace-id and span-id become this span's trace and parent.
func (t *Tracer) Start(name, traceparent string) *Span {
	s := &Span{tracer: t, Name: name, Start: time.Now(), TraceID: newID(16), SpanID: newID(8)}
	if tid, pid, ok := parseTraceparent(traceparent); ok {
		s.TraceID, s.ParentID = tid, pid
	}
	return s
}

// SetAttr records a string attribute (empty values are dropped).
func (s *Span) SetAttr(key, value string) {
	if value != "" {
		s.Attrs = append(s.Attrs, Attr{Key: key, Value: value})
	}
}

// SetError marks the span as failed.
func (s *Span) SetError() { s.Error = true }

// Traceparent returns the W3C header to propagate to downstream services.
func (s *Span) Traceparent() string { return "00-" + s.TraceID + "-" + s.SpanID + "-01" }

// End finishes the span and exports it (best-effort).
func (s *Span) End(ctx context.Context) {
	s.EndTime = time.Now()
	s.tracer.export(ctx, s)
}

func (t *Tracer) export(ctx context.Context, s *Span) {
	if t.endpoint == "" {
		return
	}
	attrs := make([]map[string]any, 0, len(s.Attrs))
	for _, a := range s.Attrs {
		attrs = append(attrs, map[string]any{"key": a.Key, "value": map[string]any{"stringValue": a.Value}})
	}
	span := map[string]any{
		"traceId":           s.TraceID,
		"spanId":            s.SpanID,
		"name":              s.Name,
		"kind":              2, // SPAN_KIND_SERVER
		"startTimeUnixNano": strconv.FormatInt(s.Start.UnixNano(), 10),
		"endTimeUnixNano":   strconv.FormatInt(s.EndTime.UnixNano(), 10),
		"attributes":        attrs,
		"status":            map[string]any{"code": statusCode(s.Error)},
	}
	if s.ParentID != "" {
		span["parentSpanId"] = s.ParentID
	}
	body := map[string]any{
		"resourceSpans": []any{map[string]any{
			"resource": map[string]any{"attributes": []any{
				map[string]any{"key": "service.name", "value": map[string]any{"stringValue": t.service}},
			}},
			"scopeSpans": []any{map[string]any{
				"scope": map[string]any{"name": "llm-gateway"},
				"spans": []any{span},
			}},
		}},
	}
	b, err := json.Marshal(body)
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, t.endpoint, bytes.NewReader(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := t.hc.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}

func statusCode(err bool) int {
	if err {
		return 2 // STATUS_CODE_ERROR
	}
	return 1 // STATUS_CODE_OK
}

// parseTraceparent parses "00-<32 hex trace>-<16 hex span>-<flags>".
func parseTraceparent(h string) (traceID, spanID string, ok bool) {
	parts := strings.Split(h, "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		return "", "", false
	}
	if !isHex(parts[1]) || !isHex(parts[2]) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

func newID(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

package prefix

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Hasher turns a request into the request's block-hash chain (shallowest-first,
// one hash per full block). This is the step that must reproduce the engine's
// hashing — its tokenizer + block size + hash recipe — so it belongs in the
// engine (or a service that replicates them), not inferred on the router.
type Hasher interface {
	Hashes(ctx context.Context, r *http.Request, body []byte) ([]string, error)
}

// HeaderHasher reads a pre-computed hash chain from a request header
// (comma-separated). Useful when an edge service or the client already
// tokenized + hashed, and for tests. An empty header yields no hashes.
type HeaderHasher struct {
	Header string // defaults to "X-KV-Hashes"
}

// Hashes implements Hasher.
func (h HeaderHasher) Hashes(_ context.Context, r *http.Request, _ []byte) ([]string, error) {
	name := h.Header
	if name == "" {
		name = "X-KV-Hashes"
	}
	v := strings.TrimSpace(r.Header.Get(name))
	if v == "" {
		return nil, nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

// HTTPHasher obtains the hash chain by POSTing the request body to an engine
// hashing endpoint. The response is either a bare JSON array of hash strings or
// an object {"hashes": [...]} (shallowest-first).
type HTTPHasher struct {
	URL    string
	Client *http.Client
}

// Hashes implements Hasher.
func (h *HTTPHasher) Hashes(ctx context.Context, r *http.Request, body []byte) ([]string, error) {
	if h.URL == "" {
		return nil, nil
	}
	client := h.Client
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("hash endpoint: status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	// Accept a bare array or {"hashes": [...]}.
	var arr []string
	if json.Unmarshal(data, &arr) == nil {
		return arr, nil
	}
	var obj struct {
		Hashes []string `json:"hashes"`
	}
	if json.Unmarshal(data, &obj) == nil {
		return obj.Hashes, nil
	}
	return nil, fmt.Errorf("hash endpoint: unrecognized response")
}

// CompositeHasher returns the first non-empty result among its hashers, in
// order (e.g. a header source, then an endpoint).
type CompositeHasher struct {
	Hashers []Hasher
}

// Hashes implements Hasher.
func (c CompositeHasher) Hashes(ctx context.Context, r *http.Request, body []byte) ([]string, error) {
	for _, h := range c.Hashers {
		if h == nil {
			continue
		}
		hs, err := h.Hashes(ctx, r, body)
		if err == nil && len(hs) > 0 {
			return hs, nil
		}
	}
	return nil, nil
}

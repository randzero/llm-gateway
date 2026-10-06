// Package stateclient is the router-side client of the shared state service
// (design §13). It makes a single call per request: it sends the request's
// block-hash chain (and its session id, if any) to POST /match and gets back the
// longest cached prefix plus the session's cached tokens. There is no periodic
// pull and no local materialization of fleet-wide state.
package stateclient

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/randzero/llm-gateway/internal/prefix"
)

// Client talks to the state service.
type Client struct {
	url    string
	hc     *http.Client
	hasher prefix.Hasher
}

// New builds a client. hasher turns a request into its block-hash chain; nil
// disables matching.
func New(url string, hasher prefix.Hasher, hc *http.Client) *Client {
	if hc == nil {
		hc = http.DefaultClient
	}
	return &Client{url: strings.TrimRight(url, "/"), hc: hc, hasher: hasher}
}

// Close implements io.Closer (no resources held).
func (*Client) Close() error { return nil }

// Match implements the router's prefix matcher. It sends the request's
// block-hash chain, its session id (if any) and the candidate worker ids, and
// gets back the longest cached prefix, the session's cached tokens, and the
// candidates' reported load.
func (c *Client) Match(ctx context.Context, r *http.Request, body []byte, candidates []string) (prefix.MatchInfo, bool) {
	var hashes []string
	if c.hasher != nil {
		if h, err := c.hasher.Hashes(ctx, r, body); err == nil {
			hashes = h
		}
	}
	sid := r.Header.Get("X-Session-ID")
	if len(hashes) == 0 && sid == "" && len(candidates) == 0 {
		return prefix.MatchInfo{}, false
	}
	payload, err := json.Marshal(map[string]any{
		"hashes":     hashes,
		"session_id": sid,
		"workers":    candidates,
	})
	if err != nil {
		return prefix.MatchInfo{}, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url+"/match", bytes.NewReader(payload))
	if err != nil {
		return prefix.MatchInfo{}, false
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.hc.Do(req)
	if err != nil {
		return prefix.MatchInfo{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return prefix.MatchInfo{}, false
	}
	var m struct {
		Tokens        int      `json:"tokens"`
		Workers       []string `json:"workers"`
		SessionWorker string   `json:"session_worker"`
		SessionTokens int      `json:"session_tokens"`
		Loads         map[string]struct {
			Running       int `json:"running"`
			Waiting       int `json:"waiting"`
			Throughput    int `json:"throughput"`
			WaitingTokens int `json:"waiting_tokens"`
		} `json:"loads"`
	}
	if json.NewDecoder(resp.Body).Decode(&m) != nil {
		return prefix.MatchInfo{}, false
	}
	info := prefix.MatchInfo{
		Tokens: m.Tokens, Workers: m.Workers,
		SessionWorker: m.SessionWorker, SessionTokens: m.SessionTokens,
	}
	if len(m.Loads) > 0 {
		info.Loads = make(map[string]prefix.WorkerLoad, len(m.Loads))
		for id, l := range m.Loads {
			info.Loads[id] = prefix.WorkerLoad{
				Running: l.Running, Waiting: l.Waiting,
				Throughput: l.Throughput, WaitingTokens: l.WaitingTokens,
			}
		}
	}
	return info, true
}

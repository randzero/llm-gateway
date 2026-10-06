package state

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

// ConsulStore is a StateStore backed by Consul KV over its HTTP API. Watches use
// Consul blocking queries; TTL uses a session (keys are bound to it and removed
// when the session expires).
//
// Limitation: Consul KV has no native per-key TTL, so all keys share one
// session TTL (the value passed to the most recent Put). etcd would map more
// directly (native lease per key).
type ConsulStore struct {
	addr   string
	prefix string
	client *http.Client

	mu      sync.Mutex
	session string
	sessTTL int
}

// NewConsulStore creates a Consul-backed store. prefix is prepended to every
// key (use "" for none).
func NewConsulStore(addr, prefix string) *ConsulStore {
	if prefix != "" && prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	return &ConsulStore{
		addr:   trimRightSlash(addr),
		prefix: prefix,
		client: &http.Client{Timeout: 40 * time.Second},
	}
}

// Get implements StateStore.
func (s *ConsulStore) Get(ctx context.Context, key string) (Value, bool, error) {
	entries, _, err := s.query(ctx, s.full(key), false, 0, "")
	if err != nil {
		return Value{}, false, err
	}
	if len(entries) == 0 {
		return Value{}, false, nil
	}
	data, _ := decodeValue(entries[0].Value)
	return Value{Data: data, Rev: int64(entries[0].ModifyIndex)}, true, nil
}

// Put implements StateStore.
func (s *ConsulStore) Put(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	u := s.addr + "/v1/kv/" + s.full(key)
	if ttl > 0 {
		sess, err := s.ensureSession(ctx, ttl)
		if err != nil {
			return err
		}
		u += "?acquire=" + sess
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// Delete implements StateStore.
func (s *ConsulStore) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.addr+"/v1/kv/"+s.full(key), nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

// Watch implements StateStore using Consul blocking queries.
func (s *ConsulStore) Watch(ctx context.Context, prefix string) (<-chan WatchedEntry, error) {
	ch := make(chan WatchedEntry, 256)
	go s.watchLoop(ctx, s.full(prefix), ch)
	return ch, nil
}

// Close implements StateStore.
func (*ConsulStore) Close() error { return nil }

func (s *ConsulStore) watchLoop(ctx context.Context, fullPrefix string, ch chan<- WatchedEntry) {
	defer close(ch)
	var index uint64
	var wait string
	prev := map[string]string{}
	for {
		entries, newIndex, err := s.query(ctx, fullPrefix, true, index, wait)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		index, wait = newIndex, "30s"
		cur := make(map[string]string, len(entries))
		for _, e := range entries {
			cur[e.Key] = deref(e.Value)
		}
		keys := make([]string, 0, len(cur))
		for k := range cur {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if prev[k] != cur[k] {
				emit(ctx, ch, WatchedEntry{Key: s.unfull(k), Value: Value{Data: decode(cur[k])}, Type: EventPut})
			}
		}
		for k := range prev {
			if _, ok := cur[k]; !ok {
				emit(ctx, ch, WatchedEntry{Key: s.unfull(k), Type: EventDelete})
			}
		}
		prev = cur
	}
}

func emit(ctx context.Context, ch chan<- WatchedEntry, ev WatchedEntry) {
	select {
	case ch <- ev:
	case <-ctx.Done():
	}
}

type consulKV struct {
	Key         string  `json:"Key"`
	Value       *string `json:"Value"`
	ModifyIndex uint64  `json:"ModifyIndex"`
}

func (s *ConsulStore) query(ctx context.Context, fullKey string, recurse bool, index uint64, wait string) ([]consulKV, uint64, error) {
	u := s.addr + "/v1/kv/" + fullKey
	q := []string{}
	if recurse {
		q = append(q, "recurse=true")
	}
	if index > 0 {
		q = append(q, "index="+strconv.FormatUint(index, 10))
	}
	if wait != "" {
		q = append(q, "wait="+wait)
	}
	if len(q) > 0 {
		u += "?" + join(q, "&")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, index, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, index, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, respIndex(resp, index), nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, index, fmt.Errorf("consul kv: status %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	var entries []consulKV
	if len(body) > 0 {
		if err := json.Unmarshal(body, &entries); err != nil {
			return nil, respIndex(resp, index), nil
		}
	}
	return entries, respIndex(resp, index), nil
}

func (s *ConsulStore) ensureSession(ctx context.Context, ttl time.Duration) (string, error) {
	secs := int(ttl / time.Second)
	if secs < 10 {
		secs = 10 // Consul minimum
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session != "" && s.sessTTL == secs {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, s.addr+"/v1/session/renew/"+s.session, nil)
		if resp, err := s.client.Do(req); err == nil {
			resp.Body.Close()
			return s.session, nil
		}
	}
	body, _ := json.Marshal(map[string]string{"TTL": fmt.Sprintf("%ds", secs), "Behavior": "delete"})
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.addr+"/v1/session/create", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		ID string `json:"ID"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	s.session = out.ID
	s.sessTTL = secs
	return out.ID, nil
}

func (s *ConsulStore) full(key string) string { return s.prefix + key }

func (s *ConsulStore) unfull(key string) string {
	if s.prefix != "" && len(key) >= len(s.prefix) {
		return key[len(s.prefix):]
	}
	return key
}

func respIndex(resp *http.Response, fallback uint64) uint64 {
	if v := resp.Header.Get("X-Consul-Index"); v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			return n
		}
	}
	return fallback
}

func decodeValue(v *string) ([]byte, bool) {
	if v == nil {
		return nil, false
	}
	b, err := base64.StdEncoding.DecodeString(*v)
	if err != nil {
		return nil, false
	}
	return b, true
}

func decode(s string) []byte { b, _ := base64.StdEncoding.DecodeString(s); return b }
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
func trimRightSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}
func join(ss []string, sep string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}

package prefix

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// keyNamespace is prepended to every block hash. Keys are many small values
// (`kvprefix:<hash>` -> set of workers), which is what keeps the index free of
// big keys.
const keyNamespace = "kvprefix:"

// RedisIndex is an Index backed by Redis. It uses a UniversalClient so the same
// code covers a single node, a cluster (hash-slot sharded), or sentinel.
//
// Writes are best-effort upserts of a per-hash set; reads fan out (a pipeline of
// SMEMBERS, one per hash) and are merged locally into the longest prefix. The
// index is derived state: a lost update only costs a later cache miss, never a
// correctness error.
type RedisIndex struct {
	client redis.UniversalClient
	ttl    time.Duration
}

// NewRedisIndex wraps an existing client. ttl, if > 0, is refreshed on every
// Add so entries for blocks that stop being (re)reported expire on their own.
func NewRedisIndex(client redis.UniversalClient, ttl time.Duration) *RedisIndex {
	return &RedisIndex{client: client, ttl: ttl}
}

// NewRedisIndexAddr builds a RedisIndex from one or more addresses. With more
// than one address it connects as a cluster; with one, as a single node.
func NewRedisIndexAddr(addrs []string, ttl time.Duration) *RedisIndex {
	return NewRedisIndex(redis.NewUniversalClient(&redis.UniversalOptions{Addrs: addrs}), ttl)
}

func key(hash string) string { return keyNamespace + hash }

// Add implements Index.
func (r *RedisIndex) Add(ctx context.Context, worker, hash string) error {
	k := key(hash)
	pipe := r.client.Pipeline()
	pipe.SAdd(ctx, k, worker)
	if r.ttl > 0 {
		pipe.Expire(ctx, k, r.ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Remove implements Index.
func (r *RedisIndex) Remove(ctx context.Context, worker, hash string) error {
	return r.client.SRem(ctx, key(hash), worker).Err()
}

// Lookup implements Index.
func (r *RedisIndex) Lookup(ctx context.Context, hash string) ([]string, error) {
	return r.client.SMembers(ctx, key(hash)).Result()
}

// Match implements Index: fetch all sets in one pipeline, then scan
// shallowest-first and stop at the first miss. Under a cluster the pipeline
// groups commands per slot-owning node and runs one goroutine per node
// (go-redis ClusterPipeline), so the fan-out is concurrent — about one RTT,
// not one per hash.
func (r *RedisIndex) Match(ctx context.Context, hashes []string) (int, []string, error) {
	if len(hashes) == 0 {
		return 0, nil, nil
	}
	pipe := r.client.Pipeline()
	cmds := make([]*redis.StringSliceCmd, len(hashes))
	for i, h := range hashes {
		cmds[i] = pipe.SMembers(ctx, key(h))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, nil, err
	}

	matched := 0
	var workers []string
	for i := range hashes {
		ws, err := cmds[i].Result()
		if err != nil {
			return matched, workers, err
		}
		if len(ws) == 0 {
			break // stop at the first miss: the prefix must be contiguous
		}
		matched++
		workers = ws
	}
	if matched == 0 {
		return 0, nil, nil
	}
	return matched, workers, nil
}

// Close implements Index.
func (r *RedisIndex) Close() error { return r.client.Close() }

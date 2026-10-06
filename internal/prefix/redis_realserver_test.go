package prefix

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestRedisIndexRealServer exercises the RedisIndex against a real server.
// Gated on LLMGATEWAY_TEST_REDIS=host:port (skipped otherwise).
func TestRedisIndexRealServer(t *testing.T) {
	addr := os.Getenv("LLMGATEWAY_TEST_REDIS")
	if addr == "" {
		t.Skip("set LLMGATEWAY_TEST_REDIS=host:port to run against a real Redis")
	}
	ctx := context.Background()
	idx := NewRedisIndexAddr([]string{addr}, time.Minute)
	defer idx.Close()

	// Unique hashes per run so we never clobber real data; cleaned up below.
	tag := "rtest" + strconv.FormatInt(time.Now().UnixNano(), 36) + ":"
	hashes := []string{tag + "h0", tag + "h1", tag + "h2"}
	defer func() {
		for _, h := range hashes {
			_ = idx.Remove(ctx, "w1", h)
			_ = idx.Remove(ctx, "w2", h)
		}
	}()

	for _, h := range hashes[:2] { // w1 has h0,h1
		if err := idx.Add(ctx, "w1", h); err != nil {
			t.Fatal(err)
		}
	}
	_ = idx.Add(ctx, "w2", hashes[0]) // w2 shares h0

	// Contiguous longest prefix: request h0,h1,h2,h3 -> 2 blocks on w1.
	matched, workers, err := idx.Match(ctx, append(append([]string{}, hashes...), tag+"h3"))
	if err != nil {
		t.Fatal(err)
	}
	if matched != 2 || len(workers) != 1 || workers[0] != "w1" {
		t.Fatalf("match = %d,%v want 2,[w1]", matched, workers)
	}

	// Shared prefix returns both workers.
	_, both, _ := idx.Match(ctx, hashes[:1])
	if len(both) != 2 {
		t.Fatalf("shared prefix workers = %v, want 2", both)
	}

	// Remove moves the boundary.
	_ = idx.Remove(ctx, "w1", hashes[1])
	if m, _, _ := idx.Match(ctx, hashes); m != 1 {
		t.Fatalf("after remove matched = %d, want 1", m)
	}
}

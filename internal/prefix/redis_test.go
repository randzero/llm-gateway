package prefix

import (
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newTestRedisIndex(t *testing.T) *RedisIndex {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	return NewRedisIndexAddr([]string{mr.Addr()}, time.Hour)
}

func TestRedisIndexMatch(t *testing.T) {
	ctx := context.Background()
	idx := newTestRedisIndex(t)
	defer idx.Close()

	for _, h := range []string{"h0", "h1", "h2", "h3"} {
		if err := idx.Add(ctx, "w1", h); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range []string{"h0", "h1"} {
		if err := idx.Add(ctx, "w2", h); err != nil {
			t.Fatal(err)
		}
	}

	matched, workers, err := idx.Match(ctx, []string{"h0", "h1", "h2", "h3", "h4"})
	if err != nil {
		t.Fatal(err)
	}
	if matched != 4 {
		t.Fatalf("matched = %d, want 4", matched)
	}
	if !reflect.DeepEqual(workers, []string{"w1"}) {
		t.Fatalf("workers = %v, want [w1]", workers)
	}

	// Shallow prefix is shared by both workers.
	_, both, _ := idx.Match(ctx, []string{"h0", "h1"})
	sort.Strings(both)
	if !reflect.DeepEqual(both, []string{"w1", "w2"}) {
		t.Fatalf("workers = %v, want [w1 w2]", both)
	}
}

func TestRedisIndexStopAtMissAndRemove(t *testing.T) {
	ctx := context.Background()
	idx := newTestRedisIndex(t)
	defer idx.Close()

	_ = idx.Add(ctx, "w1", "h0")
	_ = idx.Add(ctx, "w1", "h2") // h1 never added

	if m, _, _ := idx.Match(ctx, []string{"h0", "h1", "h2"}); m != 1 {
		t.Fatalf("matched = %d, want 1 (stop at h1 miss)", m)
	}

	// Removing the shared block empties the set; the prefix stops there.
	_ = idx.Add(ctx, "w2", "h0")
	_ = idx.Remove(ctx, "w2", "h0")
	if got, _ := idx.Lookup(ctx, "h0"); !reflect.DeepEqual(got, []string{"w1"}) {
		t.Fatalf("Lookup(h0) = %v, want [w1]", got)
	}
}

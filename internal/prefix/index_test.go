package prefix

import (
	"context"
	"reflect"
	"testing"
)

func TestSampleIndices(t *testing.T) {
	cases := []struct {
		blocks int
		want   []int
	}{
		{0, nil},
		{1, []int{0}},
		{2, []int{0, 1}},
		{3, []int{0, 1, 2}},
		{5, []int{0, 1, 2, 4}},
		{20, []int{0, 1, 2, 4, 8, 16}},
	}
	for _, c := range cases {
		if got := SampleIndices(c.blocks); !reflect.DeepEqual(got, c.want) {
			t.Errorf("SampleIndices(%d) = %v, want %v", c.blocks, got, c.want)
		}
	}
}

func TestInProcIndexMatchLongestPrefix(t *testing.T) {
	ctx := context.Background()
	idx := NewInProcIndex()
	// worker w1 cached blocks h0..h3; w2 cached h0..h1.
	for _, h := range []string{"h0", "h1", "h2", "h3"} {
		_ = idx.Add(ctx, "w1", h)
	}
	for _, h := range []string{"h0", "h1"} {
		_ = idx.Add(ctx, "w2", h)
	}

	// A request whose full chain is h0..h4: w1 holds the longest (4 blocks).
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

	// A request starting with a miss (hc) has no cached prefix.
	if m, w, _ := idx.Match(ctx, []string{"hc", "h0", "h1"}); m != 0 || w != nil {
		t.Fatalf("Match with leading miss = %d,%v want 0,nil", m, w)
	}
}

// A hole means reuse is not contiguous from the start, so the match must stop.
func TestInProcIndexMatchStopsAtHole(t *testing.T) {
	ctx := context.Background()
	idx := NewInProcIndex()
	_ = idx.Add(ctx, "w1", "h0")
	_ = idx.Add(ctx, "w1", "h2") // h1 missing
	matched, _, _ := idx.Match(ctx, []string{"h0", "h1", "h2"})
	if matched != 1 {
		t.Fatalf("matched = %d, want 1 (stop at missing h1)", matched)
	}
}

func TestInProcIndexRemove(t *testing.T) {
	ctx := context.Background()
	idx := NewInProcIndex()
	_ = idx.Add(ctx, "w1", "h0")
	_ = idx.Add(ctx, "w2", "h0")
	_ = idx.Remove(ctx, "w1", "h0")
	if got, _ := idx.Lookup(ctx, "h0"); !reflect.DeepEqual(got, []string{"w2"}) {
		t.Fatalf("Lookup after remove = %v, want [w2]", got)
	}
	_ = idx.Remove(ctx, "w2", "h0")
	if got, _ := idx.Lookup(ctx, "h0"); got != nil {
		t.Fatalf("Lookup after removing last = %v, want nil", got)
	}
}

package policies

import (
	"context"
	"testing"
)

func cs(ids ...string) []Candidate {
	out := make([]Candidate, len(ids))
	for i, id := range ids {
		out[i] = Candidate{ID: id, Healthy: true}
	}
	return out
}

func TestRoundRobinCycles(t *testing.T) {
	p := NewRoundRobin()
	c := cs("a", "b", "c")
	got := make([]string, 0, 6)
	for i := 0; i < 6; i++ {
		idx, err := p.Select(context.Background(), &RequestMeta{}, c)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, c[idx].ID)
	}
	want := []string{"a", "b", "c", "a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("round robin = %v, want %v", got, want)
		}
	}
}

func TestPowerOfTwoPicksLeastLoaded(t *testing.T) {
	p := NewPowerOfTwo(1)
	c := cs("a", "b")
	c[0].Load = 0
	c[1].Load = 1000
	for i := 0; i < 100; i++ {
		idx, err := p.Select(context.Background(), &RequestMeta{}, c)
		if err != nil {
			t.Fatal(err)
		}
		if c[idx].ID != "a" {
			t.Fatalf("expected least-loaded 'a', got %q", c[idx].ID)
		}
	}
}

func TestConsistentHashIsSticky(t *testing.T) {
	p := NewConsistentHash()
	c := cs("a", "b", "c", "d")
	req := &RequestMeta{SessionID: "session-xyz"}
	first, err := p.Select(context.Background(), req, c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		idx, err := p.Select(context.Background(), req, c)
		if err != nil {
			t.Fatal(err)
		}
		if idx != first {
			t.Fatal("same session routed to different workers")
		}
	}
}

func TestNoHealthyCandidate(t *testing.T) {
	p := NewRoundRobin()
	c := []Candidate{{ID: "a"}, {ID: "b"}} // all unhealthy
	if _, err := p.Select(context.Background(), &RequestMeta{}, c); err != ErrNoCandidate {
		t.Fatalf("err = %v, want ErrNoCandidate", err)
	}
}

package state

import "testing"

func TestPrefixLength(t *testing.T) {
	tr := newPrefixTrie(1 << 20)
	tr.insert("hello world")
	if got := tr.prefixLen("hello world!!"); got != len("hello world") {
		t.Fatalf("prefixLen = %d, want %d", got, len("hello world"))
	}
	if got := tr.prefixLen("help"); got != 3 { // h,e,l shared; 'p' diverges from "hello"'s 'l'
		t.Fatalf("prefixLen = %d, want 3", got)
	}
	if got := tr.prefixLen("xyz"); got != 0 {
		t.Fatalf("prefixLen = %d, want 0", got)
	}
}

func TestApproximateProviderObserveAndQuery(t *testing.T) {
	p := NewApproximateProvider(1 << 20)
	if _, ok := p.CachedTokens("w1", "anything"); !ok {
		t.Fatal("expected ok=true for unknown worker with non-empty prompt")
	}
	if n, _ := p.CachedTokens("w1", "anything"); n != 0 {
		t.Fatalf("empty tree should give 0, got %d", n)
	}

	p.Observe("w1", "You are a helpful assistant. Question one.")
	n, ok := p.CachedTokens("w1", "You are a helpful assistant. Question two.")
	if !ok || n == 0 {
		t.Fatalf("expected nonzero cached tokens for shared prefix, got %d (ok=%v)", n, ok)
	}
	// a different worker has no overlap
	if other, _ := p.CachedTokens("w2", "You are a helpful assistant. Question two."); other != 0 {
		t.Fatalf("w2 should have no overlap, got %d", other)
	}
}

package control

import (
	"testing"
	"time"
)

func worker(url, model string) *Worker {
	return NewWorker(url, model, 0, "", 5, time.Second)
}

func TestForModelSinglePool(t *testing.T) {
	r := NewRegistry([]*Worker{worker("http://a", ""), worker("http://b", "")})
	if r.MultiModel() {
		t.Fatal("no models declared -> single-pool mode")
	}
	if got := r.ForModel("qwen"); len(got) != 2 {
		t.Fatalf("single-pool should ignore model and return all, got %d", len(got))
	}
}

func TestForModelMultiModel(t *testing.T) {
	r := NewRegistry([]*Worker{
		worker("http://a", "qwen"),
		worker("http://b", "qwen"),
		worker("http://c", "llama"),
	})
	if !r.MultiModel() {
		t.Fatal("expected multi-model mode")
	}
	if got := r.ForModel("qwen"); len(got) != 2 {
		t.Fatalf("qwen -> %d workers, want 2", len(got))
	}
	if got := r.ForModel("llama"); len(got) != 1 {
		t.Fatalf("llama -> %d workers, want 1", len(got))
	}
	if got := r.ForModel("unknown"); len(got) != 0 {
		t.Fatalf("unknown model must NOT fall back to other models, got %d", len(got))
	}
}

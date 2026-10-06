import os
from types import SimpleNamespace

from llm_gateway_vllm.config import load
from llm_gateway_vllm.reporter import Reporter


def _stats(running, waiting, kv, queries, hits):
    return SimpleNamespace(
        num_running_reqs=running,
        num_waiting_reqs=waiting,
        kv_cache_usage=kv,
        prefix_cache_stats=SimpleNamespace(queries=queries, hits=hits),
        running_lora_adapters={"lora-a": 1},
        iteration_details=SimpleNamespace(num_prefill_tokens=2048, num_scheduled_tokens=8192),
    )


def _stored(*hashes, block_size=16, session_id=None):
    # BlockStored carries token_ids; BlockRemoved does not (duck-typed in reporter).
    return SimpleNamespace(
        block_hashes=list(hashes),
        block_size=block_size,
        token_ids=[0] * block_size,
        session_id=session_id,
    )


def _removed(*hashes):
    return SimpleNamespace(block_hashes=list(hashes))


def _batch(events, rank=0):
    return SimpleNamespace(events=events, data_parallel_rank=rank)


def test_disabled_by_default(monkeypatch):
    monkeypatch.delenv("LLMGATEWAY_ENDPOINT", raising=False)
    r = Reporter(load())
    assert not r.active
    r.on_scheduler_stats(0, _stats(1, 2, 0.5, 10, 5))  # no-op
    assert r.worker_snapshot(0) is None


def test_aggregates_stats_and_events(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "tcp://router:5555")
    monkeypatch.setenv("LLMGATEWAY_TOPK", "4")
    r = Reporter(load())

    r.on_scheduler_stats(0, _stats(12, 3, 0.62, 123456, 98765))
    r.on_kv_events(_batch([_stored("h1", "h2"), _stored("h3")]))

    w = r.worker_snapshot(0)
    assert w["load"] == {"running": 12, "waiting": 3, "preemptions": 0}
    assert w["kv_usage"] == 0.62
    assert w["prefill"]["tokens_to_prefill"] == 2048
    assert w["prefix_cache"] == {"query_tokens": 123456, "hit_tokens": 98765}
    assert w["loras"]["running"] == ["lora-a"]

    p = r.prefix_snapshot(0)
    hashes = [x["hash"] for x in p["prefixes"]]
    assert set(hashes) == {"h1", "h2", "h3"}
    assert p["prefixes"][0]["hash"] == "h3"  # most-recent first


def test_topk_is_bounded_lru(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "x")
    monkeypatch.setenv("LLMGATEWAY_TOPK", "2")
    r = Reporter(load())
    r.on_kv_events(_batch([_stored("a"), _stored("b"), _stored("c")]))
    hashes = {x["hash"] for x in r.prefix_snapshot(0)["prefixes"]}
    assert hashes == {"b", "c"}  # "a" evicted (LRU, topk=2)


def test_block_removed(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "x")
    r = Reporter(load())
    r.on_kv_events(_batch([_stored("a"), _stored("b")]))
    r.on_kv_events(_batch([_removed("a")]))
    hashes = {x["hash"] for x in r.prefix_snapshot(0)["prefixes"]}
    assert hashes == {"b"}


def test_drain_kv_tracks_adds_and_removes(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "http://router:8000/state")
    r = Reporter(load())
    r.on_kv_events(_batch([_stored("a", "b"), _stored("c")]))
    r.on_kv_events(_batch([_removed("b")]))
    adds, removes = r.drain_kv()
    assert set(adds) == {"a", "c"}
    assert removes == ["b"]
    # Drained: a second call is empty.
    assert r.drain_kv() == ([], [])


def test_kv_endpoint_derived(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "http://router:8000/state")
    assert load().kv_endpoint == "http://router:8000/kv"
    monkeypatch.setenv("LLMGATEWAY_KV_ENDPOINT", "http://other:9/kv")
    assert load().kv_endpoint == "http://other:9/kv"


def test_waiting_tokens_discounted_by_hit_ratio(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "http://router:8000/state")
    from llm_gateway_vllm import sched_probe

    class R:
        def __init__(self, prompt, computed=0):
            self.num_prompt_tokens = prompt
            self.num_computed_tokens = computed

    class S:
        waiting = [R(1000)]  # waiting full prompt (hits unknown)
        running = [R(200, 100)]  # remaining 100, already cache-adjusted

    r = Reporter(load())
    old = sched_probe._scheduler
    sched_probe._scheduler = S()
    try:
        # 50% hit ratio from the latest prefix-cache interval.
        r.on_scheduler_stats(0, _stats(1, 2, 0.5, queries=1000, hits=500))
        snap = r.worker_snapshot(0)
        # waiting: 1000 * (1 - 0.5) = 500; running remaining 100 -> 600.
        assert snap["prefill"]["waiting_tokens"] == 600
        assert snap["prefill"]["hit_ratio"] == 0.5
    finally:
        sched_probe._scheduler = old


def test_waiting_tokens_unknown_without_probe(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_ENDPOINT", "http://router:8000/state")
    from llm_gateway_vllm import sched_probe

    r = Reporter(load())
    old = sched_probe._scheduler
    sched_probe._scheduler = None
    try:
        assert r.worker_snapshot(0) is None  # no stats yet
        r.on_scheduler_stats(0, _stats(1, 2, 0.5, 10, 5))
        assert r.worker_snapshot(0)["prefill"]["waiting_tokens"] == -1
    finally:
        sched_probe._scheduler = old

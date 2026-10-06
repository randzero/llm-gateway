"""Aggregate high-frequency vLLM stats/events into bounded snapshots.

This is the core of the plugin. Two writers feed it:

* :meth:`on_scheduler_stats` — called every engine iteration (very high
  frequency) via the stat logger; **must not** emit anything per call.
* :meth:`on_kv_events` — called per KV batch via the event publisher.

A bounded, last-value snapshot is produced by :meth:`worker_snapshot` /
:meth:`prefix_snapshot`, which a background task (M3) pushes to the StateStore.

Everything is a no-op unless the plugin is active (``LLMGATEWAY_ENDPOINT`` set).
"""

from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field

from .config import Config, load
from . import sched_probe


@dataclass
class _EngineAgg:
    """Accumulated, bounded state for a single engine (DP) index."""

    running: int = 0
    waiting: int = 0
    preemptions: int = 0
    kv_usage: float = 0.0
    prefill_throughput: int = 0
    tokens_to_prefill: int = 0
    prefix_query_tokens: int = 0
    prefix_hit_tokens: int = 0
    # Smoothed prefix-cache hit ratio (tokens hit / queried), for discounting the
    # waiting backlog whose hits the engine has not computed yet. None until seen.
    hit_ratio: "float | None" = None
    loras_running: list = field(default_factory=list)
    # recency-ordered (LRU) map of block/prefix hash -> tokens cached
    prefixes: "OrderedDict[str, int]" = field(default_factory=OrderedDict)
    # session_id -> cached tokens (from BlockStored.session_id)
    sessions: dict = field(default_factory=dict)


def _get(obj, name, default=None):
    return getattr(obj, name, default) if obj is not None else default


class Reporter:
    """Thread-safe aggregator. One instance per process."""

    def __init__(self, cfg: Config | None = None):
        self.cfg = cfg or load()
        self._lock = threading.Lock()
        self._engines: dict[int, _EngineAgg] = {}
        # Blocks to report to the router's /kv ingest: hash -> is_add (last
        # event wins). Bounded by cfg.kv_batch_max.
        self._pending: "OrderedDict[str, bool]" = OrderedDict()

    @property
    def active(self) -> bool:
        return self.cfg.active

    # -- writers ------------------------------------------------------------

    def on_scheduler_stats(self, engine_index: int, scheduler_stats, iteration_stats=None) -> None:
        """Runs every iteration; keep it cheap and allocation-light."""
        if not self.active or scheduler_stats is None:
            return
        with self._lock:
            a = self._engines.setdefault(engine_index, _EngineAgg())
            a.running = _get(scheduler_stats, "num_running_reqs", a.running)
            a.waiting = _get(scheduler_stats, "num_waiting_reqs", a.waiting)
            a.kv_usage = float(_get(scheduler_stats, "kv_cache_usage", a.kv_usage))
            a.preemptions += int(_get(iteration_stats, "num_preempted_reqs", 0) or 0)

            pcs = _get(scheduler_stats, "prefix_cache_stats")
            if pcs is not None:
                a.prefix_query_tokens = int(_get(pcs, "queries", a.prefix_query_tokens) or 0)
                a.prefix_hit_tokens = int(_get(pcs, "hits", a.prefix_hit_tokens) or 0)
                if a.prefix_query_tokens > 0:
                    ratio = a.prefix_hit_tokens / a.prefix_query_tokens
                    ratio = min(1.0, max(0.0, ratio))
                    a.hit_ratio = ratio if a.hit_ratio is None else 0.7 * a.hit_ratio + 0.3 * ratio

            running_loras = _get(scheduler_stats, "running_lora_adapters")
            if running_loras:
                a.loras_running = list(running_loras.keys())

            details = _get(scheduler_stats, "iteration_details")
            if details is not None:
                a.tokens_to_prefill = int(_get(details, "num_prefill_tokens", a.tokens_to_prefill) or 0)
                a.prefill_throughput = int(_get(details, "num_scheduled_tokens", a.prefill_throughput) or 0)

    def on_kv_events(self, batch) -> None:
        """Consume a vLLM ``EventBatch`` of BlockStored/BlockRemoved."""
        if not self.active or batch is None:
            return
        rank = _get(batch, "data_parallel_rank", 0) or 0
        events = _get(batch, "events", []) or []
        with self._lock:
            a = self._engines.setdefault(rank, _EngineAgg())
            for ev in events:
                # Duck-typed: BlockStored carries token_ids, BlockRemoved does not.
                if not hasattr(ev, "block_hashes"):
                    continue
                if hasattr(ev, "token_ids"):
                    block_size = int(_get(ev, "block_size", 16) or 16)
                    hashes = _get(ev, "block_hashes", []) or []
                    for h in hashes:
                        a.prefixes[h] = block_size
                        a.prefixes.move_to_end(h)  # touch recency
                        self._mark_pending(h, True)
                    self._trim(a)
                    sid = _get(ev, "session_id")
                    if sid:
                        a.sessions[sid] = a.sessions.get(sid, 0) + block_size * len(hashes)
                        self._trim_sessions(a)
                else:
                    for h in _get(ev, "block_hashes", []) or []:
                        a.prefixes.pop(h, None)
                        self._mark_pending(h, False)
            self._trim_pending()

    def _mark_pending(self, hash_value, is_add: bool) -> None:
        # Called under self._lock. str() matches the router's string hash keys.
        key = str(hash_value)
        self._pending[key] = is_add
        self._pending.move_to_end(key)

    def _trim_pending(self) -> None:
        while len(self._pending) > self.cfg.kv_batch_max:
            self._pending.popitem(last=False)

    def drain_kv(self) -> "tuple[list[str], list[str]]":
        """Return (adds, removes) of pending block hashes and clear them."""
        with self._lock:
            if not self._pending:
                return [], []
            adds, removes = [], []
            for h, is_add in self._pending.items():
                (adds if is_add else removes).append(h)
            self._pending.clear()
            return adds, removes

    def push_kv(self, adds: list, removes: list) -> None:
        """Best-effort POST of block add/remove events to the /kv ingest."""
        if not self.active or not self.cfg.kv_endpoint or not (adds or removes):
            return
        try:
            import urllib.request

            body = json.dumps(
                {"worker_id": self.cfg.worker_id, "add": adds, "remove": removes}
            ).encode()
            req = urllib.request.Request(
                self.cfg.kv_endpoint,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5).close()
        except Exception:
            pass

    def _trim(self, a: _EngineAgg) -> None:
        # last-value + bounded N: evict least-recently-used beyond topk.
        while len(a.prefixes) > self.cfg.topk:
            a.prefixes.popitem(last=False)

    def _trim_sessions(self, a: _EngineAgg) -> None:
        while len(a.sessions) > self.cfg.topk:
            smallest = min(a.sessions, key=a.sessions.get)
            a.sessions.pop(smallest, None)

    # -- readers ------------------------------------------------------------

    def worker_snapshot(self, engine_index: int) -> dict | None:
        # Real prefill backlog: the engine's waiting requests' full prompt is
        # discounted by the observed cache-hit ratio (their hits are unknown);
        # running requests are already cache-adjusted. -1 when unreadable.
        bl = sched_probe.backlog()
        with self._lock:
            a = self._engines.get(engine_index)
            if a is None:
                return None
            if bl is None:
                waiting_tokens = -1
            else:
                hit = a.hit_ratio or 0.0
                waiting_full, running_remaining = bl
                waiting_tokens = int(waiting_full * (1.0 - hit)) + running_remaining
            return {
                "load": {"running": a.running, "waiting": a.waiting, "preemptions": a.preemptions},
                "kv_usage": round(a.kv_usage, 4),
                "prefill": {
                    "throughput": a.prefill_throughput,
                    "tokens_to_prefill": a.tokens_to_prefill,
                    "waiting_tokens": waiting_tokens,
                    "hit_ratio": (round(a.hit_ratio, 4) if a.hit_ratio is not None else None),
                },
                "prefix_cache": {"query_tokens": a.prefix_query_tokens, "hit_tokens": a.prefix_hit_tokens},
                "loras": {"running": a.loras_running},
                "ts": time.time(),
            }

    def prefix_snapshot(self, engine_index: int) -> dict | None:
        with self._lock:
            a = self._engines.get(engine_index)
            if a is None:
                return None
            # most-recent first, bounded by topk
            items = list(a.prefixes.items())[::-1]
            return {
                "prefixes": [{"hash": h, "tokens": t} for h, t in items],
                "ts": time.time(),
            }


    def engines(self) -> list[int]:
        with self._lock:
            return list(self._engines.keys())

    def payload(self, engine_index: int) -> dict | None:
        """Build the wire payload the router's /state ingest expects."""
        w = self.worker_snapshot(engine_index)
        if w is None:
            return None
        with self._lock:
            a = self._engines.get(engine_index)
            sessions = (
                [{"session_id": sid, "cached_tokens": n} for sid, n in a.sessions.items()]
                if a
                else []
            )
        worker_state = {"v": 1, "worker_id": self.cfg.worker_id, "engine_index": engine_index}
        worker_state.update(w)
        return {"worker_id": self.cfg.worker_id, "worker_state": worker_state, "sessions": sessions}

    def push(self, payload: dict) -> None:
        """Best-effort POST; never raises into the engine."""
        if not self.active:
            return
        try:
            import urllib.request

            req = urllib.request.Request(
                self.cfg.endpoint,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            urllib.request.urlopen(req, timeout=5).close()
        except Exception:
            pass


class Pusher(threading.Thread):
    """Background thread that periodically pushes snapshots to the router."""

    def __init__(self, reporter: Reporter):
        super().__init__(daemon=True, name="llm-gateway-pusher")
        self.reporter = reporter

    def run(self) -> None:
        r = self.reporter
        while True:
            time.sleep(r.cfg.snapshot_interval_s)
            for engine_index in r.engines():
                payload = r.payload(engine_index)
                if payload is not None:
                    r.push(payload)
            adds, removes = r.drain_kv()
            if adds or removes:
                r.push_kv(adds, removes)


_reporter: Reporter | None = None
_pusher: Pusher | None = None


def get_reporter(cfg: Config | None = None) -> Reporter:
    global _reporter
    if _reporter is None:
        _reporter = Reporter(cfg)
    return _reporter

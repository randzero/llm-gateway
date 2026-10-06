# llm-gateway-vllm

Optional **vLLM engine plugin** for [`llm-gateway`](../../). It lets a stock vLLM
instance report real KV / load / session state to the gateway's state layer.

It is a **no-op unless `LLMGATEWAY_ENDPOINT` is set**, so installing it never
changes engine behaviour until you opt in.

## How it plugs in (no fork, no patch)

The plugin registers itself through two standard vLLM entry points:

| entry-point group | value | what vLLM does |
|---|---|---|
| `vllm.general_plugins` | `llm_gateway_vllm:register` | calls it at startup |
| `vllm.stat_logger_plugins` | `llm_gateway_vllm.stats:GatewayStatLogger` | instantiates per engine, calls `record()` every step |

* `register()` registers a KV-event **publisher** via
  `EventPublisherFactory.register_publisher("llm-gateway", ...)`.
* `GatewayStatLogger` receives `SchedulerStats` / `IterationStats` each iteration.

## Install

```bash
pip install llm-gateway-vllm        # into the same env as vLLM
```

## Enable

```bash
export LLMGATEWAY_ENDPOINT=http://stated:8081/state      # required to activate
#   (in single-instance mode, point at the router's http://<router>:8000/state)
# optional
export LLMGATEWAY_SNAPSHOT_INTERVAL_S=1.0
export LLMGATEWAY_TOPK=4096
export LLMGATEWAY_PUBLISHER_NAME=llm-gateway
```

Also turn on vLLM's KV events (or let the plugin do it):

```bash
vllm serve <model> \
  --kv-events-config '{"enable_kv_cache_events": true, "publisher": "llm-gateway"}'
```

### Real prefix routing (optional)

For KV-cache-aware routing on **real** block hashes (design §13) the router needs
two things the plugin can provide:

```bash
# 1) per-block cache add/remove -> the state service's inverted index (/kv)
#    (on automatically once LLMGATEWAY_ENDPOINT is set)
export LLMGATEWAY_KV_ENDPOINT=http://gateway:8000/kv   # default: derived from endpoint
```

2) an engine-side hashing endpoint (request body -> `{"hashes":[...]}`), either:

**Preferred — API-server route (recent vLLM).** A `vllm.endpoint_plugins` entry
point adds `POST /v1/chat_cache/hashing` to the API server. It reuses the
server's own tokenization (the `OnlineRenderer`), so token ids — and therefore
hashes — match the engine exactly. Endpoint plugins are off by default and must
be allowlisted:

```bash
export VLLM_PLUGINS=llm_gateway_hashing     # the entry point name
export LLMGATEWAY_BLOCK_SIZE=16             # must match the engine
```

**Fallback — standalone hashing server (older vLLM).** A small HTTP server the
plugin starts, reusing vLLM's own hash function but re-tokenizing with the
model's HF tokenizer (close, not always byte-exact on the chat template):

```bash
export LLMGATEWAY_HASH_PORT=8100        # 0 = off; only one process wins the bind
export LLMGATEWAY_HASH_MODEL=<model>
```

Then point the router at whichever you enabled:

```bash
llm-gateway --prefix-index redis --redis-addrs r:6379 \
  --hash-endpoint http://<worker>:8100/            # standalone server
#  ...or http://<worker>:8000/v1/chat_cache/hashing  # API-server route
```

## What it reports

Bounded, **last-value** snapshots (not delta streams) per engine, POSTed to the
router's `POST /state` endpoint on `LLMGATEWAY_SNAPSHOT_INTERVAL_S`:

* load (running/waiting/preemptions), kv_cache_usage
* prefix_cache query/hit tokens, prefill tokens/throughput
* **waiting_tokens** — the engine's real prefill backlog (queued tokens), see below
* LoRA adapters (running)
* top-N hot prefix hashes (from KV `BlockStored`/`BlockRemoved`)
* per-session cached tokens (from `BlockStored.session_id`) — real session
  affinity without a tokenizer

Each POST carries a TTL on the router side, so a dead plugin's state expires.

### waiting_tokens (best-effort scheduler probe)

`SchedulerStats` exposes only *counts*; the router wants the queue **depth in
tokens** to estimate wait. The plugin reads it from the scheduler's `waiting`
queue (plus running requests' un-computed prompt remainder) by wrapping
`Scheduler.__init__` once — `register()` runs before the scheduler is built.
Waiting requests' prefix-cache hits are not yet computed by the engine, so their
full prompt is discounted by the observed hit ratio (from `prefix_cache_stats`);
running requests are already cache-adjusted. This is coupled to vLLM internals
and is **best-effort**: if anything changes it reports `-1` and the router falls
back to a count-based estimate. It is reported per process; the value is the
engine's own backlog.

## Design notes

* `record()` runs **every iteration** — the plugin only **aggregates**; it never
  emits per step.
* Prefix tracking is **bounded (top-N, LRU)** — size is `N × entry × workers`,
  independent of event volume.
* Written to be **idempotent** (vLLM may load plugins in several processes) and
  **asynchronous/bounded** so it never blocks the engine.

## Test

```bash
pip install -e ".[dev]"
pytest
```

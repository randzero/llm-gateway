# llm-gateway

**English** | [中文](README.zh-CN.md)

A general, **engine-agnostic, state-aware** gateway for LLM inference: an
OpenAI/Anthropic-compatible router + an **optional** engine plugin + a pluggable
state layer.

> **"Install the plugin and get high fidelity; skip it and it still works.
> Single instance needs no infra; a cluster shares state automatically."**

## Architecture

![llm-gateway architecture](docs/architecture.svg)

*Source: [`docs/architecture.dot`](docs/architecture.dot) — regenerate with
`dot -Tsvg docs/architecture.dot -o docs/architecture.svg`.*

- **`llm-gateway`** — the router: proxy + policies + discovery + health + tracing.
  Stateless; run as many replicas as you like.
- **`llm-gateway-vllm`** — optional engine plugin: reports load/session state to
  `/state` and per-block KV events to `/kv`, and serves the block-hash endpoint.
- **`stated`** — the shared state service (cluster): fronts Redis for both the
  state plane and the prefix index. Routers and plugins talk to it, not Redis.
- **Redis** — the durable store behind `stated`.

**Single-instance mode needs none of the middle layer** — the router runs on its
own with in-process state and no Redis.

## Why

Existing routers mostly approximate:

- **Rust gateways** (`vllm-project/router`, `sgl-model-gateway`) — great load
  balancing, but KV awareness is an **approximation** (a per-worker radix tree
  built from routing history), and **each router replica only sees a fragment
  of the traffic** → inconsistent views across a router cluster.

`llm-gateway` addresses this: **real state when available, graceful fallback
when not**, and a **shared state layer** so every router replica sees the same
view.

## Quick start

Build and run the router in front of one or more OpenAI-compatible workers:

```bash
go build -o llm-gateway ./cmd/gateway
./llm-gateway --workers http://w1:8000,http://w2:8000 --policy power_of_two
# then point your client at http://<gateway>:8000
```

Policies: `round_robin`, `random`, `power_of_two`, `consistent_hash`,
`cache_aware` (prefix affinity via a router-side approximation, no plugin
required), and `least_latency` (one objective — minimize estimated
time-to-first-token by folding cache saving, load and throughput onto one scale;
generalizes cache-aware and least-loaded). Requests are routed within their
**model's pool** (workers are grouped by model/service — see Discovery below).
Endpoints: `/v1/*` (proxied), `/metrics` (Prometheus text), `/healthz`.

### Discovery (pluggable)

Workers come from a `Discovery` backend (`--discovery`):

| Backend | Flags | Notes |
|---|---|---|
| `static` (default) | `--workers url[,url...]` or `model=url[,...]` | no infra |
| `consul` | `--consul-addr` `--consul-services` | catalog HTTP API, **no SDK**; service name == model |
| `dns` | `--dns-domain` `--dns-services` | SRV `_<model>._tcp.<domain>`, stdlib |
| `k8s` | `--k8s-selector` `--k8s-namespace` `--k8s-port-name` | EndpointSlice over the API (service-account), **no client-go** |

```bash
./llm-gateway --discovery consul \
  --consul-addr http://127.0.0.1:8500 --consul-services qwen,llama
# or DNS:  --discovery dns --dns-domain svc.cluster.local --dns-services qwen
# or k8s:  --discovery k8s --k8s-selector app=vllm --k8s-port-name http
```

### Tracing

Point `--otel-endpoint` at an OTLP/HTTP collector to get one span per request
(`llm.model`, `llm.worker`, status), with W3C `traceparent` propagation:

```bash
./llm-gateway --workers http://w1:8000 \
  --otel-endpoint http://localhost:4318/v1/traces
```

### State store (pluggable)

The state plane is a last-value KV + watch (`--state-store`):

| Backend | Notes |
|---|---|
| `inproc` (default) | single instance; no infra |
| `consul` | cluster mode: KV + blocking-query watch + session TTL; point every replica at the same Consul → **consistent views** |

```bash
./llm-gateway --discovery static --workers http://w1:8000 \
  --state-store consul --consul-addr http://127.0.0.1:8500
```

The two backends above are for single-instance / simple setups where the router
holds state itself. In a real cluster, the state plane (and the prefix index) are
served by the **state service** instead — see the next section.

### Cluster state: the state service (`stated`)

In a cluster, run the **state service** — it fronts Redis and serves both the
state plane (worker/session scalars) and the prefix index (block hash → workers).
Routers and engine plugins talk to it, **not to Redis directly** ([design §13](docs/design.md)):

```
plugin ──/state, /kv──►  stated  ──►  Redis
router ──/match────────────────►  stated
```

```bash
# the state service (one per cluster; shard by running several + Redis Cluster)
stated --redis-addrs 127.0.0.1:6379 --listen :8081

# the router in cluster mode
./llm-gateway --workers http://w1:8000 --policy cache_aware \
  --state-server http://stated:8081 \
  --hash-endpoint http://w1:8000/v1/chat_cache/hashing
```

- **Endpoints**: `/state` + `/kv` (plugin writes: worker/session scalars and
  per-block cache add/remove) and `/match` — the router's **one call per
  request**: it returns the longest-prefix merge, the session's cached tokens,
  **and** the candidates' reported load. The router keeps no local state.
- **Request hashes**: the router still needs each request's block-hash chain —
  from an engine hashing endpoint (`--hash-endpoint`) and/or a pre-computed
  `--hash-header` (default `X-KV-Hashes`). The plugin provides that endpoint: an
  API-server route via `vllm.endpoint_plugins` (recent vLLM, exact tokenization;
  allowlist with `VLLM_PLUGINS=llm_gateway_hashing`) or a standalone hashing
  server (`LLMGATEWAY_HASH_PORT`).

Single-instance (dev) needs no service — the router runs in-proc.

Optional — enable real state reporting on the vLLM side (point it at `stated`):

```bash
pip install ./plugins/vllm
export LLMGATEWAY_ENDPOINT=http://stated:8081/state
```

See [`plugins/vllm/README.md`](plugins/vllm/README.md).

## Design

See [`docs/design.md`](docs/design.md) for the full system design (architecture,
state protocol, policies, single-instance vs cluster modes, roadmap).

## Layout

```
cmd/gateway/          Go router entry point
cmd/stated/           Go state-service entry point (fronts Redis for a cluster)
internal/control/     worker registry + health (per-model pools)
internal/discovery/   Discovery interface + static / consul
internal/policies/    Policy interface + round_robin/random/power_of_two/consistent_hash
internal/prefix/      inverted block-hash Index + longest-prefix match (inproc/redis)
internal/proxy/       OpenAI/Anthropic proxy + SSE streaming
internal/resilience/  circuit breaker
internal/state/       StateStore (inproc/consul) / StateProvider / versioned schema
internal/stateserver/ the state service (state plane + prefix index, fronting Redis)
internal/stateclient/ router-side client of the state service
internal/otel/        minimal OTLP/HTTP tracing + W3C traceparent (stdlib, no SDK)
internal/observability/  Prometheus-text metrics (stdlib, no deps)
plugins/vllm/         optional vLLM engine plugin (Python)
```

## Status

M1 (router data plane), M2 (router-side cache-aware routing + graceful
degradation), and M3 (real-state ingest) are implemented and tested. Workers are
grouped by model/service; discovery (static/Consul) is pluggable.

M4 (design §13) is **implemented**: the inverted block-hash index
([`internal/prefix`](internal/prefix)) with a hot-prefix cache and exponential
sampling; a **shared state service** ([`internal/stateserver`](internal/stateserver)
+ [`cmd/stated`](cmd/stated)) that fronts Redis for both the state plane and the
prefix index — routers and plugins talk to it, not Redis; the router-side client
([`internal/stateclient`](internal/stateclient)); and the vLLM plugin reports
per-block add/remove plus an engine-side **hashing endpoint**.

Not verified end-to-end against a live vLLM/real Redis yet (the plugin and the
service are unit-tested; the router was smoke-tested against a state service on
an in-process Redis). See the roadmap in the design doc.

## License

Apache-2.0 (see `LICENSE`).

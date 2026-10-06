# llm-gateway — 系统设计文档

> 版本：v0.1（设计稿）· 状态：草案
> 定位：一个**引擎无关、状态感知、可集群扩展**的 LLM 网关（router + 可选引擎插件 + 可插拔状态层）。

---

## 1. 背景与目标

### 1.1 背景

LLM 推理规模化后，多个推理实例（vLLM / SGLang / …）之前需要一个 router 承担：负载均衡、**前缀/KV 缓存感知**、会话亲和、P/D 分离、可靠性与可观测。现有三类实现各有取舍：

| 实现 | 优点 | 痛点 |
|---|---|---|
| `vllm-project/router`（Rust） | LB 策略全、WASM 中间件、program scheduling、gRPC | KV 感知是**近似树**（按路由历史猜）；**多副本时每个副本只有局部视图** |
| `sgl-model-gateway`（Rust） | 功能最全（原生解析栈 / MCP / 多模型） | 绑 SGLang 运行时；KV 同样是近似 |

两家的共性问题：**KV 感知是近似（不准）**；且 **router 多副本下的状态一致性**基本没人正面解决。

### 1.2 目标

- **G1 通用**：引擎无关（先 vLLM，协议预留 SGLang）；OpenAI / Anthropic 兼容；不绑私有协议。
- **G2 傻瓜式**：`pip install` 即用；引擎插件**可选**——装了就增强，不装也能跑（近似回退）。
- **G3 状态感知且可扩展**：支持 **router 集群**，所有副本感知同一份状态；**不退化成中心单点**。
- **G4 优雅降级**：有权重状态时用真实 KV/负载；无则自动回退到近似，**逐 worker 降级**。

### 1.3 非目标（Non-Goals）

- 不做模型服务本身、不做训练/校准、不做推理调度逻辑。
- 不追求 Rust 级极限吞吐（v1 先正确、通用、易贡献；热点可后续下沉）。
- 不引入任何私有/内部依赖与协议。

---

## 2. 差异化定位（一句话）

> **"装了插件就高保真，不装也能跑；单实例省依赖，集群自动共享状态。"**

- vs `vllm-router` / `sgl-model-gateway`：它们**只有近似**且**副本间视图不一致**；我们提供**可插拔的真实状态**与**集群一致的共享视图**，同时保留近似回退，并保持**引擎无关 + router 无状态化**。

---

## 3. 总体架构

```
        ┌──────────────┐   OpenAI / Anthropic (SSE)
 client │  ...         │───────────────────────────────┐
        └──────────────┘                                ▼
                                          ┌───────────────────────────────┐
                                          │      Router 集群 (无状态, N 副本) │
                                          │  ┌ 控制面 worker registry/health │
                                          │  ├ 状态面 StateProvider          │
                                          │  ├ 策略层 Policy (LB / KV-aware) │
                                          │  ├ 数据面 OpenAI 代理 + SSE       │
                                          │  └ 可靠性: retry/CB/ratelimit    │
                                          └──────────────┬────────────────┘
                                                         │ 转发
                        ┌────────────────────────────────┼────────────────────────────┐
                        ▼                                ▼                            ▼
                  ┌───────────┐                    ┌───────────┐                ┌───────────┐
                  │ vLLM +插件 │                    │ vLLM +插件 │      ...       │ vLLM(裸)   │
                  └─────┬─────┘                    └─────┬─────┘                └───────────┘
                        │  发布状态 (可选)                 │
                        └───────────────┬─────────────────┘
                                        ▼
                        ┌───────────────────────────────┐
                        │   状态层 StatePlane            │
                        │  单实例: 进程内                 │
                        │  集群:   Consul / Redis        │
                        └───────────────────────────────┘
                          ▲  所有 router 副本订阅/读取同一份
                          └──────────────────────────────────
```

**三个交付物**：

1. **`llm-gateway`**（router 进程）── 数据面代理 + 策略 + 状态面；**无状态化**，可任意多副本。
2. **`llm-gateway-vllm`**（引擎插件，Python）── 通过 vLLM `general_plugins` 自动加载，注册 KV 事件 publisher + 周期快照；**可选、可 no-op**。
3. **状态层 StatePlane**── 单实例时进程内；集群时外部（**Consul；Redis 计划中**），对 router 是 **Get(当前值) + Watch(变更)** 接口（见 §12）。

---

## 4. 组件设计

### 4.1 Router

| 子系统 | 职责 | 关键点 |
|---|---|---|
| **控制面** | worker 注册、健康检查、模型/角色元数据 | 来源：`Discovery` 抽象——静态列表 / Consul（k8s/DNS 计划中）；每个发现后端产出 `Endpoint{URL,Model,...}`，`Registry.Sync` **按 model 分组建池**；**健康=轮询 `/health` + 熔断状态** |
| **状态面** | 感知 worker 状态（负载 / KV / 会话） | `StateProvider` 抽象：`PluginState`（Watch StateStore）优先，无则 `ApproximateState`（本地近似树） |
| **策略层** | 选 worker | `Policy` 接口 + 注册表；见 §6.3 |
| **数据面** | OpenAI / Anthropic 兼容代理、SSE 流式 | 透传 header（session/user/tenant）、DP rank；SSE 原样转发 |
| **可靠性** | retry(抖动) / 熔断 / 令牌桶 / 有界队列 | 熔断打开或健康差的 worker 直接排除 |
| **可观测** | Prometheus + OpenTelemetry | 指标：请求/选择/命中/RPC/熔断/队列 |

**关键设计：router 自身无状态**。所有共享状态都放在 StatePlane；router 内存只放"本地缓存 + 近似树"，因此可随意扩/重启。

### 4.2 引擎插件 `llm-gateway-vllm`

**只用开源 vLLM 的一等公民扩展点，不 fork、不 patch。** 插件是一个 Python 包，声明两个 entry point：

```toml
[project.entry-points."vllm.general_plugins"]
llm_gateway = "llm_gateway_vllm:register"                       # 自动加载 + 注册 KV publisher

[project.entry-points."vllm.stat_logger_plugins"]
llm_gateway_stats = "llm_gateway_vllm.stats:GatewayStatLogger"  # 每步回吐 SchedulerStats
```

`pip install` 到**标准 vLLM** 后由 `load_general_plugins()` 与 `load_stat_logger_plugin_factories()` 自动加载。

**数据来源映射（所需字段 → 标准 vLLM 的数据源，全部可得）：**

| 字段 | 来源 |
|---|---|
| running / waiting / preemptions | `SchedulerStats.num_running_reqs / num_waiting_reqs` |
| `kv_cache_usage` | `SchedulerStats.kv_cache_usage` |
| prefix query / hit tokens | `SchedulerStats.prefix_cache_stats` |
| prefill 吞吐 / 待 prefill token | `IterationStats`（每步调度 token 数）|
| KV block 增/删 | KV events（`EventPublisherFactory.register_publisher`）|
| lora 适配器 | `SchedulerStats.running/waiting_lora_adapters` |
| model / dp_rank / P/D role / downstream | 部署配置（env/config）|

**纪律（否则会踩坑）：**

- **聚合，不逐事件**：`StatLogger.record()` 是**每迭代**回调（超高频）→ 插件**去抖/聚合**后再写状态，**绝不逐迭代上报**。
- **未配置即 no-op**：读 `LLMGATEWAY_ENDPOINT`；**未设 = 完全不动作**（不注册 publisher、不开 KV 事件），装插件不改变引擎行为。
- **幂等**：可能被多次加载（多进程/rank），`register()` 可重入。
- **多 engine/DP**：stat logger 按 engine 实例创建 → 每个都上报，带 `engine_index`/`dp_rank`。
- **KV 事件需开启**：`enable_kv_cache_events` 默认关；插件在配置了 endpoint 时替你打开。
- **版本容错**：`SchedulerStats` 字段跨版本可能变 → `getattr(...)` 容错 + 声明最低 vLLM 版本。
- **异步 + 有界**：上报走独立任务/队列，绝不阻塞引擎。

### 4.3 状态层 StatePlane

| 模式 | 实现 | 适用 |
|---|---|---|
| **单实例** | 进程内（router 自己聚合） | dev / 小规模，零依赖 |
| **集群（默认推荐）** | **Consul（已落地）/ Redis（计划中）** | 生产，多副本 |

抽象为 **`StateStore`**（`Get`/`Watch`/`Put`+TTL，见 §12）——插件与 router 都只认这个接口，**不关心底层是进程内还是 Consul/Redis**。

---

## 5. 状态协议（版本化 Schema）

> 原则：**只报聚合摘要，不报原始流**；字段可演进（`v`）。

```jsonc
// WorkerState —— 周期快照（低频，~1s）；字段对齐标准 vLLM 的 SchedulerStats/IterationStats
{
  "v": 1,
  "worker_id": "w1",                 // 稳定标识
  "endpoint": "http://10.0.0.1:8000",
  "model": "qwen3-...", "dp_rank": 0, "engine_index": 0, "pd_role": "decode",
  "load": { "running": 12, "waiting": 3, "preemptions": 0 },
  "kv_usage": 0.62,                  // 0..1
  "prefill": { "throughput": 10000, "tokens_to_prefill": 2048 },
  "prefix_cache": { "query_tokens": 123456, "hit_tokens": 98765 },
  "loras": { "running": ["lora-a"], "waiting": [] },
  "ts": 1699999999.123
}

// PrefixState —— KV 前缀摘要（中频），做 KV-aware 路由的关键
{
  "v": 1, "worker_id": "w1", "ts": ...,
  // top-N：该 worker 上"最热的 N 段缓存前缀"（按 LRU/频次挑，有界 N）
  "prefixes": [ { "hash": "ab12...", "tokens": 8192 }, { "hash": "cd34...", "tokens": 4096 } ],
  "hit_tokens_total": 123456
}

// SessionState —— 会话级缓存（可选，低频），对齐"真实会话缓存"精度
{ "v": 1, "worker_id": "w1", "session_id": "s-abc", "cached_tokens": 4096, "ts": ... }
```

**`PrefixState.top-N` 语义**：vLLM 前缀缓存按 block 哈希存，单 worker 可有成千上万条。
`PrefixState` 只保留**最值得知道的 N 条**（默认 ~4096，可配），按 **LRU/频次**挑选——即"最热的 N 段缓存前缀"，够路由用。
- **调优**：暴露 `coverage = top-N 捕获的命中 token / 全量命中 token`，把 N 调到目标覆盖率即可；实务上 **N≈1k–10k 到饱和拐点**（>95% 覆盖），再往上为计费级、不建议无上限。
- **不膨胀**：因是 last-value（覆盖写），大小 = `N × 条目大小 × worker 数`，**与事件量无关**；N=10k×~64B×100 worker ≈ 64MB，可控。

**字段来源**（详见 §4.2）：`load/kv_usage/prefill/prefix_cache/loras` 均来自 vLLM `SchedulerStats`/`IterationStats`（插件 stat logger）；`prefixes` 来自 KV 事件聚合。

**来源映射**：`PrefixState` ← vLLM `BlockStored` 事件聚合；`WorkerState` ← 周期快照；`SessionState` ← 快照（可选）。

---

## 6. 关键机制

### 6.1 逐 worker 优雅降级

- 某 worker **有** PluginState → 用真实负载/KV 做决策；
- 某 worker **无**（裸 vLLM / 上报中断）→ 对**该 worker** 回退近似树。
- 两者可**混部**，整体永不"因为没插件而不能用"。

### 6.2 单实例 vs 集群

- **自动判定**：配置里 router 副本数 = 1 → 进程内 StatePlane；> 1 → 要求共享 StatePlane。
- 集群模式下 router **无状态**，所有副本订阅同一 StatePlane → **视图一致**。
- （此问题对"近似树"同样存在：多副本 = 局部视图。**共享层同时修复近似与 push 两种模式的集群一致性。**）

### 6.3 决策输入与策略

**决策输入**：
| 输入 | 来源 |
|---|---|
| 健康 / 熔断状态 | 控制面（`/health` + CB） |
| 负载（running/waiting）| StatePlane `WorkerState`，回退本地计数 |
| 前缀命中 | StatePlane `PrefixState`，回退**近似树**（字符） |
| 会话/用户/租户 key | 请求 header（`X-Session-ID`/`X-User-ID`/`X-Tenant-ID`）或 body |
| 模型 | 请求 model → per-model 策略/池 |
| DP rank / P/D 角色 | worker 元数据 |

**策略（Policy 接口，可注册）**：
| 策略 | 用到的输入 | 说明 |
|---|---|---|
| `round_robin` / `random` | 健康 | 基础 |
| `power_of_two` | 健康 + 负载 | 随机取二，选负载低的 |
| `consistent_hash` | 会话 key | 一致性哈希环（虚拟节点），会话粘性 |
| `cache_aware` | 前缀命中 + 负载 | 命中率 ≥ 阈值走命中，否则取最小负载 |
| `least_latency` | 前缀命中 + 负载 + **真实积压** + 吞吐 | **单一目标**：最小化预估 TTFT `(排队token + P − Cached)/Throughput`；排队 token 优先用**引擎真积压**，否则退化为 `Load·P` 估计；并列随机摊平 |

> **排队 token 怎么来**（`least_latency` 的核心）：插件在引擎进程内**读真实队列**（`scheduler.waiting`/`running`），拼成
> `waiting_full·(1 − 命中率) + running_未算`——
> `running` 那半用 `num_prompt_tokens − num_computed_tokens`（**已含命中**，`num_computed_tokens` 就是缓存命中的块）；
> `waiting` 那半引擎**还没做缓存查找**、命中未知，故用**历史命中率**（`prefix_cache_stats` 的 hits/queries，动态 EMA）**打折**。
> 于是：**完成状态真实**（读引擎队列，请求真完成即真移除）+ **缓存盲区动态补偿**，且**无新写路径、不改引擎**（best-effort，读不到退 `Load·P`）。
> 这是对"缓存优先（`cache_aware`）"与"最小负载"的**统一**——因为它把缓存、负载、吞吐放在同一尺度上比。

> 负载与吞吐来自引擎上报（经状态服务 `/match` 一并返回，见 §13.7）。

---

## 7. 部署形态

- **单机**：`llm-gateway --discovery static --workers http://w1,http://w2` → 立即可用（无插件、无状态层依赖）。
- **集群**：起 **N 个 router + 状态服务 `stated`（前置 Redis）**；router 用 `--state-server` 指向它，worker 侧 `pip install llm-gateway-vllm` + `LLMGATEWAY_ENDPOINT=<stated>`。**router 与插件都只跟 `stated` 打交道，不直连 Redis**（见 §13）。
- **状态服务**（`stated`，集群必需）：前置 Redis，同时管状态面与前缀索引；端点 `/state`、`/kv`（插件写）、`/match`（router 每请求一次，返回前缀合并 + 会话）。
- **服务发现**（可插拔，`--discovery`）：
  - `static` —— `--workers url[,url...]` 或 `model=url[,...]`；
  - `consul` —— `--consul-addr` + `--consul-services`（服务名即模型名），走 Consul catalog HTTP API，**零 SDK 依赖**；
  - `dns` —— SRV `_<model>._tcp.<domain>`，标准库；
  - `k8s` —— `--k8s-selector` 走 EndpointSlice API（service-account），**不引 client-go**。
- **共享状态存储**（可插拔，`--state-store`）：`inproc`（单实例）/ `consul`（集群，KV + blocking-query watch + session TTL）；集群默认经 **状态服务 `stated`**（前置 Redis）。所有 router 副本 watch 同一份 → 集群视图一致。

---

## 8. 路线图

| 阶段 | 内容 | 产出 |
|---|---|---|
| **M1** | router 数据面：OpenAI 代理 + SSE + 静态 worker + `round_robin/random/power_of_two/consistent_hash` + health/熔断 + Prometheus + **按 model/service 分组的路由池** | **能替代 vLLM 做 LB，零插件** |
| **M2** | `StateStore`（Get/Watch）/ `StateProvider` 抽象 + `cache_aware_approx` 近似树 + **逐 worker 降级** | 前缀亲和 + 可插拔骨架 |
| **M3** | **vLLM 插件**（general_plugins + register_publisher + 快照 POST 到 `/state`）+ 单/簇两种 StatePlane | **差异化核心** |
| **M4** | 真实前缀路由（§13）+ **状态服务 `stated`**（前置 Redis，`/match` 一次拿全：前缀 + 会话 + **负载**，负载已接入策略、跨副本可见）+ **DNS/k8s 发现** + **Anthropic 请求解析** + **OTel** 均已实现；待做：真机端到端验证与生产化打包 | 生产化 |

---

## 9. 风险与合规

- **版本漂移**：依赖 vLLM 的 `general_plugins` / `kv_events` 内部 API，跨版本需维护（可借机 upstream 稳定接口）。
- **事件高频**：KV 事件量大 → 必须**聚合 + 有界 + 异步**，否则拖慢引擎。
- **一致性**：StatePlane 为**最终一致**；需定义"过期即回退近似"。
- **原创性**：全部原创实现、引擎无关；设计与公开项目（vllm-router / sgl-model-gateway）的对照仅用于说明定位，不复制其代码。

---

## 10. 模块布局（拟）

```
llm-gateway/
├── docs/design.md              # 本文
├── cmd/gateway/main.go         # Go: router 入口
├── cmd/stated/main.go          # Go: 状态服务入口（前置 Redis）
├── internal/                   # Go: router 实现
│   ├── control/                #   registry / health（按 model 分组建池）
│   ├── discovery/              #   Discovery 接口 + static / consul
│   ├── state/                  #   StateStore(inproc/consul) / StateProvider / 近似树
│   ├── prefix/                 #   倒排 Index(inproc/redis) + 热缓存 + 最长前缀匹配
│   ├── stateserver/            #   状态服务（状态面 + 前缀索引，前置 Redis）
│   ├── stateclient/            #   router 侧的状态服务客户端
│   ├── policies/               #   Policy 接口 + 各策略
│   ├── proxy/                  #   OpenAI/Anthropic 代理 + SSE
│   ├── resilience/             #   retry / circuit breaker / rate limit
│   └── observability/
├── plugins/vllm/               # Python: vLLM 插件（可选安装）
│   ├── pyproject.toml          #   声明 vllm.general_plugins entry point
│   └── src/llm_gateway_vllm/   #   plugin / publisher / transport
├── tests/
├── go.mod
└── README.md
```

---

## 11. 技术选型

| 组件 | 语言 | 理由 |
|---|---|---|
| **Router（主项目）** | **Go（推荐）** | 网关是 **I/O 密集、高并发、低单请求 CPU** 的场景，Go 的 `net/http` + goroutine 正是为此而生；**生态天然对齐**：`etcd`、`NATS`、`k8s`、`Prometheus`、`OTel` 都有**一等公民的 Go 库**——我们的"状态层/发现"几乎全靠它们；**单一静态二进制**，部署/容器最简单（利于"傻瓜式"）；**上手/贡献门槛最低**，最有利于拉外部贡献者 |
| vLLM 插件 | **Python（必须）** | vLLM 是 Python；插件要走 `vllm.general_plugins` + `register_publisher`，只能 Python |
| 备选：Router | **Rust** | 若要**极致性能 / 对标现有 Rust 竞品 / 回推上游**，选 Rust（tokio）。代价：迭代慢、贡献门槛高、"同样功能写更多代码" |
| 备选：Router | **C++** | 仅当目标是做 **Envoy 扩展 / Gateway API**（Envoy 是 C++）。否则不推荐——构建/维护成本最高、贡献者池最小 |

**结论：Router 用 Go，插件用 Python。** 这是一个常见且自洽的组合（服务用 Go、引擎侧插件用 Python）。Go 对"网关"是**正确的工具**，且在"通用 + 傻瓜式 + 易贡献"三个目标上全面占优；只有在明确以"压过 Rust 竞品性能"为目标时才换 Rust。

> 性能担心可打消：网关上 Go 的 GC 停顿是微秒级、可忽略；真正的瓶颈永远在**下游推理引擎**，不在网关的 CPU。Go 单机轻松扛十万级连接。

---

## 12. 存储抽象（StateStore：last-value + watch）

**不推荐纯 pub/sub 作为状态主存。** router 对状态的需求是"**我现在就要当前值 + 之后的变化**"，而不是"给我一串事件流"。纯 pub/sub 缺 retained（重启/新副本冷启动盲区）、无法查当前值、无 TTL/租约（死 worker 不自动过期）、且 at-most-once。因此抽象成 **StateStore**（状态即键值 + watch），这是 **k8s controller 模式**：

```go
// StateStore：last-value KV + watch（"读什么 + 怎么感知变化"）
type StateStore interface {
    Get(key string) (val []byte, rev int64, ok bool)      // 当前值：新/重启副本立即可得
    Watch(prefix string) (<-chan Event, error)             // 变更推送（增量）
    Put(key string, val []byte, ttl time.Duration) error   // 插件侧写入 + 心跳租约
}
// 事件：KeyPut / KeyDelete / Expired

// 键布局：workers/<id>/state、workers/<id>/prefix、sessions/<sid>
```

- **后端**：**in-proc**（单实例：map + 条件变量）| **Consul KV**（已落地：blocking-query watch + session TTL）| **Redis**（计划中：原生 per-key TTL，一个 Redis 可同时当前缀索引）| **NATS JetStream KV**（计划中）。
- **Consul 注意点**：Consul KV **无原生 per-key TTL**，故所有键共用一个 session TTL（取最近一次 `Put` 的 TTL）；**Redis 的原生 per-key TTL 更贴合**，故状态面的后续首选是 Redis（可与前缀索引共用一个实例）。
- **每 worker 一个 key + TTL/租约** → 死掉的 worker **自动过期**，无需显式注销。
- **热路径不读网络**：router 用 `Watch` 维护**本地物化视图**，路由时读本地内存。
- **高频 KV 事件先聚合**：插件把 block 事件聚合成 `top-K 前缀 hash + 版本` 摘要，**低频 `Put`**；**不把原始事件灌进 store**。
- **注意键布局**：**正排**（`worker → 前缀列表`）会随缓存块数增长成大 key；**真实前缀索引用倒排**（`prefix:<hash> → worker`，多小 key）放 Redis，见 **§13**。
- **纯 pub/sub（ZMQ / NATS core / Redis pub-sub）降级为可选的事件通道**，不作状态主存。
- **单实例 vs 集群**只是"用哪个 StateStore 实现"：单实例 → `in-proc`；集群 → `Consul`/`Redis`（所有 router 副本 `Watch` 同一份 → 视图一致）。

---

## 13. 前缀感知路由：设计空间与选型

> 本章是与公开项目（vllm-router / sgl-model-gateway）对照得出的设计结论；核心取舍是：**要真实前缀匹配，就得拿到"请求的 block hash"并把它放进一个"不产生大 key 的索引"**。

### 13.1 一个"不可能三角"

要在**多副本 router** 上做**真实（非近似）**前缀路由，三个诉求无法同时满足：

- **精确**：准确知道每个 worker 缓存了哪些 block；
- **有界**：共享存储里不出现大 key / 高基数；
- **通用 KV**：用现成的小数据 KV（如 Consul）承载。

信息论上，精确回答一个 K 元素集合的成员查询至少需要 ~K bit 的信息——**要么把集合放进专用/分片索引，要么让集合根本不进共享层**。所以优雅的方向不是"把大集合优雅地塞进 KV"，而是**换问题**。

### 13.2 "精确"与"无漏报"的分野

- **精确（exact）**：假阳、假阴皆零；
- **无漏报（no false-negative）**：绝不漏掉真实命中，允许**可控误报**（精确 ⊋ 无漏报）。

只有在被迫做**有损压缩**（如 filter）时才退到"无漏报"。**能拿到权威集合（引擎上报 / 倒排索引）就应追求精确**——本方案的默认目标是精确，"无漏报"只是压缩方案的兜底语义。

### 13.3 前置：先拿到"请求的 block hash 链"

匹配需要的是**请求自己**的 block hash，而请求是**文本**。两条路：

| | 谁算 | 要不要引擎端点 | 代价 |
|---|---|---|---|
| **引擎算** | 引擎 / 插件 | **要**（如 `/v1/chat_cache/hashing`）| 精确、零漂移；**引擎耦合** |
| **router 算** | router | 不要（或借 `/tokenize`）| 需每模型 tokenizer + **复刻引擎 hash recipe** → **易漂移** |

> **KV 事件只给"已缓存的块"，不给"请求"的 hash**——它**不能**替代这一步。唯一能两边都免的是客户端直接发 token_ids（非标准）。

**hash 是链式的**：`hᵢ = H(hᵢ₋₁, block_tokensᵢ, extra_keys)`，`NONE_HASH` 起始；`hashᵢ` 编码"从头到第 i 块的整段前缀"。所以：

- **指数采样压缩**：只输出 block 下标 `{0,1,2,4,8,…}` 的 hash → 一个请求只有 **~log₂(块数)** 个（长上下文 ~12 个），**一次 RPC 带走**；
- **计算 O(B)、输出 O(log)**——省的是**传输/查询**，不是计算；
- 代价是**分辨率粗（~2×）、会略低估**，但**采样点本身精确**（与引擎逐位一致）。

### 13.4 布局决定一切：正排 vs 倒排

| 布局 | 结构 | 后果 |
|---|---|---|
| **正排** | `worker:<id> → 它有的一串 hash`（或 per-worker filter）| value 随缓存块数增长 → **大 key**（撞 Consul 单 value 上限 ~512KB）❌ |
| **倒排** | `prefix:<hash> → {worker 位图}` | 每个 value 极小（上界 = worker 数）→ **无大 key** ✅；大的是 **key 数量** |

> **大 key 是布局问题，不是存储问题**：换 Redis **不解决**"正排"；**倒排**才解决。倒排下"大"的是 key 基数——这正是 Redis 擅长的（Consul/etcd 不擅长：Raft 全复制）。

### 13.5 四条路线

- **A. 确定性放置（无索引）**：`worker = consistent_hash(前缀)`——命中无损、**零共享状态、多副本天然一致**（纯函数）、零查询；热点用 **bounded-load 一致哈希**压；代价是只保证"**放置键**"（如 system-prompt hash）的局部性。
- **B. 中心查询索引**：一个权威索引，由引擎事件喂，router 每请求查一次。**精确、全局最优**；代价是**一跳 + 一个（可分片/复制的）服务**。
- **C. Redis 倒排索引（可扩展的 B）**：`prefix:<hash> → worker 位图`，放 **Redis Cluster**（自带分片 + 副本 + TTL）。**无大 key、可水平扩**；router 把请求的 ~log 个 hash **按 slot 分节点并发查**（并行 fan-out ≈1 RTT）+ 本地做最长前缀合并；叠加 **router 本地热前缀缓存** → 热前缀命中即 **0 网络**，仅冷尾 fan-out。
- **（弃用）per-worker filter**：把每个 worker 的块集合压成 filter 存进共享 store。否决理由：① filter 仍是 **O(n) 位**（~10 bit/块，非 O(1)），大 KV / 小模型下 value 会**超 Consul 512KB**；② **正排布局 → 大 key**。**不用。**

### 13.6 分片的固有代价：fan-out ↔ 复制 ↔ 不查

| 做法 | 每请求网络 | 内存 |
|---|---|---|
| 分片（按 hash）| 并行 fan-out ≈1 RTT | 分片，可扩 |
| 复制（全量副本，查一个）| 1 RTT | ×R |
| 单节点全量 | 1 RTT | 单机上限 |
| 本地热缓存命中 | **0** | 小 |
| 确定性放置（A）| **0** | 0 |

> Redis **MGET 只在同 slot 内有效**；hash 散在多个 slot → 客户端须**按节点并发**（别同步逐个，否则真 N×RTT）。in-DC 并行 fan-out 约 0.2–0.5ms，相对推理（百 ms~秒）可忽略。

### 13.7 系统图：事件 → 倒排索引 → 查询/合并

> 实现上，写路径与读路径都经过**状态服务 `stated`**（见 §7）：插件把 `/state`、`/kv` 发给它，
> router 每请求发一次 **`/match`**（前缀 + 会话）；`stated` 再操作 Redis。下面画的是它内部的逻辑。

**状态路径（写；引擎事件驱动，异步低频）：**

```
  ┌──────────────┐  block add/remove 事件     ┌──────────────────┐      ┌───────────────────────────┐
  │ vLLM + 插件   │ ────────────────────────► │ stated: /kv      │ ───► │ Redis 倒排索引             │
  └──────────────┘  (KV events, 每块 hash)    │ (ingest)         │      │  prefix:<hash> → {worker} │
                                              └──────────────────┘      │  (+ per-key TTL)          │
                                                                        └───────────────────────────┘
```

**查询路径（读；每请求）：**

```
  client ─► router
             │
             │ ① 取请求的 block hash 链（指数采样，~log 个）
             │      · 引擎端点 /hashing    → 精确、低漂移
             │
             │ ② POST stated /match {hashes, session_id?, workers}
             │      · stated 内：热缓存命中 → 直接返回（0 后端访问）
             │      · 否则并发查 Redis 倒排（按 slot fan-out，≈1 RTT）+ 合并
             │      · 返回 {matched, workers, tokens, session_worker, session_tokens,
             │              loads: {worker: {running, waiting}}}
             ▼
          forward ─► worker（engine 用自身真 cache 复核 + prefill）
```

> router **无本地状态**：不拉全量、不物化内存——每请求一次 `/match` 拿全（前缀 + 会话 + 负载）。

> 合并（最长命中前缀）在 `stated` 里做，router 只收到结果——router 因此变薄，也不直连 Redis。

要点：**读路径只查倒排（多小 key，无大 key）；热前缀走本地缓存 → 常见情况零网络；冷尾才 fan-out（并行 ≈1 RTT）。索引是权威（引擎上报），router 不复制、不推断。**

### 13.8 与两家对照

**机制：**

| | vllm-router | sgl-gateway | **llm-gateway（本方案）** |
|---|---|---|---|
| 前缀感知 | 近似：字符 radix tree（本机历史）| 近似：同源树 + 可选 mesh 同步 | 真实 block hash（引擎算 + 倒排索引）|
| 消费引擎 KV 事件 | ✘ | ✘ | ✔ |
| tokenizer 位置 | router 内，仅 chat/preprocess/流式 | router 内（cache 路径不用）| 引擎侧 |
| 匹配精度 | 近似 | 近似 | **精确** |
| 匹配粒度 | 字符 | 字符 | block |

**部署与代价：**

| | vllm-router | sgl-gateway | **llm-gateway** |
|---|---|---|---|
| 匹配结构 | router 进程内 | router 内存 + mesh | **倒排索引（Redis，可选 Consul 热区档）** |
| 跨副本共享 | ✘ | 可选（mesh 广播）| ✔ 共享索引（权威）|
| 新副本冷启动 | 空，靠自身流量重建 | 从 mesh restore | 查即有（共享权威）|
| 共享状态规模 | 无 | O(请求) 广播 | **多小 key（无大 key，可扩）** |
| 额外依赖 | 无 | smg-mesh + Redis | 引擎 hashing 端点 + Redis（可选档：Consul 热区，零依赖）|
| 实现状态 | 开源 | 开源 | **本设计提案** |

### 13.9 选型决策表

| 约束 / 目标 | 选 |
|---|---|
| 零外部依赖、可接受"只覆盖热区" | **热前缀索引**（Consul，K 封顶）+ 冷尾近似 |
| 精确 + 可水平扩 | **Redis 倒排（Cluster）** + 本地热缓存 |
| 零查询 / 最省 | **确定性放置（A）** |
| 单模型极大 | 倒排 + 按 hash 分片 |
| 完全可控 / 无外部依赖 / 极端定制 | **自建**：log-as-truth + 分片物化视图 + 边缘合并 |

**默认推荐：C（Redis 倒排）+ 本地热缓存**；无依赖档退到**热前缀索引**；能改路由语义则选 **A**。配合三个收敛动作：**hash 引擎侧算** / **push 不 poll** / **聚合有界**。

---

## 附录 A：与两家的能力对照

| 能力 | vllm-router | sgl-gateway | **llm-gateway** |
|---|---|---|---|
| 引擎无关 | 部分 | ✘（绑 SGLang）| **✔（协议预留）** |
| KV 感知 | 近似 | 近似 | **近似→真实，可插拔** |
| 集群状态一致 | ✘（局部视图）| 可选（mesh 同步近似树）| **✔（共享 StatePlane）** |
| 插件可选/无感 | — | — | **✔（pip 即用，未设 env 则 no-op）** |
| 部署复杂度 | 低 | 低 | **单实例低 / 集群中** |

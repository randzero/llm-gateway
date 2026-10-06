"""Plugin configuration — read from the environment.

The plugin is **disabled unless ``LLMGATEWAY_ENDPOINT`` is set**, so `pip
install` alone does not change engine behaviour.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit


@dataclass(frozen=True)
class Config:
    endpoint: str | None
    kv_endpoint: str | None
    worker_id: str
    snapshot_interval_s: float
    topk: int
    kv_batch_max: int
    publisher_name: str
    # Engine-side block-hash endpoint (design §13.3).
    hash_port: int
    hash_addr: str
    hash_model: str | None
    hash_algo: str
    block_size: int

    @property
    def active(self) -> bool:
        return self.endpoint is not None


def _f(name: str, default: str) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return float(default)


def _i(name: str, default: str) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return int(default)


def _derive_kv(endpoint: str | None) -> str | None:
    if not endpoint:
        return None
    # Same host as the /state endpoint, but the /kv ingest path.
    p = urlsplit(endpoint)
    if not p.netloc:
        return None
    return urlunsplit((p.scheme, p.netloc, "/kv", "", ""))


def load() -> Config:
    endpoint = os.environ.get("LLMGATEWAY_ENDPOINT", "").strip() or None
    kv_endpoint = os.environ.get("LLMGATEWAY_KV_ENDPOINT", "").strip() or _derive_kv(endpoint)
    return Config(
        endpoint=endpoint,
        kv_endpoint=kv_endpoint,
        worker_id=os.environ.get("LLMGATEWAY_WORKER_ID", "").strip()
        or socket.gethostname(),
        snapshot_interval_s=_f("LLMGATEWAY_SNAPSHOT_INTERVAL_S", "1.0"),
        topk=_i("LLMGATEWAY_TOPK", "4096"),
        kv_batch_max=_i("LLMGATEWAY_KV_BATCH_MAX", "8192"),
        publisher_name=os.environ.get("LLMGATEWAY_PUBLISHER_NAME", "llm-gateway"),
        hash_port=_i("LLMGATEWAY_HASH_PORT", "0"),
        hash_addr=os.environ.get("LLMGATEWAY_HASH_ADDR", "127.0.0.1").strip() or "127.0.0.1",
        hash_model=os.environ.get("LLMGATEWAY_HASH_MODEL", "").strip() or None,
        hash_algo=os.environ.get("LLMGATEWAY_HASH_ALGO", "sha256").strip() or "sha256",
        block_size=_i("LLMGATEWAY_BLOCK_SIZE", "16"),
    )


def enabled() -> bool:
    return load().active

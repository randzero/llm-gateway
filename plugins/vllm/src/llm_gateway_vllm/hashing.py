"""Block-hash chain for the router (design §13.3).

The plugin runs inside the vLLM environment, so it can reuse vLLM's *own*
hashing (``hash_block_tokens`` + the configured hash algorithm) and produce
hashes that match the engine bit-for-bit — no cross-language drift.

``hash_fn`` / ``none_hash`` are injected so the logic is testable without vLLM.
"""

from __future__ import annotations

from typing import Callable, Optional, Sequence

HashFn = Callable[[object], bytes]


def sample_indices(num_blocks: int) -> list[int]:
    """The exponential block indices {0,1,2,4,8,...} < num_blocks. Mirrors the
    router's Go ``prefix.SampleIndices`` so both sides sample identically."""
    out: list[int] = []
    k = 0
    while True:
        idx = k if k < 2 else 1 << (k - 1)
        if idx >= num_blocks:
            break
        out.append(idx)
        k += 1
    return out


def block_hash_chain(
    token_ids: Sequence[int],
    block_size: int,
    hash_fn: HashFn,
    none_hash,
    extra_keys=None,
) -> list:
    """Chained per-block hashes: h_i = hash_fn((h_{i-1}, block_i, extra)); the
    first block chains from ``none_hash``. Only full blocks are hashed."""
    parent = none_hash
    chain: list = []
    n = len(token_ids) // block_size
    for i in range(n):
        block = tuple(token_ids[i * block_size : (i + 1) * block_size])
        parent = hash_fn((parent, block, extra_keys))
        chain.append(parent)
    return chain


def sampled_hash_keys(
    token_ids: Sequence[int],
    block_size: int,
    hash_fn: HashFn,
    none_hash,
) -> list[str]:
    """The request's sampled block-hash chain, as the router's index keys
    (shallowest-first)."""
    chain = block_hash_chain(token_ids, block_size, hash_fn, none_hash)
    return [str(chain[i]) for i in sample_indices(len(chain))]


def load_vllm_hasher(hash_algo: Optional[str] = None):
    """Return ``(hash_fn, none_hash)`` from vLLM, or ``(None, None)`` when vLLM
    is unavailable (keeps this module importable outside an engine)."""
    try:
        from vllm.utils.hashing import get_hash_fn_by_name
        from vllm.v1.core import kv_cache_utils as kv
    except Exception:
        return None, None
    try:
        hash_fn = get_hash_fn_by_name(hash_algo or "sha256")
        init = getattr(kv, "init_none_hash", None)
        if init is not None:
            init(hash_fn)
        return hash_fn, getattr(kv, "NONE_HASH", None)
    except Exception:
        return None, None

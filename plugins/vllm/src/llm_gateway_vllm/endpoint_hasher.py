"""Exact request -> block-hash chain, using the API server's own tokenization.

The router needs a request's sampled block-hash chain (design §13.3). Running
inside the API server process, we reuse the *same* renderer/tokenization the
engine uses for inference, so the token ids — and therefore the hashes — match
what the engine (and the plugin's /kv reporting) stores.
"""

from __future__ import annotations

import json
import os
from typing import Any, Optional

BlockSizeDefault = 16


class EndpointHasher:
    def __init__(self, serving_tokenization, model, block_size, hash_algo):
        self._tok = serving_tokenization
        self._model = model
        self._block_size = block_size
        self._hash_algo = hash_algo
        self._hf = None

    @classmethod
    def from_state(cls, state: Any, args: Any) -> "EndpointHasher":
        return cls(
            serving_tokenization=getattr(state, "serving_tokenization", None),
            model=getattr(args, "model", None),
            block_size=_env_int("LLMGATEWAY_BLOCK_SIZE", BlockSizeDefault),
            hash_algo=os.environ.get("LLMGATEWAY_HASH_ALGO", "sha256").strip() or "sha256",
        )

    def _hasher(self):
        if self._hf is None:
            from .hashing import load_vllm_hasher

            self._hf, self._none = load_vllm_hasher(self._hash_algo)
        return self._hf, self._none

    async def hashes(self, body: bytes, raw_request) -> list[str]:
        from .hashing import sampled_hash_keys

        tokens = await self._tokenize(body, raw_request)
        hf, none = self._hasher()
        if hf is None or none is None:
            raise RuntimeError("vLLM hash function unavailable")
        return sampled_hash_keys(tokens, self._block_size, hf, none)

    async def _tokenize(self, body: bytes, raw_request) -> list[int]:
        req = json.loads(body)
        # Exact path: the same tokenization the engine's serving layer uses.
        if self._tok is not None:
            tokens = await self._tokenize_via_serving(req, raw_request)
            if tokens:
                return tokens
        # Fallback: HF tokenizer (close, not always byte-exact on chat template).
        from .hash_server import default_tokenizer

        return default_tokenizer(self._model)(body)

    async def _tokenize_via_serving(self, req: dict, raw_request) -> Optional[list[int]]:
        try:
            from vllm.entrypoints.serve.tokenize.protocol import (
                TokenizeChatRequest,
                TokenizeCompletionRequest,
            )
        except Exception:
            return None
        model = req.get("model") or self._model
        try:
            if "messages" in req:
                treq = TokenizeChatRequest(model=model, messages=req["messages"])
            else:
                treq = TokenizeCompletionRequest(model=model, prompt=req.get("prompt", ""))
            resp = await self._tok.create_tokenize(treq, raw_request)
            return getattr(resp, "tokens", None)
        except Exception:
            return None


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default

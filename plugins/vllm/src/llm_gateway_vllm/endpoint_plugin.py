"""``vllm.endpoint_plugins`` entry point: engine-side block-hash endpoint.

Adds ``POST /v1/chat_cache/hashing`` to the OpenAI API server, returning the
request's sampled block-hash chain (design §13.3). Because it runs in the API
server process it reuses the server's own tokenization (the OnlineRenderer via
``ServingTokenization``) and vLLM's own hash function, so the hashes line up
with what the engine stores.

Requires a recent vLLM exposing the ``vllm.endpoint_plugins`` group, and must be
allowlisted (endpoint plugins are off by default)::

    VLLM_PLUGINS=llm_gateway_hashing   # the entry point name
"""

from __future__ import annotations

import os

# Entry point name is ``llm_gateway_hashing``; this is what VLLM_PLUGINS matches.
_PLUGIN_NAME = "llm_gateway_hashing"


class HashingEndpointPlugin:
    name = _PLUGIN_NAME
    required_tasks: tuple[str, ...] | None = None

    def attach_router(self, app) -> None:  # app: FastAPI
        from fastapi import HTTPException, Request

        @app.post("/v1/chat_cache/hashing")
        async def chat_cache_hashing(raw_request: Request):
            hasher = getattr(raw_request.app.state, "llm_gateway_hasher", None)
            if hasher is None:
                raise HTTPException(status_code=503, detail="llm-gateway hashing not configured")
            body = await raw_request.body()
            hashes = await hasher.hashes(body, raw_request)
            return {"hashes": hashes}

    async def init_state(self, engine_client, state, args) -> None:
        # Only wire up when the gateway plugin is active.
        if not os.environ.get("LLMGATEWAY_ENDPOINT"):
            return
        from .endpoint_hasher import EndpointHasher

        state.llm_gateway_hasher = EndpointHasher.from_state(state, args)

"""Engine-side block-hash endpoint (design §13.3).

Started by the plugin when ``LLMGATEWAY_HASH_PORT`` is set. A POST of a chat or
completion request body returns ``{"hashes": [<sampled block hashes>]}``
(shallowest-first) — the request's block-hash chain the router matches against
its inverted index. Hashes come from vLLM's own hash function, so they line up
with what the engine (and the plugin's /kv reporting) stores.

Caveat: the *tokenization* must match the engine's too (chat template included).
``tokenize`` is injected; the default builds it from the model's HF tokenizer,
which is a close but not always exact match of the engine's own request
pipeline. Prefer an engine-server endpoint when exact tokenization is required.
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .hashing import sampled_hash_keys


class _Handler(BaseHTTPRequestHandler):
    tokenize = None
    block_size = 16
    hash_fn = None
    none_hash = None

    def log_message(self, *args) -> None:  # keep the engine log quiet
        pass

    def do_GET(self) -> None:  # liveness
        self.send_response(200)
        self.send_header("Content-Length", "2")
        self.end_headers()
        self.wfile.write(b"ok")

    def do_POST(self) -> None:
        n = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(n)
        try:
            if self.hash_fn is None or self.none_hash is None:
                raise RuntimeError("vLLM hash function unavailable")
            token_ids = self.tokenize(body)
            hashes = sampled_hash_keys(token_ids, self.block_size, self.hash_fn, self.none_hash)
            data = json.dumps({"hashes": hashes}).encode()
            self.send_response(200)
        except Exception as exc:  # noqa: BLE001 — surface as a 400
            data = json.dumps({"error": f"{type(exc).__name__}: {exc}"}).encode()
            self.send_response(400)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def make_server(addr, port, tokenize, block_size, hash_fn, none_hash) -> ThreadingHTTPServer:
    # staticmethod so these are not turned into bound methods when read via self.
    _Handler.tokenize = staticmethod(tokenize)
    _Handler.block_size = block_size
    _Handler.hash_fn = staticmethod(hash_fn) if hash_fn is not None else None
    _Handler.none_hash = none_hash
    return ThreadingHTTPServer((addr, port), _Handler)


def default_tokenizer(model: str):
    """Build a ``body -> list[int]`` tokenizer from the model's HF tokenizer.
    Best-effort: applies the chat template for chat requests."""
    from vllm.transformers_utils.tokenizer import get_tokenizer

    tok = get_tokenizer(model)

    def _tokenize(body: bytes) -> list[int]:
        req = json.loads(body)
        if "prompt" in req:
            text = req["prompt"]
            if isinstance(text, list):  # legacy multi-prompt form
                text = text[0]
        else:
            text = tok.apply_chat_template(
                req.get("messages", []), add_generation_prompt=True, tokenize=False
            )
        return tok.encode(text)

    return _tokenize


def start_hash_server(cfg, tokenize=None):
    """Start the hashing server on a daemon thread. Returns the server, or None
    if disabled or the port is already bound (multiple plugin loads/processes)."""
    if not cfg.hash_port:
        return None
    from .hashing import load_vllm_hasher

    hash_fn, none_hash = load_vllm_hasher(cfg.hash_algo)
    if tokenize is None and cfg.hash_model:
        try:
            tokenize = default_tokenizer(cfg.hash_model)
        except Exception:
            tokenize = None
    if tokenize is None:
        tokenize = lambda _body: []  # noqa: E731 — yields empty hash lists
    try:
        srv = make_server(cfg.hash_addr, cfg.hash_port, tokenize, cfg.block_size, hash_fn, none_hash)
    except OSError:
        return None  # already bound by another rank/process
    threading.Thread(target=srv.serve_forever, name="llm-gateway-hash", daemon=True).start()
    return srv

"""llm-gateway-vllm — the optional vLLM engine plugin.

Loaded automatically by vLLM through two standard entry points:

* ``vllm.general_plugins``  -> :func:`register` (registers the KV event publisher)
* ``vllm.stat_logger_plugins`` -> :class:`llm_gateway_vllm.stats.GatewayStatLogger`

The plugin is a **no-op unless ``LLMGATEWAY_ENDPOINT`` is set**, so installing it
never changes engine behaviour until you opt in.
"""

from __future__ import annotations

from .config import Config, enabled, load

__all__ = ["register", "Config", "enabled", "load"]

_registered = False


def register() -> None:
    """Entry point for ``vllm.general_plugins``.

    Called by vLLM at startup. Idempotent (vLLM may load plugins in several
    processes/ranks).
    """
    global _registered
    if not enabled() or _registered:
        return
    _registered = True

    # Imported lazily so the package stays importable outside a vLLM env.
    from vllm.distributed.kv_events import EventPublisherFactory

    from .publisher import GatewayPublisher
    from .reporter import Pusher, get_reporter

    cfg = load()
    try:
        EventPublisherFactory.register_publisher(cfg.publisher_name, GatewayPublisher)
    except KeyError:
        # Already registered (another load in the same process).
        pass

    # Best-effort: capture the scheduler to report its real prefill backlog
    # (waiting prefill tokens). No-op if vLLM's internals differ.
    from . import sched_probe

    sched_probe.install()

    # Start the background push loop once per process.
    global _pusher
    if _pusher is None:
        _pusher = Pusher(get_reporter(cfg))
        _pusher.start()

    # Optional engine-side block-hash endpoint (design §13.3). Only one process
    # wins the bind; the others no-op.
    global _hash_server
    if _hash_server is None and cfg.hash_port:
        from .hash_server import start_hash_server

        _hash_server = start_hash_server(cfg)


_pusher = None
_hash_server = None

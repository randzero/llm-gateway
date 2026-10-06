"""KV event publisher — consumes vLLM's built-in ``KVCacheEvent`` stream.

Registered into ``EventPublisherFactory`` under ``LLMGATEWAY_PUBLISHER_NAME``
(default ``"llm-gateway"``). Enable it on the vLLM side with::

    --kv-events-config '{"enable_kv_cache_events": true, "publisher": "llm-gateway"}'
"""

from __future__ import annotations

from vllm.distributed.kv_events import EventPublisher

from .config import load
from .reporter import get_reporter


class GatewayPublisher(EventPublisher):
    def __init__(self, data_parallel_rank: int = 0, **kwargs):
        # ``kwargs`` absorbs KVEventsConfig fields (endpoint/hwm/topic/...).
        super().__init__(data_parallel_rank=data_parallel_rank)
        self.cfg = load()
        self.reporter = get_reporter(self.cfg)

    def publish(self, events) -> None:
        if not self.cfg.active:
            return
        self.reporter.on_kv_events(events)

    def shutdown(self) -> None:
        pass

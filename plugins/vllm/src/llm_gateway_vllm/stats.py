"""Stat logger plugin — receives ``SchedulerStats`` every engine iteration.

Registered via the ``vllm.stat_logger_plugins`` entry point; vLLM instantiates
one per engine and calls :meth:`record` each step.
"""

from __future__ import annotations

from vllm.v1.metrics.loggers import StatLoggerBase

from .config import load
from .reporter import get_reporter


class GatewayStatLogger(StatLoggerBase):
    def __init__(self, vllm_config, engine_index: int = 0):
        self.vllm_config = vllm_config
        self.engine_index = engine_index
        self.cfg = load()
        self.reporter = get_reporter(self.cfg)

    def record(
        self,
        scheduler_stats,
        iteration_stats,
        mm_cache_stats=None,
        engine_idx: int = 0,
    ) -> None:
        # Runs every iteration — cheap, aggregated, never emits.
        if not self.cfg.active:
            return
        self.reporter.on_scheduler_stats(
            engine_idx or self.engine_index, scheduler_stats, iteration_stats
        )

    def log_engine_initialized(self) -> None:
        pass

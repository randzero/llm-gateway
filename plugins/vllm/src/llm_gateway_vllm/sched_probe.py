"""Best-effort probe of the vLLM scheduler's prefill backlog.

vLLM's ``SchedulerStats`` exposes only *counts* (``num_running_reqs``,
``num_waiting_reqs``); the real queue depth in tokens — how much prefill work is
queued ahead of a new request — lives in the scheduler's ``waiting`` queue (and
the not-yet-computed part of ``running``). This module reaches it by wrapping
``Scheduler.__init__`` once: the plugin's ``register()`` runs at the very start
of ``EngineCore.__init__``, before the scheduler is constructed.

This is coupled to vLLM internals and is therefore best-effort: if anything
changes (class path, queue shape, field names), every probe returns ``None`` and
callers fall back to the count-based estimate. It mutates no vLLM source and is
disabled when the plugin itself is disabled.
"""

from __future__ import annotations

import threading
from typing import Optional

_lock = threading.Lock()
_scheduler = None
_patched = False


def install() -> None:
    """Wrap ``Scheduler.__init__`` to capture the instance. Idempotent; no-op if
    vLLM's layout is not what we expect."""
    global _patched
    with _lock:
        if _patched:
            return
        _patched = True
        try:
            from vllm.v1.core.sched.scheduler import Scheduler
        except Exception:
            return
        try:
            original = Scheduler.__init__

            def wrapped(self, *args, **kwargs):
                global _scheduler
                _scheduler = self
                return original(self, *args, **kwargs)

            Scheduler.__init__ = wrapped
        except Exception:
            pass


def backlog() -> Optional[tuple[int, int]]:
    """(waiting_full_tokens, running_remaining_tokens), or ``None`` if
    unavailable.

    ``waiting_full_tokens`` is every waiting request's *full* prompt (the engine
    has not done its prefix-cache lookup yet, so the hit part is unknown —
    callers should discount it by a hit ratio). ``running_remaining_tokens`` is
    ``num_prompt_tokens - num_computed_tokens``, already cache-adjusted because
    ``num_computed_tokens`` includes the prefix-cache hits."""
    s = _scheduler
    if s is None:
        return None
    try:
        waiting = 0
        for req in _iter(getattr(s, "waiting", None)):
            waiting += _int(getattr(req, "num_prompt_tokens", 0))
        running = 0
        for req in _iter(getattr(s, "running", None)):
            remaining = _int(getattr(req, "num_prompt_tokens", 0)) - _int(
                getattr(req, "num_computed_tokens", 0)
            )
            if remaining > 0:
                running += remaining
        return waiting, running
    except Exception:
        return None


def waiting_tokens() -> Optional[int]:
    """Total un-computed prefill tokens (waiting full + running remaining), or
    ``None``. Raw — no hit-ratio discount."""
    bl = backlog()
    if bl is None:
        return None
    return bl[0] + bl[1]


def _iter(q):
    # RequestQueue implements __iter__; guard anyway.
    if q is None:
        return ()
    try:
        return list(q)
    except Exception:
        return ()


def _int(v) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0

from llm_gateway_vllm import sched_probe


def test_install_and_degrade_without_vllm():
    # No vLLM here: install() is a no-op and waiting_tokens() returns None.
    sched_probe.install()
    assert sched_probe.waiting_tokens() is None


def test_probe_sums_waiting_and_running_remaining():
    class R:
        def __init__(self, prompt, computed=0):
            self.num_prompt_tokens = prompt
            self.num_computed_tokens = computed

    class S:
        waiting = [R(100), R(200)]  # full prompt each
        running = [R(50, 50), R(300, 100)]  # remaining 0 and 200

    old = sched_probe._scheduler
    sched_probe._scheduler = S()
    try:
        assert sched_probe.waiting_tokens() == 100 + 200 + 0 + 200
    finally:
        sched_probe._scheduler = old

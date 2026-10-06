from llm_gateway_vllm.endpoint_hasher import EndpointHasher
from llm_gateway_vllm.endpoint_plugin import HashingEndpointPlugin


def test_plugin_metadata():
    p = HashingEndpointPlugin()
    assert p.name == "llm_gateway_hashing"
    assert p.required_tasks is None


def test_modules_import_without_vllm():
    # Lazy imports keep these importable in a plain environment (no vllm/fastapi).
    import llm_gateway_vllm.endpoint_hasher  # noqa: F401
    import llm_gateway_vllm.endpoint_plugin  # noqa: F401


def test_from_state_builds_hasher(monkeypatch):
    monkeypatch.setenv("LLMGATEWAY_BLOCK_SIZE", "32")
    monkeypatch.setenv("LLMGATEWAY_HASH_ALGO", "sha256")

    class State:
        serving_tokenization = object()

    class Args:
        model = "m"

    h = EndpointHasher.from_state(State(), Args())
    assert h._block_size == 32
    assert h._model == "m"
    assert h._hash_algo == "sha256"

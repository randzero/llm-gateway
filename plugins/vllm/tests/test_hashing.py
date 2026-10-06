import hashlib
import json
import urllib.error
import urllib.request

from llm_gateway_vllm.hashing import (
    block_hash_chain,
    sampled_hash_keys,
    sample_indices,
)
from llm_gateway_vllm.hash_server import make_server


def _hf(obj):
    return hashlib.sha256(repr(obj).encode()).digest()


NONE = _hf("NONE")


def test_sample_indices_matches_go():
    # Same set as prefix.SampleIndices.
    assert sample_indices(0) == []
    assert sample_indices(1) == [0]
    assert sample_indices(3) == [0, 1, 2]
    assert sample_indices(20) == [0, 1, 2, 4, 8, 16]


def test_block_hash_chain_only_full_blocks():
    # 32 tokens / block 16 -> 2 full blocks (a trailing partial block is dropped).
    chain = block_hash_chain([0] * 40, 16, _hf, NONE)
    assert len(chain) == 2


def test_sampled_hash_keys():
    token_ids = list(range(20 * 16))  # 20 full blocks
    keys = sampled_hash_keys(token_ids, 16, _hf, NONE)
    assert len(keys) == len(sample_indices(20)) == 6
    assert all(isinstance(k, str) for k in keys)


def test_hash_server_roundtrip():
    tok = lambda body: json.loads(body)["tokens"]  # noqa: E731
    srv = make_server("127.0.0.1", 0, tok, 16, _hf, NONE)
    import threading

    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        body = json.dumps({"tokens": list(range(20 * 16))}).encode()
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/", data=body, method="POST"
        )
        resp = json.loads(urllib.request.urlopen(req, timeout=5).read())
        assert resp["hashes"] == sampled_hash_keys(range(20 * 16), 16, _hf, NONE)
    finally:
        srv.shutdown()


def test_hash_server_reports_error_without_hasher():
    srv = make_server("127.0.0.1", 0, lambda b: [1, 2, 3], 16, None, None)
    import threading

    threading.Thread(target=srv.serve_forever, daemon=True).start()
    port = srv.server_address[1]
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/", data=b"{}", method="POST"
        )
        try:
            urllib.request.urlopen(req, timeout=5)
            assert False, "expected HTTP error"
        except urllib.error.HTTPError as e:
            assert e.code == 400
    finally:
        srv.shutdown()

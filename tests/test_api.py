"""HTTP 层集成测试：投递 / 重组 / 恢复 / 追赶 / 查询与可区分错误码。"""

from __future__ import annotations

from app.models import GENESIS_PARENT, Block


def _bdict(b):
    return {
        "height": b.height,
        "hash": b.hash,
        "parent_hash": b.parent_hash,
        "transactions": [
            {
                "tx_hash": t.tx_hash,
                "sender": t.sender,
                "payload": t.payload,
            }
            for t in b.transactions
        ],
    }


def _feed(client, chain, n):
    blocks = [chain.produce(payload=f"p{i}") for i in range(n)]
    for b in blocks:
        r = client.post("/blocks", json=_bdict(b))
        assert r.status_code == 200, r.text
    return blocks


def test_health_progress_and_queries(client, chain):
    blocks = _feed(client, chain, 5)
    assert client.get("/health").json() == {"status": "ok"}

    p = client.get("/progress").json()
    assert p == {"cursor": 4, "confirmed_watermark": 1, "confirmation_depth": 3}

    r = client.get(f"/blocks/1")
    assert r.status_code == 200
    body = r.json()
    assert body["hash"] == blocks[1].hash
    assert body["confirmed"] is True
    assert body["transactions"][0]["tx_hash"] == blocks[1].transactions[0].tx_hash

    r = client.get("/blocks/4")
    assert r.json()["confirmed"] is False

    assert client.get("/blocks/99").status_code == 404
    tx = blocks[0].transactions[0].tx_hash
    assert client.get(f"/transactions/{tx}").status_code == 200
    assert client.get("/transactions/0xnope").status_code == 404


def test_duplicate_and_gap_error_codes(client, chain):
    blocks = _feed(client, chain, 4)

    r = client.post("/blocks", json=_bdict(blocks[2]))
    assert r.status_code == 409
    assert r.json()["error"] == "duplicate_delivery"
    assert r.json()["height"] == 2

    r = client.post("/blocks", json=_bdict(Block(8, "0xh8", "0xp")))
    assert r.status_code == 409
    assert r.json()["error"] == "height_gap"

    # parent 不符
    r = client.post("/blocks", json=_bdict(Block(4, "0xh4", "0xwrong")))
    assert r.status_code == 409
    assert r.json()["error"] == "parent_mismatch"


def test_reorg_endpoint_and_visibility(client, chain):
    blocks = _feed(client, chain, 6)  # wm=2

    forked = chain.produce_fork(fork_at=4)  # 新 h=5
    r = client.post("/reorg", json={"path": [_bdict(x) for x in forked]})
    assert r.status_code == 200
    body = r.json()
    assert body["action"] == "reorg"
    assert body["rolled_back"] == [5]

    assert client.get("/blocks/5").json()["hash"] == forked[0].hash
    # 旧交易不再可见
    old_tx = blocks[5].transactions[0].tx_hash
    assert client.get(f"/transactions/{old_tx}").status_code == 404
    new_tx = forked[0].transactions[0].tx_hash
    assert client.get(f"/transactions/{new_tx}").status_code == 200

    # 深叉 -> 422 reorg_too_deep，数据不变
    deep = [Block(2, "0xd2", blocks[1].hash)]
    r = client.post("/reorg", json={"path": [_bdict(x) for x in deep]})
    assert r.status_code == 422
    assert r.json()["error"] == "reorg_too_deep"
    assert client.get("/progress").json()["cursor"] == 5


def test_resume_and_catchup_endpoints(client, chain):
    _feed(client, chain, 5)

    assert client.post("/resume").json()["cursor"] == 4

    r = client.post("/catchup", json={"target_height": 2})
    assert r.status_code == 422 and r.json()["error"] == "progress_rollback"

    r = client.post("/catchup", json={"target_height": 40})
    assert r.status_code == 422 and r.json()["error"] == "catchup_too_far"

    r = client.post("/catchup", json={"target_height": 4})
    assert r.status_code == 200 and r.json()["cursor"] == 4


def test_genesis_ingest_via_http(client):
    b = Block(0, "0xg", GENESIS_PARENT)
    r = client.post("/blocks", json=_bdict(b))
    assert r.status_code == 200 and r.json()["action"] == "accepted"

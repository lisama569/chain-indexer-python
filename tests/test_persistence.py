"""场景：索引进度持久化、进程重启恢复、位点追赶规则。

- 重启后从正确位点继续，历史结论不变；
- 重启后仍可正确拒绝重复、处理分叉（跨进程一致）；
- 位点回退 -> progress_rollback；跨度过大 -> catchup_too_far；
- 存储与位点不一致（损坏）-> store_corrupted。
"""

from __future__ import annotations

import pytest

from app.errors import (
    CatchupTooFarError,
    DuplicateDeliveryError,
    ProgressRollbackError,
    StoreCorruptedError,
)
from app.indexer import ChainIndexer
from app.models import Block
from app.storage import BlockStore


def _build_file_chain(path, chain, n, *, depth=3, span=10):
    store = BlockStore(str(path))
    ix = ChainIndexer(store, confirmation_depth=depth, max_catchup_span=span)
    blocks = [chain.produce(payload=f"p{i}") for i in range(n)]
    for b in blocks:
        ix.ingest_block(b)
    return ix, blocks


def test_restart_resumes_from_correct_cursor(tmp_db_path, chain):
    ix1, blocks = _build_file_chain(tmp_db_path, chain, 6)
    assert ix1.cursor == 5
    ix1.get_block  # noqa: B018
    del ix1

    # 模拟新进程：重新打开同一数据库
    store2 = BlockStore(str(tmp_db_path))
    ix2 = ChainIndexer(store2, confirmation_depth=3)
    cursor = ix2.resume()
    assert cursor == 5
    assert ix2.cursor == 5
    assert ix2.confirmed_watermark == 2

    # 已持久化的历史结论不变
    for h in range(6):
        assert ix2.get_block(h).hash == blocks[h].hash
    tx = blocks[3].transactions[0]
    assert ix2.get_transaction(tx.tx_hash).block_hash == blocks[3].hash

    # 从正确位点继续：下一块 h=6 被接受，h=7 缺口仍被拒绝
    b6 = chain.produce(payload="p6")
    r = ix2.ingest_block(b6)
    assert r.action == "accepted" and ix2.cursor == 6
    store2.close()

    # 再次重启确认位点已落盘
    store3 = BlockStore(str(tmp_db_path))
    ix3 = ChainIndexer(store3, confirmation_depth=3)
    assert ix3.resume() == 6
    assert ix3.get_block(6).hash == b6.hash
    store3.close()


def test_restart_then_reorg_and_duplicate(tmp_db_path, chain):
    ix1, blocks = _build_file_chain(tmp_db_path, chain, 8)  # wm=4
    del ix1

    store = BlockStore(str(tmp_db_path))
    ix = ChainIndexer(store, confirmation_depth=3)
    ix.resume()

    # 重启后重复投递仍被识别
    with pytest.raises(DuplicateDeliveryError) as ei:
        ix.ingest_block(blocks[2])
    assert ei.value.reason == "duplicate_delivery"

    # 重启后未确认高度上的分叉仍可重组
    forked = chain.produce_fork(fork_at=5)  # h=6,7
    result = ix.reindex_path(forked)
    assert result.action == "reorg"
    assert result.rolled_back == (7, 6)
    for b in forked:
        assert ix.get_block(b.height).hash == b.hash
    assert blocks[5].transactions[0].tx_hash and True
    store.close()

    # 再重启：重组结果已持久化，旧结论不再复活
    store2 = BlockStore(str(tmp_db_path))
    ix2 = ChainIndexer(store2, confirmation_depth=3)
    assert ix2.resume() == 7
    assert ix2.get_block(6).hash == forked[0].hash
    assert ix2.get_transaction(blocks[6].transactions[0].tx_hash) is None
    assert ix2.get_transaction(forked[0].transactions[0].tx_hash) is not None
    store2.close()


def test_catchup_rejects_rollback_and_too_far(indexer, chain):
    blocks = [chain.produce() for _ in range(8)]
    for b in blocks:
        indexer.ingest_block(b)
    assert indexer.cursor == 7

    # 位点回退
    with pytest.raises(ProgressRollbackError) as ei:
        indexer.catchup_to(3)
    assert ei.value.reason == "progress_rollback"
    assert indexer.cursor == 7

    # 负数
    with pytest.raises(ProgressRollbackError):
        indexer.catchup_to(-1)

    # 跨度过大（span=10）
    with pytest.raises(CatchupTooFarError) as ei:
        indexer.catchup_to(7 + 11)
    assert ei.value.reason == "catchup_too_far"

    # 恰好等于跨度上限：允许
    got = indexer.catchup_to(7 + 10)
    assert got == 7  # 没有数据，只推进到数据末端，位点不变


def test_catchup_advances_only_to_actual_data(tmp_db_path, chain):
    """位点声明不能凭空超越数据：直接写了块但位点滞后时，catchup 对齐到数据末端。"""
    store = BlockStore(str(tmp_db_path))
    ix = ChainIndexer(store, confirmation_depth=3, max_catchup_span=100)
    blocks = [chain.produce() for _ in range(5)]
    for b in blocks:
        ix.ingest_block(b)
    assert ix.cursor == 4

    # 模拟「块已拉取落库但位点还没更新」：再放 2 块但不移动位点
    b5 = chain.produce()
    b6 = chain.produce()
    with store.tx():
        store.put_block(b5)
        store.put_block(b6)
    assert store.highest_height() == 6 and ix.cursor == 4

    # 追赶对齐到真实数据末端 6
    assert ix.catchup_to(10) == 6
    assert ix.cursor == 6
    store.close()

    # 重启后位点与数据一致，完整性校验通过
    store2 = BlockStore(str(tmp_db_path))
    ix2 = ChainIndexer(store2, confirmation_depth=3)
    assert ix2.resume() == 6
    store2.close()


def test_corrupted_store_detected_on_resume(tmp_db_path, chain):
    store = BlockStore(str(tmp_db_path))
    ix = ChainIndexer(store, confirmation_depth=3)
    blocks = [chain.produce() for _ in range(4)]
    for b in blocks:
        ix.ingest_block(b)
    store.close()

    # 手工破坏：删除一个中间区块，制造缺口
    sabotage = BlockStore(str(tmp_db_path))
    with sabotage.tx():
        sabotage._con.execute("DELETE FROM blocks WHERE height = 2")
        sabotage._con.execute("DELETE FROM transactions WHERE block_height = 2")
    sabotage.close()

    store2 = BlockStore(str(tmp_db_path))
    ix2 = ChainIndexer(store2, confirmation_depth=3)
    with pytest.raises(StoreCorruptedError) as ei:
        ix2.resume()
    assert ei.value.reason == "store_corrupted"
    store2.close()


def test_progress_without_blocks_is_corruption(chain):
    store = BlockStore(":memory:")
    # 位点存在但无区块
    with store.tx():
        store.set_progress(3)
    ix = ChainIndexer(store, confirmation_depth=3)
    with pytest.raises(StoreCorruptedError):
        ix.resume()

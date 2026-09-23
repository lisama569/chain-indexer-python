"""覆盖重复投递、分叉重组与重启恢复场景。"""
from __future__ import annotations

import logging

import pytest

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError, RejectReason
from chain_indexer.indexer import Indexer, IngestResult
from chain_indexer.store import IndexStore


@pytest.fixture()
def env():
    chain = LocalChain()
    store = IndexStore()
    return chain, store, Indexer(store, chain, max_reorg_depth=4, max_catchup_span=10)


# ---------- 重复投递与乱序 ----------

def test_duplicate_delivery_is_idempotent(env, caplog):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(3)]
    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        assert idx.ingest_block(blocks[0]) == IngestResult.APPLIED
        assert idx.ingest_block(blocks[0]) == IngestResult.DUPLICATE
        assert idx.ingest_block(blocks[1]) == IngestResult.APPLIED
        assert idx.ingest_block(blocks[1]) == IngestResult.DUPLICATE
    # 不重复入库：高度唯一、交易唯一
    assert store.get_block_by_height(0).block_hash == blocks[0].block_hash
    txid = blocks[0].transactions[0].txid
    assert store.get_transaction(txid) is not None
    assert store.get_checkpoint().height == 1
    # 事件流水包含处理位点
    texts = [r.getMessage() for r in caplog.records]
    assert any("event=applied height=0" in t for t in texts)
    assert any("event=duplicate height=1" in t for t in texts)


def test_out_of_order_and_gap_rejected(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(3)]
    # 乱序：先投高度 1（父哈希不是创世之后链尖）
    with pytest.raises(RejectError) as ei:
        idx.ingest_block(blocks[1])
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    idx.ingest_block(blocks[0])
    # 跳高度投递
    with pytest.raises(RejectError) as ei:
        idx.ingest_block(blocks[2])
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    # 父哈希不符
    bad = chain.get_block(1)
    forged = type(bad)(height=1, block_hash="x", parent_hash="wrong", transactions=())
    with pytest.raises(RejectError) as ei:
        idx.ingest_block(forged)
    assert ei.value.reason == RejectReason.ORPHAN_BLOCK
    # 被拒绝的数据未入库
    assert store.get_block_by_height(1) is None


def test_conflicting_block_unlinkable_rejected(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(2)]
    idx.sync_to(1)
    # 同高度不同哈希，且父哈希与已存前块不符 -> 明确拒绝而非部分应用
    forged = type(blocks[1])(height=1, block_hash="evil", parent_hash="unknown",
                             transactions=())
    with pytest.raises(RejectError) as ei:
        idx.ingest_block(forged)
    assert ei.value.reason == RejectReason.CONFLICTING_BLOCK
    # 已确认数据未被覆盖
    assert store.get_block_by_height(1).block_hash == blocks[1].block_hash


# ---------- 分叉重组 ----------

def test_reorg_rolls_back_and_reindexes(env, caplog):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(5)]
    idx.sync_to(4)
    old_hash_3 = store.get_block_by_height(3).block_hash
    old_tx_4 = blocks[4].transactions[0].txid

    chain.fork(from_height=2, length=4)  # 新分支覆盖高度 3..6
    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        assert idx.sync_to(6) == 6

    # 回滚范围自洽：旧分支区块与交易不可见，新分支可见
    assert store.get_block_by_height(3).block_hash != old_hash_3
    assert store.get_block_by_height(3).block_hash == chain.get_block(3).block_hash
    assert store.get_transaction(old_tx_4) is None
    assert store.get_checkpoint().height == 6
    assert store.get_checkpoint().block_hash == chain.get_block(6).block_hash
    # 高度 0..2 未受影响
    for h in range(3):
        assert store.get_block_by_height(h).block_hash == chain.get_block(h).block_hash
    texts = [r.getMessage() for r in caplog.records]
    assert any("event=rollback fork_point=2 depth=2" in t for t in texts)


def test_single_block_fork_ingest_triggers_reorg(env):
    chain, store, idx = env
    [chain.append_block() for _ in range(3)]
    idx.sync_to(2)
    new_blocks = chain.fork(from_height=1, length=2)
    # 直接投递新分支的高度 2 区块：回滚旧高度 2 后落库
    assert idx.ingest_block(new_blocks[0]) == IngestResult.REORG_APPLIED
    assert store.get_block_by_height(2).block_hash == new_blocks[0].block_hash
    assert idx.ingest_block(new_blocks[1]) == IngestResult.APPLIED
    assert store.get_checkpoint().height == 3


def test_deep_reorg_rejected_without_partial_apply(env):
    chain, store, idx = env  # max_reorg_depth=4
    [chain.append_block() for _ in range(8)]
    idx.sync_to(7)
    before = store.get_checkpoint()
    chain.fork(from_height=1, length=7)  # 深度 6 > 4
    with pytest.raises(RejectError) as ei:
        idx.sync_to(7)
    assert ei.value.reason == RejectReason.REORG_TOO_DEEP
    # 未部分应用：位点与历史保持不变
    assert store.get_checkpoint() == before
    assert store.get_block_by_height(7).block_hash == before.block_hash


# ---------- 持久化与重启恢复 ----------

def test_restart_resumes_from_checkpoint(tmp_path, caplog):
    db = str(tmp_path / "idx.db")
    chain = LocalChain()
    [chain.append_block() for _ in range(6)]

    store1 = IndexStore(db)
    idx1 = Indexer(store1, chain, max_catchup_span=10)
    idx1.sync_to(3)
    cp_before = store1.get_checkpoint()
    store1.close()

    # 模拟进程重启：新 store 实例加载同一数据库
    store2 = IndexStore(db)
    idx2 = Indexer(store2, chain, max_catchup_span=10)
    assert store2.get_checkpoint() == cp_before  # 历史结论保留
    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        assert idx2.sync_to(5) == 5  # 从正确位点继续
    assert store2.get_checkpoint().height == 5
    for h in range(6):
        assert store2.get_block_by_height(h).block_hash == chain.get_block(h).block_hash
    texts = [r.getMessage() for r in caplog.records]
    assert any("event=applied height=4" in t for t in texts)
    assert not any("event=applied height=3 " in t for t in texts)  # 未重复索引


def test_checkpoint_regression_and_oversized_catchup_rejected(tmp_path):
    db = str(tmp_path / "idx.db")
    chain = LocalChain()
    [chain.append_block() for _ in range(25)]
    store = IndexStore(db)
    idx = Indexer(store, chain, max_catchup_span=10)
    idx.sync_to(9)

    with pytest.raises(RejectError) as ei:
        idx.sync_to(5)  # 位点回退
    assert ei.value.reason == RejectReason.CHECKPOINT_REGRESSION

    with pytest.raises(RejectError) as ei:
        idx.sync_to(20)  # 跨度 11 > 10
    assert ei.value.reason == RejectReason.CATCHUP_TOO_LARGE

    with pytest.raises(RejectError) as ei:
        idx.sync_to(100)  # 超出本地链尖
    assert ei.value.reason == RejectReason.HEIGHT_GAP

    assert store.get_checkpoint().height == 9  # 拒绝后位点不变

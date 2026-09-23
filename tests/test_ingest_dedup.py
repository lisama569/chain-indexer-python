"""场景：按高度追加索引、重复投递、乱序到达。

失败原因必须可区分：duplicate_delivery / height_gap / stale_height /
parent_mismatch / confirmed_data_overwrite，且任何拒绝都不得改动已有数据。
"""

from __future__ import annotations

import pytest

from app.errors import (
    ConfirmedDataOverwriteError,
    DuplicateDeliveryError,
    GapError,
    ParentMismatchError,
    StaleHeightError,
)
from app.models import GENESIS_PARENT, Block


def _build(indexer, chain, n: int):
    blocks = [chain.produce(payload=f"p{i}") for i in range(n)]
    for b in blocks:
        indexer.ingest_block(b)
    return blocks


def test_sequential_ingest_and_queries(indexer, chain):
    blocks = _build(indexer, chain, 5)

    assert indexer.cursor == 4
    # 确认边界：cursor - depth = 1
    assert indexer.confirmed_watermark == 1

    for i, b in enumerate(blocks):
        assert indexer.get_block(i).hash == b.hash
        tx = b.transactions[0]
        assert indexer.get_transaction(tx.tx_hash).block_hash == b.hash

    # 未索引高度返回 None
    assert indexer.get_block(99) is None
    assert indexer.get_transaction("0xmissing") is None


def test_watermark_none_until_enough_depth(indexer, chain):
    """确认深度为 3 时：0..2 块没有任何确认高度，第 4 块起 h=0 才确认。"""
    blocks = [chain.produce() for _ in range(3)]
    for b in blocks:
        indexer.ingest_block(b)
    assert indexer.cursor == 2
    assert indexer.confirmed_watermark is None  # 2-3 < 0

    b3 = chain.produce()
    indexer.ingest_block(b3)
    assert indexer.confirmed_watermark == 0  # 3-3 = 0


def test_first_block_must_be_genesis(indexer, chain):
    chain.produce()  # 有一个高度 0，但直接投递高度 1
    b1 = chain.get_block(0)
    from app.models import Block as B

    with pytest.raises(GapError) as ei:
        indexer.ingest_block(B(1, "0xh1", "0xh0"))
    assert ei.value.reason == "height_gap"
    assert indexer.cursor is None  # 空库未被污染

    # 创世块父哈希错误
    bad = Block(0, "0xgenesis", "0xnotzero")
    with pytest.raises(ParentMismatchError) as ei:
        indexer.ingest_block(bad)
    assert ei.value.reason == "parent_mismatch"


def test_duplicate_delivery_is_idempotent_and_rejected(indexer, chain):
    blocks = _build(indexer, chain, 5)

    # 重复投递同一块（模拟网络至少一次投递语义）
    with pytest.raises(DuplicateDeliveryError) as ei:
        indexer.ingest_block(blocks[2])
    assert ei.value.reason == "duplicate_delivery"
    assert ei.value.height == 2

    # 位点与数据不变
    assert indexer.cursor == 4
    assert indexer.get_block(2).hash == blocks[2].hash

    # 再次重复，结果仍然确定
    with pytest.raises(DuplicateDeliveryError):
        indexer.ingest_block(blocks[2])
    assert indexer.cursor == 4


def test_out_of_order_gap_is_rejected(indexer, chain):
    blocks = _build(indexer, chain, 3)  # 高度 0..2，位点 2

    # 高度 5 先到，高度 3..4 缺失
    ahead = Block(5, "0xh5", "0xh4?")
    with pytest.raises(GapError) as ei:
        indexer.ingest_block(ahead)
    assert ei.value.reason == "height_gap"

    # 补齐正确顺序后仍可继续推进
    b3 = chain.produce(payload="p3")
    result = indexer.ingest_block(b3)
    assert result.action == "accepted"
    assert indexer.cursor == 3


def test_parent_hash_mismatch_rejected(indexer, chain):
    _build(indexer, chain, 3)
    wrong = Block(3, "0xh3new", "0xdeadbeef")
    with pytest.raises(ParentMismatchError) as ei:
        indexer.ingest_block(wrong)
    assert ei.value.reason == "parent_mismatch"
    assert indexer.cursor == 2  # 位点未推进


def test_stale_fork_block_without_path_is_rejected(indexer, chain):
    """同高度不同哈希但父链接不上库中链：无法定位基点 -> stale_height。"""
    blocks = _build(indexer, chain, 5)

    # h=4 未确认（wm=1），但 parent_hash 指向一个未知父块
    orphan = Block(4, "0xfork4", "0xunknownparent")
    with pytest.raises(StaleHeightError) as ei:
        indexer.ingest_block(orphan)
    assert ei.value.reason == "stale_height"
    assert indexer.get_block(4).hash == blocks[4].hash  # 原数据不动


def test_confirmed_height_cannot_be_overwritten(indexer, chain):
    blocks = _build(indexer, chain, 5)

    # h=1 <= wm=1 已确认，同高度不同哈希必须拒绝
    confirmed_fork = Block(1, "0xrewrite1", blocks[0].hash)
    with pytest.raises(ConfirmedDataOverwriteError) as ei:
        indexer.ingest_block(confirmed_fork)
    assert ei.value.reason == "confirmed_data_overwrite"
    assert ei.value.height == 1
    assert indexer.get_block(1).hash == blocks[1].hash
    assert indexer.cursor == 4

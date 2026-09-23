"""场景：链重组（分叉回滚 + 重新索引）。

覆盖：
- 未确认高度上的单块 / 多块分叉，回滚范围、位点与查询可见性自洽；
- 分叉路径不连续 / 断链 / 基点不符的可区分拒绝；
- 基点落入确认深度 -> reorg_too_deep，且不发生任何部分应用；
- 重组后旧交易不可见、新交易可查，最终一致。
"""

from __future__ import annotations

import pytest

from app.errors import (
    DuplicateDeliveryError,
    ParentMismatchError,
    ReorgTooDeepError,
    StaleHeightError,
)
from app.models import Block


def _build(indexer, chain, n):
    blocks = [chain.produce(payload=f"p{i}") for i in range(n)]
    for b in blocks:
        indexer.ingest_block(b)
    return blocks


def test_single_block_unconfirmed_reorg(indexer, chain):
    blocks = _build(indexer, chain, 5)  # wm = 1

    # h=4 未确认，同父的兄弟块替换它：回滚 [4] 再重建
    sibling = Block(
        4,
        "0xsibling4",
        blocks[3].hash,
        (),
    )
    result = indexer.ingest_block(sibling)
    assert result.action == "reorg"
    assert result.rolled_back == (4,)
    assert indexer.cursor == 4

    assert indexer.get_block(4).hash == "0xsibling4"
    # 旧块交易被移除
    old_tx = blocks[4].transactions[0].tx_hash
    assert indexer.get_transaction(old_tx) is None
    # 确认高度数据不受影响
    assert indexer.get_block(1).hash == blocks[1].hash


def test_multi_block_reorg_rolls_back_and_reindexes(indexer, chain):
    blocks = _build(indexer, chain, 8)  # wm = 4，h<=4 已确认
    old_txs = {b.transactions[0].tx_hash for b in blocks[5:]}

    # 从 h=5（基点 h=4 已确认，h=5 未确认）分叉，替换 5..7
    forked = chain.produce_fork(fork_at=4)
    assert [b.height for b in forked] == [5, 6, 7]

    result = indexer.reindex_path(forked)
    assert result.action == "reorg"
    assert result.rolled_back == (7, 6, 5)  # 降序回滚
    assert indexer.cursor == 7

    # 新链可见、旧链交易消失
    for b in forked:
        assert indexer.get_block(b.height).hash == b.hash
        assert indexer.get_transaction(b.transactions[0].tx_hash) is not None
    for tx in old_txs:
        assert indexer.get_transaction(tx) is None

    # 基点及之前完全不动
    for h in range(0, 5):
        assert indexer.get_block(h).hash == blocks[h].hash


def test_reorg_can_shorten_then_extend(indexer, chain):
    """分叉新链短于旧链：游标随新链尖下降，之后仍可继续顺序追加。"""
    blocks = _build(indexer, chain, 8)  # wm=4

    # 手工构造从 h=6 开始的短分叉（只有一块，新链尖 6）
    short = [Block(6, "0xshort6", blocks[5].hash)]
    result = indexer.reindex_path(short)
    assert result.action == "reorg"
    assert result.rolled_back == (7, 6)
    assert indexer.cursor == 6
    assert indexer.get_block(7) is None

    # 在缩短后的链上继续推进
    b7 = Block(7, "0xnew7", "0xshort6")
    r = indexer.ingest_block(b7)
    assert r.action == "accepted"
    assert indexer.cursor == 7
    assert indexer.get_block(7).hash == "0xnew7"


def test_reorg_too_deep_is_fully_rejected(indexer, chain):
    blocks = _build(indexer, chain, 8)  # wm = 4

    # 路径自身合法（连续、相连），但首高度 h=2 <= wm=4
    path = [
        Block(2, "0xf2", blocks[1].hash),
        Block(3, "0xf3", "0xf2"),
    ]
    snapshot = [(h, indexer.get_block(h).hash) for h in range(8)]

    with pytest.raises(ReorgTooDeepError) as ei:
        indexer.reindex_path(path)
    assert ei.value.reason == "reorg_too_deep"
    assert ei.value.height == 2

    # 无部分应用：全部数据、位点保持原状
    assert indexer.cursor == 7
    for h, hsh in snapshot:
        assert indexer.get_block(h).hash == hsh


def test_reorg_path_validation_errors_are_distinguishable(indexer, chain):
    blocks = _build(indexer, chain, 5)  # wm=1

    # 1) 空路径
    with pytest.raises(ParentMismatchError) as ei:
        indexer.reindex_path([])
    assert ei.value.reason == "parent_mismatch"

    # 2) 高度不连续
    bad_gap = [Block(4, "0xa", blocks[3].hash), Block(6, "0xb", "0xa")]
    with pytest.raises(ParentMismatchError) as ei:
        indexer.reindex_path(bad_gap)
    assert ei.value.reason == "parent_mismatch"

    # 3) parent 断链
    bad_link = [Block(4, "0xa", blocks[3].hash), Block(5, "0xb", "0xother")]
    with pytest.raises(ParentMismatchError) as ei:
        indexer.reindex_path(bad_link)
    assert ei.value.reason == "parent_mismatch"

    # 4) 基点不存在（从未来高度开始）
    with pytest.raises(StaleHeightError) as ei:
        indexer.reindex_path([Block(9, "0xa", "0xp")])
    assert ei.value.reason == "stale_height"

    # 5) 首块与基点哈希不符
    with pytest.raises(ParentMismatchError) as ei:
        indexer.reindex_path([Block(4, "0xa", "0xwrongbase")])
    assert ei.value.reason == "parent_mismatch"

    # 6) 起点已索引且哈希相同 = 重复投递
    with pytest.raises(DuplicateDeliveryError) as ei:
        indexer.reindex_path([blocks[4]])
    assert ei.value.reason == "duplicate_delivery"

    # 所有失败后状态不变
    assert indexer.cursor == 4


def test_chain_fork_then_reorg_idempotent_on_repeat(indexer, chain):
    """分叉路径投递成功后，再次投递同一路径 -> duplicate，不重复入库。"""
    blocks = _build(indexer, chain, 9)  # wm=5
    forked = chain.produce_fork(fork_at=6)  # h=7..8，均未确认

    r1 = indexer.reindex_path(forked)
    assert r1.action == "reorg"

    with pytest.raises(DuplicateDeliveryError) as ei:
        indexer.reindex_path(forked)
    assert ei.value.reason == "duplicate_delivery"

    assert indexer.cursor == 8
    assert indexer.get_block(8).hash == forked[-1].hash

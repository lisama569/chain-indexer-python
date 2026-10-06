"""预演对"先触发重组、最终被拒"的追赶请求：回滚范围完整呈现且严格零写入。"""
from __future__ import annotations

import logging

import pytest

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError, RejectReason
from chain_indexer.indexer import Indexer
from chain_indexer.plan import (
    STATUS_ACCEPTED,
    STATUS_REJECTED,
    IngestOp,
    SyncOp,
)
from chain_indexer.store import IndexStore


@pytest.fixture()
def env():
    chain = LocalChain()
    store = IndexStore()
    idx = Indexer(store, chain, max_reorg_depth=4, max_catchup_span=10)
    return chain, store, idx


def test_plan_reorg_then_rejected_shows_full_rollback_scope_and_zero_write(env, caplog):
    """追赶会先重组回滚、再因目标超出链尖被拒：预演必须给出候选回滚范围。"""
    chain, store, idx = env
    [chain.append_block() for _ in range(6)]
    idx.sync_to(5)
    chain.fork(from_height=3, length=3)  # 新分支覆盖 4..6，链尖 = 6
    before = store.snapshot()

    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        plan = idx.plan_sync(9)  # 会先回滚 5、4，再因 9 超出链尖 6 被拒

    # 完整执行计划：拒绝原因可分辨 + 候选回滚范围不被吞掉
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.HEIGHT_GAP
    assert plan.rolled_back == (5, 4)
    assert plan.outcomes[0].status == STATUS_REJECTED
    assert plan.outcomes[0].reject_reason == RejectReason.HEIGHT_GAP.value
    assert plan.outcomes[0].rolled_back == (5, 4)
    # 实际位点未变
    assert plan.cp_after == plan.cp_before
    # 预演严格只读：位点/区块/交易与没跑过一样
    assert store.snapshot() == before

    # 随后真执行：同一类原因被拒，同样什么都不动
    with pytest.raises(RejectError) as ei:
        idx.sync_to(9)
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before
    assert store.get_checkpoint().height == 5

    # 日志能看到评估过程：候选回滚事件、拒绝事件、最终决策
    texts = [r.getMessage() for r in caplog.records]
    assert any("event=rollback fork_point=3 depth=2" in t for t in texts)
    assert any("event=reject reason=height_gap" in t for t in texts)
    assert any(
        "event=decision mode=plan" in t and "status=rejected" in t
        and "rolled_back=[5, 4]" in t
        for t in texts
    )


def test_plan_reorg_then_catchup_too_large_shows_rollback_scope(env):
    """先回滚、再因跨度过大被拒：同一口径给出候选回滚范围，零写入。"""
    chain, store, idx = env  # max_catchup_span=10
    [chain.append_block() for _ in range(20)]
    idx.sync_to(9)
    idx.sync_to(12)  # 分两段推进，避免初始追赶本身超跨度
    chain.fork(from_height=8, length=11)  # 新分支覆盖 9..19，链尖 = 19
    before = store.snapshot()

    plan = idx.plan_sync(19)  # 回滚 12..9 后，跨度 19-8=11 > 10 被拒
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.CATCHUP_TOO_LARGE
    assert plan.rolled_back == (12, 11, 10, 9)
    assert plan.outcomes[0].rolled_back == (12, 11, 10, 9)
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.execute_sync(19)
    assert ei.value.reason == RejectReason.CATCHUP_TOO_LARGE
    assert store.snapshot() == before


def test_plan_rejected_without_reorg_has_empty_rollback_scope(env):
    """位点回退等拒绝发生在回滚评估之前：回滚范围自然为空，原因仍可分辨。"""
    chain, store, idx = env
    [chain.append_block() for _ in range(4)]
    idx.sync_to(3)
    before = store.snapshot()

    plan = idx.plan_sync(1)
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.CHECKPOINT_REGRESSION
    assert plan.rolled_back == ()
    assert plan.outcomes[0].rolled_back == ()
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.sync_to(1)
    assert ei.value.reason == RejectReason.CHECKPOINT_REGRESSION
    assert store.snapshot() == before


def test_retry_same_target_after_reorg_reject_is_stable(env):
    """被拒后按同样目标重试：链数据没变则结论稳定，不重复入库也不漏数据。"""
    chain, store, idx = env
    [chain.append_block() for _ in range(6)]
    idx.sync_to(5)
    chain.fork(from_height=3, length=3)
    before = store.snapshot()

    first = idx.plan_sync(9)
    for _ in range(3):
        plan = idx.plan_sync(9)
        assert plan.status == STATUS_REJECTED
        assert plan.reject_reason == first.reject_reason
        assert plan.rolled_back == first.rolled_back == (5, 4)
        with pytest.raises(RejectError) as ei:
            idx.sync_to(9)
        assert ei.value.reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before  # 全程零写入

    # 链追上目标后同一请求成功：回滚范围与此前预演的候选一致，只应用一次
    chain.append_block()
    chain.append_block()
    chain.append_block()  # 链尖到 9
    assert idx.sync_to(9) == 9
    _, hashes, txids = store.snapshot()
    assert sorted(hashes) == list(range(10))
    assert len(txids) == 10  # 无重复入库
    # 再按同样目标重试：幂等 noop
    again = idx.execute_sync(9)
    assert again.applied == () and again.rolled_back == ()
    assert store.snapshot()[2] == txids


def test_batch_plan_per_op_rollback_scope_on_reorg_then_reject(env):
    """批量预演：每个操作分别给出会不会触发重组、涉及哪些回滚。"""
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(6)]
    idx.sync_to(5)
    chain.fork(from_height=3, length=3)  # 链尖 = 6
    before = store.snapshot()

    ops = [
        IngestOp(chain.get_block(4)),  # 单块触发重组：回滚 5、4 后落库 4
        SyncOp(6),                     # 顺势推进 5、6
        SyncOp(9),                     # 超出链尖被拒（此前无新增回滚）
    ]
    plan = idx.plan_batch(ops)

    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.HEIGHT_GAP
    # 逐项结论与逐项回滚范围
    assert [o.status for o in plan.outcomes] == [
        STATUS_ACCEPTED, STATUS_ACCEPTED, STATUS_REJECTED,
    ]
    assert plan.outcomes[0].result == "reorg_applied"
    assert plan.outcomes[0].rolled_back == (5, 4)
    assert plan.outcomes[1].rolled_back == ()
    assert plan.outcomes[2].reject_reason == RejectReason.HEIGHT_GAP.value
    assert plan.outcomes[2].rolled_back == ()
    # 顶层候选汇总：前两个操作的候选动作 + 全量候选回滚
    assert plan.rolled_back == (5, 4)
    assert plan.applied == (4, 5, 6)
    # 整批零写入
    assert store.snapshot() == before

    # 真执行：同一类原因被拒，整批不动
    with pytest.raises(RejectError) as ei:
        idx.execute_batch(ops)
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before
    assert store.get_checkpoint().height == 5
    # 旧分支数据原样保留（没有被部分回滚）
    assert store.get_block_by_height(5).block_hash == blocks[5].block_hash


def test_rejected_decision_journal_keeps_candidate_rollback_scope(tmp_path):
    """被拒决策落流水后重启仍可查到候选回滚范围与分类原因。"""
    db = str(tmp_path / "journal.db")
    chain = LocalChain()
    [chain.append_block() for _ in range(6)]

    store1 = IndexStore(db)
    idx1 = Indexer(store1, chain, max_reorg_depth=4, max_catchup_span=10)
    idx1.sync_to(5)
    chain.fork(from_height=3, length=3)
    plan = idx1.plan_sync(9)
    with pytest.raises(RejectError):
        idx1.sync_to(9)
    store1.close()

    store2 = IndexStore(db)
    idx2 = Indexer(store2, chain, max_reorg_depth=4, max_catchup_span=10)
    rejects = idx2.list_decisions(status=STATUS_REJECTED)
    assert len(rejects) == 2
    for d in rejects:
        assert d.reject_reason == RejectReason.HEIGHT_GAP
        assert d.rolled_back == (5, 4)           # 候选回滚范围随流水持久化
        assert d.outcomes[0].rolled_back == (5, 4)
        assert d.cp_after == d.cp_before         # 实际位点未变
    assert store2.get_checkpoint().height == 5   # 重启后位点恢复，无部分应用
    store2.close()

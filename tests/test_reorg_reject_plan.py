"""本轮修复：预演对"先触发重组、最终又被拒绝"的追赶请求给出完整回滚范围。

覆盖：
- 单块追赶：预演给出候选回滚高度且零写入，紧接执行被同一类原因拒绝且零写入；
- 其它拒绝类别（跨度过大、位点回退）同样给全信息且可分辨；
- 批量预演逐操作给出各自的回滚范围；
- 被拒后按同样目标重试结论稳定、不重复入库；
- 候选回滚随决策流水持久化，重启后可查；
- 事件日志能看到处理位点、回滚高度与拒绝原因。
"""
from __future__ import annotations

import logging

import pytest

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError, RejectReason
from chain_indexer.indexer import Indexer
from chain_indexer.models import Checkpoint
from chain_indexer.plan import (
    MODE_EXECUTE,
    MODE_PLAN,
    IngestOp,
    STATUS_ABORTED,
    STATUS_ACCEPTED,
    STATUS_REJECTED,
    SyncOp,
)
from chain_indexer.store import IndexStore


@pytest.fixture()
def env():
    chain = LocalChain()
    store = IndexStore()
    idx = Indexer(store, chain, max_reorg_depth=4, max_catchup_span=10)
    return chain, store, idx


def _forked_state(env, indexed: int = 5, fork_from: int = 3, fork_len: int = 3):
    """建链 -> 追赶到 indexed -> 在 fork_from 处分叉（链尖高度与原链一致）。"""
    chain, store, idx = env
    [chain.append_block() for _ in range(indexed + 1)]
    idx.sync_to(indexed)
    old_hashes = {
        h: store.get_block_by_height(h).block_hash for h in range(indexed + 1)
    }
    chain.fork(from_height=fork_from, length=fork_len)
    return old_hashes


# ---------- 核心：先回滚、后因目标超出链尖被拒 ----------

def test_plan_reorg_then_reject_shows_full_rollback_scope_read_only(env, caplog):
    chain, store, idx = env
    old_hashes = _forked_state(env)
    # 索引仍在旧链位点 5；本地链已在高度 3 分叉，链尖还是 6。
    # 追赶到 7：评估时必须先回滚旧 5、4，随后才发现 7 超出链尖 6 -> 拒绝。
    before = store.snapshot()
    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        plan = idx.plan_sync(7)

    assert plan.mode == MODE_PLAN
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.HEIGHT_GAP
    # 关键：回滚范围不能因为最终被拒而被吞掉
    assert plan.rolled_back == (5, 4)
    assert plan.applied == ()
    assert plan.duplicates == ()
    # 单块追赶的呈现与批量逐项口径一致
    (outcome,) = plan.outcomes
    assert outcome.status == STATUS_REJECTED
    assert outcome.reject_reason == RejectReason.HEIGHT_GAP.value
    assert outcome.rolled_back == (5, 4)
    assert outcome.applied == ()
    # 预演严格只读：位点/区块/交易与跑之前完全一致
    assert plan.cp_after == plan.cp_before == Checkpoint(5, old_hashes[5])
    assert store.snapshot() == before
    for h, hsh in old_hashes.items():
        assert store.get_block_by_height(h).block_hash == hsh

    # 日志能看到处理位点、候选回滚高度与拒绝事件
    texts = [r.getMessage() for r in caplog.records]
    assert any("event=op_start" in t and "target=7" in t and "cp=5" in t
               for t in texts)
    assert any("event=rollback" in t and "heights=[5, 4]" in t for t in texts)
    assert any("event=reject reason=height_gap" in t for t in texts)
    assert any("status=rejected" in t and "rolled_back=[5, 4]" in t for t in texts)


def test_execute_after_rejected_plan_same_reason_and_zero_write(env):
    chain, store, idx = env
    _forked_state(env)
    before = store.snapshot()

    plan = idx.plan_sync(7)
    assert plan.status == STATUS_REJECTED

    # 紧接着真执行：必须被同一类原因拒绝
    with pytest.raises(RejectError) as ei:
        idx.execute_sync(7)
    assert ei.value.reason == plan.reject_reason == RejectReason.HEIGHT_GAP
    # 严格零部分应用：位点、区块、交易没有任何变化
    assert store.snapshot() == before
    assert store.get_checkpoint() == before[0]
    assert store.get_block_by_height(5).block_hash == before[1][5]
    assert store.get_block_by_height(4).block_hash == before[1][4]

    # 兼容旧签名：sync_to 同样抛出
    with pytest.raises(RejectError) as ei:
        idx.sync_to(7)
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before


# ---------- 其它拒绝类别：口径一致、原因仍可分辨 ----------

def test_plan_reorg_then_catchup_too_large_keeps_rollback_scope(env):
    """先重组、后因跨度过大被拒：同样要看到候选回滚，且原因不是 height_gap。"""
    chain, store, idx = env  # max_catchup_span=10
    _forked_state(env, indexed=5, fork_from=3, fork_len=3)  # 链尖 6
    [chain.append_block() for _ in range(20)]               # 链尖 26
    before = store.snapshot()

    plan = idx.plan_sync(26)
    # 回滚后位点退到 3；跨度 26-3=23 > 10
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.CATCHUP_TOO_LARGE
    assert plan.rolled_back == (5, 4)
    assert plan.applied == ()
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.execute_sync(26)
    assert ei.value.reason == RejectReason.CATCHUP_TOO_LARGE
    assert store.snapshot() == before


def test_plan_regression_rejected_with_empty_candidate_scope(env):
    """位点回退：不会触发任何回滚，候选范围为空，原因仍独立可分辨。"""
    chain, store, idx = env
    _forked_state(env)
    before = store.snapshot()

    plan = idx.plan_sync(2)
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.CHECKPOINT_REGRESSION
    assert plan.rolled_back == ()
    assert plan.applied == ()
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.sync_to(2)
    assert ei.value.reason == RejectReason.CHECKPOINT_REGRESSION
    assert store.snapshot() == before


def test_reorg_too_deep_still_rejected_before_touching_data(env):
    """重组深度超限时在回滚之前就拒绝：候选范围为空、零写入（原分类不变）。"""
    chain, store, idx = env  # max_reorg_depth=4
    [chain.append_block() for _ in range(8)]
    idx.sync_to(7)
    chain.fork(from_height=1, length=7)  # 深度 6 > 4
    before = store.snapshot()

    plan = idx.plan_sync(7)
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.REORG_TOO_DEEP
    assert plan.rolled_back == ()
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.execute_sync(7)
    assert ei.value.reason == RejectReason.REORG_TOO_DEEP
    assert store.snapshot() == before


# ---------- 批量预演：逐项回滚范围 ----------

def test_plan_batch_single_reorg_then_reject_matches_single_op_presentation(env):
    """批量里只放一个"先回滚后拒绝"操作：逐项结论与单块追赶口径一致。"""
    chain, store, idx = env
    _forked_state(env)
    before = store.snapshot()

    plan = idx.plan_batch([SyncOp(7)])
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.HEIGHT_GAP
    # 顶层与逐项都是完整候选回滚范围
    assert plan.rolled_back == (5, 4)
    (outcome,) = plan.outcomes
    assert outcome.status == STATUS_REJECTED
    assert outcome.reject_reason == RejectReason.HEIGHT_GAP.value
    assert outcome.rolled_back == (5, 4)
    assert outcome.applied == ()
    assert plan.cp_after == plan.cp_before
    assert store.snapshot() == before


def test_plan_batch_per_op_rollback_scopes_are_separated(env):
    """前一个操作成功重组到链尖，后一个操作"先评估回滚后被拒"：
    两个操作的候选范围必须能分别看出，整批仍零写入。"""
    chain, store, idx = env
    _forked_state(env)  # 索引旧链到 5；链在 3 分叉后链尖 6
    before = store.snapshot()

    plan = idx.plan_batch([SyncOp(6), SyncOp(7)])
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.HEIGHT_GAP

    op0, op1 = plan.outcomes
    # 操作 0（候选成功）：回滚旧 5、4，应用新 4、5、6
    assert op0.status == STATUS_ACCEPTED
    assert op0.result == "reorg_applied"
    assert op0.rolled_back == (5, 4)
    assert op0.applied == (4, 5, 6)
    # 操作 1（拒绝点）：评估时链已与本地链一致，无额外回滚，原因清晰
    assert op1.status == STATUS_REJECTED
    assert op1.reject_reason == RejectReason.HEIGHT_GAP.value
    assert op1.applied == ()
    # 顶层是所有操作候选动作的并集（按发生顺序）
    assert plan.rolled_back == (5, 4)
    assert plan.applied == (4, 5, 6)
    assert plan.duplicates == ()
    # 整批预演只读
    assert store.snapshot() == before

    # 真执行整批：同一类原因拒绝，且操作 0 的候选写入也随事务回滚
    with pytest.raises(RejectError) as ei:
        idx.execute_batch([SyncOp(6), SyncOp(7)])
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before
    assert store.get_checkpoint().height == 5
    assert store.get_block_by_height(5).block_hash == before[1][5]


def test_plan_batch_rejected_tail_ops_aborted_with_empty_scopes(env):
    chain, store, idx = env
    _forked_state(env)
    blocks = [chain.get_block(h) for h in range(7)]
    before = store.snapshot()

    plan = idx.plan_batch([SyncOp(7), IngestOp(blocks[0])])
    op0, op1 = plan.outcomes
    assert op0.status == STATUS_REJECTED
    assert op0.rolled_back == (5, 4)
    assert op1.status == STATUS_ABORTED
    assert op1.applied == () and op1.rolled_back == ()
    assert store.snapshot() == before


# ---------- 重试稳定：结论一致、不重复入库、不漏数据 ----------

def test_retry_reorg_then_reject_is_stable_and_idempotent(env):
    chain, store, idx = env
    _forked_state(env)  # 链尖 6，索引旧链位点 5
    before = store.snapshot()

    # 同样目标反复预演/执行：结论稳定，候选回滚范围一致，数据始终不动
    for _ in range(3):
        plan = idx.plan_sync(7)
        assert plan.status == STATUS_REJECTED
        assert plan.reject_reason == RejectReason.HEIGHT_GAP
        assert plan.rolled_back == (5, 4)
        with pytest.raises(RejectError) as ei:
            idx.execute_sync(7)
        assert ei.value.reason == RejectReason.HEIGHT_GAP
        assert store.snapshot() == before

    # 换成可达目标后成功一次：回滚 + 重索引恰好一份
    ok = idx.execute_sync(6)
    assert ok.status == STATUS_ACCEPTED
    assert ok.rolled_back == (5, 4)
    assert ok.applied == (4, 5, 6)
    assert store.get_checkpoint() == Checkpoint(6, chain.get_block(6).block_hash)
    _, hashes, txids = store.snapshot()
    assert sorted(hashes) == [0, 1, 2, 3, 4, 5, 6]
    assert len(txids) == 7  # 不重复入库

    # 再按原目标（现在高于位点且超链尖）重试：仍稳定拒绝；
    # 按当前位点目标重试：noop，不重复入库
    with pytest.raises(RejectError):
        idx.sync_to(7)
    retry = idx.execute_sync(6)
    assert retry.applied == () and retry.rolled_back == ()
    assert store.snapshot()[2] == txids


# ---------- 流水持久化：候选回滚范围重启后仍可查 ----------

def test_candidate_rollback_scope_persists_in_journal_across_restart(tmp_path):
    db = str(tmp_path / "journal.db")
    chain = LocalChain()
    [chain.append_block() for _ in range(6)]
    store1 = IndexStore(db)
    idx1 = Indexer(store1, chain, max_reorg_depth=4, max_catchup_span=10)
    idx1.sync_to(5)
    chain.fork(from_height=3, length=3)
    p = idx1.plan_sync(7)
    assert p.status == STATUS_REJECTED and p.rolled_back == (5, 4)
    with pytest.raises(RejectError):
        idx1.execute_sync(7)
    store1.close()

    store2 = IndexStore(db)
    idx2 = Indexer(store2, chain, max_reorg_depth=4, max_catchup_span=10)
    rows = idx2.list_decisions(status=STATUS_REJECTED)
    assert len(rows) == 2  # 预演拒绝 + 执行拒绝各一行
    for d in rows:
        assert d.reject_reason == RejectReason.HEIGHT_GAP
        assert d.rolled_back == (5, 4)
        assert d.applied == ()
        assert d.cp_after == d.cp_before
        assert d.outcomes[0].rolled_back == (5, 4)
        assert d.reject_detail
    # 重启后索引数据仍是被拒前的状态
    assert store2.get_checkpoint().height == 5
    store2.close()

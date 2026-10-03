"""本轮新增行为：预演只读且与执行一致、批量/并发原子幂等、决策流水持久化。"""
from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError, RejectReason
from chain_indexer.indexer import Indexer
from chain_indexer.models import Checkpoint
from chain_indexer.plan import (
    MODE_EXECUTE,
    MODE_PLAN,
    IngestOp,
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


# ---------- 预演：零写入，结论与执行一致 ----------

def test_plan_sync_is_read_only_and_execute_matches(env, caplog):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(4)]

    before = store.snapshot()
    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        plan = idx.plan_sync(3)

    assert plan.mode == MODE_PLAN
    assert plan.status == STATUS_ACCEPTED
    assert plan.applied == (0, 1, 2, 3)
    assert plan.rolled_back == ()
    assert plan.cp_after == Checkpoint(3, blocks[3].block_hash)
    # 预演后位点/区块/交易与预演前完全一致
    assert store.snapshot() == before
    assert store.get_checkpoint() is None

    # 随后真正执行：成功，且推进范围与预演完全一致
    execd = idx.execute_sync(3)
    assert execd.status == STATUS_ACCEPTED
    assert execd.applied == plan.applied
    assert execd.cp_after == plan.cp_after
    assert store.get_checkpoint().height == 3
    texts = [r.getMessage() for r in caplog.records]
    assert any("event=decision mode=plan" in t and "cp_after=3" in t for t in texts)
    assert any("event=applied height=0" in t for t in texts)


def test_plan_rejected_execute_rejected_same_reason_and_zero_write(env):
    chain, store, idx = env
    [chain.append_block() for _ in range(3)]
    idx.sync_to(2)
    before = store.snapshot()

    # 预演：超出链尖 -> 返回 rejected 计划（不抛异常），且零写入
    plan = idx.plan_sync(9)
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before

    # 执行：必须被拒绝，且原因同一种
    with pytest.raises(RejectError) as ei:
        idx.sync_to(9)
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    assert store.snapshot() == before

    # 位点回退：预演与执行同样拒绝
    plan_reg = idx.plan_sync(0)
    assert plan_reg.status == STATUS_REJECTED
    assert plan_reg.reject_reason == RejectReason.CHECKPOINT_REGRESSION
    with pytest.raises(RejectError) as ei:
        idx.sync_to(0)
    assert ei.value.reason == RejectReason.CHECKPOINT_REGRESSION
    assert store.get_checkpoint().height == 2


def test_plan_reorg_shows_rollback_scope_without_touching_data(env):
    """既触发回滚、又要判断目标可达性的请求：预演给出回滚范围但零写入。"""
    chain, store, idx = env
    [chain.append_block() for _ in range(6)]
    idx.sync_to(5)
    old_hashes = {h: store.get_block_by_height(h).block_hash for h in range(6)}

    chain.fork(from_height=3, length=3)  # 新分支覆盖高度 4..6
    before = store.snapshot()
    plan = idx.plan_sync(6)

    assert plan.status == STATUS_ACCEPTED
    # 回滚旧 5、4，再应用链上新的 4、5、6
    assert plan.rolled_back == (5, 4)
    assert plan.applied == (4, 5, 6)
    assert plan.cp_after.height == 6
    # 预演本身什么都没动
    assert store.snapshot() == before
    for h, hsh in old_hashes.items():
        assert store.get_block_by_height(h).block_hash == hsh

    # 执行后范围与预演完全一致
    assert idx.sync_to(6) == 6
    assert store.get_block_by_height(4).block_hash == chain.get_block(4).block_hash
    assert store.get_checkpoint().height == 6


def test_plan_deep_reorg_rejected_and_execute_also_rejected(env):
    """回滚深度超限且目标高于位点：必须先因重组过深整体拒绝、零部分应用。"""
    chain, store, idx = env  # max_reorg_depth=4
    [chain.append_block() for _ in range(8)]
    idx.sync_to(7)
    chain.fork(from_height=1, length=7)  # 深度 6 > 4

    before = store.snapshot()
    plan = idx.plan_sync(7)
    assert plan.status == STATUS_REJECTED
    assert plan.reject_reason == RejectReason.REORG_TOO_DEEP
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.execute_sync(7)
    assert ei.value.reason == RejectReason.REORG_TOO_DEEP
    assert store.snapshot() == before


# ---------- 批量：要么全成，要么全不动 ----------

def test_batch_all_or_nothing_on_rejection(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(6)]
    idx.sync_to(2)
    before = store.snapshot()

    # 第一个 sync 本可推进到 4，第二个却投一个跳高度块 -> 整批必须拒绝
    forged = type(blocks[0])(
        height=9, block_hash="x", parent_hash="nope", transactions=()
    )
    ops = [SyncOp(4), IngestOp(forged)]

    plan = idx.plan_batch(ops)
    assert plan.status == STATUS_REJECTED
    assert [o.status for o in plan.outcomes] == ["accepted", "rejected"]
    assert plan.outcomes[1].reject_reason == RejectReason.HEIGHT_GAP.value
    assert store.snapshot() == before

    with pytest.raises(RejectError) as ei:
        idx.execute_batch(ops)
    assert ei.value.reason == RejectReason.HEIGHT_GAP
    # 第一个操作已写入的内容也必须随事务回滚
    assert store.snapshot() == before
    assert store.get_checkpoint().height == 2
    assert store.get_block_by_height(3) is None
    assert store.get_block_by_height(4) is None


def test_batch_mixed_duplicates_and_applies_succeeds_once(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(5)]
    idx.sync_to(1)  # 已有 0、1

    decision = idx.execute_batch(
        [
            IngestOp(blocks[0]),  # 重复
            SyncOp(3),            # 推进 2、3
            IngestOp(blocks[3]),  # 同批刚应用后再投递 -> 仍判重复
            SyncOp(4),            # 推进 4
        ]
    )
    assert decision.status == STATUS_ACCEPTED
    assert [o.result for o in decision.outcomes] == [
        "duplicate", "applied", "duplicate", "applied"
    ]
    assert decision.applied == (2, 3, 4)
    assert decision.duplicates == (0, 3)
    assert store.get_checkpoint().height == 4
    _, hashes, txids = store.snapshot()
    assert sorted(hashes) == [0, 1, 2, 3, 4]
    assert len(txids) == 5  # 无重复入库


# ---------- 失败后重试：稳定、不重复、不漏 ----------

def test_retry_after_rejection_is_stable_and_idempotent(env):
    chain, store, idx = env  # max_catchup_span=10
    [chain.append_block() for _ in range(20)]
    idx.sync_to(4)

    # 跨度超限（span=11）被拒；同样目标重试多次，结论稳定
    for _ in range(3):
        with pytest.raises(RejectError) as ei:
            idx.sync_to(15)
        assert ei.value.reason == RejectReason.CATCHUP_TOO_LARGE
        assert store.get_checkpoint().height == 4
    assert store.get_block_by_height(5) is None

    # 合法目标成功一次
    assert idx.sync_to(14) == 14
    _, _, txids_ok = store.snapshot()
    assert len(txids_ok) == 15

    # 再按同样目标重试：幂等 noop，不重复入库
    decision = idx.execute_sync(14)
    assert decision.applied == ()
    _, _, txids_retry = store.snapshot()
    assert txids_retry == txids_ok

    # 重复投递旧块重试：始终 duplicate，行数不变
    block0 = chain.get_block(0)
    for _ in range(3):
        assert idx.ingest_block(block0) == "duplicate"
    assert store.snapshot()[2] == txids_ok


# ---------- 并发：结果确定、不串数据、不重复入库 ----------

def test_concurrent_same_target_syncs_converge_without_duplicates(env):
    """同一目标高度并发追赶：全部成功，最终链确定、数据恰好一份。"""
    chain, store, idx = env
    [chain.append_block() for _ in range(8)]
    barrier = threading.Barrier(10)

    def call(_):
        barrier.wait()
        return idx.sync_to(7)

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(call, range(10)))

    # 每个请求都得到确定结果 7（先到者应用，后到者幂等 noop）
    assert results == [7] * 10
    cp, hashes, txids = store.snapshot()
    assert cp == Checkpoint(7, chain.get_block(7).block_hash)
    assert sorted(hashes) == list(range(8))
    assert len(txids) == 8  # 无重复入库、无串数据


def test_concurrent_ingests_with_retry_apply_each_block_once(env):
    """多个来源几乎同时乱序投递同一批块，再并发重复投递：每块恰好应用一次。"""
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(8)]
    start = threading.Event()

    def push_until_applied(h: int) -> None:
        start.wait()
        for _ in range(1000):
            try:
                idx.ingest_block(blocks[h])
                return
            except RejectError as e:
                # 缺口/孤儿块：等链推进后重试；重复即成功
                assert e.reason in (
                    RejectReason.HEIGHT_GAP, RejectReason.ORPHAN_BLOCK
                )
        raise AssertionError(f"高度 {h} 始终未被接受")

    # 每个高度都有专属来源保证被推一次；推进完成后再并发推一遍（必然 duplicate）
    def two_waves(h: int) -> None:
        push_until_applied(h)
        assert idx.ingest_block(blocks[h]) == "duplicate"

    start.set()
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(two_waves, range(8)))

    cp, hashes, txids = store.snapshot()
    assert cp == Checkpoint(7, chain.get_block(7).block_hash)
    assert sorted(hashes) == list(range(8))
    assert all(hashes[h] == blocks[h].block_hash for h in range(8))
    assert len(txids) == 8  # 每个高度恰好一份，重复波不入库


def test_concurrent_final_state_is_reproducible(env):
    """两次独立的并发追赶工作负载，最终提交状态完全一致（可复现）。"""
    def fresh_state():
        ch = LocalChain()
        st = IndexStore()
        ix = Indexer(st, ch, max_catchup_span=10)
        [ch.append_block() for _ in range(8)]
        b = threading.Barrier(6)

        def call(t):
            b.wait()
            try:
                return ("ok", ix.sync_to(t))
            except RejectError as e:
                return ("reject", e.reason.value)

        with ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(call, [7, 3, 5, 7, 6, 2]))
        cp, hashes, txids = st.snapshot()
        return cp, tuple(sorted(hashes)), tuple(sorted(txids))

    assert fresh_state() == fresh_state()
    cp, heights, txids = fresh_state()
    assert cp.height == 7
    assert list(heights) == list(range(8))
    assert len(txids) == 8


def test_concurrent_rejected_batches_leave_nothing(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(6)]
    idx.sync_to(1)
    forged = type(blocks[0])(
        height=9, block_hash="x", parent_hash="nope", transactions=()
    )
    bad_ops = [SyncOp(4), IngestOp(forged)]

    errors = []

    def worker():
        try:
            idx.execute_batch(bad_ops)
        except RejectError as e:
            errors.append(e.reason)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(errors) == 8
    assert set(errors) == {RejectReason.HEIGHT_GAP}
    # 没有任何请求留下半成品
    assert store.get_checkpoint().height == 1
    assert store.get_block_by_height(2) is None


# ---------- 决策流水：持久化、重启可查、原因可分辨 ----------

def test_decision_journal_persists_across_restart(tmp_path, caplog):
    db = str(tmp_path / "journal.db")
    chain = LocalChain()
    [chain.append_block() for _ in range(6)]

    store1 = IndexStore(db)
    idx1 = Indexer(store1, chain, max_reorg_depth=4, max_catchup_span=10)
    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        p1 = idx1.plan_sync(2)           # 预演接受
        idx1.sync_to(2)                  # 执行接受
        p_bad = idx1.plan_sync(9)        # 预演拒绝
        with pytest.raises(RejectError):
            idx1.sync_to(9)              # 执行拒绝
    store1.close()

    # 重启后流水仍在
    store2 = IndexStore(db)
    idx2 = Indexer(store2, chain, max_reorg_depth=4, max_catchup_span=10)

    by_id = {d.id: d for d in idx2.list_decisions()}
    assert set(by_id) == {p1.id, p1.id + 1, p_bad.id, p_bad.id + 1}

    plan_row = store2.get_decision(p1.id)
    assert plan_row.mode == MODE_PLAN
    assert plan_row.status == STATUS_ACCEPTED
    assert plan_row.applied == (0, 1, 2)
    assert plan_row.cp_before is None
    assert plan_row.cp_after == Checkpoint(2, chain.get_block(2).block_hash)
    assert plan_row.request_json  # 请求内容留存，可复盘

    exec_row = store2.get_decision(p1.id + 1)
    assert exec_row.mode == MODE_EXECUTE
    assert exec_row.applied == (0, 1, 2)

    # 拒绝原因分类清晰可查，不会糊成一句"失败"
    rejects = idx2.list_decisions(status=STATUS_REJECTED)
    assert len(rejects) == 2
    assert {d.reject_reason for d in rejects} == {RejectReason.HEIGHT_GAP}
    assert all(d.reject_detail for d in rejects)
    assert all(d.cp_after == d.cp_before for d in rejects)

    # 位点也从库中恢复，没有重复入库
    assert store2.get_checkpoint().height == 2
    store2.close()

    texts = [r.getMessage() for r in caplog.records]
    assert any("event=decision mode=plan" in t and "status=rejected" in t
               for t in texts)


def test_journal_records_reorg_rollback_and_duplicate_heights(env):
    """流水足以解释'某高度为何是这个哈希''某交易为何查不到'。"""
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(5)]
    idx.sync_to(4)
    old_tx_4 = blocks[4].transactions[0].txid
    chain.fork(from_height=2, length=4)
    idx.sync_to(6)

    last = idx.list_decisions(mode=MODE_EXECUTE, kind="sync")[-1]
    assert last.rolled_back == (4, 3)
    assert last.applied == (3, 4, 5, 6)
    assert last.cp_after == Checkpoint(6, chain.get_block(6).block_hash)
    # 旧交易已随回滚消失
    assert store.get_transaction(old_tx_4) is None

    # 重复投递也进流水
    d = idx.execute_ingest(chain.get_block(0))
    assert d.outcomes[0].result == "duplicate"
    assert d.duplicates == (0,)
    assert store.get_decision(d.id).duplicates == (0,)


# ---------- 单块预演 ----------

def test_plan_ingest_matches_execute_and_rejections(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(3)]

    # 预演高度 0
    p = idx.plan_ingest(blocks[0])
    assert p.status == STATUS_ACCEPTED
    assert p.applied == (0,)
    assert store.get_checkpoint() is None
    # 执行结论一致
    e = idx.execute_ingest(blocks[0])
    assert e.applied == p.applied and e.cp_after == p.cp_after
    # 重复投递预演
    pd = idx.plan_ingest(blocks[0])
    assert pd.status == STATUS_ACCEPTED and pd.duplicates == (0,)
    assert store.get_checkpoint().height == 0
    # 跳高度预演：拒绝计划
    pg = idx.plan_ingest(blocks[2])
    assert pg.status == STATUS_REJECTED
    assert pg.reject_reason == RejectReason.HEIGHT_GAP
    assert store.snapshot()[0].height == 0

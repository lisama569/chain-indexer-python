"""本轮新增行为：只读预演、批量/并发原子性与幂等、持久化决策流水。"""
from __future__ import annotations

import logging
import threading

import pytest

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError
from chain_indexer.indexer import Indexer, IngestResult
from chain_indexer.models import DecisionMode, PlanStatus
from chain_indexer.store import IndexStore


@pytest.fixture()
def env():
    chain = LocalChain()
    store = IndexStore()
    idx = Indexer(store, chain, max_reorg_depth=4, max_catchup_span=10)
    return chain, store, idx


def _texts(caplog):
    return [r.getMessage() for r in caplog.records]


# ---------- 需求一：预演只读且与执行一致 ----------

def test_dry_run_is_readless(env, caplog):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(4)]

    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        plan = idx.plan_sync(3)

    assert plan.status == PlanStatus.ACCEPTED
    assert plan.applied == (0, 1, 2, 3)
    assert plan.after.height == 3 and plan.after.block_hash == blocks[3].block_hash
    # 关键：预演后位点/区块/交易与未跑时完全一致
    assert store.get_checkpoint() is None
    for h in range(4):
        assert store.get_block_by_height(h) is None
    assert store.get_transaction(blocks[0].transactions[0].txid) is None

    # 预演日志体现处理位点与事件
    texts = _texts(caplog)
    assert any("event=dry_run" in t and "status=accepted" in t for t in texts)


def test_dry_run_then_execute_ranges_match(env):
    chain, store, idx = env
    [chain.append_block() for _ in range(5)]

    plan = idx.plan_sync(4)
    assert idx.sync_to(4) == plan.after.height
    assert store.get_checkpoint().height == 4
    # 已落库哈希与预演计划逐一对应
    for h in plan.applied:
        assert store.get_block_by_height(h).block_hash == chain.get_block(h).block_hash

    # 重复预演 + 重复执行：稳定，不重复入库
    again_plan = idx.plan_sync(4)
    assert again_plan.status == PlanStatus.ACCEPTED
    assert again_plan.applied == ()
    assert idx.sync_to(4) == 4
    assert store.get_checkpoint().height == 4


def test_dry_run_rejected_matches_execute_reason(env):
    chain, store, idx = env
    [chain.append_block() for _ in range(6)]
    idx.sync_to(4)

    plan = idx.plan_sync(2)  # 位点回退
    assert plan.status == PlanStatus.REJECTED
    assert plan.reason.value == "checkpoint_regression"
    assert plan.after == plan.before  # 零写入

    with pytest.raises(RejectError) as ei:
        idx.sync_to(2)
    assert ei.value.reason == plan.reason
    assert store.get_checkpoint().height == 4


def test_dry_run_ingest_gap_matches_execution(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(3)]

    plan = idx.plan_ingest(blocks[2])  # 跳高度
    assert plan.status == PlanStatus.REJECTED
    assert plan.reason.value == "height_gap"

    with pytest.raises(RejectError) as ei:
        idx.ingest_block(blocks[2])
    assert ei.value.reason == plan.reason
    assert store.get_checkpoint() is None


# ---------- 需求二：拒绝零部分应用（重组 + 目标不可达）----------

def test_reorg_plus_unreachable_target_rejected_without_partial(env, caplog):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(5)]
    idx.sync_to(4)
    old_hash_3 = store.get_block_by_height(3).block_hash
    old_tx_4 = blocks[4].transactions[0].txid

    # 分叉覆盖高度 3..5，但目标 9 超出新链尖：
    # 既本会触发回滚（高度 3），又因目标不可达而必须拒绝
    chain.fork(from_height=2, length=3)

    with caplog.at_level(logging.INFO, logger="chain_indexer"):
        plan = idx.plan_sync(9)

    assert plan.status == PlanStatus.REJECTED
    assert plan.reason.value == "height_gap"
    assert plan.rolled_back == (4, 3)  # 计划里看得出"本会回滚哪些高度"

    # 预演只读，数据不变
    assert store.get_checkpoint().height == 4
    assert store.get_block_by_height(3).block_hash == old_hash_3

    # 真正执行同样被拒绝，且零部分应用：旧数据没被删、位点没退
    with pytest.raises(RejectError) as ei:
        idx.sync_to(9)
    assert ei.value.reason.value == "height_gap"
    assert store.get_checkpoint().height == 4
    assert store.get_block_by_height(3).block_hash == old_hash_3
    assert store.get_transaction(old_tx_4) is not None

    texts = _texts(caplog)
    # 拒绝事件要带 would_rollback，且不得出现真实 applied/rollback
    assert any("event=rejected" in t and "would_rollback=[4, 3]" in t for t in texts)
    assert not any("event=rollback" in t for t in texts)
    assert not any("event=applied" in t for t in texts)


def test_deep_reorg_batch_is_all_or_nothing(env):
    chain, store, idx = env  # max_reorg_depth=4
    [chain.append_block() for _ in range(8)]
    idx.sync_to(7)
    before = store.get_checkpoint()

    # 批量同步多目标，其中最大目标触发超深重组 -> 整批拒绝
    chain.fork(from_height=1, length=7)  # 深度 6 > 4
    plan = idx.plan_batch_sync([5, 7])
    assert plan.status == PlanStatus.REJECTED
    assert plan.reason.value == "reorg_too_deep"
    assert plan.applied == ()

    with pytest.raises(RejectError) as ei:
        idx.sync_batch([5, 7])
    assert ei.value.reason.value == "reorg_too_deep"
    # 位点/区块/交易自洽，无中间状态
    assert store.get_checkpoint() == before
    for h in range(8):
        assert store.get_block_by_height(h) is not None


def test_batch_ingest_one_bad_block_rejects_all(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(4)]  # 高度 0..3
    idx.ingest_block(blocks[0])

    far = chain.append_block()  # 高度 4（相对当前位点 0 仍是跳高度）
    # 合法的 1、2 与跳高度的 4 同批：整批拒绝，1/2 也不许落库
    plan = idx.plan_batch_ingest([blocks[1], blocks[2], far])
    assert plan.status == PlanStatus.REJECTED
    assert plan.reason.value == "height_gap"
    assert plan.applied == ()

    with pytest.raises(RejectError):
        idx.ingest_blocks([blocks[1], blocks[2], far])
    assert store.get_checkpoint().height == 0
    assert store.get_block_by_height(1) is None
    assert store.get_block_by_height(2) is None


def test_batch_ingest_conflicting_heights_within_batch_rejected(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(2)]
    forged = type(blocks[0])(
        height=0, block_hash="other", parent_hash="genesis", transactions=()
    )
    with pytest.raises(RejectError) as ei:
        idx.ingest_blocks([blocks[0], forged])
    assert ei.value.reason.value == "conflicting_block"
    assert store.get_checkpoint() is None


# ---------- 需求二：失败后重试不重复、结果稳定 ----------

def test_retry_after_rejection_is_stable(env):
    chain, store, idx = env
    [chain.append_block() for _ in range(6)]
    idx.sync_to(3)

    # 不可达目标：连续以同样目标重试，结果稳定且无任何数据变化
    for _ in range(3):
        with pytest.raises(RejectError) as ei:
            idx.sync_to(100)
        assert ei.value.reason.value == "height_gap"
    assert store.get_checkpoint().height == 3

    # 合法重试同样幂等：执行两次结果一致、区块/交易不重复
    assert idx.sync_to(5) == 5
    assert idx.sync_to(5) == 5
    assert store.get_checkpoint().height == 5
    for h in range(6):
        assert store.get_block_by_height(h) is not None


def test_batch_retry_does_not_duplicate(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(4)]
    idx.ingest_blocks(blocks[:3])
    # 整批重放：全部成为 duplicate，位点不变、无重复行
    replay = idx.ingest_blocks(blocks[:3])
    assert replay.status == PlanStatus.ACCEPTED
    assert replay.applied == ()
    assert set(replay.duplicates) == {0, 1, 2}
    assert store.get_checkpoint().height == 2
    # 交易仍只查得到一次
    for b in blocks[:3]:
        assert store.get_transaction(b.transactions[0].txid) is not None


# ---------- 需求二：并发结果确定 ----------

def test_concurrent_requests_are_serialized_and_deterministic():
    chain = LocalChain()
    [chain.append_block() for _ in range(20)]

    final_tips: list[str] = []

    # 多轮并发压跑：每轮一个全新存储，同一组请求无论怎么调度，最终状态必须一致
    for _ in range(10):
        store = IndexStore()
        idx = Indexer(store, chain, max_reorg_depth=10, max_catchup_span=100)
        errors: list[str] = []
        barrier = threading.Barrier(5)

        def run(fn):
            barrier.wait()
            try:
                fn()
            except RejectError as e:
                errors.append(e.reason.value)

        targets = [5, 5, 10, 3, 19]
        threads = [
            threading.Thread(target=run, args=(lambda t=t: idx.sync_to(t),))
            for t in targets
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 回退请求（目标 3）必然被拒绝；其余结果取决于调度，但全部合法
        assert "checkpoint_regression" in errors
        assert all(reason == "checkpoint_regression" for reason in errors)
        # 无论谁先执行，最终位点都确定为最高目标 19，数据连续自洽
        cp = store.get_checkpoint()
        assert cp.height == 19
        assert cp.block_hash == chain.get_block(19).block_hash
        for h in range(20):
            assert store.get_block_by_height(h).block_hash == \
                chain.get_block(h).block_hash
        final_tips.append(cp.block_hash)

    # 结果可复现：10 轮最终位点哈希完全相同
    assert set(final_tips) == {chain.get_block(19).block_hash}


def test_concurrent_batch_and_single_do_not_corrupt():
    chain = LocalChain()
    store = IndexStore()
    idx = Indexer(store, chain, max_reorg_depth=10, max_catchup_span=100)
    blocks = [chain.append_block() for _ in range(12)]
    barrier = threading.Barrier(2)

    def batch():
        barrier.wait()
        idx.ingest_blocks(blocks[:6])

    def single():
        barrier.wait()
        idx.sync_to(11)  # 覆盖同一区间，另一路

    t1 = threading.Thread(target=batch)
    t2 = threading.Thread(target=single)
    t1.start(); t2.start(); t1.join(); t2.join()

    # 无论谁先执行，最终位点都是 11，数据连续自洽，无重复入库
    assert store.get_checkpoint().height == 11
    for h in range(12):
        assert store.get_block_by_height(h) is not None
    txids = {b.transactions[0].txid for b in blocks}
    assert all(store.get_transaction(tx) is not None for tx in txids)


# ---------- 需求三：决策流水持久化、重启可查、原因可分辨 ----------

def test_decision_journal_persists_across_restart(tmp_path):
    db = str(tmp_path / "idx.db")
    chain = LocalChain()
    [chain.append_block() for _ in range(6)]

    store1 = IndexStore(db)
    idx1 = Indexer(store1, chain, max_catchup_span=10)

    idx1.plan_sync(3)                       # 预演 accepted
    idx1.sync_to(3)                         # 执行 accepted
    try:
        idx1.sync_to(1)                     # 执行 rejected
    except RejectError:
        pass
    idx1.plan_sync(50)                      # 预演 rejected（超链尖）
    store1.close()

    store2 = IndexStore(db)  # 重启
    recs = list(reversed(store2.list_decisions(100)))
    assert len(recs) == 4

    modes = [r.mode for r in recs]
    assert modes == [
        DecisionMode.DRY_RUN,
        DecisionMode.EXECUTE,
        DecisionMode.EXECUTE,
        DecisionMode.DRY_RUN,
    ]

    # 第一条：预演追赶，应用 0..3，位点 -1 -> 3
    r0 = recs[0]
    assert r0.status == PlanStatus.ACCEPTED
    assert r0.applied == (0, 1, 2, 3)
    assert r0.before_height is None and r0.after_height == 3
    assert r0.rolled_back == ()

    # 第二条：真实执行，同样范围
    r1 = recs[1]
    assert r1.applied == (0, 1, 2, 3)

    # 第三条：位点回退拒绝，原因可分辨，应用列表为空
    r2 = recs[2]
    assert r2.status == PlanStatus.REJECTED
    assert r2.reason.value == "checkpoint_regression"
    assert r2.applied == () and r2.after_height == 3

    # 第四条：预演超链尖拒绝
    r3 = recs[3]
    assert r3.status == PlanStatus.REJECTED and r3.mode == DecisionMode.DRY_RUN
    assert r3.reason.value == "height_gap"

    # seq 可直接取
    assert store2.get_decision(r0.seq).seq == r0.seq
    store2.close()


def test_decision_journal_distinguishes_reject_reasons(env):
    chain, store, idx = env
    [chain.append_block() for _ in range(30)]  # 链尖 29
    idx.sync_to(9)

    cases = [
        (lambda: idx.plan_sync(5), "checkpoint_regression"),
        (lambda: idx.plan_sync(20), "catchup_too_large"),
        (lambda: idx.plan_sync(100), "height_gap"),
    ]
    seen = set()
    for fn, reason in cases:
        p = fn()
        assert p.status == PlanStatus.REJECTED
        assert p.reason.value == reason
        seen.add(p.reason.value)
    assert seen == {"checkpoint_regression", "catchup_too_large", "height_gap"}

    reasons = {r.reason.value for r in idx.journal.list_all() if r.reason}
    assert {"checkpoint_regression", "catchup_too_large", "height_gap"} <= reasons


def test_journal_records_reorg_and_duplicates(env):
    chain, store, idx = env
    blocks = [chain.append_block() for _ in range(5)]
    idx.sync_to(4)
    chain.fork(from_height=2, length=3)  # 覆盖 3..5

    idx.sync_to(5)
    idx.sync_to(5)  # 重复追赶

    recs = idx.journal.list_all()
    reorg_rec = next(r for r in recs if r.rolled_back)
    assert reorg_rec.rolled_back == (4, 3)
    assert reorg_rec.applied == (3, 4, 5)
    dup_rec = next(r for r in recs if r.duplicates)
    assert dup_rec.duplicates == (5,)
    # 旧交易解释性：被回滚交易已查不到，流水说明了原因（回滚高度 3）
    assert store.get_transaction(blocks[4].transactions[0].txid) is None


# ---------- 属性式：任意场景下预演与真实执行结论一致 ----------

def _plan_matches_execute(make_chain, make_start, request_fn, plan_method, exec_method):
    """在两个相同起始状态上分别"只预演"与"真正执行"，断言结论一致。"""
    import tempfile, os
    # 预演侧
    chain_dry = make_chain()
    store_dry = IndexStore(":memory:")
    idx_dry = Indexer(store_dry, chain_dry, max_reorg_depth=4, max_catchup_span=10)
    make_start(idx_dry, chain_dry)
    before_dry = store_dry.snapshot()

    # 执行侧：从同一数据库文件复制起始状态
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    os.unlink(path)
    chain_ex = make_chain()
    store_ex = IndexStore(path)
    idx_ex = Indexer(store_ex, chain_ex, max_reorg_depth=4, max_catchup_span=10)
    make_start(idx_ex, chain_ex)

    payload = request_fn(chain_dry)
    plan = plan_method(idx_dry, payload)

    if plan.status == PlanStatus.REJECTED:
        with pytest.raises(RejectError) as ei:
            exec_method(idx_ex, payload)
        assert ei.value.reason == plan.reason
        # 执行侧状态必须等于预演侧（都未被该请求改变）
        assert store_ex.get_checkpoint() == before_dry.checkpoint
    else:
        exec_method(idx_ex, payload)
        cp = store_ex.get_checkpoint()
        assert (cp.height, cp.block_hash) == (
            plan.after.height, plan.after.block_hash
        ), (cp, plan.after)
        for h in plan.applied:
            assert store_ex.get_block_by_height(h) is not None
    store_ex.close()
    os.unlink(path)


def test_plan_execute_consistency_across_scenarios():
    from chain_indexer.models import Block as _B

    def chain_with_fork():
        ch = LocalChain()
        [ch.append_block() for _ in range(6)]
        return ch

    scenarios = []

    # 场景 1：正常追赶
    scenarios.append((
        chain_with_fork,
        lambda idx, ch: idx.sync_to(2),
        lambda ch: 5,
        lambda i, p: i.plan_sync(p),
        lambda i, p: i.sync_to(p),
    ))
    # 场景 2：位点回退
    scenarios.append((
        chain_with_fork,
        lambda idx, ch: idx.sync_to(4),
        lambda ch: 1,
        lambda i, p: i.plan_sync(p),
        lambda i, p: i.sync_to(p),
    ))
    # 场景 3：超链尖
    scenarios.append((
        chain_with_fork,
        lambda idx, ch: idx.sync_to(3),
        lambda ch: 99,
        lambda i, p: i.plan_sync(p),
        lambda i, p: i.sync_to(p),
    ))
    # 场景 4：分叉后正常重组追赶
    def forked():
        ch = chain_with_fork()
        return ch
    def start_fork(idx, ch):
        idx.sync_to(5)
        ch.fork(from_height=3, length=3)  # 覆盖 4..6
    scenarios.append((
        forked, start_fork, lambda ch: 6,
        lambda i, p: i.plan_sync(p),
        lambda i, p: i.sync_to(p),
    ))
    # 场景 5：分叉但目标不可达（回滚 + 拒绝）
    scenarios.append((
        forked, start_fork, lambda ch: 50,
        lambda i, p: i.plan_sync(p),
        lambda i, p: i.sync_to(p),
    ))
    # 场景 6：批量投递
    scenarios.append((
        chain_with_fork,
        lambda idx, ch: None,
        lambda ch: [ch.get_block(h) for h in range(3)],
        lambda i, p: i.plan_batch_ingest(p),
        lambda i, p: i.ingest_blocks(p),
    ))
    # 场景 7：批量同步多目标
    scenarios.append((
        chain_with_fork,
        lambda idx, ch: idx.sync_to(2),
        lambda ch: [1, 2, 4],
        lambda i, p: i.plan_batch_sync(p),
        lambda i, p: i.sync_batch(p),
    ))

    for make_chain, make_start, payload_fn, plan_m, exec_m in scenarios:
        _plan_matches_execute(
            make_chain, make_start, payload_fn, plan_m, exec_m
        )


# ---------- 事务中途失败：整体回滚，无中间状态 ----------

def test_transaction_failure_leaves_no_intermediate_state(monkeypatch):
    chain = LocalChain()
    blocks = [chain.append_block() for _ in range(4)]
    store = IndexStore()
    idx = Indexer(store, chain, max_catchup_span=10)
    idx.sync_to(1)
    cp_before = store.get_checkpoint()
    txid_before = blocks[0].transactions[0].txid

    # 让"第二个待应用区块"落库时抛错，模拟事务中途失败
    from chain_indexer import store as store_mod
    orig = store_mod._Transaction.apply_block

    def flaky(self, block):
        if block.height == 3:
            raise RuntimeError("boom during apply")
        return orig(self, block)

    monkeypatch.setattr(store_mod._Transaction, "apply_block", flaky)

    with pytest.raises(RuntimeError):
        idx.sync_to(3)

    # 整体回滚：位点仍在 1，高度 2/3 无区块无交易，旧数据完好
    assert store.get_checkpoint() == cp_before
    assert store.get_block_by_height(2) is None
    assert store.get_block_by_height(3) is None
    assert store.get_transaction(blocks[2].transactions[0].txid) is None
    assert store.get_block_by_height(1).block_hash == blocks[1].block_hash
    assert store.get_transaction(txid_before) is not None

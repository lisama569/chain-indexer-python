"""索引器：按高度推进、重复投递去重、重组回滚、追赶与"先预演再执行"。

所有请求先由只读的 :class:`~chain_indexer.planner.Planner` 推演出
:class:`~chain_indexer.models.Plan`：

- 预演（``plan_*`` / ``dry_run_*``）：只计算计划并记录决策流水，绝不写索引数据，
  跑完位点、区块、交易与未跑时完全一致；
- 执行（``submit_*``，以及原有 ``ingest_block`` / ``sync_to``）：在同一把提交锁内
  重新计算计划，再于单一 SQLite 事务中物化，全成或全不动。

因为执行与预演共用同一个计划引擎，且执行时在锁内重算，预演结论与执行结果
必然一致；并发请求也被提交锁完全串行化，结果确定、可复现。
"""
from __future__ import annotations

import logging
import threading

from .chain import GENESIS_HASH, LocalChain
from .errors import RejectError, RejectReason
from .journal import DecisionJournal
from .models import Block, DecisionMode, Plan
from .planner import Planner, Prepared
from .store import IndexStore

logger = logging.getLogger("chain_indexer")


class IngestResult:
    APPLIED = "applied"            # 正常落库并推进位点
    DUPLICATE = "duplicate"        # 重复投递：内容一致，幂等忽略
    REORG_APPLIED = "reorg_applied"  # 触发回滚后作为新分支落库


class Indexer:
    def __init__(
        self,
        store: IndexStore,
        chain: LocalChain,
        max_reorg_depth: int = 64,
        max_catchup_span: int = 1000,
    ) -> None:
        self._store = store
        self._chain = chain
        self._planner = Planner(chain, max_reorg_depth, max_catchup_span)
        self._journal = DecisionJournal(store)
        # 提交锁：串行化所有"计划 + 物化"，保证并发结果确定、不串数据
        self._commit_lock = threading.RLock()

    @property
    def journal(self) -> DecisionJournal:
        return self._journal

    # ================= 单块投递（兼容原有接口）=================
    def ingest_block(self, block: Block) -> str:
        """投递单个区块，返回 applied / duplicate / reorg_applied。"""
        prep = self._submit(
            self._planner.plan_ingest, block, kind="ingest"
        )
        if prep.plan.rolled_back:
            return IngestResult.REORG_APPLIED
        if prep.plan.applied:
            return IngestResult.APPLIED
        return IngestResult.DUPLICATE

    def plan_ingest(self, block: Block) -> Plan:
        """只读预演一次单块投递。"""
        return self._dry_run(self._planner.plan_ingest, block, kind="ingest")

    # ================= 追赶（兼容原有接口）=================
    def sync_to(self, target_height: int) -> int:
        """追赶到目标高度，返回最终位点高度。"""
        prep = self._submit(
            self._planner.plan_sync, target_height, kind="sync"
        )
        return prep.plan.after.height

    def plan_sync(self, target_height: int) -> Plan:
        """只读预演一次追赶。"""
        return self._dry_run(
            self._planner.plan_sync, target_height, kind="sync"
        )

    # ================= 批量 =================
    def ingest_blocks(self, blocks: list[Block]) -> Plan:
        """一次请求投递多个区块（可覆盖一段高度），要么全成要么全不动。"""
        return self._submit(
            self._planner.plan_batch_ingest, list(blocks), kind="batch_ingest"
        ).plan

    def plan_batch_ingest(self, blocks: list[Block]) -> Plan:
        """只读预演一次批量投递。"""
        return self._dry_run(
            self._planner.plan_batch_ingest, list(blocks), kind="batch_ingest"
        )

    def sync_batch(self, targets: list[int]) -> Plan:
        """一次请求覆盖多个追赶目标（取最大有效高度），原子完成。"""
        return self._submit(
            self._planner.plan_batch_sync, list(targets), kind="batch_sync"
        ).plan

    def plan_batch_sync(self, targets: list[int]) -> Plan:
        """只读预演一次多目标追赶。"""
        return self._dry_run(
            self._planner.plan_batch_sync, list(targets), kind="batch_sync"
        )

    # ================= 预演 / 执行 内部 =================
    def _dry_run(self, plan_fn, payload, *, kind: str) -> Plan:
        with self._commit_lock:
            snap = self._store.snapshot()
            prep = plan_fn(snap, payload)
            plan = prep.plan
        self._journal.record(DecisionMode.DRY_RUN, plan)
        logger.info(
            "event=dry_run kind=%s target=%s status=%s at=%d -> %s"
            " applied=%s duplicates=%s rolled_back=%s reason=%s",
            kind,
            plan.target,
            plan.status.value,
            plan.before.height if plan.before else -1,
            plan.after.height if plan.after else None,
            list(plan.applied),
            list(plan.duplicates),
            list(plan.rolled_back),
            plan.reason.value if plan.reason else "-",
        )
        return plan

    def _submit(self, plan_fn, payload, *, kind: str) -> Prepared:
        with self._commit_lock:
            # 锁内基于最新数据重算，避免"预演后、执行前"的状态漂移
            snap = self._store.snapshot()
            prep = plan_fn(snap, payload)
            plan = prep.plan

            if plan.status.value == "rejected":
                self._journal.record(DecisionMode.EXECUTE, plan)
                logger.warning(
                    "event=rejected kind=%s target=%s reason=%s detail=%s"
                    " would_rollback=%s",
                    kind,
                    plan.target,
                    plan.reason.value,
                    plan.detail,
                    list(plan.rolled_back),
                )
                raise RejectError(plan.reason, plan.detail)

            self._materialize(prep, kind=kind)
            self._journal.record(DecisionMode.EXECUTE, plan)
            return prep

    def _materialize(self, prep: Prepared, *, kind: str) -> None:
        """单一事务内完成回滚、落库、位点推进；任一步失败整体回滚。"""
        with self._store.transaction() as tx:
            removed: list[Block] = []
            if prep.rollback_from is not None:
                removed = tx.rollback_from(prep.rollback_from)
            for block in prep.apply_blocks:
                tx.apply_block(block)
            if prep.plan.after is not None:
                tx.save_checkpoint(prep.plan.after)

        # 事务已成功提交，再打印事件流水（保持与历史日志格式一致）
        if removed:
            if kind == "sync" or kind == "batch_sync":
                # sync 的回滚由分叉触发：fork_point = 最低回滚高度 - 1
                fork_point = min(b.height for b in removed) - 1
                depth = len(removed)
                logger.info(
                    "event=rollback fork_point=%d depth=%d removed=%s",
                    fork_point,
                    depth,
                    [b.block_hash for b in removed],
                )
            else:
                logger.info(
                    "event=rollback from_height=%d removed=%s",
                    min(b.height for b in removed),
                    [b.block_hash for b in removed],
                )
        for block in prep.apply_blocks:
            logger.info(
                "event=applied height=%d hash=%s txs=%d",
                block.height,
                block.block_hash,
                len(block.transactions),
            )
        for height in prep.plan.duplicates:
            stored = self._store.get_block_by_height(height)
            logger.info(
                "event=duplicate height=%d hash=%s",
                height,
                stored.block_hash if stored else "-",
            )
        logger.info(
            "event=committed kind=%s target=%s at_height=%s",
            kind,
            prep.plan.target,
            prep.plan.after.height if prep.plan.after else None,
        )

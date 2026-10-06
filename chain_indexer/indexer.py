"""索引器：按高度推进、重复投递去重、重组回滚、追赶，以及预演与批量原子执行。

所有判定走同一个事务化评估引擎：

- 预演（plan）：在事务内完整模拟后**回滚事务**，索引数据（位点/区块/交易）
  与没跑过完全一致；仅把决策写入流水。
- 执行（execute）：评估、写入、流水在同一个事务里提交；任一操作被拒绝，
  整个事务回滚，绝不留下半成品。

store 的全局 RLock 跨越整个事务，因此并发请求严格串行，结论确定可复现。
"""
from __future__ import annotations

import dataclasses
import json
import logging
from dataclasses import dataclass

from .chain import GENESIS_HASH, LocalChain
from .errors import RejectError, RejectReason
from .models import Block, Checkpoint
from .plan import (
    Decision,
    IngestOp,
    MODE_EXECUTE,
    MODE_PLAN,
    OpOutcome,
    STATUS_ACCEPTED,
    STATUS_REJECTED,
    KIND_BATCH,
    KIND_INGEST,
    KIND_SYNC,
    SyncOp,
)
from .store import IndexStore

logger = logging.getLogger("chain_indexer")


class IngestResult:
    APPLIED = "applied"            # 正常落库并推进位点
    DUPLICATE = "duplicate"        # 重复投递：内容一致，幂等忽略
    REORG_APPLIED = "reorg_applied"  # 触发回滚后作为新分支落库
    NOOP = "noop"                  # 追赶目标已就位，无需动作


@dataclass
class _Counters:
    applied: list[int] = dataclasses.field(default_factory=list)
    duplicates: list[int] = dataclasses.field(default_factory=list)
    rolled_back: list[int] = dataclasses.field(default_factory=list)


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
        self._max_reorg_depth = max_reorg_depth
        self._max_catchup_span = max_catchup_span

    # ---- 对外：单块投递（保持原有签名：成功返回结果串，拒绝抛异常）----
    def ingest_block(self, block: Block) -> str:
        decision = self._run(
            MODE_EXECUTE, KIND_INGEST, self._ingest_request_json(block),
            [(KIND_INGEST, block)],
        )
        return decision.outcomes[0].result

    # ---- 对外：追赶（保持原有签名：成功返回最终高度，拒绝抛异常）----
    def sync_to(self, target_height: int) -> int:
        decision = self._run(
            MODE_EXECUTE, KIND_SYNC, json.dumps({"target_height": target_height}),
            [(KIND_SYNC, target_height)],
        )
        return decision.cp_after.height

    # ---- 预演（只读）----
    def plan_ingest(self, block: Block) -> Decision:
        return self._run(
            MODE_PLAN, KIND_INGEST, self._ingest_request_json(block),
            [(KIND_INGEST, block)],
        )

    def plan_sync(self, target_height: int) -> Decision:
        return self._run(
            MODE_PLAN, KIND_SYNC, json.dumps({"target_height": target_height}),
            [(KIND_SYNC, target_height)],
        )

    # ---- 执行并返回完整决策 ----
    def execute_ingest(self, block: Block) -> Decision:
        return self._run(
            MODE_EXECUTE, KIND_INGEST, self._ingest_request_json(block),
            [(KIND_INGEST, block)],
        )

    def execute_sync(self, target_height: int) -> Decision:
        return self._run(
            MODE_EXECUTE, KIND_SYNC, json.dumps({"target_height": target_height}),
            [(KIND_SYNC, target_height)],
        )

    # ---- 批量：要么全成，要么全不动 ----
    def plan_batch(self, ops: list[IngestOp | SyncOp]) -> Decision:
        return self._run(
            MODE_PLAN, KIND_BATCH, self._batch_request_json(ops),
            [self._op_spec(o) for o in ops],
        )

    def execute_batch(self, ops: list[IngestOp | SyncOp]) -> Decision:
        return self._run(
            MODE_EXECUTE, KIND_BATCH, self._batch_request_json(ops),
            [self._op_spec(o) for o in ops],
        )

    # ---- 流水查询 ----
    def list_decisions(self, **kwargs) -> list[Decision]:
        return self._store.list_decisions(**kwargs)

    def get_decision(self, decision_id: int) -> Decision | None:
        return self._store.get_decision(decision_id)

    # ==================================================================
    # 核心引擎：预演与执行共用同一份评估代码，结论必然一致
    # ==================================================================
    def _run(
        self,
        mode: str,
        kind: str,
        request_json: str,
        specs: list[tuple[str, object]],
    ) -> Decision:
        cp_before = self._store.get_checkpoint()
        counters = _Counters()
        outcomes: list[OpOutcome] = []
        rejected: RejectError | None = None
        cp_after: Checkpoint | None = None
        rb_mark = 0  # 正在评估的操作在 counters.rolled_back 中的起始位置

        try:
            # plan 模式事务在退出时回滚 -> 对索引数据严格只读
            with self._store.transaction(commit=(mode == MODE_EXECUTE)) as s:
                cp_state: Checkpoint | None = s._get_checkpoint_unsafe()
                for i, (op_kind, payload) in enumerate(specs):
                    # 记录本操作评估前的回滚计数，用于切出逐项回滚范围
                    rb_mark = len(counters.rolled_back)
                    if op_kind == KIND_INGEST:
                        result = self._eval_ingest(s, payload, cp_state, counters)
                        cp_state = s._get_checkpoint_unsafe()
                    else:
                        final_cp = self._eval_sync(s, payload, cp_state, counters)
                        cp_state = final_cp
                        result = self._sync_result_label(payload, counters)
                    outcomes.append(
                        OpOutcome(
                            i, op_kind, STATUS_ACCEPTED, result=result,
                            rolled_back=tuple(counters.rolled_back[rb_mark:]),
                        )
                    )
                cp_after = s._get_checkpoint_unsafe()
                decision = Decision(
                    mode=mode,
                    kind=kind,
                    status=STATUS_ACCEPTED,
                    request_json=request_json,
                    cp_before=cp_before,
                    applied=tuple(counters.applied),
                    duplicates=tuple(counters.duplicates),
                    rolled_back=tuple(counters.rolled_back),
                    cp_after=cp_after,
                    outcomes=tuple(outcomes),
                )
                if mode == MODE_EXECUTE:
                    # 流水与索引数据同事务提交
                    did, created_at = s._append_decision_unsafe(decision)
                    decision = dataclasses.replace(
                        decision, id=did, created_at=created_at
                    )
        except RejectError as exc:
            rejected = exc
            # 事务已随异常整体回滚：被拒绝的请求没有任何最终生效动作。
            # 但顶层 applied/duplicates/rolled_back 与逐项 outcomes 仍保留
            # 评估期算出的候选结果（"若真执行会发生"的动作，含拒绝点操作
            # 在被拒前已评估出的候选回滚范围），让预演能给出完整执行计划；
            # cp_after 仍等于 cp_before，因为实际位点没有变化。
            decision = Decision(
                mode=mode,
                kind=kind,
                status=STATUS_REJECTED,
                request_json=request_json,
                cp_before=cp_before,
                applied=tuple(counters.applied),
                duplicates=tuple(counters.duplicates),
                rolled_back=tuple(counters.rolled_back),
                cp_after=cp_before,  # 实际位点未变：与请求前一致
                outcomes=tuple(
                    self._reject_outcomes(
                        specs, outcomes, exc,
                        rejected_rolled_back=tuple(counters.rolled_back[rb_mark:]),
                    )
                ),
                reject_reason=exc.reason,
                reject_detail=exc.detail,
            )

        # plan（接受或拒绝）与 execute 被拒：数据事务回滚后补记一条流水。
        # execute 接受时流水已与数据同事务写入（上方分支）。
        if mode == MODE_PLAN or rejected is not None:
            did, created_at = self._store.append_decision(decision)
            decision = dataclasses.replace(decision, id=did, created_at=created_at)

        logger.info(
            "event=decision mode=%s kind=%s status=%s id=%s cp_before=%s cp_after=%s"
            " applied=%s duplicates=%s rolled_back=%s reason=%s",
            mode, kind, decision.status, decision.id,
            cp_before.height if cp_before else None,
            decision.cp_after.height if decision.cp_after else None,
            list(decision.applied), list(decision.duplicates),
            list(decision.rolled_back),
            decision.reject_reason.value if decision.reject_reason else None,
        )
        # 预演只产出计划，不抛拒绝异常（拒绝本身就是计划的一部分）；
        # 执行保持既有语义：被拒绝时抛出带分类原因的 RejectError。
        if rejected is not None and mode == MODE_EXECUTE:
            raise rejected
        return decision

    # ---- 单块投递评估（在事务内，可能抛 RejectError）----
    def _eval_ingest(
        self, s: IndexStore, block: Block, cp: Checkpoint | None,
        counters: _Counters,
    ) -> str:
        next_height = cp.height + 1 if cp else 0
        tip_hash = cp.block_hash if cp else GENESIS_HASH

        if block.height > next_height:
            self._reject(
                RejectReason.HEIGHT_GAP,
                f"期望高度 {next_height}，收到 {block.height}，请使用追赶接口",
            )

        if block.height == next_height:
            if block.parent_hash != tip_hash:
                self._reject(
                    RejectReason.ORPHAN_BLOCK,
                    f"高度 {block.height} 的父哈希 {block.parent_hash} "
                    f"与当前链尖 {tip_hash} 不符",
                )
            self._apply(s, block, counters)
            return IngestResult.APPLIED

        stored = s.get_block_by_height(block.height)
        if stored is not None and stored.block_hash == block.block_hash:
            counters.duplicates.append(block.height)
            logger.info(
                "event=duplicate height=%d hash=%s", block.height, block.block_hash
            )
            return IngestResult.DUPLICATE

        depth = (cp.height if cp else -1) - block.height + 1
        if depth > self._max_reorg_depth:
            self._reject(
                RejectReason.REORG_TOO_DEEP,
                f"重组深度 {depth} 超过上限 {self._max_reorg_depth}",
            )
        parent = s.get_block_by_height(block.height - 1)
        parent_hash = parent.block_hash if parent else GENESIS_HASH
        if block.height > 0 and block.parent_hash != parent_hash:
            self._reject(
                RejectReason.CONFLICTING_BLOCK,
                f"高度 {block.height} 哈希冲突且父哈希无法衔接，拒绝部分应用",
            )
        self._rollback(s, block.height, counters, trigger="ingest")
        self._apply(s, block, counters)
        return IngestResult.REORG_APPLIED

    # ---- 追赶评估（在事务内，可能抛 RejectError）----
    def _eval_sync(
        self, s: IndexStore, target_height: int, cp: Checkpoint | None,
        counters: _Counters,
    ) -> Checkpoint | None:
        current = cp.height if cp else -1

        if target_height < current:
            self._reject(
                RejectReason.CHECKPOINT_REGRESSION,
                f"目标高度 {target_height} 低于已确认位点 {current}",
            )

        if cp is not None:
            fork_height = self._find_fork_point(s, cp)
            if fork_height is not None:
                depth = cp.height - fork_height
                if depth > self._max_reorg_depth:
                    self._reject(
                        RejectReason.REORG_TOO_DEEP,
                        f"重组深度 {depth} 超过上限 {self._max_reorg_depth}",
                    )
                self._rollback(
                    s, fork_height + 1, counters, trigger="sync",
                    fork_point=fork_height, depth=depth,
                )
                current = fork_height
                cp = (
                    s._get_checkpoint_unsafe()
                )

        if target_height == current:
            return cp
        if target_height > self._chain.tip_height:
            self._reject(
                RejectReason.HEIGHT_GAP,
                f"目标高度 {target_height} 超出本地链尖 {self._chain.tip_height}",
            )
        span = target_height - current
        if span > self._max_catchup_span:
            self._reject(
                RejectReason.CATCHUP_TOO_LARGE,
                f"追赶跨度 {span} 超过上限 {self._max_catchup_span}",
            )

        for height in range(current + 1, target_height + 1):
            block = self._chain.get_block(height)
            if block is None:
                self._reject(RejectReason.HEIGHT_GAP, f"本地链缺少高度 {height}")
            self._apply(s, block, counters)
        return s._get_checkpoint_unsafe()

    def _sync_result_label(self, target: int, counters: _Counters) -> str:
        if target in counters.applied:
            if counters.rolled_back:
                return IngestResult.REORG_APPLIED
            return IngestResult.APPLIED
        if counters.rolled_back:
            return IngestResult.REORG_APPLIED
        return IngestResult.NOOP

    def _find_fork_point(self, s: IndexStore, cp: Checkpoint) -> int | None:
        """返回分叉点高度（双方一致的最后一个高度）；无分叉返回 None。"""
        height = cp.height
        while height >= 0:
            chain_block = self._chain.get_block(height)
            stored = s.get_block_by_height(height)
            if chain_block is not None and stored is not None \
                    and chain_block.block_hash == stored.block_hash:
                return None if height == cp.height else height
            height -= 1
        return -1  # 连 0 高度都不一致：分叉点为创世之前

    # ---- 事务内原语 ----
    def _apply(self, s: IndexStore, block: Block, counters: _Counters) -> None:
        s._apply_block_unsafe(block)
        s._save_checkpoint_unsafe(Checkpoint(block.height, block.block_hash))
        counters.applied.append(block.height)
        logger.info(
            "event=applied height=%d hash=%s txs=%d",
            block.height, block.block_hash, len(block.transactions),
        )

    def _rollback(
        self, s: IndexStore, height: int, counters: _Counters, trigger: str,
        fork_point: int | None = None, depth: int | None = None,
    ) -> None:
        removed = s._rollback_from_unsafe(height)
        for b in removed:
            s._save_checkpoint_unsafe(
                Checkpoint(
                    b.height - 1,
                    b.parent_hash,
                )
            )
        counters.rolled_back.extend(b.height for b in removed)
        if fork_point is not None:
            # 保持既有日志格式不变
            logger.info(
                "event=rollback fork_point=%d depth=%d removed=%s",
                fork_point, depth, [b.block_hash for b in removed],
            )
        else:
            logger.info(
                "event=rollback trigger=%s from_height=%d removed=%s",
                trigger, height, [b.block_hash for b in removed],
            )

    def _reject(self, reason: RejectReason, detail: str) -> None:
        logger.info("event=reject reason=%s detail=%s", reason.value, detail)
        raise RejectError(reason, detail)

    # ---- 请求序列化 ----
    def _ingest_request_json(self, block: Block) -> str:
        return json.dumps(
            {
                "block": {
                    "height": block.height,
                    "block_hash": block.block_hash,
                    "parent_hash": block.parent_hash,
                }
            },
            ensure_ascii=False,
        )

    def _op_spec(self, op: IngestOp | SyncOp) -> tuple[str, object]:
        if isinstance(op, IngestOp):
            return KIND_INGEST, op.block
        return KIND_SYNC, op.target_height

    def _batch_request_json(self, ops: list[IngestOp | SyncOp]) -> str:
        payload = []
        for op in ops:
            if isinstance(op, IngestOp):
                b = op.block
                payload.append(
                    {
                        "op": KIND_INGEST,
                        "height": b.height,
                        "block_hash": b.block_hash,
                        "parent_hash": b.parent_hash,
                    }
                )
            else:
                payload.append({"op": KIND_SYNC, "target_height": op.target_height})
        return json.dumps(payload, ensure_ascii=False)

    def _reject_outcomes(
        self,
        specs: list[tuple[str, object]],
        evaluated: list[OpOutcome],
        exc: RejectError,
        rejected_rolled_back: tuple[int, ...] = (),
    ) -> list[OpOutcome]:
        """规范化拒绝时的逐操作结论：评估过的保留（含各自候选回滚范围），
        拒绝点单列并带上它在被拒前评估出的候选回滚范围，其后全部 aborted。"""
        outcomes: list[OpOutcome] = []
        rejected_index = len(evaluated)
        for i in range(len(specs)):
            if i < rejected_index:
                outcomes.append(evaluated[i])
            elif i == rejected_index:
                outcomes.append(
                    OpOutcome(
                        i, specs[i][0], STATUS_REJECTED,
                        reject_reason=exc.reason.value, reject_detail=exc.detail,
                        rolled_back=rejected_rolled_back,
                    )
                )
            else:
                outcomes.append(OpOutcome(i, specs[i][0], "aborted"))
        return outcomes

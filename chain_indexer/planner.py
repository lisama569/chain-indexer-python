"""纯只读的执行计划引擎：在不触碰索引数据的前提下推导一次请求的结论。

计划引擎只读取 :class:`~chain_indexer.store.StoreSnapshot` 与本地链，在内存
覆盖层（:class:`_View`）上模拟回滚/应用/跳过，绝不写库。执行端拿到计划后
在单一事务里按计划物化，因此"预演的结论"与"真正执行的结果"天然同源：
预演说能做，执行就一定成功且范围一致；预演说不行，执行必然以同种原因拒绝。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field

from .chain import GENESIS_HASH
from .errors import RejectError, RejectReason
from .models import Block, Checkpoint, Plan, PlanStatus, RequestKind
from .store import StoreSnapshot


@dataclass
class _View:
    """索引主链视图在内存中的可变异构（快照 + 模拟操作）。"""

    height: int
    tip_hash: str
    blocks: dict[int, Block]
    applied: list[int] = field(default_factory=list)
    applied_blocks: list[Block] = field(default_factory=list)
    duplicates: list[int] = field(default_factory=list)
    rolled_back: list[int] = field(default_factory=list)

    @classmethod
    def from_snapshot(cls, snap: StoreSnapshot) -> "_View":
        cp = snap.checkpoint
        return cls(
            height=cp.height if cp else -1,
            tip_hash=cp.block_hash if cp else GENESIS_HASH,
            blocks=dict(snap.blocks),
        )

    def checkpoint(self) -> Checkpoint | None:
        return Checkpoint(self.height, self.tip_hash) if self.height >= 0 else None

    def parent_hash_at(self, height: int) -> str:
        if height <= 0:
            return GENESIS_HASH
        parent = self.blocks.get(height - 1)
        return parent.block_hash if parent is not None else GENESIS_HASH

    def apply(self, block: Block) -> None:
        self.blocks[block.height] = block
        self.height = block.height
        self.tip_hash = block.block_hash
        self.applied.append(block.height)
        self.applied_blocks.append(block)

    def rollback_to(self, fork_point: int) -> None:
        """模拟回滚 fork_point 之后（不含）的全部高度，按高度降序记录。"""
        removed = sorted(
            (h for h in self.blocks if h > fork_point), reverse=True
        )
        for h in removed:
            del self.blocks[h]
        self.rolled_back.extend(removed)
        if fork_point < 0:
            self.height = -1
            self.tip_hash = GENESIS_HASH
        else:
            self.height = fork_point
            self.tip_hash = self.blocks[fork_point].block_hash


@dataclass(frozen=True)
class Prepared:
    """计划 + 物化清单。执行端只需：先回滚（若有），再顺序应用区块，最后定位点。"""

    plan: Plan
    apply_blocks: tuple[Block, ...]
    rollback_from: int | None  # 最低回滚高度；store 将删除其及以上的行


class Planner:
    def __init__(self, chain, max_reorg_depth: int, max_catchup_span: int) -> None:
        self._chain = chain
        self._max_reorg_depth = max_reorg_depth
        self._max_catchup_span = max_catchup_span

    # ================= 单块投递 =================
    def plan_ingest(self, snap: StoreSnapshot, block: Block) -> Prepared:
        view = _View.from_snapshot(snap)
        reject = self._ingest_into(view, block)
        before = snap.checkpoint
        if reject is not None:
            return self._rejected(
                RequestKind.INGEST, str(block.height), before, view, *reject
            )
        return self._accepted(RequestKind.INGEST, str(block.height), before, view)

    def _ingest_into(self, view: _View, block: Block) -> tuple[RejectReason, str] | None:
        next_height = view.height + 1

        if block.height > next_height:
            return (
                RejectReason.HEIGHT_GAP,
                f"期望高度 {next_height}，收到 {block.height}，请使用追赶接口",
            )

        if block.height == next_height:
            if block.parent_hash != view.tip_hash:
                return (
                    RejectReason.ORPHAN_BLOCK,
                    f"高度 {block.height} 的父哈希 {block.parent_hash} "
                    f"与当前链尖 {view.tip_hash} 不符",
                )
            view.apply(block)
            return None

        stored = view.blocks.get(block.height)
        if stored is not None and stored.block_hash == block.block_hash:
            view.duplicates.append(block.height)
            return None

        # 同高度不同哈希：尝试作为分叉处理
        depth = view.height - block.height + 1
        would_rollback = list(range(view.height, block.height - 1, -1))
        if depth > self._max_reorg_depth:
            view.rolled_back.extend(would_rollback)
            return (
                RejectReason.REORG_TOO_DEEP,
                f"重组深度 {depth} 超过上限 {self._max_reorg_depth}",
            )
        parent_hash = view.parent_hash_at(block.height)
        if block.height > 0 and block.parent_hash != parent_hash:
            view.rolled_back.extend(would_rollback)
            return (
                RejectReason.CONFLICTING_BLOCK,
                f"高度 {block.height} 哈希冲突且父哈希无法衔接，拒绝部分应用",
            )
        view.rollback_to(block.height - 1)
        view.apply(block)
        return None

    # ================= 追赶 =================
    def plan_sync(self, snap: StoreSnapshot, target_height: int) -> Prepared:
        view = _View.from_snapshot(snap)
        reject = self._sync_into(view, target_height)
        before = snap.checkpoint
        if reject is not None:
            return self._rejected(
                RequestKind.SYNC, str(target_height), before, view, *reject
            )
        return self._accepted(RequestKind.SYNC, str(target_height), before, view)

    def _sync_into(self, view: _View, target_height: int) -> tuple[RejectReason, str] | None:
        current = view.height
        reorged = False

        if target_height < current:
            return (
                RejectReason.CHECKPOINT_REGRESSION,
                f"目标高度 {target_height} 低于已确认位点 {current}",
            )

        # 重组检测：即使目标高度不低于位点，链上内容也可能已分叉
        if view.height >= 0:
            fork_point = self._find_fork_point(view)
            if fork_point is not None:
                reorged = True
                depth = view.height - fork_point
                would_rollback = list(range(view.height, fork_point, -1))
                if depth > self._max_reorg_depth:
                    view.rolled_back.extend(would_rollback)
                    return (
                        RejectReason.REORG_TOO_DEEP,
                        f"重组深度 {depth} 超过上限 {self._max_reorg_depth}",
                    )
                view.rollback_to(fork_point)
                current = fork_point

        if target_height == current:
            # 无重组且目标就在当前位点：重复跳过；若刚经历重组，则仅回滚不重应用
            if not reorged:
                view.duplicates.append(target_height)
            return None
        if target_height > self._chain.tip_height:
            return (
                RejectReason.HEIGHT_GAP,
                f"目标高度 {target_height} 超出本地链尖 {self._chain.tip_height}",
            )
        span = target_height - current
        if span > self._max_catchup_span:
            return (
                RejectReason.CATCHUP_TOO_LARGE,
                f"追赶跨度 {span} 超过上限 {self._max_catchup_span}",
            )

        for height in range(current + 1, target_height + 1):
            block = self._chain.get_block(height)
            if block is None:
                return (
                    RejectReason.HEIGHT_GAP,
                    f"本地链缺少高度 {height}",
                )
            if block.parent_hash != view.tip_hash:
                return (
                    RejectReason.ORPHAN_BLOCK,
                    f"高度 {height} 的父哈希 {block.parent_hash} "
                    f"与当前链尖 {view.tip_hash} 不符",
                )
            view.apply(block)
        return None

    def _find_fork_point(self, view: _View) -> int | None:
        """返回分叉点高度（双方一致的最后一个高度）；无分叉返回 None。"""
        height = view.height
        while height >= 0:
            chain_block = self._chain.get_block(height)
            stored = view.blocks.get(height)
            if (
                chain_block is not None
                and stored is not None
                and chain_block.block_hash == stored.block_hash
            ):
                return None if height == view.height else height
            height -= 1
        return -1

    # ================= 批量 =================
    def plan_batch_ingest(self, snap: StoreSnapshot, blocks: list[Block]) -> Prepared:
        target = json.dumps(sorted({b.height for b in blocks}))
        view = _View.from_snapshot(snap)

        # 同批内同高度：完全一致去重；不同哈希则整批拒绝
        seen: dict[int, Block] = {}
        ordered: list[Block] = []
        for block in blocks:
            if block.height in seen:
                if seen[block.height].block_hash != block.block_hash:
                    return self._rejected(
                        RequestKind.BATCH_INGEST,
                        target,
                        snap.checkpoint,
                        view,
                        RejectReason.CONFLICTING_BLOCK,
                        f"同一批请求中高度 {block.height} 出现不同哈希",
                    )
                continue
            seen[block.height] = block
            ordered.append(block)

        for block in sorted(ordered, key=lambda b: b.height):
            reject = self._ingest_into(view, block)
            if reject is not None:
                return self._rejected(
                    RequestKind.BATCH_INGEST, target, snap.checkpoint, view, *reject
                )
        return self._accepted(
            RequestKind.BATCH_INGEST, target, snap.checkpoint, view
        )

    def plan_batch_sync(self, snap: StoreSnapshot, targets: list[int]) -> Prepared:
        uniq = sorted(set(targets))
        target = json.dumps(uniq)
        view = _View.from_snapshot(snap)

        effective = max(uniq)
        reject = self._sync_into(view, effective)
        if reject is not None:
            return self._rejected(
                RequestKind.BATCH_SYNC, target, snap.checkpoint, view, *reject
            )

        # 已被最终位点覆盖、且本次没有重新应用的目标视为重复跳过；去重统计
        applied_set = set(view.applied)
        for t in uniq:
            if t <= view.height and t not in applied_set:
                view.duplicates.append(t)
        view.duplicates = sorted(set(view.duplicates))
        return self._accepted(
            RequestKind.BATCH_SYNC, target, snap.checkpoint, view
        )

    # ================= 组装 =================
    @staticmethod
    def _accepted(
        kind: RequestKind, target: str, before: Checkpoint | None, view: _View
    ) -> Prepared:
        plan = Plan(
            kind=kind,
            target=target,
            status=PlanStatus.ACCEPTED,
            before=before,
            after=view.checkpoint(),
            applied=tuple(view.applied),
            duplicates=tuple(view.duplicates),
            rolled_back=tuple(view.rolled_back),
        )
        rollback_from = min(view.rolled_back) if view.rolled_back else None
        return Prepared(
            plan=plan,
            apply_blocks=tuple(view.applied_blocks),
            rollback_from=rollback_from,
        )

    @staticmethod
    def _rejected(
        kind: RequestKind,
        target: str,
        before: Checkpoint | None,
        view: _View,
        reason: RejectReason,
        detail: str,
    ) -> Prepared:
        # 拒绝时不物化任何东西：applied/duplicates 置空；
        # rolled_back 保留"若放行本会回滚的高度"，用于解释重组+不可达类请求。
        # 零写入，因此 after == before。
        plan = Plan(
            kind=kind,
            target=target,
            status=PlanStatus.REJECTED,
            before=before,
            after=before,
            applied=(),
            duplicates=(),
            rolled_back=tuple(view.rolled_back),
            reason=reason,
            detail=detail,
        )
        return Prepared(plan=plan, apply_blocks=(), rollback_from=None)

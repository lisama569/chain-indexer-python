"""索引器：按高度推进、重复投递去重、重组回滚与追赶。"""
from __future__ import annotations

import logging

from .chain import GENESIS_HASH, LocalChain
from .errors import RejectError, RejectReason
from .models import Block, Checkpoint
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
        self._max_reorg_depth = max_reorg_depth
        self._max_catchup_span = max_catchup_span

    # ---- 单块投递 ----
    def ingest_block(self, block: Block) -> str:
        cp = self._store.get_checkpoint()
        next_height = cp.height + 1 if cp else 0
        tip_hash = cp.block_hash if cp else GENESIS_HASH

        if block.height > next_height:
            raise RejectError(
                RejectReason.HEIGHT_GAP,
                f"期望高度 {next_height}，收到 {block.height}，请使用追赶接口",
            )

        if block.height == next_height:
            if block.parent_hash != tip_hash:
                raise RejectError(
                    RejectReason.ORPHAN_BLOCK,
                    f"高度 {block.height} 的父哈希 {block.parent_hash} "
                    f"与当前链尖 {tip_hash} 不符",
                )
            self._apply(block)
            return IngestResult.APPLIED

        # block.height < next_height：已确认区间
        stored = self._store.get_block_by_height(block.height)
        if stored is not None and stored.block_hash == block.block_hash:
            logger.info("event=duplicate height=%d hash=%s", block.height, block.block_hash)
            return IngestResult.DUPLICATE

        # 同高度不同哈希：尝试作为分叉处理
        depth = (cp.height if cp else -1) - block.height + 1
        if depth > self._max_reorg_depth:
            raise RejectError(
                RejectReason.REORG_TOO_DEEP,
                f"重组深度 {depth} 超过上限 {self._max_reorg_depth}",
            )
        parent = self._store.get_block_by_height(block.height - 1)
        parent_hash = parent.block_hash if parent else GENESIS_HASH
        if block.height > 0 and block.parent_hash != parent_hash:
            raise RejectError(
                RejectReason.CONFLICTING_BLOCK,
                f"高度 {block.height} 哈希冲突且父哈希无法衔接，拒绝部分应用",
            )
        removed = self._store.rollback_from(block.height)
        logger.info(
            "event=rollback from_height=%d removed=%s",
            block.height,
            [b.block_hash for b in removed],
        )
        self._apply(block)
        return IngestResult.REORG_APPLIED

    # ---- 追赶 ----
    def sync_to(self, target_height: int) -> int:
        cp = self._store.get_checkpoint()
        current = cp.height if cp else -1

        if target_height < current:
            raise RejectError(
                RejectReason.CHECKPOINT_REGRESSION,
                f"目标高度 {target_height} 低于已确认位点 {current}",
            )
        # 重组检测：即使目标高度等于位点，链上内容也可能已分叉
        if cp is not None:
            fork_height = self._find_fork_point(cp)
            if fork_height is not None:
                depth = cp.height - fork_height
                if depth > self._max_reorg_depth:
                    raise RejectError(
                        RejectReason.REORG_TOO_DEEP,
                        f"重组深度 {depth} 超过上限 {self._max_reorg_depth}",
                    )
                removed = self._store.rollback_from(fork_height + 1)
                logger.info(
                    "event=rollback fork_point=%d depth=%d removed=%s",
                    fork_height,
                    depth,
                    [b.block_hash for b in removed],
                )
                current = fork_height

        if target_height == current:
            return current
        if target_height > self._chain.tip_height:
            raise RejectError(
                RejectReason.HEIGHT_GAP,
                f"目标高度 {target_height} 超出本地链尖 {self._chain.tip_height}",
            )
        span = target_height - current
        if span > self._max_catchup_span:
            raise RejectError(
                RejectReason.CATCHUP_TOO_LARGE,
                f"追赶跨度 {span} 超过上限 {self._max_catchup_span}",
            )

        for height in range(current + 1, target_height + 1):
            block = self._chain.get_block(height)
            if block is None:
                raise RejectError(RejectReason.HEIGHT_GAP, f"本地链缺少高度 {height}")
            self._apply(block)
        return target_height

    def _find_fork_point(self, cp: Checkpoint) -> int | None:
        """返回分叉点高度（双方一致的最后一个高度）；无分叉返回 None。"""
        height = cp.height
        while height >= 0:
            chain_block = self._chain.get_block(height)
            stored = self._store.get_block_by_height(height)
            if chain_block is not None and stored is not None \
                    and chain_block.block_hash == stored.block_hash:
                return None if height == cp.height else height
            height -= 1
        return -1  # 连 0 高度都不一致：分叉点为创世之前

    # ---- 内部 ----
    def _apply(self, block: Block) -> None:
        self._store.apply_block(block)
        self._store.save_checkpoint(Checkpoint(block.height, block.block_hash))
        logger.info(
            "event=applied height=%d hash=%s txs=%d",
            block.height,
            block.block_hash,
            len(block.transactions),
        )

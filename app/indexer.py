"""链索引器：区块投递、去重、重组回滚、位点推进与恢复。"""

from __future__ import annotations

import threading

from app.errors import (
    CatchupTooFarError,
    ConfirmedDataOverwriteError,
    DuplicateDeliveryError,
    GapError,
    ParentMismatchError,
    ProgressRollbackError,
    ReorgTooDeepError,
    StaleHeightError,
    StoreCorruptedError,
)
from app.events import get_logger
from app.models import (
    ACTION_ACCEPTED,
    ACTION_DUPLICATE,
    ACTION_REORG,
    GENESIS_PARENT,
    Block,
    IngestResult,
    Transaction,
)
from app.storage import BlockStore


class ChainIndexer:
    """按高度持续推进的确定性索引器。

    规则摘要（详见 README）：
    - 区块必须按 height 顺序连续追加；同高度同哈希重复投递幂等拒绝；
    - 高度超前 -> GapError；
    - 同高度不同哈希 = 分叉：单块能直接接在库中 h-1 之后时回滚 >= h 的
      高度并重建；否则 -> StaleHeightError，需用 reindex_path 提交完整分叉路径；
    - 已确认高度（cursor - height >= confirmation_depth）不可被覆盖
      -> ConfirmedDataOverwriteError；完整分叉路径的基点已确认
      -> ReorgTooDeepError。两种失败都在写入前判定，绝不部分应用；
    - 位点只能单调前进：resume() 做完整性校验，catchup_to() 拒绝回退与
      跨度过大的追赶。
    """

    DEFAULT_CONFIRMATION_DEPTH = 6
    DEFAULT_MAX_CATCHUP_SPAN = 100

    def __init__(
        self,
        store: BlockStore,
        *,
        confirmation_depth: int = DEFAULT_CONFIRMATION_DEPTH,
        max_catchup_span: int = DEFAULT_MAX_CATCHUP_SPAN,
    ) -> None:
        self._store = store
        self._confirmation_depth = confirmation_depth
        self._max_catchup_span = max_catchup_span
        self._cursor: int | None = store.get_progress()
        # 串行化写路径：内存 cursor 判定 + 存储事务必须作为一个临界区，
        # 避免 FastAPI 线程池中的并发投递交叉推进位点。
        self._lock = threading.RLock()
        self._log = get_logger()

    # ---- 属性 ----
    @property
    def cursor(self) -> int | None:
        """当前已索引最高高度。"""
        return self._cursor

    @property
    def confirmation_depth(self) -> int:
        return self._confirmation_depth

    @property
    def confirmed_watermark(self) -> int | None:
        """已确认边界：height <= watermark 的数据不允许被分叉覆盖。

        定义：cursor - height >= confirmation_depth 时高度已确认。
        链长度不足以产生任何确认块时返回 None。
        """
        if self._cursor is None:
            return None
        wm = self._cursor - self._confirmation_depth
        return wm if wm >= 0 else None

    def _is_confirmed(self, height: int) -> bool:
        wm = self.confirmed_watermark
        return wm is not None and height <= wm

    # ------------------------------------------------------------------ #
    # 写入：顺序追加 / 重复 / 分叉
    # ------------------------------------------------------------------ #
    def ingest_block(self, block: Block) -> IngestResult:
        """投递一个区块。

        返回 accepted / duplicate / reorg；非法投递抛出带可区分 reason 的异常。
        """
        with self._lock:
            return self._ingest_block_locked(block)

    def _ingest_block_locked(self, block: Block) -> IngestResult:
        h = block.height
        cur = self._cursor

        # 空库：只接受创世块
        if cur is None:
            if h != 0:
                raise GapError(
                    f"索引为空，首个区块必须是高度 0，收到高度 {h}", height=h
                )
            if block.parent_hash != GENESIS_PARENT:
                raise ParentMismatchError(
                    "创世块 parent_hash 必须是全零哨兵", height=0
                )
            return self._append(block)

        # 顺序追加
        if h == cur + 1:
            parent = self._store.get_block(cur)
            if parent is None:  # 理论上不可达，完整性兜底
                raise StoreCorruptedError(f"缺少前一高度 {cur}", height=cur)
            if block.parent_hash != parent.hash:
                raise ParentMismatchError(
                    f"parent_hash 不匹配：期望 {parent.hash}，收到 {block.parent_hash}",
                    height=h,
                )
            return self._append(block)

        # 乱序超前
        if h > cur + 1:
            raise GapError(
                f"区块高度 {h} 超前：当前位点 {cur}，下一应索引高度 {cur + 1}",
                height=h,
            )

        # h <= cur：重复或分叉
        existing = self._store.get_block(h)
        if existing is None:  # 不可达
            raise StoreCorruptedError(f"位点 {cur} 处缺少区块 {h}", height=h)

        if existing.hash == block.hash:
            result = IngestResult(
                action=ACTION_DUPLICATE,
                height=h,
                block_hash=block.hash,
                reason="already_indexed",
            )
            self._emit(result)
            raise DuplicateDeliveryError(
                f"高度 {h} 的区块 {block.hash} 已索引，重复投递被忽略", height=h
            )

        # 同高度不同哈希：已确认高度拒绝覆盖
        if self._is_confirmed(h):
            raise ConfirmedDataOverwriteError(
                f"高度 {h} 已有 {self._confirmation_depth} 个确认，"
                f"拒绝用 {block.hash} 覆盖 {existing.hash}",
                height=h,
            )

        # 未确认：尝试以「单块分叉路径」重组。父块必须能在库中 h-1 处接上，
        # 否则这是无法定位基点的陈旧分叉块，需调用方提交完整路径。
        expected_parent = (
            GENESIS_PARENT if h == 0 else self._store.get_block(h - 1).hash
        )
        if block.parent_hash != expected_parent:
            raise StaleHeightError(
                f"高度 {h} 的分叉块父哈希 {block.parent_hash} 与链上 "
                f"{expected_parent} 不符，无法定位分叉基点，"
                "请通过 reindex_path 提交完整分叉路径",
                height=h,
            )

        return self._apply_reorg_path([block])

    def reindex_path(self, path: list[Block]) -> IngestResult:
        """提交完整分叉路径并在单事务内重组。"""
        with self._lock:
            return self._reindex_path_locked(path)

    def _reindex_path_locked(self, path: list[Block]) -> IngestResult:
        """path 为从基点后第一块到新链尖的有序、高度连续、parent 相连的块序列。"""
        if not path:
            raise ParentMismatchError("分叉路径为空")
        start_h = path[0].height

        # 路径自身校验：高度连续 + parent 相连
        prev: Block | None = None
        for i, blk in enumerate(path):
            if blk.height != start_h + i:
                raise ParentMismatchError(
                    f"分叉路径高度不连续：位置 {i} 期望高度 {start_h + i}，"
                    f"实际 {blk.height}",
                    height=blk.height,
                )
            if prev is not None and blk.parent_hash != prev.hash:
                raise ParentMismatchError(
                    f"分叉路径在高度 {blk.height} 处 parent_hash 断链",
                    height=blk.height,
                )
            prev = blk

        cur = self._cursor

        # 基点必须存在于库中且与路径首块相连
        if start_h == 0:
            if cur is not None and path[0].parent_hash != GENESIS_PARENT:
                raise ParentMismatchError("创世分叉块 parent_hash 非法", height=0)
        else:
            base = self._store.get_block(start_h - 1) if cur is not None else None
            if base is None:
                raise StaleHeightError(
                    f"分叉基点高度 {start_h - 1} 尚未索引，无法重组",
                    height=start_h - 1,
                )
            if path[0].parent_hash != base.hash:
                raise ParentMismatchError(
                    f"分叉首块 parent_hash 与基点 {start_h - 1} ({base.hash}) 不符",
                    height=start_h,
                )

        # 纯顺序追加（路径恰好接在链尖之后）
        if cur is None:
            if start_h != 0:
                raise GapError(f"空库不能从高度 {start_h} 开始索引", height=start_h)
            return self._append_path(path)
        if start_h == cur + 1:
            return self._append_path(path)

        # 起点已索引：同哈希即重复投递，不做部分应用
        existing = self._store.get_block(start_h)
        if existing is not None and existing.hash == path[0].hash:
            raise DuplicateDeliveryError(
                f"分叉路径起点高度 {start_h} 的区块已存在且哈希相同",
                height=start_h,
            )

        # 分叉重组：确认深度检查（先判定，再动数据）
        wm = self.confirmed_watermark
        if wm is not None and start_h <= wm:
            raise ReorgTooDeepError(
                f"分叉基点后的首高度 {start_h} 已在确认边界 {wm} 之内"
                f"（确认深度 {self._confirmation_depth}），拒绝重组",
                height=start_h,
            )

        return self._apply_reorg_path(path)

    def reorg_to(self, block: Block) -> IngestResult:
        """显式重组入口：单块语义等价于 ingest_block 的分叉分支。"""
        return self.ingest_block(block)

    # ---- 内部：原子写入 ----
    def _append(self, block: Block) -> IngestResult:
        with self._store.tx():
            self._store.put_block(block)
            self._store.set_progress(block.height)
        self._cursor = block.height
        result = IngestResult(
            action=ACTION_ACCEPTED, height=block.height, block_hash=block.hash
        )
        self._emit(result)
        return result

    def _append_path(self, path: list[Block]) -> IngestResult:
        with self._store.tx():
            for blk in path:
                self._store.put_block(blk)
            self._store.set_progress(path[-1].height)
        self._cursor = path[-1].height
        result = IngestResult(
            action=ACTION_ACCEPTED,
            height=path[-1].height,
            block_hash=path[-1].hash,
        )
        self._emit(result)
        return result

    def _apply_reorg_path(self, path: list[Block]) -> IngestResult:
        """回滚 >= start_h 的所有高度并写入分叉路径，全程单事务。"""
        start_h = path[0].height
        cur = self._cursor
        assert cur is not None

        with self._store.tx():
            rolled_back = self._store.delete_blocks_from(start_h)
            for blk in path:
                self._store.put_block(blk)
            new_tip = path[-1].height
            self._store.set_progress(new_tip)

        self._cursor = path[-1].height
        result = IngestResult(
            action=ACTION_REORG,
            height=new_tip,
            block_hash=path[-1].hash,
            rolled_back=tuple(rolled_back),
        )
        self._emit(result)
        return result

    # ------------------------------------------------------------------ #
    # 位点恢复 / 追赶
    # ------------------------------------------------------------------ #
    def resume(self) -> int | None:
        """进程重启后从持久化位点恢复并校验完整性。"""
        with self._lock:
            self._store.verify_integrity()
            self._cursor = self._store.get_progress()
            self._log.info(
                "[cursor=%s] resume 完成：从持久化位点恢复，确认边界=%s",
                self._cursor,
                self.confirmed_watermark,
            )
            return self._cursor

    def catchup_to(self, target_height: int) -> int:
        """位点对齐 / 追赶。"""
        with self._lock:
            return self._catchup_to_locked(target_height)

    def _catchup_to_locked(self, target_height: int) -> int:
        """位点对齐规则：

        - target 小于当前位点 -> ProgressRollbackError（位点不允许回退）；
        - target - cursor 超过 max_catchup_span -> CatchupTooFarError
          （跨度过大，必须逐段推进）；
        - 只推进到数据实际末端；不凭空声明未索引的数据。
        """
        if target_height < 0:
            raise ProgressRollbackError(
                f"目标高度 {target_height} 非法（不能为负）", height=target_height
            )

        top = self._store.highest_height()
        cur = self._cursor

        if cur is None:
            # 空库：没有任何已持久化数据，位点只能停在 None
            if top is None:
                self._log.info("[cursor=None] catchup_to(%s)：库为空，不推进", target_height)
                return None  # type: ignore[return-value]
            raise StoreCorruptedError("位点为空但存在区块数据")

        if target_height < cur:
            raise ProgressRollbackError(
                f"拒绝位点回退：当前位点 {cur}，请求回到 {target_height}",
                height=target_height,
            )

        if target_height - cur > self._max_catchup_span:
            raise CatchupTooFarError(
                f"追赶跨度 {target_height - cur} 超过上限 "
                f"{self._max_catchup_span}（{cur} -> {target_height}），请分段推进",
                height=target_height,
            )

        effective = min(target_height, top if top is not None else cur)
        if effective != cur:
            self._store.set_progress(effective)
            self._cursor = effective
        self._log.info(
            "[cursor=%s] catchup_to(%s) 完成，实际位点=%s（数据末端=%s）",
            cur, target_height, effective, top,
        )
        return effective

    # ------------------------------------------------------------------ #
    # 查询
    # ------------------------------------------------------------------ #
    def get_block(self, height: int) -> Block | None:
        return self._store.get_block(height)

    def get_transaction(self, tx_hash: str) -> Transaction | None:
        return self._store.get_transaction(tx_hash)

    # ---- 日志流水 ----
    def _emit(self, result: IngestResult) -> None:
        rolled = (
            f" rolled_back={list(result.rolled_back)}" if result.rolled_back else ""
        )
        reason = f" reason={result.reason}" if result.reason else ""
        self._log.info(
            "[cursor=%s] %s height=%s hash=%s%s%s",
            self._cursor,
            result.action,
            result.height,
            result.block_hash[:10],
            rolled,
            reason,
        )

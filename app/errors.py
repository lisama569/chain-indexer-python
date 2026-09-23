"""错误类型定义。

所有索引阶段的拒绝原因都使用独立的异常类型表达，reason 为稳定的机器可读码，
便于调用方（含测试）区分「为什么被拒绝」，而不是只看错误信息文本。
"""


class IndexerError(Exception):
    """索引器错误基类。"""

    reason: str = "indexer_error"

    def __init__(self, message: str = "", *, height: int | None = None):
        self.height = height
        if height is not None:
            message = f"{message} (height={height})"
        super().__init__(message)


class DuplicateDeliveryError(IndexerError):
    """重复投递：同高度、同哈希的区块已经索引过。"""

    reason = "duplicate_delivery"


class ConfirmedDataOverwriteError(IndexerError):
    """尝试覆盖已确认（深度 >= confirmation_depth）高度的数据，必须先重组回滚。"""

    reason = "confirmed_data_overwrite"


class ParentMismatchError(IndexerError):
    """区块的 parent_hash 与前一高度不连续，分叉基点超出可回滚范围或乱序缺口。"""

    reason = "parent_mismatch"


class GapError(IndexerError):
    """区块高度超前于当前位点，存在尚未索引的缺口（乱序到达）。"""

    reason = "height_gap"


class StaleHeightError(IndexerError):
    """区块高度落后于已索引位点且哈希不同（位点回退 / 陈旧分叉块）。"""

    reason = "stale_height"


class ReorgTooDeepError(IndexerError):
    """分叉基点位于已确认高度之内，重组无法安全回滚。"""

    reason = "reorg_too_deep"


class ProgressRollbackError(IndexerError):
    """持久化位点恢复 / 追赶请求试图把位点回退到更早高度。"""

    reason = "progress_rollback"


class CatchupTooFarError(IndexerError):
    """追赶请求的目标跨度超过允许上限，必须逐段推进。"""

    reason = "catchup_too_far"


class StoreCorruptedError(IndexerError):
    """存储内区块与位点不一致（如重启后检测到缺口），拒绝继续写入。"""

    reason = "store_corrupted"

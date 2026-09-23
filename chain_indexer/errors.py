"""可区分的拒绝原因与异常。"""
from __future__ import annotations

import enum


class RejectReason(enum.Enum):
    DUPLICATE_DELIVERY = "duplicate_delivery"      # 重复投递：内容一致，幂等忽略
    CONFLICTING_BLOCK = "conflicting_block"        # 同高度不同哈希，且未构成可处理分叉
    STALE_HEIGHT = "stale_height"                  # 高度落后于已确认位点
    HEIGHT_GAP = "height_gap"                      # 高度跳跃过大，存在缺口
    ORPHAN_BLOCK = "orphan_block"                  # 父哈希未知，无法衔接
    REORG_TOO_DEEP = "reorg_too_deep"              # 重组深度超过可回滚上限
    CHECKPOINT_REGRESSION = "checkpoint_regression"  # 位点回退请求被拒绝
    CATCHUP_TOO_LARGE = "catchup_too_large"        # 追赶跨度过大被拒绝


class RejectError(Exception):
    def __init__(self, reason: RejectReason, detail: str = "") -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(f"{reason.value}: {detail}")

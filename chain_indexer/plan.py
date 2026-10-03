"""操作请求与决策记录类型：预演计划、执行结论、决策流水共用。"""
from __future__ import annotations

from dataclasses import dataclass, field

from .errors import RejectReason
from .models import Block, Checkpoint

MODE_PLAN = "plan"        # 预演：只读，不改动索引数据
MODE_EXECUTE = "execute"  # 实际执行

STATUS_ACCEPTED = "accepted"
STATUS_REJECTED = "rejected"

KIND_INGEST = "ingest"
KIND_SYNC = "sync"
KIND_BATCH = "batch"


@dataclass(frozen=True)
class IngestOp:
    """单块投递操作。"""

    block: Block


@dataclass(frozen=True)
class SyncOp:
    """追赶到指定高度操作。"""

    target_height: int


@dataclass(frozen=True)
class OpOutcome:
    """批量请求中单个操作的结论。"""

    index: int
    kind: str
    status: str
    result: str | None = None
    reject_reason: str | None = None
    reject_detail: str | None = None


@dataclass(frozen=True)
class Decision:
    """一次预演或执行的完整结论，同时作为持久化决策流水的一行。

    applied/duplicates/rolled_back 为高度列表；被拒绝的预演中这些列表
    表示"若执行将会发生"的候选动作（执行侧被拒绝时实际什么都不发生）。
    """

    mode: str
    kind: str
    status: str
    request_json: str
    cp_before: Checkpoint | None
    applied: tuple[int, ...] = ()
    duplicates: tuple[int, ...] = ()
    rolled_back: tuple[int, ...] = ()
    cp_after: Checkpoint | None = None
    outcomes: tuple[OpOutcome, ...] = ()
    reject_reason: RejectReason | None = None
    reject_detail: str = ""
    id: int | None = None
    created_at: str = ""

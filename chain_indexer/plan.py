"""操作请求与决策记录类型：预演计划、执行结论、决策流水共用。"""
from __future__ import annotations

from dataclasses import dataclass, field

from .errors import RejectReason
from .models import Block, Checkpoint

MODE_PLAN = "plan"        # 预演：只读，不改动索引数据
MODE_EXECUTE = "execute"  # 实际执行

STATUS_ACCEPTED = "accepted"
STATUS_REJECTED = "rejected"
STATUS_ABORTED = "aborted"  # 批量中因前序操作被拒而未评估

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
    """批量请求中单个操作的结论。

    applied/duplicates/rolled_back 为该操作自身涉及的高度范围；整批最终被
    拒绝时，accepted/rejected 操作上的这些高度只是"若执行将会发生"的候选
    动作（实际事务已整体回滚，什么都不生效），aborted 操作一律为空。
    """

    index: int
    kind: str
    status: str
    result: str | None = None
    reject_reason: str | None = None
    reject_detail: str | None = None
    applied: tuple[int, ...] = ()
    duplicates: tuple[int, ...] = ()
    rolled_back: tuple[int, ...] = ()


@dataclass(frozen=True)
class Decision:
    """一次预演或执行的完整结论，同时作为持久化决策流水的一行。

    applied/duplicates/rolled_back 为高度列表，语义分两种：

    - status="accepted"：实际（执行）或将会（预演）发生的动作；
    - status="rejected"：评估期模拟出的"若真执行将会发生"的候选动作
      （例如会先触发重组、回滚一段已确认链，随后才因目标够不到被拒）。
      候选信息不会因最终被拒而被吞掉；实际数据事务一律整体回滚，
      cp_after 始终等于 cp_before，表示位点与数据事实上没有任何变化。
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

"""数据模型：区块、交易、索引位点、执行计划与决策流水。"""
from __future__ import annotations

import enum
from dataclasses import dataclass, field

from .errors import RejectReason


@dataclass(frozen=True)
class Transaction:
    txid: str
    sender: str
    recipient: str
    amount: int


@dataclass(frozen=True)
class Block:
    height: int
    block_hash: str
    parent_hash: str
    transactions: tuple[Transaction, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class Checkpoint:
    """索引位点：已确认推进到的最高高度与对应区块哈希。"""

    height: int
    block_hash: str


class DecisionMode(enum.Enum):
    """决策来源：只读预演，或真正执行。"""

    DRY_RUN = "dry_run"
    EXECUTE = "execute"


class RequestKind(enum.Enum):
    """请求类型：单块投递、区间/多目标追赶、批量投递。"""

    INGEST = "ingest"
    SYNC = "sync"
    BATCH_INGEST = "batch_ingest"
    BATCH_SYNC = "batch_sync"


class PlanStatus(enum.Enum):
    """计划结论：可执行 / 被拒绝。"""

    ACCEPTED = "accepted"
    REJECTED = "rejected"


@dataclass(frozen=True)
class Plan:
    """一次请求的执行计划（预演结论）。

    纯只读计算的结果：``before`` 为当前位点，``after`` 为若执行将到达的位点；
    ``applied`` / ``duplicates`` / ``rolled_back`` 为将要应用、跳过、回滚的高度。
    被拒绝时 ``status`` 为 rejected 并携带 ``reason``，其余字段给出"若放行会发生什么"
    （rollback 列表体现是否本会触发重组）。
    """

    kind: RequestKind
    target: str
    status: PlanStatus
    before: Checkpoint | None
    after: Checkpoint | None
    applied: tuple[int, ...] = field(default_factory=tuple)
    duplicates: tuple[int, ...] = field(default_factory=tuple)
    rolled_back: tuple[int, ...] = field(default_factory=tuple)
    reason: RejectReason | None = None
    detail: str = ""


@dataclass(frozen=True)
class DecisionRecord:
    """一条持久化的决策流水。"""

    seq: int
    mode: DecisionMode
    kind: RequestKind
    target: str
    status: PlanStatus
    before_height: int | None
    before_hash: str | None
    after_height: int | None
    after_hash: str | None
    applied: tuple[int, ...]
    duplicates: tuple[int, ...]
    rolled_back: tuple[int, ...]
    reason: RejectReason | None
    detail: str

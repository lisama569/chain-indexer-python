"""持久化决策流水：每次预演与执行的结论均落库，重启后仍可查询。"""
from __future__ import annotations

import datetime

from .models import (
    Block,
    Checkpoint,
    DecisionMode,
    DecisionRecord,
    Plan,
    PlanStatus,
)


class DecisionJournal:
    """把 :class:`~chain_indexer.models.Plan` 结论追加到持久化存储并提供查询。

    流水只增不改：预演与执行各占一条，互不覆盖。查询结果可用于解释
    "某高度为何是这个哈希""某笔交易为何查不到"。
    """

    def __init__(self, store) -> None:
        self._store = store

    def record(self, mode: DecisionMode, plan: Plan) -> DecisionRecord:
        seq = self._store.append_decision(
            created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            mode=mode,
            kind=plan.kind,
            target=plan.target,
            status=plan.status,
            before=plan.before,
            after=plan.after,
            applied=plan.applied,
            duplicates=plan.duplicates,
            rolled_back=plan.rolled_back,
            reason=plan.reason,
            detail=plan.detail,
        )
        record = self._store.get_decision(seq)
        assert record is not None
        return record

    def get(self, seq: int) -> DecisionRecord | None:
        return self._store.get_decision(seq)

    def list_all(self, limit: int = 100) -> list[DecisionRecord]:
        return self._store.list_decisions(limit)

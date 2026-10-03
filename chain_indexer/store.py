"""SQLite 持久化存储：区块、交易、索引位点与决策流水。

只保存"已确认的主链视图"：回滚即物理删除受影响高度的行，
因此查询层永远只能看到当前一致的主链结果。

多步变更（回滚 + 批量落库 + 位点推进）通过 ``transaction()`` 放进同一个
SQLite 事务：任何一步失败整体回滚，绝不留下"删了数据但位点没退"之类的
中间状态。决策流水独立追加，预演也能只读地记录结论而不触碰索引数据。
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from dataclasses import dataclass

from .models import (
    Block,
    Checkpoint,
    DecisionMode,
    DecisionRecord,
    PlanStatus,
    RequestKind,
    RejectReason,
    Transaction,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS blocks (
    height      INTEGER PRIMARY KEY,
    hash        TEXT NOT NULL UNIQUE,
    parent_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transactions (
    txid         TEXT PRIMARY KEY,
    block_height INTEGER NOT NULL REFERENCES blocks(height) ON DELETE CASCADE,
    sender       TEXT NOT NULL,
    recipient    TEXT NOT NULL,
    amount       INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS checkpoint (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    height     INTEGER NOT NULL,
    block_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS decisions (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at    TEXT NOT NULL,
    mode          TEXT NOT NULL,
    kind          TEXT NOT NULL,
    target        TEXT NOT NULL,
    status        TEXT NOT NULL,
    before_height INTEGER,
    before_hash   TEXT,
    after_height  INTEGER,
    after_hash    TEXT,
    applied       TEXT NOT NULL,
    duplicates    TEXT NOT NULL,
    rolled_back   TEXT NOT NULL,
    reason        TEXT,
    detail        TEXT NOT NULL DEFAULT ''
);
"""


def _locked(fn):
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return fn(self, *args, **kwargs)
    return wrapper


@dataclass(frozen=True)
class StoreSnapshot:
    """某一时刻索引数据的不可变只读快照，供预演在其上推演。"""

    checkpoint: Checkpoint | None
    blocks: dict[int, Block]


class _Transaction:
    """事务内句柄：方法不自行提交，由 :meth:`IndexStore.transaction` 统一提交/回滚。"""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def apply_block(self, block: Block) -> None:
        self._conn.execute(
            "INSERT INTO blocks(height, hash, parent_hash) VALUES (?, ?, ?)",
            (block.height, block.block_hash, block.parent_hash),
        )
        self._conn.executemany(
            "INSERT INTO transactions(txid, block_height, sender, recipient, amount)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (tx.txid, block.height, tx.sender, tx.recipient, tx.amount)
                for tx in block.transactions
            ],
        )

    def rollback_from(self, height: int) -> list[Block]:
        """删除 height 及以上（含）的区块与交易，按高度降序返回被回滚的区块。"""
        rows = self._conn.execute(
            "SELECT height, hash, parent_hash FROM blocks WHERE height >= ?"
            " ORDER BY height DESC",
            (height,),
        ).fetchall()
        removed: list[Block] = []
        for h, bhash, phash in rows:
            txs = self._conn.execute(
                "SELECT txid, sender, recipient, amount FROM transactions"
                " WHERE block_height = ?",
                (h,),
            ).fetchall()
            removed.append(
                Block(
                    height=h,
                    block_hash=bhash,
                    parent_hash=phash,
                    transactions=tuple(
                        Transaction(txid=t, sender=s, recipient=r, amount=a)
                        for t, s, r, a in txs
                    ),
                )
            )
            self._conn.execute(
                "DELETE FROM transactions WHERE block_height = ?", (h,)
            )
            self._conn.execute("DELETE FROM blocks WHERE height = ?", (h,))
        return removed

    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        self._conn.execute(
            "INSERT INTO checkpoint(id, height, block_hash) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET height = excluded.height,"
            " block_hash = excluded.block_hash",
            (checkpoint.height, checkpoint.block_hash),
        )


class IndexStore:
    def __init__(self, db_path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._lock = threading.RLock()
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.executescript(_SCHEMA)
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    # ---- 事务 ----
    @contextlib.contextmanager
    def transaction(self):
        """在单一 SQLite 事务 + 排他锁内执行多步变更，全成或全不动。"""
        with self._lock:
            with self._conn:
                yield _Transaction(self._conn)

    # ---- 只读快照 ----
    @_locked
    def snapshot(self) -> StoreSnapshot:
        rows = self._conn.execute(
            "SELECT height, hash, parent_hash FROM blocks ORDER BY height"
        ).fetchall()
        blocks = {
            h: self._row_to_block((h, bhash, phash)) for h, bhash, phash in rows
        }
        cp_row = self._conn.execute(
            "SELECT height, block_hash FROM checkpoint WHERE id = 1"
        ).fetchone()
        cp = Checkpoint(cp_row[0], cp_row[1]) if cp_row is not None else None
        return StoreSnapshot(checkpoint=cp, blocks=blocks)

    # ---- 写入（单步便捷接口，各自独立事务）----
    @_locked
    def apply_block(self, block: Block) -> None:
        """原子写入一个区块及其全部交易。"""
        with self._conn:
            _Transaction(self._conn).apply_block(block)

    @_locked
    def rollback_from(self, height: int) -> list[Block]:
        with self._conn:
            return _Transaction(self._conn).rollback_from(height)

    # ---- 位点 ----
    @_locked
    def get_checkpoint(self) -> Checkpoint | None:
        row = self._conn.execute(
            "SELECT height, block_hash FROM checkpoint WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        return Checkpoint(height=row[0], block_hash=row[1])

    @_locked
    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        with self._conn:
            _Transaction(self._conn).save_checkpoint(checkpoint)

    # ---- 查询 ----
    @_locked
    def get_block_by_height(self, height: int) -> Block | None:
        row = self._conn.execute(
            "SELECT height, hash, parent_hash FROM blocks WHERE height = ?",
            (height,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_block(row)

    @_locked
    def get_block_by_hash(self, block_hash: str) -> Block | None:
        row = self._conn.execute(
            "SELECT height, hash, parent_hash FROM blocks WHERE hash = ?",
            (block_hash,),
        ).fetchone()
        if row is None:
            return None
        return self._row_to_block(row)

    @_locked
    def get_transaction(self, txid: str) -> Transaction | None:
        row = self._conn.execute(
            "SELECT txid, sender, recipient, amount FROM transactions WHERE txid = ?",
            (txid,),
        ).fetchone()
        if row is None:
            return None
        return Transaction(txid=row[0], sender=row[1], recipient=row[2], amount=row[3])

    def _row_to_block(self, row: tuple) -> Block:
        height, bhash, phash = row
        txs = self._conn.execute(
            "SELECT txid, sender, recipient, amount FROM transactions"
            " WHERE block_height = ? ORDER BY rowid",
            (height,),
        ).fetchall()
        return Block(
            height=height,
            block_hash=bhash,
            parent_hash=phash,
            transactions=tuple(
                Transaction(txid=t, sender=s, recipient=r, amount=a)
                for t, s, r, a in txs
            ),
        )

    # ---- 决策流水 ----
    @_locked
    def append_decision(
        self,
        *,
        created_at: str,
        mode: DecisionMode,
        kind: RequestKind,
        target: str,
        status: PlanStatus,
        before: Checkpoint | None,
        after: Checkpoint | None,
        applied: tuple[int, ...],
        duplicates: tuple[int, ...],
        rolled_back: tuple[int, ...],
        reason: RejectReason | None,
        detail: str,
    ) -> int:
        with self._conn:
            cur = self._conn.execute(
                "INSERT INTO decisions(created_at, mode, kind, target, status,"
                " before_height, before_hash, after_height, after_hash,"
                " applied, duplicates, rolled_back, reason, detail)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    created_at,
                    mode.value,
                    kind.value,
                    target,
                    status.value,
                    before.height if before else None,
                    before.block_hash if before else None,
                    after.height if after else None,
                    after.block_hash if after else None,
                    json.dumps(list(applied)),
                    json.dumps(list(duplicates)),
                    json.dumps(list(rolled_back)),
                    reason.value if reason else None,
                    detail,
                ),
            )
            return int(cur.lastrowid)

    @_locked
    def get_decision(self, seq: int) -> DecisionRecord | None:
        row = self._conn.execute(
            "SELECT seq, mode, kind, target, status, before_height, before_hash,"
            " after_height, after_hash, applied, duplicates, rolled_back, reason, detail"
            " FROM decisions WHERE seq = ?",
            (seq,),
        ).fetchone()
        return self._row_to_decision(row) if row is not None else None

    @_locked
    def list_decisions(self, limit: int = 100) -> list[DecisionRecord]:
        rows = self._conn.execute(
            "SELECT seq, mode, kind, target, status, before_height, before_hash,"
            " after_height, after_hash, applied, duplicates, rolled_back, reason, detail"
            " FROM decisions ORDER BY seq DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [self._row_to_decision(row) for row in rows]

    @staticmethod
    def _row_to_decision(row: tuple) -> DecisionRecord:
        (seq, mode, kind, target, status, bh, bhash, ah, ahash,
         applied, duplicates, rolled_back, reason, detail) = row

        def cp_or_none(h, bhash):
            return Checkpoint(h, bhash) if h is not None else None

        return DecisionRecord(
            seq=seq,
            mode=DecisionMode(mode),
            kind=RequestKind(kind),
            target=target,
            status=PlanStatus(status),
            before_height=bh,
            before_hash=bhash,
            after_height=ah,
            after_hash=ahash,
            applied=tuple(json.loads(applied)),
            duplicates=tuple(json.loads(duplicates)),
            rolled_back=tuple(json.loads(rolled_back)),
            reason=RejectReason(reason) if reason is not None else None,
            detail=detail,
        )

"""SQLite 持久化存储：区块、交易、索引位点与决策流水。

只保存"已确认的主链视图"：回滚即物理删除受影响高度的行，
因此查询层永远只能看到当前一致的主链结果。

引擎层可通过 transaction() 获取连接级事务，把"回滚+重索引+位点推进"
或整批请求放进同一个事务：任一环节被拒绝则整体回滚，绝不留下
"旧数据已删而位点未退"之类的中间状态。
"""
from __future__ import annotations

import contextlib
import json
import sqlite3
import threading
from datetime import datetime, timezone

from .models import Block, Checkpoint, Transaction
from .plan import Decision, OpOutcome

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
CREATE TABLE IF NOT EXISTS decision_journal (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at        TEXT NOT NULL,
    mode              TEXT NOT NULL,
    kind              TEXT NOT NULL,
    status            TEXT NOT NULL,
    request_json      TEXT NOT NULL,
    cp_before_height  INTEGER,
    cp_before_hash    TEXT,
    cp_after_height   INTEGER,
    cp_after_hash     TEXT,
    applied           TEXT NOT NULL,
    duplicates        TEXT NOT NULL,
    rolled_back       TEXT NOT NULL,
    outcomes          TEXT NOT NULL,
    reject_reason     TEXT,
    reject_detail     TEXT NOT NULL DEFAULT ''
);
"""


def _locked(fn):
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return fn(self, *args, **kwargs)
    return wrapper


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
    def transaction(self, commit: bool = True):
        """连接级事务 + 全局串行锁。

        事务内只能调用 *_unsafe 系列写方法（嵌套 with self._conn 会提前
        提交，破坏原子性）；正常退出时按 commit 决定提交或回滚，
        异常退出一律整体回滚。预演使用 commit=False 实现严格只读。
        """
        with self._lock:
            try:
                yield self
            except BaseException:
                self._conn.rollback()
                raise
            else:
                if commit:
                    self._conn.commit()
                else:
                    self._conn.rollback()

    # ---- 区块写入（unsafe 变体必须在 transaction() 内调用）----
    def apply_block(self, block: Block) -> None:
        """原子写入一个区块及其全部交易。"""
        with self.transaction():
            self._apply_block_unsafe(block)

    def _apply_block_unsafe(self, block: Block) -> None:
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

    @_locked
    def rollback_from(self, height: int) -> list[Block]:
        """删除 height 及以上（含）的所有区块与交易，按高度降序返回被回滚的区块。"""
        with self._conn:
            return self._rollback_from_unsafe(height)

    def _rollback_from_unsafe(self, height: int) -> list[Block]:
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

    # ---- 位点 ----
    @_locked
    def get_checkpoint(self) -> Checkpoint | None:
        return self._get_checkpoint_unsafe()

    def _get_checkpoint_unsafe(self) -> Checkpoint | None:
        row = self._conn.execute(
            "SELECT height, block_hash FROM checkpoint WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        return Checkpoint(height=row[0], block_hash=row[1])

    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        with self.transaction():
            self._save_checkpoint_unsafe(checkpoint)

    def _save_checkpoint_unsafe(self, checkpoint: Checkpoint) -> None:
        self._conn.execute(
            "INSERT INTO checkpoint(id, height, block_hash) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET height = excluded.height,"
            " block_hash = excluded.block_hash",
            (checkpoint.height, checkpoint.block_hash),
        )

    # ---- 决策流水 ----
    def append_decision(self, decision: Decision) -> tuple[int, str]:
        """在独立短事务中持久化一条决策记录，返回 (流水 id, 记录时间)。"""
        with self.transaction() as s:
            return s._append_decision_unsafe(decision)

    def _append_decision_unsafe(self, decision: Decision) -> tuple[int, str]:
        cp_b = decision.cp_before
        cp_a = decision.cp_after
        created_at = decision.created_at or datetime.now(timezone.utc).isoformat()
        cur = self._conn.execute(
            "INSERT INTO decision_journal(created_at, mode, kind, status, request_json,"
            " cp_before_height, cp_before_hash, cp_after_height, cp_after_hash,"
            " applied, duplicates, rolled_back, outcomes, reject_reason, reject_detail)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                created_at,
                decision.mode,
                decision.kind,
                decision.status,
                decision.request_json,
                cp_b.height if cp_b else None,
                cp_b.block_hash if cp_b else None,
                cp_a.height if cp_a else None,
                cp_a.block_hash if cp_a else None,
                json.dumps(list(decision.applied)),
                json.dumps(list(decision.duplicates)),
                json.dumps(list(decision.rolled_back)),
                json.dumps([o.__dict__ for o in decision.outcomes]),
                decision.reject_reason.value if decision.reject_reason else None,
                decision.reject_detail,
            ),
        )
        return int(cur.lastrowid), created_at

    @_locked
    def get_decision(self, decision_id: int) -> Decision | None:
        row = self._conn.execute(
            "SELECT id, created_at, mode, kind, status, request_json,"
            " cp_before_height, cp_before_hash, cp_after_height, cp_after_hash,"
            " applied, duplicates, rolled_back, outcomes, reject_reason, reject_detail"
            " FROM decision_journal WHERE id = ?",
            (decision_id,),
        ).fetchone()
        return self._row_to_decision(row) if row else None

    @_locked
    def list_decisions(
        self,
        limit: int | None = None,
        mode: str | None = None,
        kind: str | None = None,
        status: str | None = None,
    ) -> list[Decision]:
        sql = (
            "SELECT id, created_at, mode, kind, status, request_json,"
            " cp_before_height, cp_before_hash, cp_after_height, cp_after_hash,"
            " applied, duplicates, rolled_back, outcomes, reject_reason, reject_detail"
            " FROM decision_journal WHERE 1=1"
        )
        params: list = []
        if mode is not None:
            sql += " AND mode = ?"
            params.append(mode)
        if kind is not None:
            sql += " AND kind = ?"
            params.append(kind)
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        sql += " ORDER BY id"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_decision(r) for r in rows]

    def _row_to_decision(self, row: tuple) -> Decision:
        from .errors import RejectReason

        (
            did, created_at, mode, kind, status, request_json,
            bh, bhash, ah, ahash, applied, duplicates, rolled_back,
            outcomes, reject_reason, reject_detail,
        ) = row
        cp_before = Checkpoint(height=bh, block_hash=bhash) if bh is not None else None
        cp_after = Checkpoint(height=ah, block_hash=ahash) if ah is not None else None
        return Decision(
            id=did,
            created_at=created_at,
            mode=mode,
            kind=kind,
            status=status,
            request_json=request_json,
            cp_before=cp_before,
            cp_after=cp_after,
            applied=tuple(json.loads(applied)),
            duplicates=tuple(json.loads(duplicates)),
            rolled_back=tuple(json.loads(rolled_back)),
            outcomes=tuple(OpOutcome(**o) for o in json.loads(outcomes)),
            reject_reason=RejectReason(reject_reason) if reject_reason else None,
            reject_detail=reject_detail or "",
        )

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

    @_locked
    def snapshot(self) -> tuple:
        """当前主链视图快照：(位点, {高度: 哈希}, 全部 txid 集合)。

        供测试与上层断言"预演零写入 / 拒绝零部分应用"。
        """
        cp = self._get_checkpoint_unsafe()
        hashes = {
            h: hsh
            for h, hsh in self._conn.execute(
                "SELECT height, hash FROM blocks ORDER BY height"
            ).fetchall()
        }
        txids = {
            t for (t,) in self._conn.execute("SELECT txid FROM transactions").fetchall()
        }
        return cp, hashes, txids

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

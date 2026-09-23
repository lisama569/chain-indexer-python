"""SQLite 持久化存储：区块、交易与索引位点。

只保存"已确认的主链视图"：回滚即物理删除受影响高度的行，
因此查询层永远只能看到当前一致的主链结果。
"""
from __future__ import annotations

import sqlite3
import threading

from .models import Block, Checkpoint, Transaction

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

    # ---- 写入 ----
    @_locked
    def apply_block(self, block: Block) -> None:
        """原子写入一个区块及其全部交易。"""
        with self._conn:
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
        rows = self._conn.execute(
            "SELECT height, hash, parent_hash FROM blocks WHERE height >= ?"
            " ORDER BY height DESC",
            (height,),
        ).fetchall()
        removed: list[Block] = []
        with self._conn:
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
        row = self._conn.execute(
            "SELECT height, block_hash FROM checkpoint WHERE id = 1"
        ).fetchone()
        if row is None:
            return None
        return Checkpoint(height=row[0], block_hash=row[1])

    @_locked
    def save_checkpoint(self, checkpoint: Checkpoint) -> None:
        with self._conn:
            self._conn.execute(
                "INSERT INTO checkpoint(id, height, block_hash) VALUES (1, ?, ?)"
                " ON CONFLICT(id) DO UPDATE SET height = excluded.height,"
                " block_hash = excluded.block_hash",
                (checkpoint.height, checkpoint.block_hash),
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

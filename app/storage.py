"""SQLite 持久化存储：区块、交易、索引位点。"""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from app.errors import StoreCorruptedError
from app.models import Block, Transaction


class BlockStore:
    """区块 / 交易 / 位点的持久化存储。

    表结构：
    - progress: 单行表，保存已索引最高高度（cursor）；
    - blocks:   height 为主键，hash 唯一；
    - transactions: tx_hash 为主键，归属某个区块高度。

    所有多步写操作通过 ``with store.tx()`` 在单事务内完成，保证
    「回滚区块 + 重建新区块 + 更新位点」的原子性：进程崩溃时只会停留在
    变更前或变更后，不会出现半成品状态。
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._path = path
        # FastAPI 同步路由在线程池中执行，需允许连接跨线程；
        # 写事务由 _write_lock 串行化，避免并发写互相踩状态。
        self._con = sqlite3.connect(path, check_same_thread=False)
        self._write_lock = threading.RLock()
        self._con.row_factory = sqlite3.Row
        self._con.execute("PRAGMA foreign_keys = ON")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._con:
            self._con.executescript(
                """
                CREATE TABLE IF NOT EXISTS progress (
                    id     INTEGER PRIMARY KEY CHECK (id = 1),
                    height INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS blocks (
                    height      INTEGER PRIMARY KEY,
                    hash        TEXT NOT NULL UNIQUE,
                    parent_hash TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    tx_hash      TEXT PRIMARY KEY,
                    block_height INTEGER NOT NULL,
                    block_hash   TEXT NOT NULL,
                    sender       TEXT NOT NULL,
                    payload      TEXT NOT NULL
                );
                """
            )

    def close(self) -> None:
        self._con.close()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """显式事务上下文，供索引器把回滚 + 重建 + 位点更新包成原子操作。"""
        with self._write_lock:
            try:
                self._con.execute("BEGIN")
                yield self._con
                self._con.commit()
            except Exception:
                self._con.rollback()
                raise

    # ---- 位点 ----
    def get_progress(self) -> int | None:
        """已索引到的最高高度；空库返回 None。"""
        row = self._con.execute("SELECT height FROM progress WHERE id = 1").fetchone()
        return None if row is None else int(row["height"])

    def set_progress(self, height: int) -> None:
        sql = (
            "INSERT INTO progress(id, height) VALUES (1, ?) "
            "ON CONFLICT(id) DO UPDATE SET height = excluded.height"
        )
        # 处于调用方显式事务内时不自行提交（保证回滚+重建+位点原子性）；
        # 裸调用（如 catchup 对齐位点）时自行提交，避免隐式事务悬空。
        if self._con.in_transaction:
            self._con.execute(sql, (height,))
        else:
            with self._write_lock:
                self._con.execute(sql, (height,))
                self._con.commit()

    # ---- 区块 ----
    def get_block(self, height: int) -> Block | None:
        row = self._con.execute(
            "SELECT height, hash, parent_hash FROM blocks WHERE height = ?",
            (height,),
        ).fetchone()
        if row is None:
            return None
        txs = self.get_transactions_by_height(height)
        return Block(
            height=int(row["height"]),
            hash=row["hash"],
            parent_hash=row["parent_hash"],
            transactions=tuple(txs),
        )

    def put_block(self, block: Block) -> None:
        """写入区块及其交易。

        height 主键、hash 唯一、tx_hash 主键三处约束兜底，任何重复写入
        都会触发 sqlite3.IntegrityError 而不是静默覆盖。
        在显式事务内时不自行提交，由调用方保证原子性。
        """
        con = self._con
        bare = not con.in_transaction
        try:
            con.execute(
                "INSERT INTO blocks(height, hash, parent_hash) VALUES (?, ?, ?)",
                (block.height, block.hash, block.parent_hash),
            )
            con.executemany(
                "INSERT INTO transactions(tx_hash, block_height, block_hash, sender, payload) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (tx.tx_hash, tx.block_height, tx.block_hash, tx.sender, tx.payload)
                    for tx in block.transactions
                ],
            )
            if bare:
                with self._write_lock:
                    con.commit()
        except Exception:
            if bare:
                con.rollback()
            raise

    def delete_blocks_from(self, height: int) -> list[int]:
        """删除所有 >= height 的区块与交易，返回被删高度列表（降序）。"""
        rows = self._con.execute(
            "SELECT height FROM blocks WHERE height >= ? ORDER BY height DESC",
            (height,),
        ).fetchall()
        heights = [int(r["height"]) for r in rows]
        self._con.execute("DELETE FROM transactions WHERE block_height >= ?", (height,))
        self._con.execute("DELETE FROM blocks WHERE height >= ?", (height,))
        return heights

    def highest_height(self) -> int | None:
        row = self._con.execute("SELECT MAX(height) AS m FROM blocks").fetchone()
        return None if row is None or row["m"] is None else int(row["m"])

    # ---- 交易 ----
    def get_transaction(self, tx_hash: str) -> Transaction | None:
        row = self._con.execute(
            "SELECT tx_hash, block_height, block_hash, sender, payload "
            "FROM transactions WHERE tx_hash = ?",
            (tx_hash,),
        ).fetchone()
        if row is None:
            return None
        return Transaction(
            tx_hash=row["tx_hash"],
            block_height=int(row["block_height"]),
            block_hash=row["block_hash"],
            sender=row["sender"],
            payload=row["payload"],
        )

    def get_transactions_by_height(self, height: int) -> list[Transaction]:
        rows = self._con.execute(
            "SELECT tx_hash, block_height, block_hash, sender, payload "
            "FROM transactions WHERE block_height = ? ORDER BY tx_hash",
            (height,),
        ).fetchall()
        return [
            Transaction(
                tx_hash=r["tx_hash"],
                block_height=int(r["block_height"]),
                block_hash=r["block_hash"],
                sender=r["sender"],
                payload=r["payload"],
            )
            for r in rows
        ]

    # ---- 完整性 ----
    def verify_integrity(self) -> None:
        """重启时调用：校验位点与区块一致、高度连续、交易归属合法。

        任一条件不满足即抛 StoreCorruptedError，拒绝在损坏状态上继续写入，
        避免把历史结论悄悄搞坏。
        """
        progress = self.get_progress()
        top = self.highest_height()

        if top is None:
            if progress is not None:
                raise StoreCorruptedError(
                    f"progress={progress} 但区块表为空，位点无数据支撑"
                )
            return

        if progress != top:
            raise StoreCorruptedError(
                f"位点 cursor={progress} 与区块最高高度 {top} 不一致"
            )

        # 高度必须从 0 连续到 top
        (cnt,) = self._con.execute("SELECT COUNT(*) FROM blocks").fetchone()
        if cnt != top + 1:
            raise StoreCorruptedError(
                f"区块不连续：共 {cnt} 块但最高高度为 {top}（期望 0..{top}）"
            )

        # 交易的 block_height 必须存在于 blocks
        bad = self._con.execute(
            "SELECT COUNT(*) FROM transactions t "
            "WHERE NOT EXISTS (SELECT 1 FROM blocks b WHERE b.height = t.block_height)"
        ).fetchone()[0]
        if bad:
            raise StoreCorruptedError(f"发现 {bad} 笔交易归属的区块高度不存在")

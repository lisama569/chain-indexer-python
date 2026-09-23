"""本地链模拟器：确定性地追加区块、制造分叉，供索引器消费。"""
from __future__ import annotations

import hashlib

from .models import Block, Transaction

GENESIS_HASH = "genesis"


def _make_hash(parent_hash: str, height: int, txids: tuple[str, ...], salt: int) -> str:
    payload = f"{parent_hash}|{height}|{','.join(txids)}|{salt}".encode()
    return hashlib.sha256(payload).hexdigest()[:16]


class LocalChain:
    """内存中的模拟链：同一高度只保留主链区块，分叉时整体替换尾部。"""

    def __init__(self) -> None:
        self._blocks: dict[int, Block] = {}
        self._fork_salt = 0
        self._tx_seq = 0

    def _next_txid(self) -> str:
        self._tx_seq += 1
        return f"tx-{self._tx_seq}"

    def append_block(self, transactions: list[Transaction] | None = None) -> Block:
        """在当前链尖之后追加一个区块；未提供交易时自动生成一笔。"""
        height = self.tip_height + 1
        parent = self._blocks[height - 1].block_hash if height > 0 else GENESIS_HASH
        if transactions is None:
            transactions = [
                Transaction(
                    txid=self._next_txid(),
                    sender=f"miner-{height}",
                    recipient="treasury",
                    amount=50,
                )
            ]
        block = Block(
            height=height,
            block_hash=_make_hash(
                parent, height, tuple(t.txid for t in transactions), self._fork_salt
            ),
            parent_hash=parent,
            transactions=tuple(transactions),
        )
        self._blocks[height] = block
        return block

    def fork(self, from_height: int, length: int) -> list[Block]:
        """从 from_height（含）之后制造分叉：废弃其后的主链区块，
        基于 from_height 处的区块重新生成 length 个新区块并返回它们。"""
        if from_height < 0 or from_height > self.tip_height:
            raise ValueError(f"非法分叉点: {from_height}")
        for h in [h for h in self._blocks if h > from_height]:
            del self._blocks[h]
        self._fork_salt += 1  # 保证分叉区块哈希与原链不同
        new_blocks = []
        for _ in range(length):
            new_blocks.append(self.append_block())
        return new_blocks

    def get_block(self, height: int) -> Block | None:
        return self._blocks.get(height)

    @property
    def tip_height(self) -> int:
        return max(self._blocks, default=-1)

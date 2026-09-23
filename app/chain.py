"""本地链数据模拟。

用内存中的确定性链来模拟区块 / 交易的追加、分叉与（由调用方控制的）
重复投递、乱序投递。不引入真实节点或网络；哈希由种子确定性派生，
同一脚本每次运行结果一致，便于断言与复现。
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator

from app.models import GENESIS_PARENT, Block, Transaction


def _h(seed: str) -> str:
    """确定性伪哈希（仅用于本地模拟，不做安全承诺）。"""
    return "0x" + hashlib.sha256(seed.encode()).hexdigest()


class LocalChain:
    """一条可追加、可分叉的本地模拟链。

    内部用 blocks 列表表示当前规范链，下标即高度。
    """

    def __init__(self, tag: str = "chain") -> None:
        self._tag = tag
        self._blocks: list[Block] = []
        self._fork_seq = 0

    @property
    def height(self) -> int:
        """当前链尖高度。"""
        return len(self._blocks) - 1

    @property
    def tag(self) -> str:
        return self._tag

    def _make_block(
        self, height: int, parent: str, seed: str, sender: str, payload: str
    ) -> Block:
        block_hash = _h(f"{self._tag}|block|{seed}|h={height}|p={parent}")
        tx_hash = _h(f"{self._tag}|tx|{block_hash}")
        tx = Transaction(
            tx_hash=tx_hash,
            block_height=height,
            block_hash=block_hash,
            sender=sender,
            payload=payload,
        )
        return Block(height=height, hash=block_hash, parent_hash=parent, transactions=(tx,))

    def get_block(self, height: int) -> Block:
        """取某高度的当前规范块。"""
        return self._blocks[height]

    def produce(self, sender: str = "alice", payload: str = "") -> Block:
        """在链尖追加一个新区块。"""
        height = len(self._blocks)
        parent = self._blocks[-1].hash if self._blocks else GENESIS_PARENT
        block = self._make_block(
            height, parent, seed=f"tip#{height}", sender=sender, payload=payload
        )
        self._blocks.append(block)
        return block

    def produce_fork(
        self, fork_at: int, sender: str = "bob", payload: str = "fork"
    ) -> list[Block]:
        """从 fork_at（基点，保留）之后分叉，重连到不短于原链尖的高度。

        分叉基点 fork_at 处的块保持不变；fork_at+1..原链尖 全部替换成新块，
        因此返回块的 parent_hash 链最终落在 fork_at 的块哈希上。
        返回分叉上新产生的块（高度 fork_at+1 .. 新链尖）。
        """
        if not 0 <= fork_at <= self.height:
            raise ValueError(f"fork_at={fork_at} 超出链范围 0..{self.height}")

        old_tip = self.height
        self._fork_seq += 1
        fork_id = self._fork_seq
        base = self._blocks[fork_at]

        new_tail: list[Block] = []
        parent = base.hash
        for h in range(fork_at + 1, old_tip + 1):
            block = self._make_block(
                h,
                parent,
                seed=f"fork#{fork_id}@{h}",
                sender=sender,
                payload=f"{payload}:{h}",
            )
            new_tail.append(block)
            parent = block.hash

        self._blocks = self._blocks[: fork_at + 1] + new_tail
        return new_tail

    def iter_blocks(self, start: int = 0, stop: int | None = None) -> Iterator[Block]:
        """按高度迭代规范块 [start, stop)，用于 pull 式同步与重复 / 乱序投递模拟。"""
        end = len(self._blocks) if stop is None else min(stop, len(self._blocks))
        for i in range(start, end):
            yield self._blocks[i]

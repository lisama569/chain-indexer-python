"""数据模型：区块、交易、索引位点。"""
from __future__ import annotations

from dataclasses import dataclass, field


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

"""核心领域模型：区块、交易与索引结果。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class Transaction:
    """一笔交易。tx_hash 为业务主键。"""

    tx_hash: str
    block_height: int
    block_hash: str
    sender: str
    payload: str = ""


@dataclass(frozen=True, slots=True)
class Block:
    """一个区块。

    height 从 0 起；height=0 的 parent_hash 约定为 GENESIS_PARENT。
    链上的追加 / 分叉完全由 (height, hash, parent_hash) 的关系描述。
    """

    height: int
    hash: str
    parent_hash: str
    transactions: tuple[Transaction, ...] = field(default_factory=tuple)


# 创世块父哈希哨兵
GENESIS_PARENT = "0x0000000000000000000000000000000000000000000000000000000000000000"


# 处理结果动作码（写入 / 拒绝），供日志流水与测试断言使用
ACTION_ACCEPTED = "accepted"
ACTION_DUPLICATE = "duplicate"
ACTION_REORG = "reorg"
ACTION_REJECTED = "rejected"
ACTION_ROLLBACK = "rollback"
ACTION_RESTART = "restart"


@dataclass(frozen=True, slots=True)
class IngestResult:
    """一次区块投递的处理结果。"""

    action: str
    height: int
    block_hash: str
    rolled_back: tuple[int, ...] = ()
    reason: str | None = None

    def as_dict(self) -> dict:
        return {
            "action": self.action,
            "height": self.height,
            "block_hash": self.block_hash,
            "rolled_back": list(self.rolled_back),
            "reason": self.reason,
        }

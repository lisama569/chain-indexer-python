"""本地区块索引与查询服务。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError
from chain_indexer.indexer import Indexer
from chain_indexer.models import Block, DecisionRecord, Plan, Transaction
from chain_indexer.store import IndexStore

DB_PATH = os.environ.get("INDEXER_DB", "indexer.db")

chain = LocalChain()
store = IndexStore(DB_PATH)
indexer = Indexer(store, chain)

app = FastAPI(title="chain-indexer")


class TransactionIn(BaseModel):
    txid: str
    sender: str
    recipient: str
    amount: int


class BlockIn(BaseModel):
    height: int
    block_hash: str
    parent_hash: str
    transactions: list[TransactionIn] = []


def _block_out(block: Block) -> dict:
    return {
        "height": block.height,
        "block_hash": block.block_hash,
        "parent_hash": block.parent_hash,
        "transactions": [tx.__dict__ for tx in block.transactions],
    }


@app.get("/checkpoint")
def get_checkpoint() -> dict:
    cp = store.get_checkpoint()
    if cp is None:
        return {"height": None, "block_hash": None}
    return {"height": cp.height, "block_hash": cp.block_hash}


@app.get("/blocks/{height}")
def get_block(height: int) -> dict:
    block = store.get_block_by_height(height)
    if block is None:
        raise HTTPException(status_code=404, detail="block not found")
    return _block_out(block)


@app.get("/blocks/by-hash/{block_hash}")
def get_block_by_hash(block_hash: str) -> dict:
    block = store.get_block_by_hash(block_hash)
    if block is None:
        raise HTTPException(status_code=404, detail="block not found")
    return _block_out(block)


@app.get("/transactions/{txid}")
def get_transaction(txid: str) -> dict:
    tx = store.get_transaction(txid)
    if tx is None:
        raise HTTPException(status_code=404, detail="transaction not found")
    return tx.__dict__


@app.post("/ingest")
def ingest(block: BlockIn) -> dict:
    b = Block(
        height=block.height,
        block_hash=block.block_hash,
        parent_hash=block.parent_hash,
        transactions=tuple(Transaction(**tx.model_dump()) for tx in block.transactions),
    )
    try:
        result = indexer.ingest_block(b)
    except RejectError as e:
        raise HTTPException(
            status_code=409, detail={"reason": e.reason.value, "detail": e.detail}
        )
    return {"result": result}


@app.post("/sync/{target_height}")
def sync(target_height: int) -> dict:
    try:
        height = indexer.sync_to(target_height)
    except RejectError as e:
        raise HTTPException(
            status_code=409, detail={"reason": e.reason.value, "detail": e.detail}
        )
    return {"height": height}


# ---- 计划序列化 ----
def _cp_out(cp) -> dict | None:
    if cp is None:
        return None
    return {"height": cp.height, "block_hash": cp.block_hash}


def _plan_out(plan: Plan) -> dict:
    return {
        "kind": plan.kind.value,
        "target": plan.target,
        "status": plan.status.value,
        "before": _cp_out(plan.before),
        "after": _cp_out(plan.after),
        "applied": list(plan.applied),
        "duplicates": list(plan.duplicates),
        "rolled_back": list(plan.rolled_back),
        "reject_reason": plan.reason.value if plan.reason else None,
        "detail": plan.detail,
    }


def _blocks_in(blocks: list[BlockIn]) -> list[Block]:
    return [
        Block(
            height=b.height,
            block_hash=b.block_hash,
            parent_hash=b.parent_hash,
            transactions=tuple(
                Transaction(**tx.model_dump()) for tx in b.transactions
            ),
        )
        for b in blocks
    ]


def _reject_http(e: RejectError) -> HTTPException:
    return HTTPException(
        status_code=409, detail={"reason": e.reason.value, "detail": e.detail}
    )


# ---- 预演（只读，返回执行计划，绝不改数据）----
@app.post("/plan/ingest")
def plan_ingest(block: BlockIn) -> dict:
    (b,) = _blocks_in([block])
    return _plan_out(indexer.plan_ingest(b))


@app.post("/plan/sync/{target_height}")
def plan_sync(target_height: int) -> dict:
    return _plan_out(indexer.plan_sync(target_height))


@app.post("/plan/ingest-batch")
def plan_ingest_batch(blocks: list[BlockIn]) -> dict:
    return _plan_out(indexer.plan_batch_ingest(_blocks_in(blocks)))


@app.post("/plan/sync-batch")
def plan_sync_batch(targets: list[int]) -> dict:
    return _plan_out(indexer.plan_batch_sync(targets))


# ---- 批量执行（要么全成，要么全不动）----
@app.post("/ingest-batch")
def ingest_batch(blocks: list[BlockIn]) -> dict:
    try:
        plan = indexer.ingest_blocks(_blocks_in(blocks))
    except RejectError as e:
        raise _reject_http(e)
    return _plan_out(plan)


@app.post("/sync-batch")
def sync_batch(targets: list[int]) -> dict:
    try:
        plan = indexer.sync_batch(targets)
    except RejectError as e:
        raise _reject_http(e)
    return _plan_out(plan)


# ---- 决策流水 ----
def _decision_out(rec: DecisionRecord) -> dict:
    return {
        "seq": rec.seq,
        "mode": rec.mode.value,
        "kind": rec.kind.value,
        "target": rec.target,
        "status": rec.status.value,
        "before": (
            {"height": rec.before_height, "block_hash": rec.before_hash}
            if rec.before_height is not None
            else None
        ),
        "after": (
            {"height": rec.after_height, "block_hash": rec.after_hash}
            if rec.after_height is not None
            else None
        ),
        "applied": list(rec.applied),
        "duplicates": list(rec.duplicates),
        "rolled_back": list(rec.rolled_back),
        "reject_reason": rec.reason.value if rec.reason else None,
        "detail": rec.detail,
    }


@app.get("/decisions")
def list_decisions(limit: int = 100) -> dict:
    return {"decisions": [_decision_out(r) for r in indexer.journal.list_all(limit)]}


@app.get("/decisions/{seq}")
def get_decision(seq: int) -> dict:
    rec = indexer.journal.get(seq)
    if rec is None:
        raise HTTPException(status_code=404, detail="decision not found")
    return _decision_out(rec)


@app.post("/demo/append")
def demo_append() -> dict:
    """本地链模拟：追加一个区块并返回。"""
    return _block_out(chain.append_block())


@app.post("/demo/fork")
def demo_fork(from_height: int, length: int) -> dict:
    """本地链模拟：制造分叉并返回新分支区块。"""
    return {"blocks": [_block_out(b) for b in chain.fork(from_height, length)]}

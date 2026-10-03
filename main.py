"""本地区块索引与查询服务。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError
from chain_indexer.indexer import Indexer
from chain_indexer.models import Block, Transaction
from chain_indexer.plan import (
    Decision,
    IngestOp,
    SyncOp,
)
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


class OpIn(BaseModel):
    """批量操作单元：op='ingest' 带 block；op='sync' 带 target_height。"""

    op: str
    block: BlockIn | None = None
    target_height: int | None = None


def _block_out(block: Block) -> dict:
    return {
        "height": block.height,
        "block_hash": block.block_hash,
        "parent_hash": block.parent_hash,
        "transactions": [tx.__dict__ for tx in block.transactions],
    }


def _to_block(block: BlockIn) -> Block:
    return Block(
        height=block.height,
        block_hash=block.block_hash,
        parent_hash=block.parent_hash,
        transactions=tuple(
            Transaction(**tx.model_dump()) for tx in block.transactions
        ),
    )


def _checkpoint_out(cp) -> dict | None:
    if cp is None:
        return None
    return {"height": cp.height, "block_hash": cp.block_hash}


def _decision_out(d: Decision) -> dict:
    """决策计划 / 流水行。预演或执行的结论结构完全一致。"""
    return {
        "id": d.id,
        "created_at": d.created_at,
        "mode": d.mode,          # plan | execute
        "kind": d.kind,          # ingest | sync | batch
        "status": d.status,      # accepted | rejected
        "request": d.request_json,
        "checkpoint_before": _checkpoint_out(d.cp_before),
        "checkpoint_after": _checkpoint_out(d.cp_after),
        "applied_heights": list(d.applied),
        "duplicate_heights": list(d.duplicates),
        "rolled_back_heights": list(d.rolled_back),
        "outcomes": [
            {
                "index": o.index,
                "op": o.kind,
                "status": o.status,  # accepted | rejected | aborted
                "result": o.result,
                "reject_reason": o.reject_reason,
                "reject_detail": o.reject_detail,
            }
            for o in d.outcomes
        ],
        "reject_reason": d.reject_reason.value if d.reject_reason else None,
        "reject_detail": d.reject_detail,
    }


def _to_ops(ops: list[OpIn]) -> list[IngestOp | SyncOp]:
    parsed: list[IngestOp | SyncOp] = []
    for item in ops:
        if item.op == "ingest":
            if item.block is None:
                raise HTTPException(status_code=400, detail="ingest 操作缺少 block")
            parsed.append(IngestOp(block=_to_block(item.block)))
        elif item.op == "sync":
            if item.target_height is None:
                raise HTTPException(status_code=400, detail="sync 操作缺少 target_height")
            parsed.append(SyncOp(target_height=item.target_height))
        else:
            raise HTTPException(
                status_code=400, detail=f"未知操作类型: {item.op!r}（ingest|sync）"
            )
    return parsed


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
    b = _to_block(block)
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


# ---- 预演：只读，永远返回计划（含被拒绝及原因），不会 409 ----

@app.post("/plan/ingest")
def plan_ingest(block: BlockIn) -> dict:
    return _decision_out(indexer.plan_ingest(_to_block(block)))


@app.post("/plan/sync/{target_height}")
def plan_sync(target_height: int) -> dict:
    return _decision_out(indexer.plan_sync(target_height))


@app.post("/plan/batch")
def plan_batch(ops: list[OpIn]) -> dict:
    return _decision_out(indexer.plan_batch(_to_ops(ops)))


# ---- 批量执行：整批原子，任一被拒绝则整批不动 ----

@app.post("/batch")
def batch(ops: list[OpIn]) -> dict:
    try:
        decision = indexer.execute_batch(_to_ops(ops))
    except RejectError as e:
        raise HTTPException(
            status_code=409, detail={"reason": e.reason.value, "detail": e.detail}
        )
    return _decision_out(decision)


# ---- 决策流水：持久化，重启后可查 ----

@app.get("/decisions")
def list_decisions(
    limit: int | None = None,
    mode: str | None = None,
    kind: str | None = None,
    status: str | None = None,
) -> dict:
    rows = indexer.list_decisions(limit=limit, mode=mode, kind=kind, status=status)
    return {"decisions": [_decision_out(d) for d in rows]}


@app.get("/decisions/{decision_id}")
def get_decision(decision_id: int) -> dict:
    d = indexer.get_decision(decision_id)
    if d is None:
        raise HTTPException(status_code=404, detail="decision not found")
    return _decision_out(d)


@app.post("/demo/append")
def demo_append() -> dict:
    """本地链模拟：追加一个区块并返回。"""
    return _block_out(chain.append_block())


@app.post("/demo/fork")
def demo_fork(from_height: int, length: int) -> dict:
    """本地链模拟：制造分叉并返回新分支区块。"""
    return {"blocks": [_block_out(b) for b in chain.fork(from_height, length)]}

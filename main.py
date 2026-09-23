"""本地区块索引与查询服务。"""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from chain_indexer.chain import LocalChain
from chain_indexer.errors import RejectError
from chain_indexer.indexer import Indexer
from chain_indexer.models import Block, Transaction
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


@app.post("/demo/append")
def demo_append() -> dict:
    """本地链模拟：追加一个区块并返回。"""
    return _block_out(chain.append_block())


@app.post("/demo/fork")
def demo_fork(from_height: int, length: int) -> dict:
    """本地链模拟：制造分叉并返回新分支区块。"""
    return {"blocks": [_block_out(b) for b in chain.fork(from_height, length)]}

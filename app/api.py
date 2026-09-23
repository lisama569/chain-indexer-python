"""FastAPI 路由：区块投递、重组、位点恢复 / 追赶、区块与交易查询。"""

from __future__ import annotations

from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.responses import JSONResponse

from app.errors import IndexerError
from app.indexer import ChainIndexer
from app.models import GENESIS_PARENT, Block, Transaction

# 错误码 -> HTTP 状态：
# 重复 / 乱序 / 覆盖 / 父链不符 = 409（与当前索引状态冲突）
# 重组过深 / 位点回退 / 跨度过大      = 422（请求语义不允许）
# 存储损坏                           = 500
_ERROR_STATUS = {
    "duplicate_delivery": 409,
    "confirmed_data_overwrite": 409,
    "parent_mismatch": 409,
    "height_gap": 409,
    "stale_height": 409,
    "reorg_too_deep": 422,
    "progress_rollback": 422,
    "catchup_too_far": 422,
    "store_corrupted": 500,
}


def _tx_from_dict(data: dict, height: int, block_hash: str) -> Transaction:
    return Transaction(
        tx_hash=data["tx_hash"],
        block_height=height,
        block_hash=block_hash,
        sender=data.get("sender", ""),
        payload=data.get("payload", ""),
    )


def _block_from_payload(data: dict) -> Block:
    """把请求 JSON 转成 Block，并校验交易归属字段的自洽性。"""
    height = int(data["height"])
    block_hash = data["hash"]
    txs = tuple(_tx_from_dict(t, height, block_hash) for t in data.get("transactions", []))
    return Block(
        height=height,
        hash=block_hash,
        parent_hash=data.get("parent_hash", GENESIS_PARENT),
        transactions=txs,
    )


def _block_dict(block: Block) -> dict:
    return {
        "height": block.height,
        "hash": block.hash,
        "parent_hash": block.parent_hash,
        "confirmed": False,  # 由 get_block 路由按确认边界覆盖
        "transactions": [
            {
                "tx_hash": t.tx_hash,
                "block_height": t.block_height,
                "block_hash": t.block_hash,
                "sender": t.sender,
                "payload": t.payload,
            }
            for t in block.transactions
        ],
    }


def create_app(indexer: ChainIndexer) -> FastAPI:
    app = FastAPI(title="chain-indexer-python", version="0.1")

    @app.exception_handler(IndexerError)
    async def _indexer_error_handler(request, exc: IndexerError):
        status = _ERROR_STATUS.get(exc.reason, 400)
        return JSONResponse(
            status_code=status,
            content={
                "error": exc.reason,
                "message": str(exc),
                "height": exc.height,
            },
        )

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/progress")
    def progress():
        return {
            "cursor": indexer.cursor,
            "confirmed_watermark": indexer.confirmed_watermark,
            "confirmation_depth": indexer.confirmation_depth,
        }

    @app.post("/blocks", status_code=200)
    def ingest_block(payload: dict):
        """投递单个区块（追加 / 重复 / 单块分叉）。"""
        block = _block_from_payload(payload)
        result = indexer.ingest_block(block)
        return result.as_dict()

    @app.post("/reorg", status_code=200)
    def reorg(payload: dict):
        """提交完整分叉路径进行原子重组。body: {"path": [block, ...]}"""
        path = [_block_from_payload(b) for b in payload["path"]]
        result = indexer.reindex_path(path)
        return result.as_dict()

    @app.post("/resume", status_code=200)
    def resume():
        """从持久化位点恢复（启动时已自动执行一次，可手动再校验）。"""
        cursor = indexer.resume()
        return {"cursor": cursor}

    @app.post("/catchup", status_code=200)
    def catchup(payload: dict):
        """位点对齐：body {"target_height": n}。回退 / 跨度过大将被拒绝。"""
        target = int(payload["target_height"])
        effective = indexer.catchup_to(target)
        return {"requested": target, "cursor": effective}

    @app.get("/blocks/{height}")
    def get_block(height: int):
        block = indexer.get_block(height)
        if block is None:
            raise HTTPException(status_code=404, detail=f"height {height} 未索引")
        data = _block_dict(block)
        wm = indexer.confirmed_watermark
        data["confirmed"] = wm is not None and block.height <= wm
        return data

    @app.get("/transactions/{tx_hash}")
    def get_transaction(tx_hash: str):
        tx = indexer.get_transaction(tx_hash)
        if tx is None:
            raise HTTPException(status_code=404, detail=f"tx {tx_hash} 未索引")
        wm = indexer.confirmed_watermark
        return {
            "tx_hash": tx.tx_hash,
            "block_height": tx.block_height,
            "block_hash": tx.block_hash,
            "sender": tx.sender,
            "payload": tx.payload,
            "confirmed": wm is not None and tx.block_height <= wm,
        }

    return app

"""服务入口。

存储路径由环境变量 INDEXER_DB 指定，默认 data/indexer.db。
启动时自动 resume()：从持久化位点恢复并校验完整性。
"""

from __future__ import annotations

import os

from app.api import create_app
from app.events import configure_logging
from app.indexer import ChainIndexer
from app.storage import BlockStore

configure_logging(os.environ.get("INDEXER_LOG_LEVEL", "INFO"))

_db_path = os.environ.get("INDEXER_DB", "data/indexer.db")
if _db_path != ":memory:" and os.path.dirname(_db_path):
    os.makedirs(os.path.dirname(_db_path), exist_ok=True)

_store = BlockStore(_db_path)
_indexer = ChainIndexer(_store)
_indexer.resume()

app = create_app(_indexer)

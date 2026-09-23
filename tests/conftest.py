"""pytest 公共夹具与日志配置。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from app.chain import LocalChain
from app.events import configure_logging
from app.indexer import ChainIndexer
from app.storage import BlockStore


def pytest_configure(config):
    # 单测日志：打开 DEBUG，打印处理位点与事件流水，形如
    # [cursor=3] accepted height=4 hash=0x...
    logger = configure_logging("DEBUG")
    logger.setLevel(logging.DEBUG)


@pytest.fixture
def store():
    s = BlockStore(":memory:")
    yield s
    s.close()


@pytest.fixture
def indexer(store):
    # 较小的确认深度 / 追赶上限，便于在有限区块数内覆盖确认语义
    return ChainIndexer(store, confirmation_depth=3, max_catchup_span=10)


@pytest.fixture
def client(indexer):
    from fastapi.testclient import TestClient

    from app.api import create_app

    return TestClient(create_app(indexer))


@pytest.fixture
def chain():
    return LocalChain("test")


@pytest.fixture
def tmp_db_path(tmp_path: Path) -> Path:
    return tmp_path / "indexer.db"


@pytest.fixture
def file_store(tmp_db_path):
    s = BlockStore(str(tmp_db_path))
    yield s
    s.close()

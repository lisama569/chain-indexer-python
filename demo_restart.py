"""本地端到端演示：追加、重复投递、分叉重组、进程重启恢复。

运行：uv run python demo_restart.py
过程中会打印处理位点（[cursor=n]）与事件流水。
"""

from __future__ import annotations

import os
import tempfile

from app.chain import LocalChain
from app.events import configure_logging
from app.indexer import ChainIndexer
from app.storage import BlockStore


def main() -> None:
    configure_logging("DEBUG")
    tmp = tempfile.mkdtemp(prefix="chain-indexer-demo-")
    db = os.path.join(tmp, "indexer.db")

    chain = LocalChain("demo")

    # ---- 进程 1：索引 0..7，重复投递，再分叉重组 ----
    print("=== 进程 1 启动 ===")
    store = BlockStore(db)
    ix = ChainIndexer(store, confirmation_depth=3, max_catchup_span=10)
    ix.resume()

    blocks = [chain.produce(payload=f"p{i}") for i in range(8)]
    for b in blocks:
        ix.ingest_block(b)

    print("--- 重复投递 h=3（应被拒绝 duplicate_delivery）---")
    try:
        ix.ingest_block(blocks[3])
    except Exception as e:
        print(f"拒绝：{type(e).__name__}: {getattr(e, 'reason', '')}")

    print("--- 未确认区间 h=6..7 分叉重组 ---")
    forked = chain.produce_fork(fork_at=5)
    result = ix.reindex_path(forked)
    print(f"重组结果：{result.as_dict()}")
    store.close()

    # ---- 进程 2：重新打开同一数据库，位点与结论必须原样恢复 ----
    print("=== 进程 2 启动（重启恢复）===")
    store2 = BlockStore(db)
    ix2 = ChainIndexer(store2, confirmation_depth=3, max_catchup_span=10)
    cursor = ix2.resume()
    assert cursor == 7
    assert ix2.get_block(6).hash == forked[0].hash
    print(f"恢复位点={cursor}，h=6 哈希={ix2.get_block(6).hash[:12]}，重组结论保持")

    # 重启后继续推进
    b8 = chain.produce(payload="after-restart")
    r = ix2.ingest_block(b8)
    print(f"重启后追加：{r.as_dict()}")
    store2.close()
    print(f"数据库文件：{db}")


if __name__ == "__main__":
    main()

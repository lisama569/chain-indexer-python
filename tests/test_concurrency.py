"""并发投递：线程池下位点推进必须确定，每个高度恰好入库一次。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from app.errors import DuplicateDeliveryError


def test_concurrent_duplicate_delivery_of_each_height(indexer, chain):
    """同一高度的多份副本并发到达：恰好一份 accepted，其余 duplicate。"""
    blocks = [chain.produce(payload=f"p{i}") for i in range(12)]

    for b in blocks:
        copies = [b] * 6
        accepted = 0
        duplicates = 0
        with ThreadPoolExecutor(max_workers=6) as pool:
            futs = [pool.submit(indexer.ingest_block, c) for c in copies]
            for f in futs:
                try:
                    r = f.result()
                    if r.action == "accepted":
                        accepted += 1
                except DuplicateDeliveryError:
                    duplicates += 1
        assert accepted == 1
        assert duplicates == 5
        assert indexer.cursor == b.height

    # 最终链与顺序构造完全一致
    for h, b in enumerate(blocks):
        assert indexer.get_block(h).hash == b.hash

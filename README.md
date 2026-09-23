# chain-indexer-python

本地区块索引与查询服务：用本地模拟链演示区块/交易的追加、分叉与重复投递，
保证重组、重复拉取与进程重启后的索引结果确定且可解释。

## 结构

- `chain_indexer/models.py` — `Block` / `Transaction` / `Checkpoint` 数据模型
- `chain_indexer/errors.py` — `RejectReason` 可区分的拒绝原因枚举
- `chain_indexer/store.py` — SQLite 持久化（区块、交易、位点），只保存当前主链视图
- `chain_indexer/chain.py` — `LocalChain` 本地链模拟器（追加 / 分叉）
- `chain_indexer/indexer.py` — `Indexer` 索引器（推进、去重、重组回滚、追赶）
- `main.py` — FastAPI 查询与投递接口

## 索引协议

索引按高度严格推进，位点 `Checkpoint(height, block_hash)` 随每个区块原子持久化。

单块投递 `Indexer.ingest_block(block)` 的结果：

| 情形 | 结果 |
| --- | --- |
| 高度 == 位点+1 且父哈希衔接链尖 | `applied`：落库并推进位点 |
| 高度 ≤ 位点且哈希与已存一致 | `duplicate`：幂等忽略，不重复入库 |
| 高度 ≤ 位点、哈希不同但父哈希可衔接 | `reorg_applied`：回滚后作为新分支落库 |
| 高度 ≤ 位点、哈希不同且父哈希无法衔接 | 拒绝 `conflicting_block`，不部分应用 |
| 高度 > 位点+1 | 拒绝 `height_gap`（须走追赶接口） |
| 父哈希与链尖不符 | 拒绝 `orphan_block` |

追赶 `Indexer.sync_to(target)` 的拒绝原因：

| 情形 | 拒绝原因 |
| --- | --- |
| 目标高度 < 已确认位点 | `checkpoint_regression`（位点回退） |
| 目标高度超出本地链尖 | `height_gap` |
| 追赶跨度 > `max_catchup_span` | `catchup_too_large` |
| 重组深度 > `max_reorg_depth` | `reorg_too_deep`，且不做任何部分应用 |

## 重组回滚规则

- 每次 `sync_to` 先校验已确认位点的哈希是否仍与本地链一致；不一致则自位点向下
  找到双方最后一个一致高度（分叉点）。
- 回滚范围 = 分叉点+1 至旧链尖，整段物理删除（区块与其交易同事务移除），
  再从分叉点+1 重新索引到目标高度。查询层因此永远只能看到当前一致的主链视图。
- 深度超过 `max_reorg_depth` 的重组在执行任何删除前整体拒绝，已确认历史保持不变。
- 回滚与重索引事件均打印日志：`event=rollback fork_point=.. depth=.. removed=[..]`、
  `event=applied height=.. hash=..`。

## 恢复语义

- 位点与数据同库存储（SQLite），每个区块落库后即更新位点，崩溃最多丢失
  最后一个进行中的区块。
- 重启后用同一数据库文件重建 `IndexStore` / `Indexer`，位点自动恢复，
  继续 `sync_to` 只索引位点之后的高度，已持久化的历史结论不被重写。
- 位点回退（`checkpoint_regression`）与跨度过大的追赶（`catchup_too_large`）
  被明确拒绝并说明原因。

## HTTP 接口

```bash
uv run uvicorn main:app --reload
```

- `GET  /checkpoint` — 当前索引位点
- `GET  /blocks/{height}` / `GET /blocks/by-hash/{hash}` — 区块查询
- `GET  /transactions/{txid}` — 交易查询
- `POST /ingest` — 单块投递（409 时返回 `{"reason": ..., "detail": ...}`）
- `POST /sync/{target_height}` — 追赶到目标高度
- `POST /demo/append`、`POST /demo/fork?from_height=&length=` — 本地链模拟

数据库路径通过环境变量 `INDEXER_DB` 指定（默认 `indexer.db`）。

## 本地验证

```bash
uv run pytest tests/ -q
```

测试覆盖：重复投递幂等、乱序/缺口/孤儿块拒绝、冲突块拒绝、分叉重组回滚、
单块投递触发重组、超深重组拒绝且零部分应用、重启后从位点恢复、
位点回退与超大跨度追赶拒绝。测试日志直接打印处理位点与事件流水
（`event=applied/duplicate/rollback ...`）。

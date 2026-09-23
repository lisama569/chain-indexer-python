# chain-indexer-python

本地区块索引与数据查询服务。用内存中确定性的本地链模拟区块 / 交易的**追加、
分叉、重复投递与乱序到达**，由 SQLite 持久化区块、交易与索引位点，FastAPI
对外提供投递、重组、恢复与查询接口。

目标：在重复拉取、链重组（reorg）、进程重启三类干扰下，索引结果保持
**确定、可解释、最终一致**——非法输入一律被拒绝并给出可区分的原因码，
绝不静默丢数据，也不做半套写入。

## 目录结构

```
app/
  models.py    # Block / Transaction / IngestResult 与动作码
  errors.py    # 全部拒绝原因（独立异常类型 + 稳定 reason 码）
  events.py    # 日志：[cursor=n] 事件流水
  storage.py   # SQLite 存储：区块 / 交易 / 位点，事务与完整性校验
  indexer.py   # ChainIndexer：投递、去重、分叉重组、恢复、追赶
  chain.py     # LocalChain：本地链模拟（追加 / produce_fork）
  api.py       # FastAPI 路由与错误码 -> HTTP 状态映射
main.py        # 服务入口（启动自动 resume）
demo_restart.py# 端到端演示脚本
tests/         # 重复投递 / 分叉重组 / 重启恢复 场景测试
```

## 快速开始

```bash
uv sync --group dev          # 安装依赖
uv run pytest                # 全部单测（日志打开，可见位点与事件流水）
uv run python demo_restart.py# 追加 -> 重复投递 -> 重组 -> 重启恢复 演示

# 启动 HTTP 服务
uv run uvicorn main:app --reload
# 默认数据库 data/indexer.db，可用环境变量覆盖：
#   INDEXER_DB=/tmp/idx.db
#   INDEXER_LOG_LEVEL=DEBUG
```

## 数据模型

- `Block(height, hash, parent_hash, transactions)`，高度从 0 起；
  创世块 `parent_hash` 为全零哨兵 `GENESIS_PARENT`。
- `Transaction(tx_hash, block_height, block_hash, sender, payload)`，
  `tx_hash` 全局唯一。
- **确认深度** `confirmation_depth`（默认 6，测试中取 3）：
  当 `cursor - height >= confirmation_depth` 时，该高度视为**已确认**。
  确认水位线 `confirmed_watermark = cursor - confirmation_depth`（链不够长时为 `null`）。

## 索引协议（区块投递规则）

投递入口：`POST /blocks`，或代码中 `ChainIndexer.ingest_block(block)`。

| 情形 | 处理 | 动作 / 错误码 | HTTP |
|---|---|---|---|
| 高度 = cursor+1，parent 链连续 | 写入并推进位点 | `accepted` | 200 |
| 同高度、**同哈希**再次到达 | 幂等拒绝，数据不动 | `duplicate_delivery` | 409 |
| 高度 > cursor+1（乱序超前，有缺口） | 拒绝 | `height_gap` | 409 |
| 顺序块的 parent_hash 与链上不符 | 拒绝 | `parent_mismatch` | 409 |
| 同高度**不同哈希**，但高度已确认 | 拒绝覆盖 | `confirmed_data_overwrite` | 409 |
| 同高度不同哈希、未确认，且 parent 能接上库中 h-1 | 回滚该高度后重建 | `reorg` | 200 |
| 同高度不同哈希，但 parent 接不上（陈旧分叉块） | 拒绝，要求提交完整分叉路径 | `stale_height` | 409 |
| 空库首块不是高度 0 / 创世 parent 非法 | 拒绝 | `height_gap` / `parent_mismatch` | 409 |

数据库另有三重唯一约束（`blocks.height` 主键、`blocks.hash` 唯一、
`transactions.tx_hash` 主键）作为兜底，即使绕过协议也无法重复入库或静默覆盖。

## 链重组与回滚规则

完整分叉路径入口：`POST /reorg`，body `{"path": [block, ...]}`；
代码中 `ChainIndexer.reindex_path(path)`。

`path` 是**分叉基点之后第一块到新链尖**的有序块序列，必须高度连续、
`parent_hash` 依次相连、首块 parent 与库中基点块一致。

处理流程（全部在**单个 SQLite 事务**内）：

1. 校验路径自身：空路径、高度不连续、parent 断链、基点不存在、
   首块与基点不符 —— 分别拒绝（`parent_mismatch` / `stale_height`）；
2. 若起点已索引且首块哈希相同，按 `duplicate_delivery` 拒绝，不重复应用；
3. **确认深度检查先行**：`start_height <= confirmed_watermark` 时，基点已在
   确认区间，抛 `reorg_too_deep`（422），此时尚未删除任何数据，
   不会出现部分应用；
4. 通过后：删除所有 `height >= start_height` 的区块与交易（回滚范围在
   响应 `rolled_back` 中按**降序**返回），写入分叉路径，位点更新为新链尖。

自洽性保证：

- 回滚范围外（基点及之前）一字节不动；
- 旧分叉的交易随区块一起删除，查询立即不可见；新交易随新块立即可查；
- 新链可以比旧链**短**：位点随新链尖下降，之后仍可从新链尖顺序追加
  （位点下降只允许由一次合法重组造成，外部请求无法直接回退位点）；
- 回滚 + 重建 + 位点更新原子提交，进程崩溃只会停留在变更前或变更后。

## 位点持久化与恢复语义

- 每次写入 / 重组 / 位点对齐都随事务落盘到 `progress` 单行表；
- 启动（`main.py` import 时）自动调用 `resume()`，也可 `POST /resume`：
  1. `verify_integrity()`：校验位点 == 区块最高高度、高度从 0 连续、
     每笔交易归属的区块都存在；任一不满足抛 `store_corrupted`（500），
     拒绝在损坏状态上继续写入；
  2. 从持久化位点恢复内存 cursor，之后从 `cursor+1` 继续，历史结论不变；
- `POST /catchup`（`catchup_to(target)`）用于位点对齐：

| 情形 | 错误码 | HTTP |
|---|---|---|
| `target < cursor`（位点回退） | `progress_rollback` | 422 |
| `target - cursor > max_catchup_span`（默认 100，跨度过大） | `catchup_too_far` | 422 |
| target 超过实际数据末端 | 不报错，只对齐到数据末端，绝不凭空声明已索引 | 200 |

## HTTP 接口一览

| 方法 路径 | 说明 |
|---|---|
| `GET  /health` | 健康检查 |
| `GET  /progress` | `cursor` / `confirmed_watermark` / `confirmation_depth` |
| `POST /blocks` | 投递单块（追加 / 重复 / 单块分叉） |
| `POST /reorg` | 提交完整分叉路径，原子重组 |
| `POST /resume` | 从持久化位点恢复并做完整性校验 |
| `POST /catchup` | 位点对齐 `{"target_height": n}` |
| `GET  /blocks/{height}` | 查区块（含交易与 `confirmed` 标记），404 表示未索引 |
| `GET  /transactions/{tx_hash}` | 按哈希查交易，404 表示不存在（含被重组掉的旧交易） |

错误响应统一为：

```json
{"error": "duplicate_delivery", "message": "...", "height": 1}
```

## 日志与可解释性

所有处理都经 `chain_indexer` logger 打印位点与事件流水，单测以 DEBUG 开启：

```
[cursor=3] accepted height=4 hash=0x6014800a
[cursor=7] duplicate height=3 hash=0x4cb0909a reason=already_indexed
[cursor=7] reorg height=7 hash=0x758873ac rolled_back=[7, 6]
[cursor=7] resume 完成：从持久化位点恢复，确认边界=4
[cursor=4] catchup_to(10) 完成，实际位点=6（数据末端=6）
```

## 测试覆盖

`uv run pytest`（26 个用例）覆盖：

- `test_ingest_dedup.py`：顺序追加与查询、创世约束、重复投递幂等、
  乱序缺口、parent 不符、陈旧分叉块、确认高度不可覆盖、水位线边界；
- `test_reorg.py`：单块 / 多块 / 缩短型分叉的回滚范围与查询可见性、
  重组过深完全拒绝（无部分应用）、路径校验各错误码可区分、重组重复投递；
- `test_persistence.py`：重启从正确位点继续、重启后仍能识别重复与处理
  分叉且结论跨进程保持、位点回退与跨度过大拒绝、追赶只对齐真实数据、
  存储损坏在 resume 时被检出；
- `test_api.py`：上述语义经 HTTP 的状态码、错误码与响应体断言。

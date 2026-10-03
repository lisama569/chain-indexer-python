# chain-indexer-python

本地区块索引与查询服务：用本地模拟链演示区块/交易的追加、分叉与重复投递，
保证重组、重复拉取与进程重启后的索引结果确定且可解释。

## 结构

- `chain_indexer/models.py` — `Block` / `Transaction` / `Checkpoint` / `Plan` / `DecisionRecord` 数据模型
- `chain_indexer/errors.py` — `RejectReason` 可区分的拒绝原因枚举
- `chain_indexer/store.py` — SQLite 持久化（区块、交易、位点、决策流水），只保存当前主链视图；多步变更走单一事务
- `chain_indexer/chain.py` — `LocalChain` 本地链模拟器（追加 / 分叉）
- `chain_indexer/planner.py` — 只读执行计划引擎：在内存覆盖层上推演应用/跳过/回滚，绝不写库
- `chain_indexer/journal.py` — 持久化决策流水服务
- `chain_indexer/indexer.py` — `Indexer` 索引器（推进、去重、重组回滚、追赶、预演、批量）
- `main.py` — FastAPI 查询、投递、预演、批量与决策流水接口

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

## 先预演、再执行（dry run）

真正动数据之前，可以先拿到一份只读的执行计划 `Plan`：

| 接口 | 说明 |
| --- | --- |
| `Indexer.plan_sync(target)` | 预演追赶 |
| `Indexer.plan_ingest(block)` | 预演单块投递 |
| `Indexer.plan_batch_sync(targets)` | 预演多目标追赶 |
| `Indexer.plan_batch_ingest(blocks)` | 预演批量投递 |

计划字段：

- `status`：`accepted` / `rejected`；
- `before` / `after`：当前位点，与若执行后将到达的位点；
- `applied`：将新应用的高度；`duplicates`：内容一致、将被跳过的高度；
- `rolled_back`：将回滚的高度（被拒绝时也会填，表示"若放行本会回滚哪些高度"，
  因此像"既触发重组、目标又不可达"的请求，也能在计划里同时看出这两点）；
- `reason` / `detail`：被拒绝时的可分辨原因与说明。

语义保证：

1. **预演严格只读**：计划引擎只读取快照在内存覆盖层上推演，不写区块、交易、位点。
   预演跑完之后，索引数据与未跑时完全一致。预演产生的唯一持久化内容是一条
   `mode=dry_run` 的决策流水（用于审计），它不触碰任何索引数据。
2. **预演与执行结论一致**：执行与预演共用同一个计划引擎（单一事实来源），
   且执行时在提交锁内基于最新数据重新计算。预演说能做，执行必然成功且
   `applied`/`rolled_back`/最终位点与计划一致；预演说不行，执行必然以**同一种**
   `reason` 被拒绝。

## 批量与并发：原子性与幂等

- 批量接口 `ingest_blocks(blocks)` 与 `sync_batch(targets)` 一次请求覆盖一段高度
  或多个目标，按高度顺序处理。
- **要么全成、要么全不动**：一次请求的回滚、落库、位点推进放在同一个 SQLite 事务
  中提交。任何一个目标/区块最终被拒绝，整批都不会留下半成品——不会出现
  "旧数据删了但位点没退"或"位点变了却没有对应新数据"，位点、区块、交易始终自洽。
- 同批内同一高度出现不同哈希，整批以 `conflicting_block` 拒绝。
- **并发确定**：所有"计划 + 物化"由一把提交锁串行化，多个来源几乎同时投递/追赶时
  结果确定、可复现，不串数据、不重复入库。
- **失败重试幂等**：被拒绝后按同样目标重试，只要链数据没变，结论稳定；
  已成功的部分在重试时成为 `duplicates` 被跳过，既不重复入库也不漏。

## 决策流水与拒绝原因分类

每次预演和执行都会把决定追加到持久化表 `decisions`（只增不改），字段包括：

- `mode`：`dry_run` / `execute`；`kind`：`ingest` / `sync` / `batch_ingest` / `batch_sync`；
- 处理前/后位点，`applied` / `duplicates` / `rolled_back` 高度列表；
- 被拒绝时的 `reason` 与 `detail`。

通过 `Indexer.journal.list_all(limit)` / `journal.get(seq)`（或 HTTP
`GET /decisions`、`GET /decisions/{seq}`）查询；流水随数据库持久化，**进程重启后
仍可查**。可据此解释"某个高度现在为什么是这个哈希"（看回滚/应用记录）、
"某笔交易为什么查不到"（其所在高度被回滚）。

可分辨的拒绝原因（`RejectReason`，拒绝时不糊成一句"失败"）：

| reason | 含义 |
| --- | --- |
| `duplicate_delivery` | 重复投递：内容一致，幂等忽略 |
| `conflicting_block` | 同高度不同哈希且无法衔接（含同批冲突） |
| `stale_height` | 高度落后于已确认位点 |
| `height_gap` | 高度跳跃有缺口，或目标超出本地链尖，或链上缺块 |
| `orphan_block` | 父哈希未知，无法衔接 |
| `reorg_too_deep` | 重组深度超过 `max_reorg_depth` |
| `checkpoint_regression` | 位点回退请求被拒绝 |
| `catchup_too_large` | 追赶跨度超过 `max_catchup_span` |

## 兼容性

- 默认行为保持不变：`ingest_block()` 仍返回 `applied` / `duplicate` /
  `reorg_applied`，`sync_to()` 仍返回最终高度；原有 HTTP 接口路径、入参、
  409 响应体（`{"reason", "detail"}`）均不变。
- 存储层在原有表之外新增 `decisions` 表（`CREATE TABLE IF NOT EXISTS`），
  对已有数据库文件自动生效，不影响历史数据。
- 本轮新增能力全部以新方法/新路由暴露，不改变原有调用方式。

## HTTP 接口

```bash
uv run uvicorn main:app --reload
```

- `GET  /checkpoint` — 当前索引位点
- `GET  /blocks/{height}` / `GET /blocks/by-hash/{hash}` — 区块查询
- `GET  /transactions/{txid}` — 交易查询
- `POST /ingest` — 单块投递（409 时返回 `{"reason": ..., "detail": ...}`）
- `POST /sync/{target_height}` — 追赶到目标高度
- `POST /plan/ingest`、`POST /plan/sync/{target_height}` — 只读预演，返回计划
- `POST /plan/ingest-batch`（body 为区块数组）、`POST /plan/sync-batch`（body 为高度数组）— 批量预演
- `POST /ingest-batch`、`POST /sync-batch` — 批量执行（原子），返回执行后的计划
- `GET  /decisions?limit=`、`GET /decisions/{seq}` — 决策流水查询
- `POST /demo/append`、`POST /demo/fork?from_height=&length=` — 本地链模拟

数据库路径通过环境变量 `INDEXER_DB` 指定（默认 `indexer.db`）。

## 本地验证

```bash
uv run pytest tests/ -q
```

测试覆盖：

- 原有：重复投递幂等、乱序/缺口/孤儿块拒绝、冲突块拒绝、分叉重组回滚、
  单块投递触发重组、超深重组拒绝且零部分应用、重启后从位点恢复、
  位点回退与超大跨度追赶拒绝；
- 本轮：预演零写入且计划与执行范围一致、预演拒绝与执行拒绝同因、
  "既触发回滚又目标不可达"的请求被拒绝且零部分应用、批量中单个坏块导致整批不动、
  超深重组的批量请求全或无、同批冲突拒绝、失败后同目标重试结论稳定不重复、
  并发（含批量与单路混合）结果确定且数据自洽、决策流水在重启后仍可查、
  各类拒绝原因可分辨、流水正确记录重组与重复。

测试开启了实时日志（`log_cli`），可直接看到每一步处理到的位点与事件流水：
`event=dry_run ...`、`event=applied height=.. hash=..`、
`event=duplicate height=..`、`event=rollback fork_point=.. depth=.. removed=[..]`、
`event=rejected kind=.. reason=.. would_rollback=[..]`、`event=committed ..`。

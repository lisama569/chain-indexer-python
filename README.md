# chain-indexer-python

本地区块索引与查询服务：用本地模拟链演示区块/交易的追加、分叉与重复投递，
保证重组、重复拉取与进程重启后的索引结果确定且可解释；并支持
**先预演再执行、批量原子提交、并发安全与持久化决策流水**。

## 结构

- `chain_indexer/models.py` — `Block` / `Transaction` / `Checkpoint` 数据模型
- `chain_indexer/errors.py` — `RejectReason` 可区分的拒绝原因枚举
- `chain_indexer/store.py` — SQLite 持久化（区块、交易、位点、决策流水）与连接级事务
- `chain_indexer/chain.py` — `LocalChain` 本地链模拟器（追加 / 分叉）
- `chain_indexer/plan.py` — 操作（`IngestOp`/`SyncOp`）与决策（`Decision`/`OpOutcome`）类型
- `chain_indexer/indexer.py` — 索引器：单事务评估引擎、预演、执行、批量、流水
- `main.py` — FastAPI 查询、投递、预演、批量与流水接口

## 索引协议（原有行为，保持不变）

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
| 目标高度超出本地链尖 / 缺块 | `height_gap` |
| 追赶跨度 > `max_catchup_span` | `catchup_too_large` |
| 重组深度 > `max_reorg_depth` | `reorg_too_deep`，且不做任何部分应用 |

## 预演（dry-run）：先看计划，再动数据

- `plan_sync(target)` / `plan_ingest(block)` / `plan_batch(ops)`：在一个
  **最终回滚的数据库事务**里完整走一遍判定，因此严格只读——跑完之后位点、
  区块、交易与没跑过完全一致。唯一的新增写入是一条 `mode=plan` 的决策流水
  （流水本身就是预演结论的留痕，不属于索引数据）。
- 预演**永远返回 `Decision` 而不抛拒绝异常**：`status="accepted"` 时给出
  将推进到的位点（`cp_after`）、将应用的高度（`applied`）、将跳过的重复高度
  （`duplicates`）、将回滚的高度（`rolled_back`，降序）；`status="rejected"`
  时给出分类的 `reject_reason` 与 `reject_detail`。
- **被拒绝的请求同样给出完整执行计划**：有些追赶请求评估过程中会先触发重组
  （需要回滚一段已确认的链），随后才因目标不可达（如超出本地链尖
  `height_gap`、跨度超限 `catchup_too_large`）被拒绝。此时 `rolled_back`、
  `applied`、`duplicates` 以及逐项 `outcomes[*].rolled_back` 保留评估期算出的
  **候选结果**——即"若真执行会发生"的回滚与应用范围，不会因为最终被拒就吞掉；
  而 `cp_after` 仍等于 `cp_before`，因为实际位点没有变化。拒绝发生在回滚评估
  之前的请求（如位点回退 `checkpoint_regression`），候选回滚范围自然为空，
  口径一致。拒绝原因始终按 `RejectReason` 分类给出，不会糊成一句"失败"。
- 预演与执行共用同一套事务化评估代码（`execute_*` 只是把事务改为提交），
  因此结论严格一致：预演说能做，随后执行就成功且 `applied`/`rolled_back`/
  `cp_after` 范围完全一致；预演说不行，执行必然被同一类原因拒绝、同样
  什么都不动（事务整体回滚，零部分应用）。
  该一致性的前提是预演与执行之间链数据与索引位点没有被其他写入改变
  （可对比执行时的请求前位点与计划里的 `cp_before` 来确认）。

## 批量与并发：要么全成，要么全不动

- `execute_batch([IngestOp(block) ..., SyncOp(height) ...])` / `plan_batch(...)`
  一次请求可覆盖多个投递与一段段追赶目标；所有操作在**同一个数据库事务**里
  顺序评估。任一操作被拒绝，整个事务回滚：不会出现"旧数据删了但位点没退"
  或"位点变了却没有新数据"的中间状态，位点、区块、交易始终自洽。
- 返回的 `Decision.outcomes` 逐个给出每个操作的结论：
  `accepted`（含 `applied`/`duplicate`/`reorg_applied`/`noop` 结果）、
  `rejected`（拒绝点，带分类原因）、`aborted`（因前面被拒而未执行）。
  每项结论还带 `rolled_back`：该操作评估期间会触发的高度回滚（降序），
  因此批量预演里每个操作都能分别看出会不会触发重组、涉及哪些回滚——
  即使整批最终被拒，各操作的候选回滚范围也完整保留。
- 存储层用一个跨事务的全局锁串行化所有写入，因此不同来源几乎同时投递/追赶时：
  请求不会互相串数据或重复入库，最终提交状态确定；同一目标的并发追赶都会
  成功收敛到同一结果（先到者应用、后到者幂等 `noop`）。
- **幂等与重试**：按同样目标重试，只要链数据没变，结果稳定——重复投递永远
  返回 `duplicate`、追赶已到位点返回 `noop`，既不重复入库也不漏数据；
  被拒绝的请求重试仍被同一原因拒绝，且不留下任何半成品。

## 决策流水（持久化）

每次预演和执行都在 SQLite 表 `decision_journal` 留下一行（执行成功时流水与
索引数据同事务提交；执行被拒绝时在回滚之后补记）。字段包括：

- `mode`：`plan` / `execute`；`kind`：`ingest` / `sync` / `batch`；
  `status`：`accepted` / `rejected`
- `cp_before` / `cp_after`：处理前、后的位点
- `applied` / `duplicates` / `rolled_back`：应用、重复跳过、回滚的高度列表
  （被拒绝的请求中为"若真执行会发生"的候选范围）
- `outcomes`：批量请求中每个操作的逐项结论（含逐项 `rolled_back` 候选回滚范围）
- `reject_reason` / `reject_detail`：分类拒绝原因与细节（拒绝原因彼此可分辨，
  不会糊成一句"失败"）
- `request_json` / `created_at`：原始请求与时间，便于复盘

据此可以解释"某个高度现在为什么是这个哈希"（看最近一次作用于该高度的
`rolled_back`/`applied` 记录）以及"某笔交易为什么查不到"（其所在高度曾被
回滚，旧交易随区块同事务删除）。流水随数据库文件持久化，进程重启后可查。

## 恢复语义

- 位点、数据与决策流水同库存储（SQLite），每个区块落库后即更新位点，
  崩溃最多丢失最后一个进行中的区块。
- 重启后用同一数据库文件重建 `IndexStore` / `Indexer`，位点与历史流水自动恢复，
  继续 `sync_to` 只索引位点之后的高度，已持久化的历史结论不被重写。
- 位点回退（`checkpoint_regression`）与跨度过大的追赶（`catchup_too_large`）
  被明确拒绝并说明原因。

## HTTP 接口

```bash
uv run uvicorn main:app --reload
```

原有接口（行为不变）：

- `GET  /checkpoint` — 当前索引位点
- `GET  /blocks/{height}` / `GET /blocks/by-hash/{hash}` — 区块查询
- `GET  /transactions/{txid}` — 交易查询
- `POST /ingest` — 单块投递（409 时返回 `{"reason": ..., "detail": ...}`）
- `POST /sync/{target_height}` — 追赶到目标高度

本轮新增：

- `POST /plan/sync/{target_height}` / `POST /plan/ingest` — 只读预演，
  始终返回 200 与计划（即使会被拒绝，也以 `status="rejected"` 体现在计划里；
  计划中的 `rolled_back_heights` 与逐项 `outcomes[*].rolled_back_heights`
  给出"若真执行会发生"的候选回滚范围）
- `POST /plan/batch` — 批量预演，请求体为
  `[{"op":"sync","target_height":3}, {"op":"ingest","block":{...}}]`
- `POST /batch` — 批量执行，整批原子；任一被拒绝返回 409 且整批不动
- `GET  /decisions?mode=&kind=&status=&limit=` — 查询决策流水
- `GET  /decisions/{id}` — 查询单条决策

- `POST /demo/append`、`POST /demo/fork?from_height=&length=` — 本地链模拟

数据库路径通过环境变量 `INDEXER_DB` 指定（默认 `indexer.db`）。

## 本地验证

```bash
uv run pytest tests/ -q
```

测试覆盖（28 个用例）：

- 原有：重复投递幂等、乱序/缺口/孤儿块拒绝、冲突块拒绝、分叉重组回滚、
  单块投递触发重组、超深重组拒绝且零部分应用、重启后从位点恢复、
  位点回退与超大跨度追赶拒绝。
- 预演与批量：
  - 预演零写入（位点/区块/哈希/交易快照前后一致）且与执行范围一致；
  - 预演/执行对拒绝（含"既可能触发回滚、又要判断目标可达"的请求与超深重组）
    结论一致、原因同类；
  - 批量任一被拒即整批回滚（含第一个操作的半成品也被撤销）、混合重复与应用只入库一次；
  - 失败后按同样目标重试结论稳定、不重复入库、不漏数据；
  - 同目标并发追赶全部成功收敛、并发乱序投递+重试每块恰好应用一次、
    并发被拒批量不留半成品、并发最终状态可复现；
  - 决策流水持久化、重启后可查、拒绝原因分类清晰，并能解释回滚与重复高度。
- 本轮新增（"先重组、后被拒"的完整计划呈现）：
  - 预演对"先触发重组回滚、最终因超出链尖/跨度过大被拒"的追赶请求给出完整
    候选回滚范围（顶层与逐项 `rolled_back`）且严格零写入，紧接着真执行被
    同一类原因拒绝、同样零写入；
  - 位点回退等"拒绝发生在回滚评估之前"的请求回滚范围为空、原因仍可分辨；
  - 批量预演中同类请求的逐项结论与逐项回滚范围正确，整批原子不动；
  - 被拒后按同样目标重试结论稳定，链追上后同一请求只应用一次、再重试幂等；
  - 被拒决策的候选回滚范围随流水持久化，重启后可查。

`log_cli` 已开启，测试日志逐步打印处理位点与事件：
`event=applied height=..`、`event=duplicate height=..`、
`event=rollback fork_point=.. depth=.. removed=[..]`、
`event=reject reason=..`、
`event=decision mode=.. kind=.. status=.. cp_before=.. cp_after=.. applied=[..] ...`。

# colbatch 配送归属数仓

`warehouse-apply` / `warehouse-export` / `warehouse-status` 在现有 CLI 之上
增加一个 **SQLite 配送归属数仓**。它直接消费 `colbatch run` 清洗产物
（manifest + success/failure 流），可选叠加运单撤销文件与站点归属改动
文件，把结果发布成**可持续增量更新、可复现历史版本**的运单明细与归属日
汇总。

设计目标：

* **增量**：后续批次只重判本批涉及的运单和改动区间覆盖的运单，不全扫事
  实表、不重建整份发布数据。
* **历史可复现**：任一已发布版本在后续更正、源文件被搬走、进程重启后都
  仍能完整导出同一份数字。
* **整批原子**：结构错误的批次整批拒绝，版本头与任何一类数据都保持原样。
* **可审计**：发布回执、版本头、源状态、明细、日汇总、排除记录作为一组生
  效；导出自校验版本指纹。

---

## 1. 命令

```bash
# 入库发布（一次一个批次，成功即一个新版本）
python3 -m colbatch.cli warehouse-apply  --config examples/warehouse/batch_v1.json

# 取数：指定历史版本或 latest；默认输出单条带类型标记的 JSONL 流
python3 -m colbatch.cli warehouse-export --db out/warehouse_demo/warehouse.sqlite --revision 1
python3 -m colbatch.cli warehouse-export --db ... --revision latest --out facts.jsonl

# 等价的文件组接口：header.json + facts/daily/exclusions.jsonl
python3 -m colbatch.cli warehouse-export --db ... --revision 2 --format files --out out/rev2

# 查看版本列表、单个版本头或某个批次的回执
python3 -m colbatch.cli warehouse-status --db ... 
python3 -m colbatch.cli warehouse-status --db ... --batch-id delivery-2025-06-01-1
```

**退出码**

| 命令 | 0 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|
| `warehouse-apply` | 已发布（或相同批次重试返回原版本） | 配置错误 | 整批数据错误 | 执行错误（未发布） | 冲突 |
| `warehouse-export` | 导出并自校验通过 | — | — | 缺库 / 未知版本 / 状态不一致 | — |
| `warehouse-status` | 打印成功 | 配置错误 | — | 缺库 | — |

`run` / `preflight` / `compare` 的既有用法与退出码完全不变。

---

## 2. 一个可运行的小样例

```bash
# 1) 用真实清洗路径生成两版产物（normalize 语义 + 运单/站点透传）
python3 scripts/warehouse_demo.py --outdir out/warehouse_demo

# 2) 或手动走一遍：清洗 → 入库 → 取数
python3 -m colbatch.cli run --config out/warehouse_demo/v1_run.json
python3 -m colbatch.cli warehouse-apply  --config out/warehouse_demo/b1_batch.json
python3 -m colbatch.cli warehouse-apply  --config out/warehouse_demo/b2_batch.json
python3 -m colbatch.cli warehouse-export --db out/warehouse_demo/warehouse.sqlite --revision 1
python3 -m colbatch.cli warehouse-export --db out/warehouse_demo/warehouse.sqlite --revision latest
```

演示输出（与交付检查的时间切片一致）：

```
== after batch 1: revision 1
   2025-06-01  west            4 shipments 500 cents
   2025-06-01  <unattributed>  3 shipments 300 cents

== after batch 2: revision 2
   2025-06-01  east            2 shipments 200 cents
   2025-06-01  west            2 shipments 300 cents
   2025-06-01  <unattributed>  3 shipments 300 cents

== re-export of revision 1 (after rev 2 published): revision 1
   2025-06-01  west            4 shipments 500 cents
   2025-06-01  <unattributed>  3 shipments 300 cents
```

`examples/warehouse/` 下是等价的配置模板：

* `batch_v1.json` / `batch_v2.json`：批次配置（`db`、`batch_id`、
  `expected_parent_version`、`cleaning_run`、`assignments`、可选
  `cancellations`、`workspace_dir`、`receipt`、`fields`、`limits`）；
* `assign_v1.jsonl` / `assign_v2.jsonl`：站点归属改动；
* `cancel_example.jsonl`：运单撤销文件。

---

## 3. 查询说明

导出流每行带稳定的 `type` 字段（`header` / `fact` / `daily` /
`exclusion`），一次读取即可区分：

```bash
DB=out/warehouse_demo/warehouse.sqlite

# 日汇总（按 utc_date、区域稳定排序；未归属 region=null）
python3 -m colbatch.cli warehouse-export --db $DB --revision 2 \
  | python3 -c 'import sys,json
for l in sys.stdin:
    r=json.loads(l)
    if r["type"]=="daily": print(r["utc_date"], r["region"], r["shipments"], r["amount_cents"])'

# 按运单号排序的明细
python3 -m colbatch.cli warehouse-export --db $DB --revision 2 \
  | python3 -c 'import sys,json
for l in sys.stdin:
    r=json.loads(l)
    if r["type"]=="fact": print(r["shipment_id"], r["event_time"], r["region"], r["amount_cents"])'

# 本版排除项与计数
python3 -m colbatch.cli warehouse-export --db $DB --revision 2 \
  | python3 -c 'import sys,json,collections
c=collections.Counter()
for l in sys.stdin:
    r=json.loads(l)
    if r["type"]=="exclusion": c[r["reason"]]+=1
print(dict(c))'
```

也可直接用 SQLite 查询（明细与日汇总都是普通表）：

```sql
-- 某日某区域的笔数与金额分
SELECT shipments, amount_cents FROM daily
 WHERE utc_date='2025-06-01' AND region IS NULL AND valid_to IS NULL;

-- 某运单当前归属
SELECT shipment_id, station_id, norm_ts, assignment_id, region, source_run_id
  FROM facts WHERE shipment_id='A-02' AND valid_to IS NULL;

-- 某版本的全部明细（历史版本）
SELECT shipment_id, station_id, norm_ts, assignment_id, region
  FROM facts WHERE valid_from<=1 AND (valid_to IS NULL OR valid_to>1)
 ORDER BY shipment_id;
```

**版本选择语义**：版本 R 的明细/汇总 = `valid_from <= R AND (valid_to
IS NULL OR valid_to > R)` 的行。历史版本因此是"当时有效"的快照，之后任
何更正都不会改写它。

**一致性**：导出在**一条只读事务**内完成——版本头、明细、日汇总、排除
记录来自同一个 WAL 快照。另一个进程同时发布新版本不会让输出混合两个版
本，也不会阻塞（WAL 允许读写并发）。

**自校验**：每个版本在 `revisions` 表里存有明细与日汇总的 XOR 指纹。导
出边流式输出边重算指纹与计数，不匹配则报错退出（不会输出看似完整的
流）。`--no-verify` 仅供诊断。

---

## 4. 入库批次

### 4.1 批次配置

| 字段 | 含义 |
|---|---|
| `db` | SQLite 库路径（不存在则创建） |
| `batch_id` | 调用方提供的批次号，全局唯一 |
| `expected_parent_version` | 预期父版本；首次发布为 `0` |
| `cleaning_run.manifest` | 清洗运行 manifest（必填，若本批含清洗产物） |
| `cleaning_run.success` / `.failure` | 可省略；省略时用 manifest 里记录的路径 |
| `cancellations` | 运单撤销文件（每行 `{"shipment_id": ...}`） |
| `assignments` | 站点归属改动文件 |
| `workspace_dir` | 暂存工作区（默认 `<db>.work`） |
| `receipt` | 发布回执输出路径（可选） |
| `fields` | 业务键字段名（默认 `shipment_id` / `station_id`） |
| `limits` | 行大小、内存与暂存边界（见 §6） |

路径一律相对**配置文件所在目录**解析。批次可包含清洗产物、撤销文件、
归属改动文件任意非空组合。

### 4.2 清洗产物核验

读入时依次核验（任一失败 → 整批拒绝，退出码 4）：

1. `status == "completed"`、`abort_reason` 为空、`delivered_batches ==
   batches_formed`；
2. manifest 里两条流的 `sha256` / `rows` 与 `delivered_rows_success/failure`
   一致，且 `success_rows + failure_rows == input_rows_read`，
   `last_delivered_row_id == input_rows_read - 1`；
3. 实际消费的字节的 SHA-256 与 manifest 一致（以**消费的字节**为准，不
   看文件元数据）；
4. 两条流各自按 `row_id` 严格递增，且并集恰好覆盖 `[0, input_rows_read)`
   一次；跨流碰撞、缺号、多号、乱序都整批拒绝。

### 4.3 行判定

* `is_valid=false` 的成功行 → **排除项**（`reason=invalid_row`）。
* 失败流的行 → **排除项**（`reason` 取 `error.cause`）。
* `is_valid=true` 但 `shipment_id`/`station_id`/`norm_ts`/`norm_amount` 为
  null 或空串 → **排除项**（`reason=missing_required`），且**该运单保留原
  有状态**。
* `is_valid=true` 但非空的 `norm_ts` 不是 UTC 秒精度、`norm_amount` 不是
  合法十进制定点串、或业务键不是字符串 → **整批拒绝**（`bad_time` /
  `bad_amount` / `structure`）。

金额统一为同一币种，以**整数分**计算，允许负数（`-3.50` → `-350`）。

### 4.4 撤销与归属改动

* 撤销文件按运单号删除**当前**记录；撤销未知号、同批重复撤销、同号同时
  更新与撤销 → 整批拒绝。
* 归属改动用 `(station_id, assignment_id)` 定位区间：
  * `{"op":"write", "station_id","assignment_id","from","to","region"}`（`op`
    缺省即 write；`to=null` 表示无上界）；
  * `{"op":"delete", "station_id","assignment_id"}`，必须命中现有区间。
* 时间端点与运单同一 UTC 格式 `YYYY-MM-DDTHH:MM:SSZ`；有限端点要求
  `from < to`；`region` 非空。同批每个区间键只能写一次。
* 用**整批改动后的状态**验证同站区间不重叠；相邻区间可衔接。同一批可以
  撤下旧区间、补上新分段，或取消区间留下空档。

---

## 5. 回填与取数语义

每个版本的有效运单按 `from <= norm_ts < to` 归到该站当时的区间；匹配不
到则保留运单，归属区间与区域为 JSON `null`。

明细字段：运单号、站点、事件时间、金额分、区间标识、区域、入库来源坐标
（`run_id` / `manifest_sha256` / `success_sha256` / `stream` / `row_id`）。

日汇总按 UTC 日期与区域输出笔数和金额分；未归属单独成组；无运单的组删
除（但在旧版本中仍按旧版本存在）。区域更正导致移组时先扣旧贡献再计新贡
献。

**发布路径是增量的**：

* 运单更新按业务键（`shipment_id`）处理：写一个新版本行、把旧版本行的
  `valid_to` 置为新版本号（SCD2 历史）。
* 归属变更只需重判所改区间**修改前或修改后**覆盖时段内的运单；索引
  `facts_current_station_ts(station_id, ts_epoch) WHERE valid_to IS NULL`
  用于定位，从不全扫事实表。
* 原来没有匹配的运单也会被计入（它们落在新区间的时段内）。
* 未被触及的运单与日汇总行既不读也不写。
* 一个批次可以同时移动运单、撤单、修改旧站点区间，最终物化结果等于拿该
  版本全部有效运单和归属从头计算一次（参考器逐版本核对）。

**统计**：回执里的 `metrics` 输出本批实际数字，重点是

* `touched_shipments`：本批重判的运单数；
* `rejudged_history_facts`：其中属于**历史**运单（非本批清洗产出）的数；
* `moved_facts`：归属区域实际变化的运单数；
* `inserted_facts` / `updated_facts` / `deleted_facts` / `unchanged_facts` /
  `carried_facts`（本版未触碰、按 SCD2 有效区间自动继承的运单数）；
* `daily_deltas` / `daily_keys_changed` / `daily_rows`（本版日汇总键数）；
* `facts_read` / `daily_rows_read` / `intervals_read` / `span_rows_read`；
* `plan`：`EXPLAIN QUERY PLAN` 文本（证明走索引而非全表扫描）；
* `access_audit`（`limits.access_audit=true` 时）：按语句类别的实际计数与
  按表的命中计数；
* `workspace`：本批暂存库的磁盘占用字节数。

---

## 6. 有界性声明

输入、待重判键、改动和导出行都可能超出内存。实现用**有界解析 + 磁盘工
作区 + 数据库状态**：

| 边界 | 默认 | 超限行为 |
|---|---|---|
| `limits.max_line_bytes` | 8 MiB | 单行超限 → 整批拒绝（`line_too_long`），不截断 |
| `limits.max_manifest_bytes` | 4 MiB | manifest 超限 → 整批拒绝 |
| `limits.read_block_bytes` | 1 MiB | 固定读块；每流同时最多缓冲一行 |
| `limits.sqlite_cache_kib` | 16384 KiB | SQLite 页面缓存上限；其余落盘 |
| `limits.busy_timeout_ms` | 30000 | `BEGIN IMMEDIATE` 拿不到写锁时的等待上限，超时报执行错误 |
| `limits.access_audit` | false | 打开后记录语句计数（有少量开销） |

* 清洗流按 `row_id` 有序归并，内存里每流只保留一条记录；排除项、待重判
  键、区间改动、日汇总增量全部落在工作区 SQLite 中。
* 工作区默认在 `<db>.work/.whstage-*`，批次成功/失败/取消后均删除。
* 导出一行一行写出（stdout 或临时文件），完整导出不驻留内存。

实测：`tests/test_warehouse.py::scale_locality` 生成 4000 条跨 40 站、多个
日期的运单，小范围追溯更正时断言 `facts_read < N/10`、
`touched_shipments == 该站在新/旧区间并集内的运单数`，并断言 CLI 子进程
峰值 RSS < 400 MiB。

---

## 7. 重试、冲突与恢复

* **重试**：用相同 `batch_id`、父版本和**相同输入内容**重试时，返回原版
  本与回执；允许版本头已前进。身份由实际输入字节的摘要 + 业务配置决定，
  **不依赖绝对路径**（文件搬走后仍是重试）。
* **冲突**（退出码 6）：
  * 批次号相同但内容或预期父版本不同（`batch_id_reused`）；
  * 新批次的预期父版本已过期（`stale_expected_parent`）。
* **同父版本并发**：`BEGIN IMMEDIATE` + 唯一约束保证从同一父版本同时发
  布两个不同批次时最多一个成功，另一个显式报冲突。
* **提交两侧中止均可恢复**：
  * 提交前中止 → 事务回滚，版本头与数据不变，重跑同一命令正常发布；
  * 提交后但回执未返回 → 批次行与版本在同一事务里已提交，重跑同一命令
    返回 `retry` 与已存回执。
* 回执文件用临时文件 + `os.replace` 原子写；`warehouse-status --batch-id`
  可直接从库里读出已提交回执。

---

## 8. 独立参考器

`tests/reference_warehouse.py` **不 import 任何 `colbatch.warehouse` 代码**：
它用纯 Python 字典逐版本重放清洗产物、撤销与归属改动，每个版本直接从
头做区间匹配与分组求和，得到明细、日汇总、排除项与来源坐标。

```python
from reference_warehouse import ReferenceWarehouse

ref = ReferenceWarehouse()
ref.apply_batch({"batch_id": "b1", "expected_parent_version": 0,
                 "cleaning_run": {"manifest": ..., "success": ..., "failure": ...},
                 "assignments": ...})
revision, view = ref.export(1)   # view["facts"] / ["daily"] / ["exclusions"]
```

`tests/test_warehouse.py` 的每个场景都在每版发布后用 `compare_to_reference`
逐行核对产品导出与参考器：

```bash
python3 tests/test_warehouse.py                    # 全部场景
python3 tests/test_warehouse.py slice_history      # 只跑时间切片
```

覆盖的场景：

| 场景 | 验证点 |
|---|---|
| `slice_history` | 文档时间切片两版；02 时退出西区、08 时补入东区、10 时仍未归属；第二版发布后回查第一版；源文件删除后仍可导出 |
| `move_cancel_same_batch` | 同批移站 + 撤单 + 改金额；旧组扣减、新贡献各一次；同号更新撤销整批拒绝 |
| `span_only_backfill` | 仅归属改动（无清洗产物）触发历史回填；未触及运单来源坐标不变；删区间后全部未归属 |
| `exclusions` | 四类排除项；排除行保留原状态；跨版本排除集独立 |
| `retry_and_conflicts` | 重试返回原版本；批次号重用/父版本过期冲突；搬文件后仍为重试 |
| `rejections` | 截断流、非法金额、重叠区间、重复运单、撤销未知号、删除未知区间、重复撤销、中止运行、摘要不符——全部整批拒绝且版本头/数据完全保留 |
| `export_snapshot_consistency` | 导出与未提交写入并发时只见单一完整版本；同父版本并发发布最多一个成功 |
| `crash_recovery` | 提交前中止可重跑；提交后回执未返回可重跑取回回执 |
| `move_with_interval_change` | 同批移站 + 修改旧站点区间：旧组扣一次、新组计一次，与从头重算一致；旧版本日汇总不受影响 |
| `export_file_group` | 文件组接口与流式接口内容等价；未知版本/缺库报执行错误 |
| `exclusion_history_and_daily_groups` | 排除项按版本留存；空日汇总组消失且不影响旧版本 |
| `scale_locality` | 大规模无关数据 + 小范围更正：实际访问计数、查询计划、RSS 有界、工作区有界 |

---

## 9. 已知限制

* 归属区间表本身是"当前状态"（不是历史表），历史归属通过事实行的 SCD2
  版本体现；导出历史版本无需回放区间历史。
* 日汇总的跨版本历史同样以 SCD2 行保存，键为
  `(utc_date, region_key, valid_from)`。
* 一个批次内的撤销与更新作用于同一运单会整批拒绝（见 §4.4）。
* 未归属的事实保留在明细中、在日汇总中以 `region=null` 单独成组。
* 金额按整数分精确计算；超过 int64 分的金额被视为非法。

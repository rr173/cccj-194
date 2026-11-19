# colbatch compare：两次清洗运行之间的逐行结果对账

`colbatch compare` 在**同一不可变质量快照**上，对两次完整清洗运行（基准
baseline 与候选 candidate）的交付结果做逐行对账，产出**可审阅、可复现的
逐行变化集**：哪些源记录的交付结果变了、属于哪类变化、变在哪些字段、基准
值与候选值各是什么。它回答的是“插件版本或清洗配置变化后，下游数据会因此
改变多少、改在哪里”，用于在替换下游数据之前审核转换升级。

它**不**替代 preflight 的原始输入画像（preflight 回答“输入好不好”，
compare 回答“两次交付差多少”），也**不**自动批准或发布候选清洗结果——
变化集只是评审证据。

对账以运行清单（manifest）为信任锚点：清单记录运行绑定的快照摘要、插件身
份、交付行数与**每个交付流的 SHA-256**；比较器重新哈希它消费的每个产物，
任何摘要、计数或身份不一致都拒绝对账，绝不生成看似完整的报告。

---

## 1. 命令用法

```bash
# 逐行对账并发布报告（变化是数据，不是错误；发现变化仍返回 0）
python3 -m colbatch.cli compare --config compare.json

# 只读检查已发布报告的状态与汇总（校验报告与其明细产物的一致性）
python3 -m colbatch.cli compare-status --config compare.json
```

`compare` 把报告 JSON 打印到 stdout，把一行人类可读汇总打印到 stderr。
`compare-status` 校验通过时把报告 JSON 打印到 stdout，否则把结构化错误打
印到 stderr。

### 退出状态

| 码 | 含义 |
|---|---|
| `0` | 对账完成，报告已发布（无论是否发现变化）；`compare-status` 表示存在完整且校验一致的报告 |
| `3` | **配置错误**：未知字段、缺字段、路径/目录非法、配置 JSON 损坏、资源限制参数非法 |
| `4` | **输入不一致（数据错误）**：清单缺失/损坏/状态非 completed、未绑定快照、两侧快照不同、部分交付、产物摘要或计数不符、JSONL 损坏、row_id 重复/缺失/越界/乱序、成功失败身份冲突、覆盖集合不一致、源记录不一致 |
| `5` | **执行错误**：临时目录/磁盘故障、比较期间输入被原位改写、清单在比较期间被改写、取消、发布失败 |

关键区分：`4` 是“这两次运行的产物无法可信对账”，`5` 是“比较器自身没
能跑完”，`3` 是“配置不合法”。三者都**不会**被解释为“结果未变化”，
也都**不会**发布报告、**不会**破坏先前已发布的有效报告。数据/执行错误
会在报告目录的 `last_error.json` 留下结构化记录（`kind` 为
`data_error` / `exec_error`，`reason` 为稳定的机器可读原因串），下一次成
功的对账会把它清除。

`compare-status` 的退出码：`0` 报告完整且明细产物摘要校验通过；`3` 配置
错误；`4` 报告缺失/损坏/与明细产物不符（若存在 `last_error.json`，错误输
出中会附带其内容）。

---

## 2. 配置格式

```json
{
  "baseline": {
    "manifest": "base/logs/manifest.json",
    "success": "base/success.jsonl",
    "failure": "base/failure.jsonl"
  },
  "candidate": {
    "manifest": "cand/logs/manifest.json",
    "success": "cand/success.jsonl",
    "failure": "cand/failure.jsonl"
  },
  "report_dir": "recon",
  "temp_dir": "recon/tmp",
  "read_block_bytes": 1048576,
  "max_line_bytes": 67108864
}
```

| 字段 | 说明 |
|---|---|
| `baseline` / `candidate` | 必填。各含 `manifest`、`success`、`failure` 三个路径（缺一不可，不接受其它键）。 |
| `report_dir` | 必填。对账报告目录（报告、明细、暂存都放这里）。已存在且不是目录 → 配置错误。 |
| `temp_dir` | 可选。独占暂存目录，默认 `<report_dir>/tmp`。任何失败都会清理其中未发布的暂存产物。 |
| `read_block_bytes` | 可选。流式读缓冲，默认 1 MiB，下限 4096。 |
| `max_line_bytes` | 可选。单行 JSONL 上限，默认 64 MiB，下限 4096；超限按数据错误（`line_too_long`）处理。 |

相对路径一律相对**配置文件所在目录**解析。未知字段（顶层或某一侧内）是
配置错误。

---

## 3. 运行清单（manifest）新增字段

宿主 `run` 的清单新增两节，`compare` 依赖它们做验证（旧版宿主生成的清单
缺少这些字段，会被判为 `incomplete_outputs` / `incomplete_run_identity`
数据错误——用新版宿主重新运行即可）：

```jsonc
{
  "plugin": {"name": "normalize_text", "version": "1.0"},
  "outputs": {
    "success": {"path": "/abs/success.jsonl", "sha256": "…", "rows": 120},
    "failure": {"path": "/abs/failure.jsonl", "sha256": "…", "rows": 3}
  }
}
```

* `plugin`：首个工人握手上报的插件身份；同一作业内所有工人（含崩溃后重
  建的新代次）必须上报同一身份，否则宿主拒绝该工人（运行中途替换插件文
  件是完整性问题，不是新代次）。
* `outputs.success/failure`：writer 线程在交付每个字节时同步计算的
  SHA-256 与行数。`rows` 与既有 `delivered_rows_success/failure` 一致。

`compare` 对每侧清单的完整校验：

1. `status == "completed"` 且 `abort_reason` 为 null——取消、中止、部分
   交付的运行一律拒绝；
2. `input_snapshot.sha256` 存在且为合法摘要——未走质量门禁
   （`configured_input`）的运行没有快照绑定，不可对账；
3. 两侧 `input_snapshot.sha256` **必须相等**——不同输入快照拒绝对账；
4. `delivered_rows_success + delivered_rows_failure == input_rows_read`
   且 `last_delivered_row_id == input_rows_read - 1`——覆盖必须完整；
5. `plugin.name` / `plugin.version` 为非空字符串——运行身份必须完整；
6. `outputs.*.sha256` 为合法摘要、`outputs.*.rows` 与交付计数一致；
7. 两侧 `input_rows_read` 相等——覆盖集合一致。

注意：清单中的 `outputs.*.path` 仅供运维参考；比较器以 **compare 配置中
的路径**定位产物（允许运行目录整体搬迁），以**清单中的摘要与计数**验证
产物内容。两侧 `rule_summary` 只记录不比对（同一快照完全可以在不同质量
规则下被各自准入）。

---

## 4. 身份配对语义

每侧的 success 与 failure 两个流都按源顺序（row_id 升序）交付。比较器：

1. 对每侧做**有界二路归并**（success + failure → 按 row_id 的单一有序
   流），再对两侧做**同步归并**；任何时刻内存中至多每条流一条记录；
2. 以 `row_id` 为输入记录身份，对同一快照中的每个源行**恰好配对一次**
   ——绝不按文件行号或仅成功流位置拉链；
3. 逐流验证：row_id 必须是 JSON 整数（布尔、浮点、字符串一律拒绝）、
   落在 `[0, input_rows_read)` 内、严格递增。重复（`duplicate_row_id`）、
   越界（`row_id_out_of_range`）、乱序（`row_id_out_of_order`）、缺行
   （`missing_row_id`）、同一侧成功与失败撞身份（`identity_conflict`）、
   两侧覆盖集合不一致（`coverage_mismatch`）都是数据错误；
4. 同一 row_id 两侧的 `source` 记录必须语义相等，否则
   `source_mismatch`——至少一侧产物不反映所绑定的快照；
5. 逐流核验：实际行数 == 清单 `outputs.*.rows`，全部字节的 SHA-256 ==
   清单 `outputs.*.sha256`。

流顺序错误、重复、缺失一律**检测并拒绝**，不做静默修复或重排。

---

## 5. JSON 值比较语义

六类类型格：**null、boolean、number、string、array、object**。

* 不同类型的值永不相等：`1` ≠ `true` ≠ `"1"` ≠ `null`；
* number 按数值比较：`1` 等于 `1.0`，`-0.0` 等于 `0.0`；非标准
  `NaN/Infinity` 记号在解析时即被拒绝（与 preflight 同一立场）；
* string 按码点精确比较；
* array **有序**：长度与逐元素都相等才相等（`[1,2]` ≠ `[2,1]`）；
* object 按“键 → 值”映射比较：**键顺序不制造虚假变化**；
* 布尔永不被当作数字（`true` ≠ `1`）。

### 字段差异与稳定路径

成功结果的变化以 **JSON Pointer（RFC 6901）** 路径报告，根为 output 对
象本身；对象键按 `~0`/`~1` 转义，数组下标为十进制段：

```jsonc
{"path": "/amount", "base": "1.00", "candidate": "1.01"}   // 两侧都有值
{"path": "/meta/tags/2", "candidate": "c"}                   // 候选新增
{"path": "/a~1b/~0key", "base": 1}                           // 候选删除（键含 / 与 ~）
```

对象差异递归到叶子（键按字典序遍历），数组差异按下标升序；类型不同或标
量不等时在该路径记录两侧完整子值。差异条目序列是确定性的。

### 失败记录的业务身份

失败行的 `error` 对象中，以下**运行器字段**（批记账、重试历史、进程/主
机身份、墙钟时间、临时路径）在比较前被剥离，绝不当作转换值变化：

```
batch_id, attempts, log, pid, worker_pid, process_id, slot, worker_slot,
generation, worker_generation, timestamp, ts, time, elapsed_ms, duration_ms,
started_at, finished_at, tmp_path, temp_path, temp_dir, tmp_dir,
hostname, host, machine
```

其余字段（`cause`、`detail` 及插件自行写入的业务字段）构成失败的业务身
份。两侧失败记录业务身份不同 → `failure_reason_changed`，并在明细中给出
两侧归一化后的 error 作为差异证据。

---

## 6. 逐行分类与报告格式

每个源行恰好归入五类之一：

| 分类 | 含义 |
|---|---|
| `unchanged` | 交付结果未变（成功且 output 语义相等，或失败且业务身份相同） |
| `success_fields_changed` | 两侧均成功，output 字段有差异（明细给出字段级差异） |
| `success_to_failure` | 基准成功 → 候选失败（明细给出候选的归一化 error） |
| `failure_to_success` | 基准失败 → 候选成功（明细给出基准的归一化 error） |
| `failure_reason_changed` | 两侧均失败但业务身份不同（明细给出两侧归一化 error） |

### 明细 `changes/changes-<sha256>.jsonl`

只记录**发生变化**的行（`unchanged` 只计数不写明细），每行一个规范
JSON（键排序、紧凑分隔符、UTF-8），按 row_id 升序：

```jsonc
{"row_id": 12, "class": "success_fields_changed",
 "fields": [{"path": "/amount", "base": "1.00", "candidate": "1.01"}]}
{"row_id": 40, "class": "success_to_failure",
 "error": {"cause": "plugin_error:plugin_exception"}}
{"row_id": 55, "class": "failure_reason_changed",
 "error": {"base": {"cause": "batch_timeout"},
           "candidate": {"cause": "worker_crash"}}}
```

明细文件以内容寻址（文件名为其 SHA-256）、只读（0444）发布。

### 汇总 `report.json`

```jsonc
{
  "schema_version": 1,
  "status": "completed",
  "input": {
    "snapshot_sha256": "…",                    // 两侧共享的快照摘要
    "baseline":  {"snapshot_sha256": "…", "rule_summary": "sha256:…"},
    "candidate": {"snapshot_sha256": "…", "rule_summary": "sha256:…"}
  },
  "runs": {
    "baseline": {
      "manifest_ref": "base/logs/manifest.json",   // 相对标识，无机器绝对路径
      "plugin": {"name": "…", "version": "…"},
      "input_rows": 120,
      "snapshot_sha256": "…",
      "rule_summary": "sha256:…",
      "outputs": {
        "success": {"ref": "base/success.jsonl", "sha256": "…", "rows": 118},
        "failure": {"ref": "base/failure.jsonl", "sha256": "…", "rows": 2}
      }
    },
    "candidate": { /* 同上 */ }
  },
  "summary": {
    "rows_compared": 120,          // == 每侧消费的源行数（恰好一次）
    "unchanged": 100,
    "changed_rows": 20,
    "success_fields_changed": 12,
    "success_to_failure": 3,
    "failure_to_success": 4,
    "failure_reason_changed": 1,
    "changed_fields": {"amount": 9, "meta": 3}   // 按输出字段汇总的变化*行*数
  },
  "detail": {
    "schema_version": 1,
    "ref": "changes/changes-<sha256>.jsonl",     // 相对报告目录
    "sha256": "…",
    "rows": 20
  }
}
```

* `rows_compared == unchanged + changed_rows` 恒成立；该等式即“同一快照
  上每个源行两侧各消费恰好一次”的覆盖证明。
* `changed_fields` 只统计 `success_fields_changed` 行：某行的某个顶层输
  出字段下发生 ≥1 处差异，该字段计 1 行（同一字段多处差异不重复计）。
* 报告经确定性编码（键排序），同一对输入重复对账产出**字节一致**的报
  告；路径一律为相对标识（配置目录内为相对路径，目录外为
  `external:<去根尾路径>`），不含机器绝对路径、不含时间戳。

### 成组原子发布

明细与汇总作为**一组**发布：明细先以内容寻址名落盘并置只读，报告随后
`os.replace` 原子替换；报告写入失败会回滚本次新建的明细，旧的有效组合
原样保留。发布成功后，仅回收不再被任何报告引用的旧明细文件。暂存目录
独占创建，解析、校验、写入或取消的任何失败都清理未发布暂存产物。

---

## 7. 比较期间输入变化的检测

* 四个交付流各自**只打开一次**，全程通过同一文件对象消费：文件路径被替
  换（rename）不影响已打开的句柄，比较继续基于最初打开的字节版本完成
  校验与消费，绝不前后分别打开而静默混用两个版本；
* 每个流在打开时记录 `(dev, inode, size, mtime)`，EOF 时与发布前各复核
  一次：**原位改写**（内容、大小或时间戳变化）立即拒绝发布（退出码 5，
  `input_mutated`）；
* 清单同样以打开的句柄读取并在发布前重读复核（`manifest_mutated`）；
* 流内容摘要与清单不符（且文件身份稳定）判 `artifact_digest_mismatch`
  （退出码 4）——“产物被换掉”与“比较期间被改写”是可区分的两类。

---

## 8. 资源限制与边界行为

* **内存有界**：四条流归并消费，任何时刻内存中至多每条流一条已解析记
  录 + 定长读缓冲（`read_block_bytes`）；不把两次运行的全部行载入内存。
  实测 6 万行/侧、35.9 MB 流，产品进程峰值 RSS 约 21 MB（见测试
  `scale_bounded_memory`）。
* **暂存空间**：暂存只含未发布的明细文件（O(变化行数)），独占目录创
  建，成功/失败/取消后一律清理。
* **单行上限** `max_line_bytes`：超限判数据错误（`line_too_long`）；
  超长行（如数 MiB）在限内可正常对账。
* **空输入**（0 行）、**零成功行**（全失败）、**零失败行**（全成功）、
  **末尾无换行**、**Unicode 字段名/值**、**嵌套结构**、**单侧某行由成
  功转失败**等均有确定行为（见测试 `empty_and_edge_streams` /
  `unicode_and_sources`）。
* 坏 JSON 行、空行、非对象行、缺 `output`/`error` 对象、非整数 row_id
  都是数据错误，绝不被解释为“结果未变化”。

## 9. 独立参考对账器

`tests/reference_reconcile.py` 是**独立**参考实现：不复用产品的配对/差
异代码，用纯内存字典重述同一合同（row_id 恰好一次、六类类型格、
JSON-Pointer 路径、非业务 error 键集）。`tests/test_compare.py` 用动态构
造的两侧运行数据逐行核对分类、字段差异、汇总与明细排序，并覆盖状态转
换、同值多类型、对象键序、数组重排、重复/缺失/错序 row_id、两侧快照不
同、清单摘要或计数错误、运行身份不完整、输入并发改写、发布中断、磁盘
故障、取消清理与超内存预算的大输入。大输入一律运行时生成，仓库只提交
小 fixture 与可复现脚本。

## 10. 限制说明

* 对账面向同一快照的两次**完整**运行；不比较部分交付，不比较不同输入。
* 不防御对产物/清单具备写权限的本地攻击者主动篡改：篡改会被摘要与身份
  校验发现并拒绝，但不提供权限隔离。
* 报告目录不做并发互斥：同一 `report_dir` 上并发跑两个 compare 不在支
  持范围内（与 preflight 一致）。
* 变化集是评审证据；是否用候选结果替换下游数据，由人决定。

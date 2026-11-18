# colbatch — 列批清洗宿主

把合作方提供的 Python 转换插件放进**独立操作系统进程**里跑，宿主负责构批、
派发、校验、按序交付和故障隔离。目标是**受控故障隔离**：插件卡死、直接退
出、写半个结果、返回非法数据，都不能污染别的批，也不能留下孤儿进程。

**非目标**：这不是安全沙箱（不拦截系统调用、不隔离网络/文件系统、同
uid），不承诺把恶意代码变安全；不是通用 DAG 执行器；不承诺主进程崩溃续
跑；重试不承诺外部服务恰好一次（演示插件均无外部副作用）。

## 快速开始

```bash
# 依赖：Python >= 3.10，pyarrow（如 pip install pyarrow）
cd colbatch_host

# 跑演示（生成 fixture → 两个插件各跑一遍 → 参考校验器逐行核对）
scripts/run_demo.sh

# 或直接 run 一个作业
python3 -m colbatch.cli run --config examples/normalize_run.json

# 数据质量画像与准入门禁（不起工人、不写清洗输出）
python3 -m colbatch.cli preflight --config examples/quality_run.json

# 跑全部故障注入测试（18 个场景）
python3 tests/test_host.py

# 跑 preflight 质量画像测试（18 个场景，含独立参考交叉验证与 RSS 有界证明）
python3 tests/test_preflight.py

# 动态生成任意规模输入
python3 scripts/gen_input.py --profile normalize --rows 1000000 --seed 7 --out big.jsonl
```

`run` 退出码：`0` 完成，`3` 被信号取消，`1` 中止/配置错误。

输出（路径由配置指定）：

* `success.jsonl`：成功行，`{"row_id", "source", "output"}`，严格按输入序；
  `output` 内含插件新增的标准化值与有效性列（业务 invalid 是**正常数据
  结果**，体现在 `is_valid`/`invalid_reason` 列里）；
* `failure.jsonl`：失败行，`{"row_id", "source", "error"}`，按原行身份隔
  离；`error.cause` 区分 `batch_timeout` / `worker_crash` /
  `protocol_violation:*` / `plugin_error:*` / `intake_error`；
* `manifest.json`：终态清单（状态、交付到哪个输入位置、计数、水位、每批
  结局、工人退出日志含 maxrss）；
* `stats.json`：运行期快照（默认每 200ms 原子刷新），测试与运维用它做同
  步与观测。

## 架构

```
JSONL 源 ──> 宿主主循环 ──构批(Arrow RecordBatch)──> 派发队列
              │  select()                                    │
              │              ┌── worker 进程 0 (gen n) <== 管道 ==┐
              │              └── worker 进程 1 (gen n) <== 管道 ==┤ 每工人
              │                                                  │ 一个任务
              │<── RESULT / BATCH_ERROR / 崩溃 / 超时 ───────────┘
              │  校验(行数/row_id/schema/身份)
              ▼
        已完成重排缓冲 ──按 batch_id 顺序──> writer 线程 ──> success/failure.jsonl
```

* **主循环单线程**：`selectors` 驱动所有工人管道的非阻塞读写 + 定时器
  （批超时、握手超时、kill 宽限、统计快照）。读源、派发、交付都在循环内
  按容量门控。
* **writer 线程**：唯一做输出的线程，从有界队列取已排好序的批写两个
  JSONL。慢 sink 只会堵住这条线程 → 队列满 → 主循环停止交付 → 停止读
  源，形成反向背压。
* **工人**：`python -I worker.py --plugin ...`，`start_new_session=True`
  独立进程组；stdin/stdout 即协议管道，stderr 落日志文件。一个工人同一
  时刻只处理一个批，因此一根管道上的帧永远不会有批归属歧义。

## 线协议

双向均为长度前缀帧（小端）：

```
magic    4B   "CBH1"
mtype    1B   消息类型
flags    1B   保留(0)
reserved 2B   保留(0)
length   8B   载荷长度
payload  …    4B json_len + JSON 元信息 + 剩余字节(Arrow IPC 流)
```

消息：`HELLO / HELLO_ACK / READY`（启动握手），`TASK / RESULT /
BATCH_ERROR`（任务），`SHUTDOWN / BYE`（关闭）。

* **握手**：工人启动后发送 `HELLO`（协议版本、插件名/版本、pid、输入/输
  出 schema 的 IPC 序列化）。宿主校验协议版本、插件输入 schema 与配置
  `columns` 是否一致、各代工人输出 schema 是否一致，然后回
  `HELLO_ACK{ok}`；工人回 `READY` 进入空闲。握手有超时
  （`handshake_timeout_ms`），失败计为启动失败，连续超限（默认 4 次）中
  止作业。
* **任务**：`TASK{ batch_id, generation, attempt, rows } + RecordBatch`。
  `RESULT/BATCH_ERROR` 必须带回同一 `(batch_id, generation, attempt)`。
* **关闭**：宿主向空闲工人发 `SHUTDOWN`，工人回 `BYE` 后退出；
  `bye_timeout_ms` 内不配合则按 SIGTERM→宽限→SIGKILL 处理。
* **长度/完整性**：任何帧先过 16 字节头校验——magic 不符、类型未知、
  **声明长度超过 `max_frame_bytes`** 都立即判协议违规，杀掉该工人进程
  组；宿主**不会**按声明长度分配/等待。EOF 时缓冲区里还有半个帧同样判
  违规。违规帧之后该管道不再解析（防帧错位），只排空等 EOF。

## 插件 SDK 合同

插件是单个 Python 文件，提供 `create()`（或模块级 `PLUGIN`）返回实现：

```python
class Plugin:
    name: str; version: str
    def input_schema(self)  -> pa.Schema   # 首列必须 row_id:int64
    def output_schema(self) -> pa.Schema   # 首列必须 row_id:int64
    def transform(self, batch: pa.RecordBatch) -> pa.RecordBatch
```

合同（宿主侧强制执行，工人侧也自检）：

1. 输出行数 == 输入行数；
2. 输出 `row_id` 列与输入**逐值、按序**相等——乱序/篡改/丢失/重复都拒
   绝，绝不按位置盲接；
3. 输出 schema 与握手声明的 `output_schema` 完全一致（列名、顺序、类
   型）；
4. 重复响应（同批第二个 RESULT）判协议违规。

业务语义：

* **业务 invalid 是数据**：插件通过自己的有效性列（如
  `is_valid/invalid_reason`）表达，该批整体仍是成功；
* `transform` 抛异常 → 工人发 `BATCH_ERROR`，整批执行失败（可重试）；
* 进程崩溃/超时/协议违规 → 宿主判整批执行失败。两类失败在交付中可区分
  （`error.cause`）。

### 演示插件

* `colbatch/plugins/normalize_text.py`：解析可空的时间/金额文本。
  时间格式 `YYYY-MM-DD HH:MM`、`YYYY-MM-DDTHH:MM:SSZ`、`DD/MM/YYYY`、
  `YYYY-MM-DD`，规范化为 `YYYY-MM-DDTHH:MM:SSZ`；金额支持货币符号
  （€/$/¥）、千分位逗号、`(x)` 负数、前导负号，规范化为两位小数字符
  串。null 输入 → null 输出且有效；无法解析 → 对应 `bad_ts` /
  `bad_amount`（可并列）。
* `colbatch/plugins/crossfield_derive.py`：列间校验 + 派生列。
  `qty>0`、`currency ∈ {USD,EUR,CNY}`、`unit_price` 为纯十进制（可空，
  空则 `total` 为 null）、时间窗 `end>=start`；派生 `total=qty*price`
  （两位小数）与 `duration_s`。错误按固定顺序拼接：`bad_qty,
  bad_currency, bad_price, bad_ts, bad_window`。

### 参考转换器与校验器

`tests/reference.py` 是**独立**的参考实现（不 import 插件代码），逐行从
源 JSONL 计算每行应有的成功/invalid 结果。`tests/verify.py` 按 `row_id`
核对：成功流行号严格递增、输出列值与参考完全相等、source 未被篡改；失
败流每行有非空 cause；两流并集 == 期望集合且无交集（恰好一次）；故障批
的每行在失败流中有且仅有一次隔离证据。

```bash
python3 tests/verify.py --config run.json [--expect-failed-batches 1,5] \
    [--through-batch K]   # 取消场景只核对已交付前缀
```

## 批身份、代次与恰好一次

* 批身份 = `(batch_id, generation, attempt)`。`batch_id` 全局唯一不复
  用；工人槽位被替换时 `generation` 递增；每次重试 `attempt` 递增。
* 批状态机：`queued → dispatched →(done_success | done_failed)`，重试回
  到 `queued`。**只有**当前 `(slot, generation, attempt)` 与帧身份完全
  匹配、且批处于 `dispatched` 时才接受结果；否则：健康工人身份不符 →
  协议违规杀掉；已判死工人/旧代次的帧 → 计 `stale_frames` 丢弃。
* 超时/崩溃/违规后，旧尝试的结果**永远**不再被接受——即使它随后到
  达；重试在新工人（新代次）上进行。因此一个源行要么成功一次、要么失
  败一次，绝不重复。交付来自单一有序 `completed` 映射，失败只在重试耗
  尽后写一次。
* “队列里有结果”不等于“当前任务完成”：每工人独立管道 + 身份校验，
  等待批 N 的结果时，批 M 的旧帧不会被错接。
* 取消（SIGINT/SIGTERM）：封闭派发与读源，所有工人按
  SIGTERM→宽限→SIGKILL 回收，在途批丢弃不重试，已完成的连续前缀照常
  交付；manifest 记录 `delivered_batches` 与 `last_delivered_row_id`，
  即输出只覆盖输入的 `[0, K)` 前缀。

## 背压与预算

硬约束（配置项）：

| 配置 | 含义 |
|---|---|
| `max_inflight_batches` | 管道内驻留批数上限（排队+在途+已完成待交付合计） |
| `max_pending_batches` / `max_pending_bytes` | 已完成待交付批数/字节，触发即停止读源 |
| `max_frame_bytes` | 单帧上限，超限声明即违规 |
| `writer_queue_batches` | 主循环→writer 的有界交接队列 |

读源门控：`驻留批数 < max_inflight_batches 且 pending < 上限 且
pending_bytes < 上限` 才继续读。慢 sink → writer 队列满 → 待交付积压到
`max_pending_batches` → 停止读源；在途批完成后也无法新增在途，全部有
界。IPC 缓冲天然有界（每工人 ≤ 1 帧 + 内核管道），并在 stats 中暴露
`ipc_buffer_bytes` 水位。并行批可乱序完成，交付严格按输入序（重排缓冲
同样计入预算）。

## 资源限制（Linux rlimits）与宿主预算的边界

工人在 `fork/exec` 之间（`preexec_fn`，仅做 setrlimit）设置：

* `rlimits.cpu_seconds` → `RLIMIT_CPU`（软限 N、硬限 N+1，保证先收到可
  观测的 SIGXCPU 再到 SIGKILL）：限制**CPU 时间**；
* `rlimits.as_bytes` → `RLIMIT_AS`：限制工人**虚拟地址空间**，插件分
  配超限得 `MemoryError`（工人上报 `plugin_error:memory_error`，属数据
  级失败，不杀进程）；
* `rlimits.nofile` → `RLIMIT_NOFILE`：限制 fd 数。

边界划分（两者互补，缺一不可）：

* rlimits 管**工人进程内部的资源**（CPU、地址空间、fd）。它看不见协议
  语义：工人把 10GB 写进管道后才被 AS 限制拦住也没关系——宿主侧早已
  按 `max_frame_bytes` 拒绝读取。
* 宿主字节预算管**协议与队列**：帧上限、待交付上限、驻留上限。它管不
  了工人烧 CPU 或泄漏内存——那是 rlimits 的事。
* 进程**数量**靠 `setsid` + 进程组 SIGTERM/SIGKILL 回收（含插件派生的
  子进程），不靠 `RLIMIT_NPROC`（它按 uid 计数，会误伤宿主自身）。
* 即使宿主自己有 bug，`_finalize` 也会对所有存活工人组做
  SIGKILL+wait4 清扫，保证不留孤儿。

## 故障注入与测试

故障注入只存在于工人壳（`worker.py`），由环境变量
`COLBATCH_FAULT_SPEC` 指向的 JSON 驱动，键为 `b<批号>@<尝试>` /
`b<批号>` / `*`，值为动作列表。正常作业不使用。

动作目录：前置 `ignore_term, exit:<code>, hang:<s>, task_delay:<s>,
barrier:<dir>:<n>, burn_cpu, alloc:<MB>, spawn_child_then_hang:<pidfile>`；
发送侧 `gated_result_then_exit:<gate>, tamper_row_id, bad_row_count,
bad_schema, half_frame, huge_len, duplicate_response`。

`tests/test_host.py` 的 18 个场景全部用统计文件轮询同步（不用"睡一会
没报错"当通过）：

| 场景 | 验证点 |
|---|---|
| `happy_normalize` / `happy_crossfield` / `scale_normalize` | 两插件正确性、5000 行规模、水位 ≤ 预算、BYE 干净关闭、RSS 记录 |
| `barrier_stale` | 屏障保证批2先完成、批1卡住超时；批3在复用工人的槽上执行期间，旧代次批1响应到达被丢弃（`stale_frames`），批3内容准确；槽位以新代次复用并处理后续批 |
| `exit_crash` | `os._exit(42)` → 崩溃检测、退出码记录、有限重试后成功、恰好一次 |
| `half_frame` | 半帧+EOF → `viol_truncated_frame`，重试成功 |
| `huge_len` | 2^40 声明长度 → 立即 `viol_oversized_frame`，宿主不分配不等待 |
| `tamper_row_id` / `bad_row_count` / `bad_schema` | 篡改 row_id / 行数错 / schema 错 → 拒绝并隔离，其余批不受影响 |
| `duplicate_response` | 重复响应判违规，批仍只交付一次 |
| `spawn_child` | 插件派生忽略 SIGTERM 的子进程 → 进程组 SIGKILL 回收，`/proc` 验证无孤儿 |
| `slow_sink` | 慢 sink（每批 150ms）→ 读源被反向限速：全程驻留 ≤ 上限、pending 触顶、运行时长被 sink 主导 |
| `cancel` | SIGINT → 退出码 3，manifest 标明交付前缀 `[0,K)`，校验器按前缀核对，无遗留进程 |
| `rlimit_as` / `rlimit_cpu` | 4GB 分配被 `RLIMIT_AS` 拦成 `memory_error`；死循环被 `RLIMIT_CPU` 以 SIGXCPU 杀掉（退出日志 signal=24） |
| `intake_errors` | 非法 JSON 行 → 单行 `intake_error` 失败，不影响其他行 |
| `bad_plugin_path` | 插件加载失败 → 启动重试有界，中止且不留进程 |

每个场景结束还扫描 `/proc` 确认无 `worker.py` / 故障子进程残留；预算与
回收用实测 IPC/队列水位、RSS（`/proc/<pid>/status` + `wait4` 的
`ru_maxrss`）和进程清单证明。

## 统计字段（stats.json / manifest.json）

* `gauges`：`inflight_batches`（排队+在途）、`pending_batches/bytes`（待
  交付）、`retained_batches`（驻留合计）、`ipc_buffer_bytes`、
  `writer_queue`、`dispatch_queue`；
* `hwm`：上述各量的历史高水位；
* `counters`：`batches_formed/dispatched/completed/failed`、`retries`、
  `worker_timeouts`、`worker_crashes`、`protocol_violations` 及分类
  （`viol_oversized_frame`、`viol_truncated_frame`、
  `viol_row_id_mismatch`、`viol_row_count_mismatch`、
  `viol_schema_mismatch` …）、`stale_frames`、`plugin_errors`、
  `workers_spawned`、`workers_killed`、`handshakes`、`clean_bye`、
  `intake_errors`；
* `workers[]`：槽位、代次、pid、状态、当前批、RSS、管道缓冲字节；
* `inflight[]`：在途批（批号、槽位、代次、尝试）；
* `completed_log`：每批结局（成功/失败、cause、槽位、代次、尝试数）；
* `worker_exit_log`：每次工人退出（退出码/信号、`ru_maxrss`、原因）。

## 配置参考

见 `examples/*.json`。必填：`input`、`success_out`、`failure_out`、
`plugin.path`、`columns`（`name/type/field`，type ∈ `utf8,int64`）。
常用可调：`batch_size, workers(≥2), max_attempts, batch_timeout_ms,
kill_grace_ms, handshake_timeout_ms, bye_timeout_ms,
max_inflight_batches, max_pending_batches, max_pending_bytes,
max_frame_bytes, writer_queue_batches, stats_interval_ms, rlimits.*,
fault_spec`（仅测试）、`sink_delay_ms_per_batch`（仅测试，模拟慢
sink）。相对路径一律相对配置文件所在目录解析。

## 数据质量画像与准入门禁（preflight）

`colbatch preflight --config run.json` 在启动昂贵的并行清洗**之前**，对原始
输入做完整扫描，产出可复核的质量报告并给出准入结论，**不启动插件工人、
不创建或截断清洗输出**，也不改变插件结果合同。完整文档见
[`docs/preflight.md`](docs/preflight.md)。

要点：

* **不可变快照**：先把输入流式复制为带 SHA-256、内容寻址、只读的私有快照，
  画像只针对快照；复制前后比对文件身份与头尾哈希，源文件在复制期被替换/
  改写则拒绝发布。快照与报告成组原子发布，任一步失败保留旧组合。
* **单遍、内存有界、精确基数**：快照顺序读一遍，RAM 只留 O(列数) 定长聚合；
  精确不同值写入临时 SQLite 磁盘唯一索引（非近似、非分批求和），临时状态
  独占创建并在成功/失败/取消后清理。
* **确定语义**：缺失按“字段不存在 / JSON null / 可配置空白串”三分；带类型
  标记的规范键保证字符串 `"1"`、整数 `1`、浮点 `1.0`、布尔 `true` 等跨类型
  不碰撞；空集、坏 JSON、末尾无换行、Unicode、负数、零、极大整数均有确定行为。
* **规则**：缺失比例上限、精确唯一性下限/上限（含比例）、数值 min/max、
  允许值集合、重复 id 上限，可组合，边界相等通过；非数字值不静默跳过数值检查。
* **门禁接入 run**：`quality.enabled=true` 时，run 必须存在规则摘要匹配、
  快照哈希校验通过的 admitted 报告，并以绑定快照作为输入（manifest 记录快照
  摘要）；不通过、报告过期、摘要不符或快照损坏时绝不启动工人、不改清洗输出。
  禁用时 run 行为完全兼容。
* **退出码**：preflight `0` 准入 / `2` 质量拒绝 / `3` 配置错误 / `4` 数据错误 /
  `5` 画像或发布执行错误——“质量不满足”与“画像执行失败”严格区分。
* **独立参考**：`tests/reference_profiler.py` 用纯内存集合独立实现画像与规则，
  `tests/test_preflight.py` 通过动态多批次输入逐项核对计数、精确基数、数值范围、
  规则命中与准入，并用超内存预算的数据证明 RSS 有界（产品 ~38MB vs 内存参考
  ~72MB / 15 万行）。

## 已知限制

* 不防御恶意插件：同 uid、无 seccomp/命名空间隔离；插件可读写文件、联
  网。故障隔离针对"坏运气/坏质量"的插件，不是针对攻击者。
* 单进程宿主崩溃不续跑；插件若有外部副作用，重试不保证恰好一次。
* 批内行数由 `batch_size` 决定；非法 JSON 行会形成单行失败批，使后续批
  号偏移（校验器在含非法行时不支持 `--expect-failed-batches`）。
* 非通用 DAG：单插件、单阶段、保序交付。

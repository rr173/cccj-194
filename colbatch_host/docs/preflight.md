# colbatch preflight：数据质量画像与准入门禁

`colbatch preflight` 在**不启动任何插件工人、不创建或截断清洗输出文件**的
前提下，对原始 JSONL 输入做一次完整扫描，生成可复核的质量画像报告，并按
配置规则给出**准入（admit）/ 拒绝（reject）**结论。它解决的问题是：在投入
昂贵的并行清洗之前，先判断整份输入是否满足业务质量门槛。

本能力不改变既有插件结果合同；未启用门禁时，`colbatch run` 的行为与以前
完全一致。

---

## 1. 命令用法

```bash
# 生成/刷新快照与报告（不会动 success_out / failure_out，不会起工人）
python3 -m colbatch.cli preflight --config run.json

# 门禁启用时，run 只读取报告绑定的不可变快照
python3 -m colbatch.cli run --config run.json
```

`preflight` 把报告 JSON 打印到 stdout，把一行人类可读结论打印到 stderr。

### 退出状态（与 `run` 刻意区分）

| 码 | 含义 |
|---|---|
| `0` | 画像成功且**全部规则通过（admitted）**，快照+报告已原子发布 |
| `2` | 画像成功但**质量规则未通过（rejected）**；报告正常发布，可据此复核 |
| `3` | **配置错误**：规则引用未知列、规则值类型不匹配、阈值非法、输入路径缺失等 |
| `4` | **数据错误**：存在无法解析的 JSON 行/非对象行，输入不可画像，**不发布**报告 |
| `5` | **执行错误**：快照复制期源文件变化、临时目录/磁盘故障、快照发布失败、取消等 |

关键区分：`2` 是“数据质量不满足规则”，`3/4/5` 是“画像无法成立/配置不合法”。
后三者绝不能被解读成质量未通过——它们要么不发布报告，要么在
`last_error.json` 中记录结构化执行错误。

`run` 的退出码保持原合同（`0` 完成 / `3` 取消 / `1` 中止或门禁拦截）。

---

## 2. 配置格式

在原有 run 配置中增加一个 `quality` 段即可。未配置或 `"enabled": false`
时，`run` 走原有路径，完全兼容。

```json
"quality": {
  "enabled": true,
  "blank_policy": "whitespace",
  "id_field": "order_id",
  "columns": [
    {"name": "qty",   "type": "int64",   "field": "qty"},
    {"name": "price", "type": "float64", "field": "price"},
    {"name": "currency", "type": "utf8", "field": "currency"}
  ],
  "rules": [
    {"id": "qty-missing", "rule": "max_missing_ratio", "column": "qty", "value": 0.05},
    {"id": "currency-set", "rule": "allowed_values", "column": "currency",
     "values": ["USD", "EUR", "CNY"]}
  ],
  "work_dir": ".colbatch_preflight",
  "insert_batch_rows": 2000,
  "read_block_bytes": 1048576
}
```

字段说明：

| 字段 | 说明 |
|---|---|
| `enabled` | 是否在 `run` 时强制门禁。`preflight` 无论取值都可运行。 |
| `blank_policy` | 空白字符串计数策略：`off` / `empty`（仅 `""`）/ `whitespace`（`""` 或纯空白，默认） |
| `id_field` | 可选。用于统计重复 id 的记录字段（独立于列定义）。 |
| `columns` | 可选。画像列定义；缺省继承宿主 `columns`。preflight 在 `utf8/int64` 之外额外支持 `float64`（宿主插件 schema 仍只接受 utf8/int64）。 |
| `rules` | 规则列表，可任意组合；每条规则需唯一 `id`。 |
| `work_dir` / `snapshot_store` / `temp_dir` / `report_path` | 产物目录，默认在配置文件旁的 `.colbatch_preflight/` 下。相对路径相对配置文件解析。 |
| `insert_batch_rows` | SQLite 提交批大小（有界事务），默认 2000。 |
| `read_block_bytes` | 单遍读取缓冲与快照分块大小，默认 1 MiB，下限 4096。 |

### 规则目录与边界语义

边界相等一律**通过**（`<=` / `>=` 语义），越界一步才失败。

| `rule` | 参数 | 语义（边界） |
|---|---|---|
| `max_missing_ratio` | `value` ∈ [0,1] | 缺失比例 `<= value` 通过 |
| `min_distinct` | 非负整数 | 精确不同值数 `>= value` 通过 |
| `max_distinct` | 非负整数 | 精确不同值数 `<= value` 通过 |
| `min_distinct_ratio` | [0,1] | 不同值/记录数 `>= value` |
| `max_distinct_ratio` | [0,1] | 不同值/记录数 `<= value` |
| `numeric_min` | 数值 | 数值列最小值 `>= value`；存在非数字值或无可解析数字时**失败**（不静默跳过） |
| `numeric_max` | 数值 | 数值列最大值 `<= value`；同上 |
| `allowed_values` | 非空 `values` 列表 | 列中出现任何集合外的**不同值**即失败，报告给出违规不同值计数与样本 |
| `max_duplicate_ids` | 非负整数 | `id_field` 的重复出现次数 `<= value`（需配置 `id_field`） |

规则在配置加载时即完成类型校验：数值规则只能挂在 `int64/float64` 列；
int64 列的阈值必须是整数；`allowed_values` 的每个值必须与列类型匹配，
且不允许 `null`（缺失有独立语义）。任何不合法都返回退出码 `3`，不会
伪装成质量未通过。

---

## 3. 缺失语义与类型规范（确定性）

### 3.1 缺失的三种来源，分别计数

对每条记录的每个字段：

| 情况 | 分类 | 是否计入缺失 |
|---|---|---|
| 对象中**不存在**该字段 | `missing_field` | 是 |
| 字段存在且为 JSON `null` | `null` | 是 |
| 字段为字符串且命中 `blank_policy` | `blank_string` | 是 |
| 其它任何值 | `present` | 否 |

报告的 `missing.total = missing_field + null + blank_string`，
`missing.ratio = total / record_count`（空数据集时为 `0.0`）。
三种来源在报告中**分列保留**，可单独复核。

### 3.2 规范表示：跨类型绝不碰撞

精确不同值统计基于带**类型标记前缀**的规范键：

| JSON 类型 | 键前缀 | 示例 |
|---|---|---|
| 整数 | `i:` | `1` → `i:1` |
| 浮点数 | `f:` | `1.0` → `f:1.0` |
| 字符串 | `s:` | `"1"` → `s:1` |
| 布尔 | `b:` | `true` → `b:true` |
| null | `n:` | （缺失口径处理，不参与不同值） |
| 数组/对象（嵌套 JSON） | `j:` | 确定性 canonical JSON（键排序、无空格） |

因此字符串 `"1"`、整数 `1`、浮点 `1.0`、布尔 `true` 与字符串 `"true"`
是 5 个互不相同的值；`{"a":1}` 与 `{"a":"1"}` 也互不相同。嵌套对象的键
顺序不影响其规范表示。浮点使用 IEEE-754 位语义：`+0.0` 与 `-0.0` 归一，
所有 NaN 归一为单个 `f:nan`；Python 解析器接受的非标准 `NaN/Infinity`
记号被拒绝（按坏 JSON 处理）。布尔值永不被当作数字。

### 3.3 边界与特殊数据的确定行为

* **空数据集（0 字节或 0 行）**：合法，记录数 0，各计数为 0、比例为
  `0.0`；`min_distinct:0` 等通过，数值 min/max 为 `null`。
* **文件内部的空行 / 坏 JSON 行 / 非对象行 / 非法 UTF-8**：计入
  `bad_lines`（带 1 起始行号与原因样本，上限 20 条），整份输入判数据
  错误（退出码 4），不发布报告/快照。
* **末尾无换行**：最后一行仍恰好计数一次；末尾有换行不会制造空行。
* **重复 row_id / id**：画像不去重、不丢行，记录数等于物理好行数；
  重复程度通过 `id_field` 的 `duplicates` 精确统计。
* **Unicode**：按 UTF-8 解码，报告 `ensure_ascii=false` 原样保留。
* **负数、零、极大整数**：int64 全范围（含 ±2^63）精确处理；超出
  int64 的整数记该列解析失败。
* **小数精度**：以解析后的 IEEE-754 double 为画像口径；同值的不同
  十进制写法（如 `0.1` 与 `0.10`）规范键相同。

---

## 4. 报告字段

报告经确定性 JSON 编码（键排序、固定分隔符、UTF-8），同一输入与同一
规则重复运行产出**字节一致**的业务内容。顶层结构：

```jsonc
{
  "schema_version": 1,
  "status": "admitted | rejected",       // 执行成功时；失败时无报告
  "input": {
    "source_ref": "input.jsonl",         // 稳定相对标识，绝不出现机器绝对路径
    "snapshot": {
      "ref": "snapshots/snap-<sha256>.jsonl", // 相对报告目录
      "sha256": "…",                      // 扫描所依据快照的全量哈希
      "size_bytes": 1602,
      "content_addressed": true,
      "immutable": true
    }
  },
  "rule_summary": "sha256:…",            // 规则+缺失语义+列集合的指纹
  "blank_policy": {"mode": "whitespace"},
  "id_field": "order_id",
  "id": {"field": "…", "present": 12, "distinct": 10, "duplicates": 2},
  "summary": {
    "record_count": 30, "bad_line_count": 0, "column_count": 5,
    "rule_count": 6, "rules_passed": 5, "rules_failed": 1,
    "profiler_temp_db_bytes": 12288
  },
  "columns": [ { /* 每列画像，见下 */ } ],
  "rules": [   { /* 每条规则的结论，见下 */ } ]
}
```

每列（`columns[]`）：

```jsonc
{
  "name": "qty", "field": "qty", "type": "int64", "records": 30,
  "present": 28,
  "missing": {"total": 2, "missing_field": 1, "null": 1,
              "blank_string": 0, "ratio": 0.0667},
  "distinct": 7, "distinct_ratio": 0.2333,
  "parse_failures": 0,
  "numeric": {"count": 28, "min": -3, "max": 100}, // 非数值列为 null
  "rule_ids": ["qty-missing", "qty-range"]
}
```

每条规则（`rules[]`）给出**实际值、阈值、是否通过、原因**：

```jsonc
{
  "id": "qty-missing", "rule": "max_missing_ratio", "column": "qty",
  "threshold": 0.05, "actual": 0.0667,
  "passed": false,
  "reason": "missing ratio 0.066667 exceeds bound 0.050000"
}
```

路径标识：配置目录内的路径渲染为相对路径；目录外的输入渲染为
`external:<去根的稳定尾路径>`，报告任何位置都不含机器绝对路径。

执行失败时不写报告，而在 `work_dir/last_error.json` 写
`{"status":"error","error":{"kind","message","detail"?}}`，与质量拒绝
明确区分。

---

## 5. 快照、单遍画像与资源边界

### 5.1 不可变快照与“扫描期间无版本混入”

1. 复制前记录源文件的 `(dev,inode,size,mtime)` 与**头/尾内容哈希**；
2. 以 `O_CREAT|O_EXCL` 独占创建临时文件，**流式分块**复制，同时计算
   全量 SHA-256（不需要二次读源）；
3. 复制后**发布前**再次比对身份与头尾哈希：inode 变（被替换）、size
   变或内容哈希变（原地改写）都拒绝发布并清理；
4. 通过后把快照置为只读（0444），按内容哈希命名
   `snap-<sha256>.jsonl` 原子 rename 进快照存储，再原子发布报告。

仅在报告生成后比对输入哈希无法证明扫描期间没混入新版本——本实现的
发布前复核正是为了堵住这个缺口。

### 5.2 单遍、内存有界、精确基数

* 快照被**顺序读取恰好一遍**，读缓冲固定为 `read_block_bytes`；
* RAM 中只保留 O(列数) 的定长聚合（计数、min/max），**不保留任何输入
  行、也不保留全部不同值**；
* 精确不同值写入临时 **SQLite 磁盘唯一索引**
  （`WITHOUT ROWID` 主键 `(col_idx, canonical_key)`，page cache 封顶约
  2 MiB）。某键存在与否是精确判断，**不使用 HLL 等近似基数，也不按批
  分别计数后求和**；
* 临时 DB 位于独占创建的临时目录，成功/失败/取消后一律删除；
* 实测（见测试 `scale_bounded_memory`，15 万行 / 14.9 MiB）产品进程峰值
  RSS 约 38 MB，而把全部不同值放进内存集合的独立参考实现约 72 MB，
  且产品占用不随不同值总数线性膨胀。

### 5.3 成组原子发布与生命周期

* 快照与报告是**一组**：快照先入存储，报告后 `os.replace`；报告写入
  失败会回滚本次新建的快照，旧的有效组合原样保留；
* 重新 preflight 内容不变时复用同一内容寻址快照，报告字节一致；
  内容变化时发布新组，并仅回收存储中**不再被任何报告引用**的摘要
  文件；外任务文件（非 `snap-*.jsonl`）绝不触碰；
* 删除/替换报告不会误删其它任务的文件。

---

## 6. `run` 如何消费门禁

`quality.enabled=true` 时，`run` 在做任何清洗动作之前：

1. 读取并校验报告：`schema_version` 受支持、`status == "admitted"`；
2. 比对 `rule_summary` 与当前配置指纹——规则或缺失语义改过即判**过期**，
   拦截；
3. 解析报告绑定的快照相对引用，重新计算 SHA-256 校验——快照损坏/缺失
   即拦截；
4. 全部通过后，**以该不可变快照作为本次清洗输入**，绝不重新打开可变的
   原路径；manifest 记录 `input_source="quality_gate_snapshot"` 与快照
   摘要（sha256、rule_summary、报告状态）。

以下任一情况都**不启动任何工人、不创建/截断 success/failure 输出**：
报告缺失、报告非 admitted、规则摘要不符（过期）、快照损坏/缺失、配置
错误。门禁禁用时维持原有 `run` 行为（`input_source="configured_input"`）。

---

## 7. 限制说明

* 画像面向 JSONL 记录的列级质量；不做跨文件/跨表校验，不推断业务语义。
* `float64` 仅用于 preflight 画像；插件宿主 schema 仍限 utf8/int64。
* 精确基数依赖本地临时磁盘（SQLite）；磁盘不可用或配额耗尽按执行错误
  （退出码 5）处理并清理，不会给出近似结果。
* 快照与报告保证发布时刻的原子性与一致性；宿主进程整体崩溃后的续跑不
  在本能力范围内（与既有宿主非目标一致）。
* 不防御对快照存储目录具备写权限的本地攻击者主动篡改；篡改会被 run
  的哈希校验发现并拦截，但不提供权限隔离。
* 坏 JSON 行采用“先完整扫描、汇总后一次性判数据错误”的策略，不在首个
  坏行处提前终止，以便一次列出全部问题样本（样本上限 20 条）。

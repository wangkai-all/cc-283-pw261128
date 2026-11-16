# lazybench — 延迟入仓工作台

分析者首次只取大文件的少数列，之后按需补选更多列，系统不反复完整读取源文件。
本项目从零实现：真实列存接入、列构建任务（可停止/恢复）、快照隔离查询、
全量导入对照、以及一个可操作的网页工作台。

## 运行

```bash
python3 -m lazybench.server --port 8000 --data ./data
# 打开 http://127.0.0.1:8000
```

测试（15 个端到端用例）：

```bash
python3 -m unittest lazybench.tests.test_core -v
```

## 核心问题与解法

新增列必须和**原先导入的行**精确对应，绝不能按变化后的文件行号拼接。
为此，导入时（只）建立两个行级清单，之后所有补建都锚定它：

- **行定位**：`rowindex.bin` 记录每一行在源文件中的字节偏移（u64 数组），
  补建按偏移寻址，支持断点续建；
- **行身份**：`rowhash.bin` 记录每一行原始字节的 blake2 哈希（8B/行），
  每次读取逐行校验——源文件一旦被改动（删行/改值/插入），哈希立即不符，
  补建**中止**而不是悄悄拼接错位数据；
- **结构版本**：schema + 行数 + 源哈希 + 行哈希总校验和构成一个
  structure version。源文件替换会产生新版本，旧版本的列与补建结果全部
  保留可识别，不混用。

## 三个策略旋钮的权衡（界面实时展示存储/耗时/可变性影响）

| 策略 | 额外存储 | 补建耗时 | 来源可变性的影响 |
|---|---|---|---|
| 保存源副本（快照） | 一份完整源文件 | 最快且恒定（读本地快照） | **免疫**：源被改/被删也能补建旧版本列 |
| 只留行索引+哈希（引用外部源） | ~16 B/行 | 每次补建重扫外部源相应区段 | 源变更→补建中止（可识别的失败，不发布错位数据）；仅追加且末段按导入时 EOF 截断时仍可安全补建 |
| 一次多读一些列（预取） | 预取列的段空间 | 导入耗时增加，之后这些列零补建 | 与上面两种正交，可组合 |

行索引+哈希本身约 16 B/行（10 万行 ≈ 1.6 MiB），是三种策略的共同底座。

## 安全不变量

- **补列**：逐行哈希校验 + 按原始行序号对齐；段先写 `staging/`，
  完成后追加行偏移表与页脚，再原子发布。
- **发布**：清单按代（generation）原子切换（写临时文件 + rename）；
  查询钉住开始时的代，段文件不可变，并发发布不会撕裂读取；
  旧段由引用计数保护，GC 只回收无任何活跃读者的代。
- **源替换**：产生新结构版本；在途构建若锚定旧版本，完成时标记
  `stale` 并移入 `quarantine/`（可识别、可审计），绝不发布；
  激活的过滤规则自动停用（列集已变），规则本身保留。
- **行过滤规则**：只在查询时生效，永不改写存储；调整规则不需要重建任何列。
- **查询并发**：每行结果带稳定 `row_id`；演示数据中 `id == row_id`，
  测试与页面都可直接验证对齐。

## 列段格式（真实列存，可按行随机访问）

```
LSEG1 | u16 列名长 | 列名 | u32 行数 | 16B 校验和        # 头部
(u32 长度 | 字节) × N                                    # 值区（0xFFFFFFFF = NULL）
u64 行偏移表 × N                                          # 每行在值区的偏移
u64 偏移表起点 | LENDLSEG                                 # 页脚
```

构建中的 `staging/*.segpart` 即检查点：停止/崩溃后扫描该文件得到精确
续建位置（截断半写的尾部），恢复不重复、不跳行。

## 网页工作台

- 每列**来源**：构建 ID、origin（prefetch/lazy/full）、结构版本、源哈希、
  是否读自快照、校验和、字节数、耗时；
- 构建任务：进度条、**停止/恢复**、状态（running/paused/done/stale/aborted）；
- 存储构成条：快照/行索引/列段/在途/全量基线/隔离区；
- 事件审计流：导入、发布、暂停、stale、检测到源变更、源替换、GC；
- **对照全量导入**：急切基线放入独立 `fullimport/`，逐列比对行数与内容
  校验和，不一致时给出具体 `row_id` 样本；未补建的列标为“待补建”而非错误；
- 旧结构版本的已发布列单独列表展示（旧补建结果可识别）。

## API 摘要

```
POST /api/datasets                     {name, path, keep_snapshot, prefetch[]}
GET  /api/datasets/<n>                 概览（列/构建/存储/事件/突变状态）
POST /api/datasets/<n>/columns/<c>/build
POST /api/datasets/<n>/builds/<id>/stop | /resume
POST /api/datasets/<n>/source          {path}  替换源 → 新结构版本
POST /api/datasets/<n>/query           {columns, limit, offset, rule?, count_only?}
POST /api/datasets/<n>/filters         {column, op, value, activate}
POST /api/datasets/<n>/full-import     急切全量基线（后台）
GET  /api/datasets/<n>/compare         延迟补建 vs 全量基线
POST /api/datasets/<n>/gc              回收无引用的旧段
POST /api/demo/source | /api/demo/mutate   生成/变更演示源
```

## 代码结构

```
lazybench/
  storage.py    行索引/行哈希、列段格式、分代清单、读者钉住、GC、存储统计
  ingest.py     结构版本构建、源快照、源替换、带校验的行读取（MutationError）
  builder.py    构建任务：分块、停止/恢复、崩溃恢复、发布/隔离
  query.py      快照隔离查询、查询时行过滤
  baseline.py   全量导入基线 + 对照
  server.py     HTTP API + 静态页
  static/index.html  工作台网页
  demo_data.py  演示数据生成与源变更
  tests/        15 个端到端测试
```

限制：行索引按物理行定位，含引号内换行的 CSV 会在导入时被明确拒绝；
列值按字符串存储，过滤比较在两侧可解析为数值时按数值比较。

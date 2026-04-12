# Chunky Monkey v2 最终审计报告与架构优化计划

## 一、 审计与测试总结 (Phases 1-4)

经过对全项目代码的深度阅读、无用代码清理（Step 0）、以及全面单元测试覆盖，系统目前的基础健壮性得到了验证：
1. **测试基线建立**：从 0 开始构建了完整的 pytest 测试网，覆盖了 `utils`, `constants`, `db` 的基础设施，验证了 `routers` 的路由连通性，并通过纯函数断言测透了 `etf_grid_engine`、`etf_mining_engine` 和 `scoring` 引擎的核心逻辑。当前自动化集成与单元测试用例超 40 个且全部通过。
2. **死代码清扫**：移除了跨越后台路由层、计算分析层等多个模块内未使用的 `json`、`subprocess` 及遗留配置依赖。
3. **数据分层隔离验证**：经全库扫描确认，计算引擎（如 Qlib 引擎、网格回测引擎等）完全解耦了写入行为，遵守了纯函数 `Dict/JSON` 输出的设计；所有数据落地均通过特定的上层 `mart / fact` 更新网关，符合 `raw -> dim -> fact -> mart` 的基本数据流动准则。

## 二、 核心架构隐患预警 (Critical Findings)

尽管业务计算逻辑隔离得当，底层基础设施仍存在巨大的稳定性隐患，尤其在面对规模化并发写入时：

### 1. 嵌套事务导致的 SQLite Deadlock (`SQLITE_BUSY`) 风险
- **源头定位**：在 `routers/updater.py` 等模块中，频繁出现了在外层拥有一个 `db.get_conn()` 连接的 `with` 上下文时，循环体或内层函数再次尝试去获取新的连接或执行未释放的阻塞操作。
- **危害**：由于 SQLite 是文件型数据库（即使开启了 WAL 模式），写锁是库级别的。如果在业务更新的长流程（如：大量股票指标跨网抓取并写入时）发生嵌套等待，极易由于争抢连接池/全局写锁导致整个后端完全卡死（`database is locked` 或者 120s 超时彻底崩溃）。

### 2. 前后端状态计算边界管理
- **现状**：前端 `app.js` 当前重度依赖 `fetch` 拉取后粗滤呈现，并有重试 `showToast`。
- **隐患**：若某些多维度事实的关联聚合直接交由 `routers` 乃至前端进行即席拼接，一旦业务变更，违背了“**单点计算、多处复用**”原则。

### 3. 未被完整覆盖的派生层退役机制 (Schema Versioning)
- **现状**：机构历史行为评级派生表直接基于时间戳滚动，若发生因子及分数权重调整。
- **隐患**：未看到强一致性的“部分覆盖时清空该层及下游重算”机制（或机制非常薄弱），历史数据随时可能因代码层的微调而产生口径分岔。

## 三、 执行级系统优化计划 (Next Steps Action Plan)

结合《项目架构与反模式清单》原则，制定如下优化方案：

### Phase A: 数据库事务结构化改造 (高优先级 - 防雪崩)
1. **重构连接获取链路**：废除或重写业务层的内层 `get_conn`，改造成连接全量由 `routers` 层最上游通过 `Depends` 注入（Dependency Injection），一路向下透传唯一 `conn` 句柄。
2. **拆分批量写入锁**：对于全市场量级的数据爬树与写入（如：akshare 每日快照落地），拆分成 `INSERT INTO` 批操作 (Bulk Insert) 并在外部脚本（如单线程 updater）闭环闭锁，避免占据 FastApi 所在线程池。

### Phase B: 派生库全量可重算改造
1. **引入 Schema 版本号体系**：对 `mart_` 开头的结果集引入类似 `schema_version` 字段。
2. **重算指令隔离**：建立专用的 CLI 脚本重算指令（例如 `python scripts/recalculate_mart.py --version v2 --table x`），更新时无脑 `DELETE FROM mart_xxx WHERE 1=1;` 然后游标由上游 `fact_xx` 全量重放，以此确保核心“单真相源”及“三可原则”。

### Phase C: 遗留前端浮层交互重构
1. 排查前端所有弹窗/浮层组件，确保所有 Modal 全部作为 `body` 的直属子级，解除任何置于 `display:none` 容器内的嵌套问题（彻底消除 CSS 渲染反模式）。

---

## 四、 深度代码复核后的补充意见

> 以下内容基于对项目全部 18,372 行后端代码（19 个核心文件）和 11,971 行前端代码的逐文件深度审阅，对上述三节内容的逐条核实与补充。

### 对"一、审计与测试总结"的核实

**基本结论成立，但有两处需要修正：**

1. **"计算引擎完全解耦了写入行为"这一结论不够精确。** `scoring.py` 的 `calculate_stock_scores()` 和 `calculate_institution_scores()` 内部直接执行 `conn.executemany(INSERT OR REPLACE INTO mart_stock_trend ...)` 和 `conn.executemany(INSERT OR REPLACE INTO mart_institution_profile ...)`。它们不是纯函数——它们既计算又写入。真正的纯计算引擎仅限于 `etf_grid_engine.py`（1065 行，零 SQL）。`etf_mining_engine.py` 和 `etf_engine.py` 也有 DB 读取行为（约 70-80% 纯计算）。
2. **当前测试覆盖了辅助函数的边界条件，但未触及核心业务逻辑。** `scoring.py` 的 `calculate_stock_scores()`（约 1050 行的巨型函数）和 `routers/updater.py`（3217 行）的 19 步流水线完全未被测试覆盖。这两个文件占后端总量的 30%，是最高风险区域。

### 对"二、核心架构隐患预警"的核实与补充

#### 2.1 嵌套事务 / SQLite BUSY（原文第 1 点）— 确认存在，但需细化

**原文描述的"嵌套 `get_conn()` 导致死锁"需要更精确的分类：**

实际代码中存在**三种不同的连接模式**，风险等级不同：

| 模式 | 实例 | 风险 |
|------|------|------|
| **同库嵌套**：外层持有 smartmoney.db 连接，内层再开 smartmoney.db | `updater.py` 主循环每步新开 `get_conn(timeout=120)`，步内函数可能再开连接 | 🔴 高：同库写锁争抢 |
| **跨库并发**：外层持有 smartmoney.db，内层开 market_data.db | `updater.py` 中 6 处 `get_market_conn()` 嵌套在 step 函数中（L1460, L1670, L2157, L2169, L2190, L2267） | 🟡 中：不同文件不争写锁，但 I/O 带宽竞争 |
| **连接风暴**：循环内每次迭代开关连接 | `institution.py` 的 `_latest_daily_close()` 每次调用新开 `get_market_conn()`，被股票列表循环调用 | 🟠 性能：1000+ 次 connect/close |

**原文遗漏的关键发现：**
- `updater.py` 中有 **7 处静默异常吞没** (`except Exception: pass`)，分布在 L75, L213, L971, L1005, L1257, L2297, L2311。其中 L971 位于 `inst_holdings INSERT` 循环内——任何插入失败都被完全忽略，无日志、无计数，数据缺失不可追溯。
- `institution.py` L336-342 的级联删除（6 张表）**没有 `BEGIN IMMEDIATE`**。SQLite autocommit 模式下，如果进程在第 3 条 DELETE 后崩溃，数据库将处于不一致状态（部分表有记录、部分表已清空）。

#### 2.2 前后端状态计算边界（原文第 2 点）— 确认存在，且比描述的更严重

**原文说"若某些多维度事实的关联聚合直接交由前端"，实际上这已经发生了：**

`app.js`（6969 行）内的 `renderStockList()` 和相关函数中：
- 客户端执行了完整的 **Pool/Gate/Signal 聚合统计**（`summarizeStocks()`）
- 客户端执行了 **完整的多维排序**（按 `composite_priority_score`, `attention_score` 等 6 种维度）
- 客户端执行了 **3 层过滤链**（`matchSignalFilter()` + `matchGateFilter()` + `matchAttentionFilter()`）
- 客户端独立维护了 **候选池 Top-N 筛选逻辑**

这不是"若发生"的隐患，而是确定性的违规——同一份排序/筛选逻辑，后端 `scoring.py` 产出评分后，前端又在客户端重新解释、再次判定。一旦后端口径变更（如 Pool 阈值从 75 调整为 70），前端不会同步感知。

#### 2.3 派生层退役机制（原文第 3 点）— 部分存在，不是"完全没有"

**ETF 侧已实现了 `ETF_SNAPSHOT_SCHEMA_VERSION = 6` 的版本校验机制** (`etf_snapshot_manager.py` L11, L355)。当版本变更时，snapshot cache 会被强制全量重建。这是正确的设计。

**但股票侧（`scoring.py` / `return_engine.py` / `stock_stage_engine.py`）完全没有此机制。** `mart_stock_trend`、`mart_institution_profile`、`mart_current_relationship` 这三张核心 mart 表都是直接 `INSERT OR REPLACE` 就地覆盖，没有版本号、没有全量重建入口。

#### 2.4 原文遗漏的四个重大隐患

**隐患 A：巨型上帝函数 `calculate_stock_scores()`**
- 该函数从 `scoring.py` 的约第 1200 行延伸至第 2350 行，**单函数 1050+ 行**。
- 它同时承担：9 张维度表加载 → 5 种评分计算 → 60+ 字段生成 → Pool 分配 → reason 拼接 → mart 写入。
- 嵌套层级可达 20+，无法被单元测试隔离，是项目内最大的技术债务。

**隐患 B：跨文件逻辑重复（"单点计算"原则违反）**
- `_CHANGE_MAP` 在 `event_engine.py` L14 和 `holdings.py` L235 各定义了一份，键值接近但不完全相同。
- 日期解析存在 **5 个并行实现**：`scoring.py:_parse_any_date`, `holdings.py:_parse_date_like`, `capital_client.py:_parse_date`, `financial_indicator_client.py:_parse_date`, `setup_replay.py:_parse_any_date`。
- 趋势/Setup 分类在 `etf_engine.py` 和 `etf_qlib_engine.py` 中各有一套几乎相同但独立维护的实现。

**隐患 C：77 个硬编码魔数**
- 虽然机构评分权重（`INST_SCORE_DEFAULTS`）和跟随性权重（`FOLLOW_SCORE_DEFAULTS`）支持从 `app_settings` 动态加载，但 **其余 77 个数值完全硬编码**：
  - 事件类型得分 (100/70/30/10/0)
  - 溢价区间阈值 (0/5/10/20%)
  - 复合优先级权重 (0.35/0.30/0.20/0.15)
  - Pool 划分门限 (A≥75, B≥60, C≥45, D<45)
  - 收益率公式系数 (5.4, 0.55, 0.8, 2.5, 0.35)
- 这些数值散布在 `scoring.py`, `etf_grid_engine.py`, `etf_mining_engine.py`, `stock_stage_engine.py` 等 8 个文件中，修改任意一个都无法确保一致性。违反"可追溯、可复核"原则。

**隐患 D：全局可变缓存的线程安全**
- `etf_snapshot_manager.py` 使用全局 `_ETF_SNAPSHOT_MEMORY_CACHE` dict。
- `audit.py` 使用 `_AUDIT_CACHE`, `_PLAN_CACHE`, `_TFP_CACHE` 三个全局 dict。
- `industry.py` 使用 `_industry_cache` 模块级变量。
- FastAPI 在多worker模式下（`uvicorn --workers N`），这些缓存仅在进程内有效；在单 worker 异步并发下，如果两个请求同时触发 cache invalidation + rebuild，会产生竞态条件。

### 对"三、执行级优化计划"的核实与补充

#### Phase A（数据库事务改造）— 方向正确，但方案需要调整

**原文方案 "废除内层 get_conn，改成 Depends 注入透传" 对于跨库场景不适用。**

当前系统使用了 **3 个独立的 SQLite 数据库文件**（`smartmoney.db`, `market_data.db`, `etf.db`），很多 step 函数天然需要同时读写两个库。正确的改造方向是：

1. **同库连接：** 确实应该由路由层注入并透传，禁止 service 层自行 `get_conn()`。
2. **跨库连接：** 不应该消除，而是规范化。建议引入 `StepContext` 对象，持有 `{smart_conn, mkt_conn, etf_conn}` 三连接绑定包，在主循环顶部一次性创建、步间复用、末尾一次性关闭。
3. **级联删除：** 所有多表写入操作必须包裹 `BEGIN IMMEDIATE ... COMMIT`，`institution.py` 的删除逻辑是当前最紧急的修复点。

**原文未提及但同等重要的修复：**
- `updater.py` 中 7 处 `except Exception: pass` 必须替换为 `logger.warning(...)` + 失败计数器。
- `_latest_daily_close()` 必须改为接受外部 `mkt_conn` 参数，消除连接风暴。

#### Phase B（派生库可重算）— 方向正确，建议分两步

1. **第一步（低成本）：** 在现有 `mart_stock_trend`, `mart_institution_profile`, `mart_current_relationship` 三张表中增加 `schema_version INTEGER DEFAULT 1` 字段。重算时先 `UPDATE SET schema_version = schema_version + 1`，前端加载时校验版本一致性。
2. **第二步（中成本）：** 仿照 ETF 侧 `ETF_SNAPSHOT_SCHEMA_VERSION` 模式，建立 CLI 全量重建脚本。这一步的前提是先把 `calculate_stock_scores()` 拆解为可独立调用的子模块。

#### Phase C（前端浮层重构）— 优先级应该降低

**经核实，当前唯一的 `position:fixed` 元素是 `modalOverlay`（main.css L3399），它直接是 `<body>` 的子元素（index.html L467），并不嵌套在 `display:none` 容器内。** 这条原文描述的反模式在当前代码中**不存在**。Phase C 可以降级或删除。

#### 建议新增的 Phase D-F

**Phase D：拆解 God Function（最高优先级）**

`calculate_stock_scores()` 应拆分为至少 5 个独立的 scorer 模块：
1. `_score_discovery()` — 事件发现评分（事件类型 + 时效性 + 溢价）
2. `_score_quality()` — 机构质量评分（leader 质量 + 行业命中 + 共识度）
3. `_score_stage()` — 阶段评分（路径 + 趋势 + 技术面）
4. `_score_forecast()` — 前瞻评分（Qlib + 外部关注度）
5. `_assign_pool()` — Pool 分配 + reason 拼接 + mart 写入

这是所有后续测试覆盖和配置外化的前提。

**Phase E：消除逻辑重复（"单点计算"原则落地）**

| 重复项 | 当前分布 | 目标归口 |
|--------|----------|----------|
| `_CHANGE_MAP` | event_engine.py + holdings.py | → `constants.py` |
| 日期解析 | 5 个文件各有一套 | → `utils.py:parse_date()` 统一实现 |
| 趋势/Setup 分类 | etf_engine + etf_qlib_engine | → `etf_classify.py` 共享模块 |
| 溢价/时效等级 | scoring.py 内部重复 | → 各自只出现一次，由 scorer 子模块调用 |

**Phase F：魔数配置外化**

将 77 个硬编码阈值提取到 `app_settings` 表，分为三类：
1. **权重类**（已有部分基础设施）：复合权重、评分卡权重
2. **阈值类**（需新建）：Pool 门限、溢价区间、路径判定
3. **公式类**（最复杂）：收益率系数、行业 edge 系数

每类配置带 `version` 字段和 `effective_from` 时间戳，变更时触发下游 mart 重算。

### 优先级排序总结

| 优先级 | 改造项 | 预估影响面 | 风险等级 |
|--------|--------|-----------|---------|
| P0 | 级联删除加事务 (`institution.py` L336) | 1 文件 | 🔴 数据一致性 |
| P0 | 静默异常替换为日志 (`updater.py` 7 处) | 1 文件 | 🔴 可观测性 |
| P1 | 拆解 `calculate_stock_scores()` | 1 文件 → 5 模块 | 🔴 可测试性 |
| P1 | 连接管理 StepContext 改造 | updater.py + institution.py | 🔴 稳定性 |
| P2 | 逻辑重复消除 | 8 个文件 | 🟡 维护性 |
| P2 | 魔数配置外化 | 8 个文件 | 🟡 可追溯性 |
| P3 | 客户端计算下沉到服务端 | app.js + 新 API | 🟡 一致性 |
| P3 | mart 表 schema version | db.py + scoring.py | 🟡 可重建性 |

---
*Deep review appended after full codebase audit (18,372 LOC backend + 11,971 LOC frontend).*

# 架构评审讨论

最后更新：2026-04-09（修订版：加入数据优先视角与模块化方案）

本文不是需求文档，不是计划文档，是在深度调研数据源能力、模块现状和 `DATA_ARCHITECTURE.md` / `implementation_plan.md` 之后，对整个系统提出的结构性评审意见，每个问题均给出具体解决方案。

---

## 零、设计哲学：数据优先还是决策优先

`DATA_ARCHITECTURE.md` 明确写道：

> 以后不再先从"数据有什么"出发设计流程，而是先从"系统要帮助用户做什么决策"出发，再反推业务流程与数据流程。

这是对"数据优先"的**刻意拒绝**。原始讨论中提出的却恰好相反：

```
从可获得数据出发
    → 评估每类数据能稳定支撑什么评估
    → 这些评估能支持哪些决策
    → 以此决定系统流程、页面和展示
```

两种哲学并不互斥，但**现有系统走向了一个危险的中间状态**：决策目标是按"决策优先"定的（D1-D5 五个节点），但实现时数据还没准备好，结果是每个决策节点下面都有一块"代理变量"在撑着，架构文档和实现之间存在系统性落差。

正确做法：**用"决策优先"定长期目标，用"数据优先"决定当前实现深度，并在展示层诚实标注置信度**。

---

## 一、可获得数据的诚实评估

### 1.1 六大数据源实力表

| 数据源 | 数据品类 | 可用性 | 全量覆盖 | 时效性 | 信号置信度 | 当前用法 |
|--------|---------|--------|---------|--------|----------|---------|
| **mootdx** | 个股日线 K 线 | ★★★ | ★★★（主板/中小板） | 每日 | **高** | ✅ 主力源，fallback 链第一位 |
| **mootdx** | ETF K 线 | ★★★ | ★★★ | 每日 | **高** | ✅ 已接入 ETF 引擎 |
| **mootdx** | 指数/板块日线 | ★★★ | ★★★ | 每日 | **高** | ❌ `fetch_index_kline()` 已实现，**从未调用** |
| **mootdx** | 财务快照（19字段） | ★★★ | ★★（历史深度不均） | 季报+滞后 | 中 | ✅ 最新快照同步 |
| **东方财富** | 十大流通股东 | ★★★ | ★★★（全 A 股） | 半年/年报+45天 | **高（核心信号）** | ✅ sync_raw 主链路 |
| **AKShare** | 申万行业分类 | ★★ | ★★★（分类稳定） | 年度更新 | 中（背景参考） | ✅ sync_industry |
| **AKShare** | 申万行业指数 K 线 | ★★ | ★★★ | 每日 | **高（若接入）** | ❌ 完全未使用 |
| **AKShare** | 资本行为（分红/回购/解禁） | ★★（网页抓取） | ★（覆盖不完整） | 事件触发 | 低-中 | ✅ 已接入，覆盖存疑 |
| **AKShare** | 历史财报（新浪） | ★★ | ★★（主板较全） | 季报+滞后 | 中 | ✅ 历史补充 |
| **Qlib** | Alpha158 因子 | ★★（依赖 K 线） | 同 K 线覆盖 | 每日 | 中 | ✅ 训练特征 |
| **Qlib** | ML 预测排序 | ★（依赖训练质量） | ★★ | 模型周期 | 低-中 | ✅ 回流 Forecast 层 |

### 1.2 三层信号置信度

从上表直接反推，系统所有评分信号分为三层：

```
强信号层（High Confidence，全市场适用）
├── 机构持仓事件：新进 / 增持 / 减持 / 退出（东财）
├── 价格趋势 / 波动率 / 动量（mootdx K线）
└── 行业归属（申万分类，稳定）

条件信号层（Medium Confidence，取决于覆盖深度）
├── 基本面质量（财务历史 ≥ 4 期才可信）
├── 机构历史技能（需 ≥ 10 个历史事件）
└── 板块动量（需行业分类 + 成分 K线，或指数 K线）

实验信号层（Low / Conditional Confidence）
├── Qlib ML 预测（训练数据质量与市场机制稳定性决定）
└── 资本纪律精细指标（AKShare 资本行为覆盖不完整）
```

**关键推论**：当前复合评分

```
composite = 0.35×Discovery + 0.30×Quality + 0.20×Stage + 0.15×Forecast
```

对所有股票一视同仁施以 30% Quality 权重，但对于财务历史 < 4 期的股票，这 30% 实际上是**噪音而不是信号**。评分系统必须感知并传播数据置信度。

---

## 二、模块化架构全景

### 2.1 目标分层

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 7: API / Router 层（薄封装，只做路由和序列化）          │
├─────────────────────────────────────────────────────────────┤
│  Layer 6: 管线编排层（DAG 定义 / 步骤执行 / 状态 / 日志）      │
├─────────────────────────────────────────────────────────────┤
│  Layer 5: 回测验证层（走前向验证 / 池子胜率 / 规则验证）         │
├─────────────────────────────────────────────────────────────┤
│  Layer 4: 模型层（Qlib 训练 / 预测 / 验证 Record）            │
├─────────────────────────────────────────────────────────────┤
│  Layer 3: 评分层（4 个独立 Scorer + 置信度 + 综合合并）         │
├─────────────────────────────────────────────────────────────┤
│  Layer 2: 变量加工层（11 个 Engine，各出一张事实表）            │
├─────────────────────────────────────────────────────────────┤
│  Layer 1: 数据获取层（6 个 Client，统一接口，幂等落库）          │
└─────────────────────────────────────────────────────────────┘
        ↑                                                ↑
   单向依赖（下层不知道上层）           数据置信度向上传播
```

### 2.2 各层核心原则

| 层 | 可以做 | 不可以做 |
|----|--------|---------|
| 数据获取 | 抓取、字段映射、错误重试、同步状态 | 评分、业务规则判断 |
| 变量加工 | 确定性计算、特征工程、Qlib 桥接 | 未来收益拟合、跨层写入 |
| 评分 | 读取事实层、计算分数、输出置信度 | 直接读取原始数据、写入上游表 |
| 模型 | 训练、预测、验证 | 重复实现确定性规则已能算的事 |
| 回测验证 | 走前向统计、分组收益分析 | 用当前参数重算历史 |
| 管线编排 | 步骤调度、依赖管理、状态追踪 | 业务逻辑计算 |
| API 层 | 路由、序列化、权限 | 业务计算、数据库直读重算 |

---

## 三、数据获取层（Layer 1）方案

### 3.1 当前问题

各 client 文件的职责边界模糊：`akshare_client.py` 同时包含 K 线获取、行业分类获取、交易日历获取三类职责；`financial_client.py` 混合了爬取逻辑和快照状态机管理。

### 3.2 建议拆分结构

```
backend/services/clients/
├── mootdx_kline_client.py       # K线：个股/ETF/指数（激活孤立功能）
├── mootdx_financial_client.py   # 财务快照（从 financial_client.py 提取）
├── eastmoney_holdings_client.py # 十大流通股东（从 akshare_client.py 提取）
├── akshare_industry_client.py   # 申万行业分类 + 行业指数
├── akshare_capital_client.py    # 分红/回购/解禁（capital_client.py 重命名收口）
└── akshare_financial_client.py  # 历史财报（从 financial_client.py 提取）
```

**统一接口规范**：

```python
class BaseDataClient:
    def fetch(self, codes: list, start_date: str = None, end_date: str = None) -> pd.DataFrame
    def get_sync_state(self, codes: list) -> dict   # {code: {last_date, row_count, status}}
    def mark_synced(self, code: str, result: SyncResult) -> None
```

所有 client 遵循此接口，可独立测试，可替换实现。

### 3.3 立即可做：激活孤立的 `fetch_index_kline()`

**当前状态**：`akshare_client.py` 第 585-637 行已完整实现，`grep` 全项目**零调用**。

**为什么重要**：`sector_momentum.py` 目前通过"下载所有成分股 K 线 → 合成等权指数"来获得板块 K 线，计算量大、且等权合成与申万官方指数存在偏差。直接使用申万指数 K 线更准确，且成本更低。

**解决方案**：

```python
# Step 1: 获取申万一级行业指数代码（28个）
# ak.sw_index_first_info() 返回含 industry_code 字段

# Step 2: 在 sync_market_data 步骤中增加指数 K 线同步
def sync_index_klines(industry_codes: list):
    for code in industry_codes:
        df = fetch_index_kline(code, days_needed=500)
        upsert_to_price_kline(df, code, freq='daily', adjust='none')
        # market_data.db price_kline 表结构支持（code/date/freq/adjust为PK）

# Step 3: sector_momentum.py 优先使用真实指数 K 线
def get_sector_kline(sw_level1_name: str) -> pd.DataFrame:
    index_code = get_sw_index_code(sw_level1_name)
    if index_code and has_kline_data(index_code):
        return load_from_price_kline(index_code)   # 优先
    else:
        return synthesize_from_components(sw_level1_name)  # 降级
```

**预估改动**：约 50 行代码，不影响任何现有接口。

### 3.4 AKShare 职责收口

AKShare 的职责应严格限定为**补字段源**，不再承担主链路：

| 功能 | 主源 | AKShare 角色 |
|------|------|-------------|
| 个股日线 K 线 | mootdx（第一） | 降级备选（保持现状）|
| 行业分类 | AKShare 申万 API | **主源**（无替代）|
| 板块指数 K 线 | mootdx `fetch_index_kline()` | 降级备选 |
| 财务快照（最新） | mootdx finance() | **不替换** |
| 财务历史报表 | AKShare 新浪 | **主源**（无替代）|
| 资本行为 | AKShare | **主源**，明确标注覆盖率 |

---

## 四、变量加工层（Layer 2）方案

### 4.1 现状评估

11 个 `*_engine.py` 已经相对好，每个 engine 职责清晰，有独立事实表输出，这是整个系统最健康的部分。主要需要补充的是**数据覆盖度的向上传播**。

### 4.2 统一接口：输出覆盖度元数据

每个 engine 的核心 `compute()` 方法应输出两个对象：

```python
@dataclass
class FeatureCoverage:
    stock_code: str
    has_data: bool
    data_date: str        # 最新数据日期
    completeness: str     # 'full'（8期+）/ 'partial'（4-7期）/ 'thin'（1-3期）/ 'none'
    quarters_available: int  # 可用季报数

def compute(self, codes: list) -> tuple[pd.DataFrame, dict[str, FeatureCoverage]]:
    ...
```

**覆盖度元数据用途**：
- 写入各 `fact_*` 表的 `data_coverage` 字段（新增列）
- 上浮到评分层，驱动动态权重调整
- 在前端展示"该分数基于 N 期历史数据"

### 4.3 sector_momentum 改造

如第三节所述，接入真实申万指数 K 线后，`sector_momentum.py` 的计算路径变为：

```
申万指数 K 线（price_kline）
    ↓
MACD / MA / 动量计算（保持现有逻辑）
    ↓
mart_sector_momentum（无变化）
```

原有"成分股合成"逻辑保留为降级路径，不删除。

---

## 五、评分层（Layer 3）方案

### 5.1 现状问题

`scoring.py` 是 2190 行的上帝文件，包含机构评分、股票四层评分（Discovery/Quality/Stage/Forecast）、综合评分、Setup 候选逻辑、配置管理，共 41 个函数。

**具体影响**：
- 90KB 文件无法在一次读取中完整加载，每次编辑需要多次分块读取，context decay 风险极高
- D2 Quality 和 D3 Stage 的迭代频率和验证周期完全不同，强行耦合
- 无法对单个决策层做独立单元测试

### 5.2 拆分方案

```
backend/services/scoring/
├── __init__.py
├── institution_scorer.py   # 机构技能分 / 可跟性分（约 300 行）
├── discovery_scorer.py     # D1：机构行为 × 技能 × 新鲜度（约 200 行）
├── quality_scorer.py       # D2：财务质量（约 400 行）
├── stage_scorer.py         # D3：价格路径 / 阶段判断（约 350 行）
├── forecast_scorer.py      # D4：Qlib 预测分读取 + 置信调整（约 150 行）
└── composite_scorer.py     # 综合：动态权重 + 池子分类（约 200 行）
```

每个 scorer 遵循统一接口：

```python
@dataclass
class ScoringResult:
    stock_code: str
    score: float          # 0-100
    confidence: str       # 'high' | 'medium' | 'low' | 'insufficient'
    data_date: str        # 该层最新数据日期
    components: dict      # 子分明细（供三可原则 / 追溯用）

class BaseScorer:
    def score(self, stock_codes: list) -> list[ScoringResult]:
        ...
```

### 5.3 数据置信度传播与动态权重

**核心机制**：composite_scorer 在合并前检查各层置信度，动态调整权重：

```python
def compute_composite(results: dict[str, ScoringResult]) -> ScoringResult:
    weights = {'discovery': 0.35, 'quality': 0.30, 'stage': 0.20, 'forecast': 0.15}

    # 置信度降级规则
    if results['quality'].confidence == 'insufficient':  # 财务历史 < 4 期
        weights['quality'] = 0.0
        weights['discovery'] += 0.20    # 将 Quality 权重转移到 Discovery
        weights['stage'] += 0.10

    if results['forecast'].confidence == 'insufficient':  # Qlib 未训练 / 覆盖不足
        weights['forecast'] = 0.0
        weights['stage'] += 0.10
        weights['discovery'] += 0.05

    score = sum(results[k].score * w for k, w in weights.items())
    confidence = min(r.confidence for r in results.values())  # 木桶短板
    return ScoringResult(score=score, confidence=confidence, ...)
```

**前端展示变化**：在综合分旁显示置信度标记：
- 🟢 High（所有层数据充足）
- 🟡 Medium（部分层数据有限）
- 🔴 Low / Insufficient（关键层数据缺失，分数仅供参考）

### 5.4 Qlib 定位的明确选择

当前架构文档声称"以 Qlib 为排序器"，但实际 Forecast 权重只有 15%。这个矛盾有一个数据优先的合理解释：**Qlib 当前 15% 的权重，是训练数据质量尚不足以支撑更高权重的被动结果**，而不是主动设计选择。

建议在文档中明确：

- **短期（数据覆盖充分前）**：Qlib 是辅助排序层（15%），Discovery + Quality + Stage 是主体
- **中期（财务历史覆盖提升后）**：Quality 置信度上升，Forecast 可适度提权至 25-30%
- **长期（Qlib 验证健壮后）**：D1-D3 作为进池门控，Forecast 主导最终排序

这比"今天声称 Qlib 是排序器，实际给 15%"更诚实，也更有路线图价值。

---

## 六、模型与验证层（Layer 4 & 5）方案

### 6.1 Qlib 工作流补全

`qlib_full_engine.py` 中已有 `SignalRecord`、`SigAnaRecord`、`PortAnaRecord` 的代码，但未完整串联为自动化工作流。

**建议拆分为 4 个独立模块**：

```
backend/services/models/
├── qlib_data_handler.py    # K线+财务+机构数据 → Qlib 二进制格式
├── qlib_trainer.py         # 模型训练（LGBModel 配置、滚动训练）
├── qlib_predictor.py       # 预测生成 → 回流 qlib_predictions / mart_stock_trend
└── qlib_validator.py       # SignalRecord → SigAnaRecord → PortAnaRecord → qlib_model_state
```

**训练完成后的自动化流程**：

```python
# qlib_validator.py
def run_validation_pipeline(model_id: str, recorder: Recorder):
    # 1. 生成 SignalRecord
    sig_rec = SignalRecord(model=recorder, dataset=dataset)
    sig_rec.generate()

    # 2. 生成 SigAnaRecord（IC / RankIC / 长短分析）
    sig_ana = SigAnaRecord(recorder=sig_rec)
    sig_ana.generate()

    # 3. 生成 PortAnaRecord（Long-Short 回测）
    port_ana = PortAnaRecord(recorder=sig_rec, config=backtest_config)
    port_ana.generate()

    # 4. 提取摘要回写业务库
    update_qlib_model_state(model_id, {
        'ic_mean': sig_ana.get_ic_mean(),
        'rank_ic_mean': sig_ana.get_rank_ic_mean(),
        'long_short_annual_return': port_ana.get_annual_return(),
        'max_drawdown': port_ana.get_max_drawdown(),
    })
```

### 6.2 走前向验证（Walk-Forward）

**当前问题**：验证页使用当前参数评估历史快照，等于"事后诸葛亮"，无法证明评分系统的前瞻有效性。

**解决方案**：

```python
# backend/services/validation/walk_forward.py

class WalkForwardValidator:
    """
    固定参数版本，只用快照日期之前可获得的数据，
    重新计算每个快照时刻的评分，然后与实际后续收益对比。
    """
    def validate(self, snapshot_date: str, param_version: str) -> ValidationResult:
        # 1. 加载该日期时间截面内的所有原始数据（不用今天的数据）
        holdings_at_t = load_holdings_up_to(snapshot_date)
        financials_at_t = load_financials_up_to(snapshot_date)
        klines_at_t = load_klines_up_to(snapshot_date)

        # 2. 用当时的参数版本重算评分
        scores = compute_scores(holdings_at_t, financials_at_t, klines_at_t,
                                param_version=param_version)

        # 3. 对比 snapshot_date + 30/60/120d 的实际涨跌幅
        returns = load_actual_returns(scores.codes, snapshot_date)

        return ValidationResult(
            snapshot_date=snapshot_date,
            pool_win_rates=compute_pool_win_rates(scores, returns),
            ic=compute_ic(scores, returns),
        )
```

**关键要求**：`fact_setup_snapshot` 需要新增 `param_version` 字段，记录评分时使用的配置版本，避免未来参数变更后无法还原历史决策。

### 6.3 Stage 规则后验

**问题**：Stage 的惩罚项（已充分演绎 -12、失效破坏 -28 等）是主观设定，从未被数据验证。

**解决方案**：

```python
# backend/services/validation/stage_rule_validator.py

def validate_path_state_impact():
    """
    按 path_state 分组，统计 fact_setup_snapshot 中
    各组的 10/30/60d 实际收益分布，检验惩罚方向和幅度。
    """
    query = """
        SELECT s.path_state,
               COUNT(*) as sample_n,
               AVG(s.gain_10d) as avg_10d,
               AVG(s.gain_30d) as avg_30d,
               AVG(s.gain_60d) as avg_60d,
               SUM(CASE WHEN s.gain_30d > 0 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as win_rate_30d
        FROM fact_setup_snapshot s
        WHERE s.gain_30d IS NOT NULL
        GROUP BY s.path_state
        HAVING sample_n >= 10
    """
    # 输出结果提供给用户判断：
    # - "失效破坏"组的 30d 收益是否真的明显低于"震荡待定"组？
    # - 惩罚分数是否与收益差距成比例？
```

**触发时机**：当 `fact_setup_snapshot` 积累 ≥ 100 个成熟样本后，在验证页自动展示此分析。

---

## 七、管线编排层（Layer 6）方案

### 7.1 现状问题

`updater.py` 是 3078 行，混合了：DAG 步骤定义、步骤执行函数体（1-2000 行）、UI 实时日志收集、步骤状态机管理、API 响应格式化。

### 7.2 拆分方案

```
backend/routers/pipeline/
├── dag_definition.py    # 纯数据：步骤列表、依赖关系、分组（~100行）
├── step_runners.py      # 步骤执行函数，每个函数对应一个步骤（~1500行）
├── pipeline_state.py    # step_status 表的读写封装（~200行）
├── pipeline_log.py      # UILogHandler，日志队列管理（~150行）
└── updater.py           # API 路由层，只做路由和调用编排（<200行）
```

**dag_definition.py 示例**：

```python
PIPELINE_STEPS = [
    PipelineStep(
        id='sync_raw',
        name='下载十大股东',
        group='data',
        hard_deps=[],
        soft_deps=[],
        runner='run_sync_raw',
    ),
    PipelineStep(
        id='sync_market_data',
        name='同步行情与指数',
        group='data',
        hard_deps=['match_inst'],
        soft_deps=[],
        runner='run_sync_market_data',
        # 扩展点：可配置是否同步指数 K 线
        config={'sync_index': True, 'index_lookback_days': 500},
    ),
    ...
]
```

---

## 八、原有问题清单（已更新）

### 8.1 Qlib 定位矛盾 → 已有解决路径

**问题**：声称"以 Qlib 为排序器"但实际权重 15%。

**解决**：采用三阶段权重路线图（见第五节），并在文档中明确当前阶段是"数据成熟前的过渡态"，不是终态设计。

### 8.2 验证闭环循环依赖 → 走前向验证

**问题**：用规则产生的标签来验证规则本身。

**解决**：第六节走前向验证方案，配合 `param_version` 字段实现严格历史重算。

### 8.3 scoring.py 上帝文件 → 六文件拆分

**问题**：2190 行上帝文件，四层逻辑耦合，context decay 高风险。

**解决**：第五节完整拆分方案。每个 scorer 独立可测试，按决策层分开迭代。

### 8.4 财务覆盖缺口的双重影响 → 数据门控 + 置信度标注

**问题**：覆盖不足 → 低分惩罚；数据补全后 → 虚假信号骤升。

**解决**：
1. `quality_scorer.py` 内置"就绪门控"：财务历史 < 4 期 → confidence='insufficient' → composite 中 Quality 权重归零
2. 前端对 Quality 分显示数据日期和期数标注
3. 变量加工层 engine 统一输出 FeatureCoverage（见第四节）

### 8.5 "三可原则"执行不彻底 → 子分持久化

**问题**：quality_balance_raw、quality_efficiency_raw 等中间子分不知是否落库。

**解决**：拆分后的 `quality_scorer.py` 强制要求每次评分将所有 9 个子分写入 `fact_stock_quality_features`，通过 ScoringResult.components 字典承载，统一持久化入口。

### 8.6 Stage 门槛无后验支撑 → 规则后验模块

**问题**：失效破坏 -28 分等是猜测。

**解决**：第六节 `stage_rule_validator.py`，样本成熟后自动生成 path_state 分组收益报告，持续校验。

### 8.7 Stage 压缩 Forecast 方向存疑

**问题**：Stage < 60 时 Forecast 降半权，但规则不确定时 ML 信号可能更有价值。

**解决**：在 `composite_scorer.py` 中将此逻辑改为**可配置参数**，同时在 `validation/` 层新增一个 A/B 测试：比较"stage < 60 时压缩 forecast"和"不压缩"的历史组合表现差异。

### 8.8 Legacy action_score 退役路径不具体

**问题**："最后退役 legacy"没有触发条件。

**解决**：设定可量化触发条件（满足任一即可执行退役）：
1. 新体系 A 池 20d 胜率，基于 ≥ 50 个成熟样本，稳定 > 55%
2. 新旧排序对照显示 top-30 重叠度 < 30%
3. 新体系正式运行满 180 天

---

## 九、优先级排序

优先级不再按"技术完整性"排，而是按**"当前数据支撑力 × 对用户决策质量的实际影响"**排序。

### 立即可做（改动小，收益明显）

1. **激活 `fetch_index_kline()`**，接入申万一级指数 K 线，`sector_momentum.py` 优先使用真实指数。约 50 行改动，不影响任何现有接口。

2. **`quality_scorer` 新增财务覆盖就绪门控**。财务历史 < 4 期的股票 Quality 权重归零，同时在前端显示覆盖期数。防止数据补全被误读为基本面改善。

3. **Qlib 训练完成后自动运行 SignalRecord + SigAnaRecord**。这是 Forecast 层获得统计意义的最低门槛，不做这一步就没有 IC/RankIC 的可信基础。

### 近期（下一轮结构性迭代）

4. **scoring.py → 6 个独立模块拆分**。按第五节方案执行。这是所有后续迭代的前置条件，不拆分则每次修改评分都有 context decay 风险。

5. **每个 engine 输出 FeatureCoverage 元数据**，写入事实表 `data_coverage` 字段，为置信度传播提供基础数据。

6. **composite_scorer 实现动态权重**，根据各层 confidence 自动调整，前端显示置信度标记。

### 中期（随样本积累同步推进）

7. **走前向验证模块**，`fact_setup_snapshot` 补 `param_version` 字段，为真正的历史有效性验证建立基础。

8. **Stage 规则后验**，样本满 100 个成熟记录时自动生成 path_state 分组收益报告。

9. **updater.py → 管线编排层拆分**，按第七节方案执行。降低未来管线修改的维护成本。

### 长期（数据成熟后执行）

10. Forecast 权重提升至 25-30%（当财务历史覆盖率 > 80% 且 Qlib IC > 0.03 持续稳定后）。

11. Legacy action_score 退役（触发上述量化条件后执行）。

---

## 十、总结

从可获得数据出发，当前系统的实际信号强度是：

- **东财机构事件**：强，这是整个系统存在的根本理由，应始终是主信号
- **mootdx K 线**：强，覆盖好、稳定、每日更新，支撑 Stage 和 Qlib 特征
- **申万指数 K 线**：已具备能力但从未激活，一行调用即可解锁板块分析质量
- **财务质量（Quality）**：中，覆盖不均，对覆盖不足的股票是噪音，应加门控
- **Qlib 预测（Forecast）**：低-中，当前 15% 权重是数据现状的诚实反映，不是最终设计

系统最值得做的不是继续在评分公式上精雕细刻，而是：

> **让系统知道自己知道什么，不知道什么，并把这个元知识传递给用户。**

数据置信度传播、动态权重调整、前端覆盖度标注——这三件事加在一起，能比任何评分微调都更有效地提升用户对研究结论的判断准确度。

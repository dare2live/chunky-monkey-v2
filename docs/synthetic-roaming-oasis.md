# 综合实施计划 v3 — 2026-04-09

## 背景

本计划整合三个方向的工作：
1. **上一轮遗留任务**：彼得林奇六类分类引擎已写入代码但未提交，mart_stock_trend 未更新
2. **架构评审文档**：已在 commit `6750ce4` 完成，此方向关闭
3. **本轮新需求**：ETF 页面改版（胶囊标签、行业分类、步长修复）+ Banner 导航层级重构 + ETF 预轮动 Top5 + 每日 ML 自动优化

---

## 模块一：遗留提交（立即执行）

### 1.1 提交 stock_archetype_engine.py

文件：`backend/services/stock_archetype_engine.py`（已修改，未提交）
文件：`backend/services/sector_forecast_engine.py`（新增，未跟踪）

操作：
1. 验证 INSERT 语句列数与 VALUES 占位符数量一致（53列）
2. git add + commit + push

### 1.2 更新 mart_stock_trend 暴露 Lynch 字段

目标：前端股票列表能显示 lynch_type（类型标签）和 sell_signal_score（卖出压力色条）

涉及文件：
- `backend/services/scoring.py`（或 mart 构建的对应位置）
- 在 mart_stock_trend 建表时 ALTER TABLE ADD COLUMN lynch_type TEXT, sell_signal_score REAL
- mart 构建时 LEFT JOIN dim_stock_archetype_latest 补充这两列

---

## 模块二：Banner 导航层级重构

### 现状
```
工作台 | 机构研究 | 股票研究 | Qlib | ETF量化
（扁平，五个并列导航按钮）
```

### 目标结构
```
[股东挖掘]                    [ETF研究]
  工作台 机构研究 股票研究 Qlib    ETF量化 (子标签见模块三)
```

### 设计原则
- 两大板块顶层导航：**股东挖掘** 和 **ETF研究**
- 每个板块有独立的二级子导航（横向胶囊标签）
- 板块间数据隔离：ETF研究不引用股东挖掘的任何 DOM ID，后端 API 路由也分开（`/api/etf/*` vs `/api/inst/*`、`/api/stocks/*`）
- Qlib 和 ETF 的 enabled_modules 开关继续生效：Qlib 隐藏时"股东挖掘"子导航隐藏 Qlib 按钮；ETF 模块未启用时"ETF研究"整体不可点击

### HTML 改动（index.html）

原有 `<nav class="main-nav" id="mainNav">` 替换为两层结构：

```html
<nav class="main-nav" id="mainNav">
  <!-- 顶层板块选择 -->
  <div class="nav-top-bar">
    <button class="nav-group-btn active" data-group="holder">股东挖掘</button>
    <button class="nav-group-btn" id="nav-group-etf" data-group="etf" style="display:none">ETF研究</button>
  </div>
  <!-- 股东挖掘二级导航 -->
  <div class="nav-sub-bar" id="nav-sub-holder">
    <button class="nav-btn active" data-view="dashboard">工作台</button>
    <button class="nav-btn" data-view="research">机构研究</button>
    <button class="nav-btn" data-view="stocks">股票研究</button>
    <button class="nav-btn" id="nav-qlib" data-view="qlib" style="display:none">Qlib</button>
  </div>
  <!-- ETF研究二级导航 -->
  <div class="nav-sub-bar" id="nav-sub-etf" style="display:none">
    <button class="nav-btn" data-view="etf" data-etftab="overview">整体判断</button>
    <button class="nav-btn" data-view="etf" data-etftab="list">ETF列表</button>
    <button class="nav-btn" data-view="etf" data-etftab="mining">挖掘建议</button>
    <button class="nav-btn" data-view="etf" data-etftab="rotation">轮动预测</button>
  </div>
</nav>
```

### JS 改动（app.js）

1. `showView()` 保持不变，仍切换 `.view` section
2. 新增 `showGroup(name)` 函数：
   - 切换 `.nav-group-btn` active 状态
   - 切换 `.nav-sub-bar` 显示/隐藏
   - 点击"股东挖掘"→显示 nav-sub-holder，进入 dashboard view
   - 点击"ETF研究"→显示 nav-sub-etf，进入 etf view 并激活当前 ETF 子标签
3. `checkHealth()` 中：
   - `enabled_modules.includes('etf')` → `el('nav-group-etf').style.display = ''`
   - `enabled_modules.includes('qlib')` → 仅影响 nav-sub-holder 内的 Qlib 按钮

### CSS 改动（main.css）

新增 `.nav-top-bar`、`.nav-sub-bar` 样式：
- `.nav-top-bar`: 主色背景（深色）大按钮，字体稍大
- `.nav-sub-bar`: 次级横排胶囊标签，灰底白字/深字
- `.nav-group-btn.active`: 底部下划线 + 加粗
- 板块切换时 sub-bar 之间有微弱 fade 过渡

---

## 模块三：ETF 页面重构

### 3.1 ETF 页面子标签管理

ETF 页面（`#view-etf`）改为四个子页，通过 `data-etftab` 控制显示：

| 子标签 | 内容 | 对应现有内容 |
|-------|------|------------|
| 整体判断 | 市场温度 + 状态机判断 + 关注/回避名单 | 现有 etfOverviewContainer |
| ETF列表 | 表格 + 胶囊标签筛选 + 步长修复 | 现有 etfTableContainer |
| 挖掘建议 | 网格候选 + 趋势持有 | 现有 etfMiningContainer（左半部分）|
| 轮动预测 | 预轮动 Top 5 + 每日 ML 更新 | 现有 etfMiningContainer（右半部分）+ 增强 |

子标签切换逻辑写在 `showEtfTab(tabName)` 函数，渲染时懒加载对应内容。

同步按钮移到 ETF列表 子标签内。

### 3.2 ETF 实际行业分类（解决"行业"笼统问题）

**后端改动：`backend/services/etf_engine.py`**

扩展 `_infer_etf_category(code, name)` 函数，在返回 "行业" 之前，用关键词映射到具体行业：

```python
INDUSTRY_MAP = [
    (["医疗", "医药", "生物科技", "医健", "生科", "中药", "医械"], "医疗健康"),
    (["半导体", "芯片", "集成电路", "科创50", "科技"], "科技"),
    (["新能源", "光伏", "风电", "储能", "氢能", "电池"], "新能源"),
    (["消费", "白酒", "食品", "饮料", "家电", "零售"], "消费"),
    (["银行", "券商", "保险", "金融", "证券", "理财"], "金融"),
    (["军工", "航空", "航天", "国防"], "军工"),
    (["地产", "房产", "建筑", "建材"], "地产建筑"),
    (["农业", "农林", "畜牧", "化工", "煤炭", "有色", "钢铁"], "周期资源"),
    (["游戏", "传媒", "文化", "互联网", "数字", "云计算", "大数据", "人工智能", "AI"], "数字科技"),
    (["交通", "港口", "铁路", "物流", "高速"], "交通物流"),
]
```

匹配失败时返回 `"行业·其他"` 而非仅 `"行业"`。

**API 返回**：`/api/etf/list` 的 `category` 字段变为具体名称（如 "医疗健康"、"半导体"、"新能源"）。

**前端改动（app.js）**：

在 ETF列表 子标签顶部增加胶囊标签过滤栏：

```
[全部] [宽基] [跨境] [债券] [商品] [货币] [医疗健康] [科技] [新能源] [消费] [金融] [军工] ...
```

- 标签动态从数据中提取（保证有 ETF 的分类才显示）
- 每行颜色随分类区分
- 支持单选（点击激活，再次点击取消）

### 3.3 步长空白修复

**问题根因**：`grid_step_pct` 依赖 `amplitude_20d`，后者需要至少 20 条 K 线的最高/最低价数据。新同步或 K 线不足的 ETF 返回 None。

**修复位置**：`backend/services/etf_engine.py` 中 `calc_etf_momentum()` 的步长计算段

**修复方案**：
```python
# 主路径：振幅计算
amplitude_20d = _calc_amplitude_pct(highs, lows, 20)
if amplitude_20d is not None:
    grid_step_pct = round(_clamp(amplitude_20d / 6.0, 0.8, 4.5), 1)
else:
    # 降级路径：用 20 日收益标准差（波动率）估算步长
    volatility_20d = _calc_volatility_pct(closes, 20)  # 新增辅助函数
    if volatility_20d is not None:
        grid_step_pct = round(_clamp(volatility_20d * 1.5, 0.8, 4.5), 1)
    else:
        grid_step_pct = 1.5  # 最终兜底：行业 ETF 典型步长
```

新增 `_calc_volatility_pct(closes, window)` 辅助函数：计算窗口内日涨跌幅的标准差（年化前的原始值）×100。

### 3.4 ETF 预轮动 Top 5 + 每日 ML 自动优化

#### 前端展示（"轮动预测"子标签）

- 显示预轮动 Top 5 行业，每行展示：行业名、预轮动得分、Qlib均分位、高置信股票数、当前轮动桶（leader/neutral/blacklist）
- 上次更新时间
- 置信度说明（来自 Qlib 聚合，需模型已训练）
- 若 Qlib 模型未训练，显示提示

API：`/api/etf/mining?rotation_topn=5`（已有，rotation_topn 参数已支持）

前端单独增加 `loadEtfRotation()` 函数，带图表可视化（条形图，SVG viewBox）。

#### 每日 ML 自动优化（盘后）

**触发时机**：K 线同步完成后（updater.py 的 `step_sync_kline` 完成时）

**后端改动**：
- `backend/services/sector_forecast_engine.py`（已存在但未提交）中增加 `run_daily_rotation_update(conn, mkt_conn)` 入口
- 在 `backend/routers/updater.py` 或 `backend/updater.py` 的 K 线同步后调用此函数
- 该函数做：从 `qlib_predictions` 聚合最新 Qlib 分位 → 按 `dim_stock_industry_context` 行业分组 → 计算行业平均 Qlib 分位 + 高置信计数 → 写入 `mart_sector_rotation_forecast`（新表）

**新表结构**（smartmoney.db）：
```sql
CREATE TABLE IF NOT EXISTS mart_sector_rotation_forecast (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_date TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    next_rotation_score REAL,
    avg_qlib_percentile REAL,
    high_conviction_count INTEGER,
    rotation_bucket TEXT,
    rank INTEGER,
    updated_at TEXT
);
```

每次运行先删除当日记录再插入（幂等）。

---

## 模块四：Lynch 类型前端展示

### 4.1 股票列表新增 Lynch 类型标签

**mart_stock_trend 更新**（backend/services/scoring.py 或相关 mart 构建）：
```sql
-- 确保建表时包含
ALTER TABLE mart_stock_trend ADD COLUMN lynch_type TEXT;
ALTER TABLE mart_stock_trend ADD COLUMN sell_signal_score REAL;

-- mart 构建时 LEFT JOIN
LEFT JOIN dim_stock_archetype_latest a ON a.code = m.code
```

**前端（app.js）**：在股票列表行增加 Lynch 类型胶囊标签（快速增长/缓慢增长/稳定增长/周期/困境反转/隐蔽资产），不同类型不同颜色。

**卖出压力**：在 composite 分数旁边显示小型色条（绿→黄→红），鼠标悬停显示 sell_s1_label / sell_s2_label / sell_s3_label 子分详情。

---

## 模块五：更新项目计划文档

更新 `docs/implementation_plan.md`，把以下内容追加到计划中：

1. **Lynch 六类分类引擎**（已实现，待提交）
   - stock_archetype_engine.py 完整重写
   - 六类：快速增长/缓慢增长/稳定增长/周期/困境反转/隐蔽资产
   - 每类三维卖出信号评分（sell_s1/s2/s3）
   - 新增列：lynch_type, lynch_confidence, lynch_reason, sell_signal_score 等 22 列

2. **数据置信度分层**（架构评审结论）
   - 强信号：东财十大股东 + mootdx K 线（全市场高置信）
   - 条件信号：mootdx 财务（≥4 期方有效）
   - 实验信号：Qlib 预测（依赖训练数据质量）
   - 动作项：Quality 分需覆盖就绪门控（财务<4期→不参与行业百分位）

3. **ETF 研究板块**（本轮新增）
   - 导航层级：股东挖掘 | ETF研究
   - ETF 子标签：整体判断 / ETF列表 / 挖掘建议 / 轮动预测
   - ETF 实际行业分类（替代笼统"行业"）
   - 步长兜底修复
   - 预轮动 Top 5 + 每日盘后自动优化

4. **Qlib 工作流补全**（高优先级技术债）
   - SignalRecord → SigAnaRecord → PortAnaRecord 完整流程
   - IC/RankIC 来源确认
   - Walk-forward 验证（param_version 字段）

---

## 执行顺序

```
Phase 1（后端基础）
  1. 提交 stock_archetype_engine.py + sector_forecast_engine.py
  2. 修复 _infer_etf_category（具体行业名）
  3. 修复步长 grid_step_pct 空白（降级路径）
  4. 新增 mart_sector_rotation_forecast 表 + daily 更新函数
  5. 更新 mart_stock_trend（lynch_type, sell_signal_score）

Phase 2（导航重构）
  6. index.html：双层 nav 结构
  7. app.js：showGroup() + checkHealth() 更新
  8. main.css：.nav-top-bar / .nav-sub-bar 样式

Phase 3（ETF 页面）
  9. index.html：ETF section 改为 4 个子标签
  10. app.js：loadEtf() 拆分为各子标签函数 + 胶囊标签过滤逻辑
  11. app.js：loadEtfRotation() + Top 5 SVG 条形图

Phase 4（Lynch 前端）
  12. app.js：股票列表增加 lynch_type 胶囊 + sell_signal_score 色条

Phase 5（文档）
  13. docs/implementation_plan.md 追加更新
```

---

## 关键约束

- **不引入新数据源**：所有新功能使用已有 mootdx / 东财 / AKShare
- **可扩展性**：ETF 子标签用 `data-etftab` 驱动，后续加标签只需 HTML + case 分支
- **隔离原则**：ETF研究 DOM ID 全部以 `etf-` 前缀，不与股东挖掘 DOM 交叉
- **降级原则**：Qlib 未训练时，预轮动模块显示"暂无可用模型"提示，不报错
- **步长兜底**：宁可给 1.5% 默认值也不显示空白，让用户有参考
- **每次 Phase 最多 5 个文件**（CLAUDE.md 约束）

---

## 关键文件清单

| 文件 | Phase | 改动类型 |
|------|-------|---------|
| `backend/services/stock_archetype_engine.py` | 1 | 提交（已写好）|
| `backend/services/sector_forecast_engine.py` | 1 | 新增（已写好）|
| `backend/services/etf_engine.py` | 1 | 修改（行业分类 + 步长）|
| `backend/services/scoring.py` | 1 | 修改（mart_stock_trend + lynch）|
| `backend/routers/updater.py` | 1 | 修改（daily rotation 触发）|
| `index.html` | 2+3 | 修改（nav 层级 + ETF 子标签）|
| `assets/js/app.js` | 2+3+4 | 修改（showGroup + ETF 子标签 + Lynch）|
| `assets/css/main.css` | 2 | 修改（nav-top-bar 样式）|
| `docs/implementation_plan.md` | 5 | 修改（追加计划项）|

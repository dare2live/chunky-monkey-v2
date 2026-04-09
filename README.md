# Chunky Monkey v2

A股机构事件研究系统。追踪 200+ 机构股东（QFII、社保、保险、国家队、港股通等）在全市场的持仓变动，生成跟投信号。

## 这个项目做什么

机构（外资、社保基金等）买卖股票后，会在季报中披露持仓。这些数据有 2-4 个月的滞后，但仍然有价值——如果股价还没充分演绎，就可能是跟投机会。

本系统自动化了整个分析链条：

```
东财API拉取十大股东 → 匹配跟踪机构 → 同步K线 → 生成事件（新进/加仓/减仓/退出）
→ 计算收益 → 行业分类 → 构建机构画像 → 评分排序 → 生成跟投信号
```

## 核心功能

**机构画像**
- 每个机构的历史胜率、平均收益、最大回撤
- 申万三级行业能力评估（哪个机构在哪个行业更强）
- 可跟分（信号传递效率、安全跟随胜率）

**跟投信号（Setup）**
- 综合机构质量、行业命中、溢价水平、报告时效等维度
- A1-A5 优先级分级 + follow/watch/observe/avoid 执行建议
- 前瞻验证闭环：记录信号发出后的实际表现

**AI 多因子评分（Qlib 标签页）**
- 32 个技术因子（动量/均线/波动率/RSI/MACD/布林带）+ 机构因子
- LightGBM 预测 30 日前瞻收益
- 与现有 Setup 信号对比，寻找互补机会

**救生艇**
- 独立于主系统的备用脚本
- 一键查询"跟踪机构最新新进了哪些股票"
- 主系统挂了也能用

## 技术架构

```
前端：原生 HTML/CSS/JS 单页应用
后端：Python FastAPI + SQLite（WAL模式）
数据：东财API（十大股东）+ AKShare（K线/行业/日历）
AI：LightGBM（不依赖 pyqlib，用 pandas 自算因子）
```

**数据分层**

| 层 | 职责 | 示例 |
|---|------|------|
| raw | 原始数据，只追加不覆盖 | market_raw_holdings |
| dim | 维度数据 | dim_stock_industry, inst_institutions |
| fact | 事实数据 | fact_institution_event, inst_holdings |
| mart | 派生集市，可重算 | mart_institution_profile, mart_stock_trend |

**更新管线（12 步 DAG）**

下载十大股东 → 匹配机构 → 同步K线 → 生成事件 → 计算收益 → 申万行业 → 构建当前关系 → 机构画像 → 行业统计 → 股票列表 → 机构评分 → 股票评分

## 运行

```bash
# 依赖
pip3 install fastapi uvicorn httpx akshare pandas pydantic lightgbm scikit-learn

# 启动
cd backend
python3 -m uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# 或双击
./start.command
```

浏览器打开 http://localhost:8000

## 救生艇（独立运行）

```bash
cd lifeboat
python3 fetch_and_report.py
# 浏览器打开 report.html
```

或双击 `lifeboat/run.command`

## 项目结构

```
├── index.html                  # 前端入口
├── assets/
│   ├── css/main.css
│   └── js/app.js
├── backend/
│   ├── main.py                 # FastAPI 入口
│   ├── routers/
│   │   ├── market.py           # 东财API数据下载
│   │   ├── institution.py      # 机构CRUD、持仓、事件、画像
│   │   ├── updater.py          # 12步更新管线
│   │   └── qlib.py             # AI评分API
│   ├── services/
│   │   ├── db.py               # 主数据库（smartmoney.db）
│   │   ├── market_db.py        # 行情数据库（market_data.db）
│   │   ├── event_engine.py     # 事件生成
│   │   ├── return_engine.py    # 收益计算
│   │   ├── scoring.py          # 评分引擎
│   │   ├── qlib_engine.py      # AI多因子引擎
│   │   ├── holdings.py         # 持仓查询（统一口径）
│   │   ├── industry.py         # 行业解析（单点实现）
│   │   ├── akshare_client.py   # K线/行业/日历获取
│   │   ├── audit.py            # 数据质量审计
│   │   └── ...
│   └── scripts/                # 数据迁移/回测脚本
├── lifeboat/                   # 独立救生艇
│   ├── fetch_and_report.py
│   ├── institutions.json
│   └── run.command
├── data/                       # 数据库文件（.gitignore）
│   ├── smartmoney.db
│   └── market_data.db
└── CLAUDE.md                   # 开发指引
```

## 开发说明

本项目通过 vibe coding 构建和维护。开发规范见 [CLAUDE.md](CLAUDE.md)。

关键原则：
- 原始数据只追加不覆盖
- 派生层带版本，变更时清空重算
- 同一业务事实只允许一个真相源
- 新功能替代旧功能时必须删除旧代码

## Vibe Coding

这是一个纯 vibe coding 项目。项目作者不写代码，全部代码由 AI 生成和维护。

日常工作流程：作者用中文描述需求和想法 → AI 理解意图、设计方案、编写代码、调试修复 → 作者验证效果、反馈问题 → 迭代。从第一行代码到现在的万行系统，没有一行是人工敲的。

这意味着：
- 代码风格和架构决策来自 AI 对项目上下文的理解
- Bug 修复是"跟 AI 说哪里不对"而非"打开编辑器改代码"
- 版本管理是"帮我提交并推送"而非手敲 git 命令
- 项目能走多远，取决于人机协作的效率，而非作者的编程能力

## Contributors

- **[@dare2live](https://github.com/dare2live)** — 产品设计、策略逻辑、业务决策
- **Claude Code** (Anthropic) — 主力开发、架构设计、代码实现
- **Codex** (OpenAI) — 方案研究、架构讨论
- **Gemini** (Google) — 方案研究、架构讨论

## Acknowledgments

- **[AKShare](https://github.com/akfamily/akshare)** — 开源 A 股数据接口库，本项目的 K 线、行业分类、交易日历、救生艇模块的股东数据均依赖 AKShare。感谢 AKShare 团队让个人投资者也能便捷获取金融数据。

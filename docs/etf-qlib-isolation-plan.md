# ETF-only Qlib 重构计划

## 目标

- ETF 与股票预测链严格隔离。
- 每只 ETF 同时展示买入持有收益与最优网格收益。
- ETF 的预测、参数寻优、回测评估逐步迁移到 ETF-only Qlib 管线。
- 退役当前“股票侧 Qlib -> ETF 分类映射”的旁路口径。

## 约束

- 不允许 ETF 页面继续消费股票侧 Qlib 预测或行业映射结果。
- Phase 1 到 Phase 5 分阶段推进，每阶段控制在 5 个文件以内。
- 在 ETF-only Qlib 主链可用前，ETF 工作台仍保留当前原生规则与回测结果，避免功能回退。

## Phase 1: 隔离与退役

### 目标

- 退役 ETF 因子验证页里的股票侧 Qlib 分类共识。
- 增加 ETF-only Qlib 状态接口，明确当前迁移状态和后续落点。
- 在前端展示 ETF-only 迁移状态，而不是继续展示股票映射结果。

### 交付物

- 新增 ETF-only Qlib 状态服务。
- `/api/etf/qlib-consensus` 改为返回 ETF-only 管线状态。
- ETF 因子验证页展示“已隔离、待建设”的状态卡片。

### 验证

- 后端编译通过。
- `/api/etf/qlib-consensus` 不再返回股票映射字段。
- 浏览器确认 ETF 因子验证页出现 ETF-only 状态面板。

## Phase 2: ETF-only 数据层

### 目标

- 在 etf.db 中建立 ETF-only Qlib 专属表。
- 仅使用 ETF 原生数据构建特征和标签。

### 计划表

- `etf_qlib_feature_store`
- `etf_qlib_label_store`
- `etf_qlib_model_state`
- `etf_qlib_predictions`
- `etf_qlib_backtest_result`
- `etf_qlib_param_search`

### ETF 原生特征

- 日/周动量、波动、回撤、振幅
- 均线结构、相对强弱、成交额收缩/放大
- 折溢价、流动性、跟踪误差、价量形态
- 网格成交可行性、区间稳定性、极端跳价风险

### 标签设计

- 未来 20/60 日持有收益
- 未来 20/60 日风险调整收益
- 窗口最优网格收益
- 网格相对持有超额收益
- 最优步长与参数稳定性标签

## Phase 3: ETF-only 训练管线

### 目标

- 为 ETF 建立独立于股票的 Qlib 训练入口。
- 不复用股票 universe，不继续依赖股票行业映射。

### 模型方向

- 持有收益预测模型
- 网格收益预测模型
- 参数稳定性/可执行性模型
- 策略选择模型（网格 vs 持有）

### 输出

- 每只 ETF 的持有预期收益
- 每只 ETF 的网格预期收益
- 最优参数候选及其置信度
- 推荐策略和解释字段

## Phase 4: 参数寻优与回测接管

### 目标

- 用 ETF-only Qlib 结果接管现有手工策略评分。
- 每只 ETF 保留双口径展示：持有收益、网格收益。

### 关键动作

- 用 Qlib task/workflow 管理参数搜索实验
- 用 ETF-only 回测结果统一落库
- 保留逐日买卖点、收益曲线和稳定性审计输出

## Phase 5: 主界面切换与退役

### 目标

- ETF 工作台、机会发现、全量筛选、因子验证统一消费 ETF-only Qlib 结果。
- 退役现有股票映射共识和 ETF 启发式主判定链。

### 退役项

- 股票侧 Qlib -> ETF 分类映射
- ETF 因子验证页里的股票成分共识表
- 只做边缘阈值修饰的 `qlib_support` 逻辑

## 成功标准

- ETF 模块不再读取股票 Qlib 聚合结果。
- 每只 ETF 始终并排展示持有收益、网格收益、最优参数、推荐策略。
- Qlib 成为 ETF 参数寻优、预测、回测的主实验框架，而不是旁路参考信号。
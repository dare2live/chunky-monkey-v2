# ETF-only Qlib 运行说明

## 当前状态

- ETF Qlib 已经与股票 Qlib 彻底分离，训练、预测、参数寻优和回测都落在 `etf.db`。
- ETF 页面继续并排展示买入持有收益和网格收益，Qlib 负责预测和参数寻优，不替代基准展示。
- ETF 因子验证页现在分成两部分：
  - `ETF-only Qlib 迁移状态`
  - `ETF-only Qlib 预测验证`

## 数据落点

ETF-only Qlib 相关表全部位于 `etf.db`：

- `etf_qlib_feature_store`: ETF 原生特征层
- `etf_qlib_label_store`: ETF 标签层
- `etf_qlib_model_state`: 模型状态与训练元数据
- `etf_qlib_predictions`: 最新预测输出
- `etf_qlib_backtest_result`: ETF-only 回测结果
- `etf_qlib_param_search`: 最优步长候选与排序

## 关键接口

- `GET /api/etf/qlib-summary`
  - 返回 ETF 因子快照，用于因子验证页基础面板。
- `GET /api/etf/qlib-consensus?topk=50`
  - 返回 ETF-only 模型状态、预测结果、分类汇总和迁移状态。
- `GET /api/etf/qlib-consensus?topk=50&force_refresh=true`
  - 触发 ETF-only Qlib 全链重算与训练，然后返回最新快照。
  - `topk` 会透传到训练完成后的返回结果，不再被默认值覆盖。
- `GET /api/etf/analysis/{code}`
  - 深度分析页读取 ETF-only 预测字段：推荐策略、预测持有、预测网格、预测步长、策略优势。

## 前端状态卡关注点

因子验证页的迁移状态卡会直接展示：

- 模型状态
- 最近一次训练完成时间
- 样本数、模型特征数、模型覆盖 ETF 数
- 特征行数、标签行数、预测行数、回测行数、参数寻优行数

这几组数字用于判断 ETF-only Qlib 是否只是“有表”，还是已经完整训练并产出可消费结果。

## 典型验证命令

后端回归：

```bash
python3 -m unittest \
  backend.tests.test_etf_grid_optimizer \
  backend.tests.test_etf_qlib_consensus \
  backend.tests.test_etf_qlib_engine \
  backend.tests.test_kline_sources
```

语法检查：

```bash
python3 -m py_compile \
  backend/services/etf_grid_engine.py \
  backend/services/etf_qlib_engine.py \
  backend/routers/etf.py
```

人工检查：

1. 打开 ETF研究 → 因子验证。
2. 确认同时看到 `ETF-only Qlib 迁移状态` 和 `ETF-only Qlib 预测验证`。
3. 点击预测表中的 ETF，确认深度分析页出现 `ETF-only Qlib 策略预测` 卡片。

## 常见故障判断

- 迁移状态为 `partial`:
  - 先看缺失表和 `capability_matrix`，通常是训练表为空或参数寻优结果未落库。
- `force_refresh=true` 很慢:
  - 这是正常现象，会触发 ETF-only 特征重建、训练、预测和回测落库。
- 前端数字不变:
  - 先确认 `model_finished_at` 是否更新，再确认页面已加载最新 `app.js` 版本。
- 深度分析页没有 ETF-only 卡片:
  - 优先检查 `/api/etf/analysis/{code}` 返回是否包含 `qlib_preferred_strategy`、`qlib_predicted_buy_hold_return_pct`、`qlib_predicted_grid_return_pct`。
"""
etf_mining_engine.py — ETF 挖掘建议引擎

职责：
- 对 ETF 做确定性策略分类后的进一步建议
- 通过独立网格引擎复用回测/持有基准/策略判定结果
- 买入持有：趋势和因子同时占优的标的
- 下一轮动类别：基于 ETF 原生因子聚合的观察名单

说明：
- 网格回测仍是确定性策略问题，但只使用 ETF 原生价格、强弱和结构信号
- 轮动前瞻完全来自 ETF 原生因子聚合，不再叠加股票侧预测链
"""

from __future__ import annotations

import math
from typing import Optional

from services.etf_engine import calc_etf_momentum
from services.etf_grid_engine import (
    assess_etf_tradeability,
    _build_grid_step_candidates,
    _build_strategy_decision,
    _buy_hold_stats,
    _multi_period_backtest_from_rows,
    _optimize_grid,
    _run_grid_backtest,
    _score_grid_backtest,
)
from services.etf_snapshot_manager import _price_coverage_summary, load_cached_etf_row
from services.utils import (
    safe_float as _safe_float,
    clamp as _clamp,
    percentile_ranks as _percentile_ranks,
)
from services.constants import ETF_NON_INDUSTRY_CATS


def _load_price_rows(mkt_conn, code: str, limit: int = 180) -> list[dict]:
    rows = mkt_conn.execute(
        """
        SELECT date, close
        FROM etf_price_kline
        WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
        ORDER BY date DESC
        LIMIT ?
        """,
        (code, limit),
    ).fetchall()
    return list(reversed([dict(row) for row in rows]))


def _strategy_return_snapshot(strategy_type: Optional[str], grid_ret: Optional[float], buy_hold_ret: Optional[float]) -> dict:
    strategy_type = strategy_type or "买入持有"
    if strategy_type == "网格交易" and grid_ret is not None:
        recommended_label = "最优网格"
        recommended_return = grid_ret
        comparison_label = "买入持有" if buy_hold_ret is not None else None
        comparison_return = buy_hold_ret
    else:
        recommended_label = strategy_type
        recommended_return = buy_hold_ret if buy_hold_ret is not None else grid_ret
        comparison_label = "最优网格" if grid_ret is not None else None
        comparison_return = grid_ret

    strategy_edge = None
    if recommended_return is not None and comparison_return is not None:
        strategy_edge = round(recommended_return - comparison_return, 2)

    return {
        "recommended_strategy_label": recommended_label,
        "recommended_strategy_return_pct": recommended_return,
        "comparison_strategy_label": comparison_label,
        "comparison_strategy_return_pct": comparison_return,
        "strategy_edge_pct": strategy_edge,
    }


def enrich_etf_rows_with_strategy_validation(
    rows: list[dict],
    conn,
    mkt_conn,
    *,
    analysis_limit: int = 500,
) -> list[dict]:
    if not rows:
        return []

    enriched = [dict(row) for row in rows]
    rs12_pct = _percentile_ranks([_safe_float(row.get("relative_strength_12w")) for row in enriched])
    rs4_pct = _percentile_ranks([_safe_float(row.get("relative_strength_4w")) for row in enriched])
    mom20_pct = _percentile_ranks([_safe_float(row.get("momentum_20d")) for row in enriched])
    drawdown_pct = _percentile_ranks([_safe_float(row.get("max_drawdown_60d")) for row in enriched])
    setup_score_map = {
        "收敛待发": 100.0,
        "趋势跟随": 88.0,
        "震荡观察": 62.0,
        "低波防守": 58.0,
        "待补结构": 45.0,
        "结构松散": 18.0,
    }
    trend_score_map = {"多头": 100.0, "震荡": 60.0, "空头": 10.0}

    for idx, row in enumerate(enriched):
        row["heuristic_strategy_type"] = row.get("strategy_type")
        row["heuristic_strategy_reason"] = row.get("strategy_reason")
        row["heuristic_grid_score"] = row.get("grid_score")
        row["heuristic_grid_step_pct"] = row.get("grid_step_pct")

        factor_score = (
            (rs12_pct[idx] or 0.0) * 0.30
            + (rs4_pct[idx] or 0.0) * 0.25
            + (mom20_pct[idx] or 0.0) * 0.15
            + (drawdown_pct[idx] or 0.0) * 0.10
            + setup_score_map.get(row.get("setup_state") or "", 45.0) * 0.10
            + trend_score_map.get(row.get("trend_status") or "", 45.0) * 0.10
        )
        row["factor_score"] = round(_clamp(factor_score, 0.0, 100.0), 1)

        price_rows = _load_price_rows(mkt_conn, row.get("code") or "", analysis_limit)
        tradeability = assess_etf_tradeability(
            row.get("code") or "",
            row.get("name") or "",
            row.get("category"),
            price_rows,
        )
        row["tradeability_supported"] = tradeability.get("supported")
        row["tradeability_status"] = tradeability.get("status")
        row["tradeability_reason"] = tradeability.get("reason")
        row["tradeability_profile"] = tradeability.get("profile") or {}
        if not tradeability.get("supported"):
            row["strategy_type"] = "暂不参与"
            row["strategy_reason"] = tradeability.get("reason") or "该产品暂不适合进入 ETF 交易池。"
            row["grid_step_pct"] = None
            row["backtest_best_step_pct"] = None
            row["backtest_return_pct"] = None
            row["buy_hold_return_pct"] = None
            row["backtest_excess_pct"] = None
            row["backtest_sharpe"] = None
            row["buy_hold_sharpe"] = None
            row["backtest_max_drawdown_pct"] = None
            row["buy_hold_max_drawdown_pct"] = None
            row["grid_candidate_score"] = None
            row["grid_regime_score"] = None
            row["backtest_trade_quality_score"] = None
            row["backtest_trade_count"] = None
            row["backtest_sell_count"] = None
            row["backtest_win_rate"] = None
            row["backtest_window_days"] = len(price_rows)
            row["backtest_audit_passed"] = False
            row["backtest_hard_gate_passed"] = False
            row["backtest_hard_gate_reason"] = tradeability.get("reason") or "该产品未通过 ETF 基础交易性检查。"
            row["snapshot_confident"] = False
            continue

        best = _optimize_grid(price_rows, row=row) if len(price_rows) >= 40 else None
        bh = _buy_hold_stats(price_rows) if len(price_rows) >= 40 else None
        multi_period = _multi_period_backtest_from_rows(
            price_rows,
            row=row,
            windows=[(60, "近60天"), (120, "近120天"), (250, "近一年")],
        ) if len(price_rows) >= 40 else []
        decision = _build_strategy_decision(row, best, bh, multi_period)

        grid_ret = _safe_float(best.get("return_pct")) if best else None
        bh_ret = _safe_float(bh.get("return_pct")) if bh else None
        row.update(decision)
        row["grid_step_pct"] = best.get("step_pct") if best else row.get("heuristic_grid_step_pct")
        row["backtest_best_step_pct"] = best.get("step_pct") if best else None
        row["backtest_return_pct"] = grid_ret
        row["buy_hold_return_pct"] = bh_ret
        row["backtest_excess_pct"] = best.get("backtest_excess_pct") if best else (round(grid_ret - bh_ret, 2) if grid_ret is not None and bh_ret is not None else None)
        row["backtest_sharpe"] = best.get("sharpe") if best else None
        row["buy_hold_sharpe"] = bh.get("sharpe") if bh else None
        row["backtest_max_drawdown_pct"] = best.get("max_drawdown_pct") if best else None
        row["buy_hold_max_drawdown_pct"] = bh.get("max_drawdown_pct") if bh else None
        row["grid_candidate_score"] = best.get("candidate_score") if best else None
        row["grid_regime_score"] = best.get("regime_score") if best else None
        row["backtest_trade_quality_score"] = best.get("trade_quality_score") if best else None
        row["backtest_trade_count"] = best.get("trade_count") if best else None
        row["backtest_sell_count"] = best.get("sell_count") if best else None
        row["backtest_win_rate"] = best.get("win_rate") if best else None
        row["backtest_window_days"] = best.get("days") if best else len(price_rows)
        row["backtest_audit_passed"] = (best.get("audit") or {}).get("audit_passed") if best else None
        row["backtest_hard_gate_passed"] = best.get("hard_gate_passed") if best else False
        row["backtest_hard_gate_reason"] = best.get("hard_gate_reason") if best else "未找到通过实盘硬约束的网格步长"
        row["snapshot_confident"] = len(price_rows) >= 120 and best is not None
        row.update(_strategy_return_snapshot(row.get("strategy_type"), grid_ret, bh_ret))

    enriched.sort(key=lambda item: (-(item.get("factor_score") or 0.0), item.get("code") or ""))
    for rank, row in enumerate(enriched, start=1):
        row["factor_rank"] = rank
    return enriched


def _multi_period_backtest(
    mkt_conn,
    code: str,
    *,
    row: Optional[dict] = None,
) -> list[dict]:
    """对 60/120/250/500 天窗口分别做最优网格回测。"""
    return _multi_period_backtest_from_rows(
        _load_price_rows(mkt_conn, code, 500),
        row=row,
    )


def analyze_etf_deep(conn, mkt_conn, code: str) -> Optional[dict]:
    """单只 ETF 深度量化分析。

    返回:
    - info: 基本信息 + 当前技术状态
    - all_steps: 9 个步长的完整回测数据
    - best_step: 最优步长（按综合排名）
    - buy_hold: 买入持有基准
    - best_curve / bh_curve: 最优策略 vs 买入持有的净值曲线
    - multi_period: 不同窗口下的最优策略对比
    - verdict: 量化基金经理视角的结论
    """
    # 获取 ETF 基础信息
    etf_row = conn.execute(
        "SELECT code, name, category FROM etf_asset_universe WHERE code = ?",
        (code,),
    ).fetchone()
    if not etf_row:
        return None

    # 加载 K 线（最多 500 天用于深度分析，覆盖更多市场周期）
    price_rows = _load_price_rows(mkt_conn, code, 500)
    if len(price_rows) < 40:
        return None

    # 优先读最新快照，避免详情页再次扫描全 ETF 宇宙。
    etf_info = load_cached_etf_row(conn, code)
    if not etf_info:
        all_etfs = calc_etf_momentum(conn, mkt_conn)
        for row in all_etfs:
            if row.get("code") == code:
                etf_info = row
                break
        if not etf_info:
            etf_info = {"code": code, "name": dict(etf_row).get("name"), "category": dict(etf_row).get("category")}
        etf_info = enrich_etf_rows_with_strategy_validation(
            [etf_info],
            conn,
            mkt_conn,
        )[0]

    tradeability = assess_etf_tradeability(
        etf_info.get("code") or code,
        etf_info.get("name") or dict(etf_row).get("name") or code,
        etf_info.get("category") or dict(etf_row).get("category"),
        price_rows,
    )
    etf_info["tradeability_supported"] = tradeability.get("supported")
    etf_info["tradeability_status"] = tradeability.get("status")
    etf_info["tradeability_reason"] = tradeability.get("reason")
    etf_info["tradeability_profile"] = tradeability.get("profile") or {}

    if not tradeability.get("supported"):
        reason = tradeability.get("reason") or "该产品未通过 ETF 基础交易性检查。"
        name = etf_info.get("name") or code
        return {
            "info": {
                "code": etf_info.get("code"),
                "name": name,
                "category": etf_info.get("category"),
                "qlib_consensus_score": etf_info.get("qlib_consensus_score"),
                "qlib_consensus_percentile": etf_info.get("qlib_consensus_percentile"),
                "qlib_consensus_factor_group": etf_info.get("qlib_consensus_factor_group"),
                "qlib_high_conviction_count": etf_info.get("qlib_high_conviction_count"),
                "qlib_model_status": etf_info.get("qlib_model_status"),
                "qlib_test_top50_avg_return": etf_info.get("qlib_test_top50_avg_return"),
                "qlib_preferred_strategy": etf_info.get("qlib_preferred_strategy"),
                "qlib_predicted_buy_hold_return_pct": etf_info.get("qlib_predicted_buy_hold_return_pct"),
                "qlib_predicted_grid_return_pct": etf_info.get("qlib_predicted_grid_return_pct"),
                "qlib_predicted_grid_excess_pct": etf_info.get("qlib_predicted_grid_excess_pct"),
                "qlib_predicted_best_step_pct": etf_info.get("qlib_predicted_best_step_pct"),
                "qlib_strategy_edge_pct": etf_info.get("qlib_strategy_edge_pct"),
                "setup_state": etf_info.get("setup_state"),
                "strategy_type": "暂不参与",
                "strategy_reason": reason,
                "heuristic_strategy_type": etf_info.get("heuristic_strategy_type"),
                "heuristic_grid_score": etf_info.get("heuristic_grid_score"),
                "factor_score": etf_info.get("factor_score"),
                "grid_score": etf_info.get("heuristic_grid_score"),
                "volatility_20d": etf_info.get("volatility_20d"),
                "momentum_20d": etf_info.get("momentum_20d"),
                "momentum_60d": etf_info.get("momentum_60d"),
                "max_drawdown_60d": etf_info.get("max_drawdown_60d"),
                "rotation_bucket": etf_info.get("rotation_bucket"),
                "relative_strength_4w": etf_info.get("relative_strength_4w"),
                "relative_strength_12w": etf_info.get("relative_strength_12w"),
                "grid_candidate_score": None,
                "tradeability_supported": False,
                "tradeability_status": tradeability.get("status"),
                "tradeability_reason": reason,
            },
            "optimizer_summary": {
                "candidate_step_count": 0,
                "valid_step_count": 0,
                "rejected_step_count": 0,
                "grid_available": False,
                "model_rules": [
                    reason,
                    "系统已停止为该产品输出 ETF 网格收益和逐笔交易结论。",
                ],
            },
            "all_steps": [],
            "best_step": {},
            "buy_hold": {},
            "daily_prices": [{"date": row.get("date"), "close": row.get("close")} for row in price_rows],
            "multi_period": [],
            "verdict": {
                "rating": "谨慎",
                "summary": reason,
                "lines": [
                    f"{name} 当前不再按 ETF 交易产品处理。",
                    reason,
                    "因此系统不会给出网格收益、逐笔买卖点或买入持有收益结论。",
                ],
            },
            "recommended_strategy": "暂不参与",
        }

    # 1) 全部 9 个步长的回测
    step_candidates = _build_grid_step_candidates(price_rows, row=etf_info)
    bh = _buy_hold_stats(price_rows)
    all_steps = []
    for step in step_candidates:
        bt = _run_grid_backtest(price_rows, step, full_curve=False)
        if bt:
            all_steps.append(_score_grid_backtest(bt, bh, row=etf_info))

    feasible_steps = [step for step in all_steps if step.get("hard_gate_passed")]

    # 2) 找最优步长（与列表/概览共用同一套综合排序）
    best_step_result = _optimize_grid(price_rows, row=etf_info)

    # 3) 最优步长的完整净值曲线
    best_curve_result = None
    if best_step_result:
        best_curve_result = _run_grid_backtest(
            price_rows,
            best_step_result["step_pct"],
            full_curve=True,
            include_trades=True,
        )
        if best_curve_result:
            best_curve_result = _score_grid_backtest(best_curve_result, bh, row=etf_info)

    # 4) 买入持有基准
    bh = bh or _buy_hold_stats(price_rows)

    # 5) 多周期对比
    multi_period = _multi_period_backtest(mkt_conn, code, row=etf_info)

    # 6) 策略推荐：直接复用统一验证后的策略结论
    recommended = etf_info.get("strategy_type") or "买入持有"

    # 7) 量化基金经理视角结论
    verdict = _build_verdict(etf_info, best_step_result, bh, all_steps, multi_period)

    return {
        "info": {
            "code": etf_info.get("code"),
            "name": etf_info.get("name"),
            "category": etf_info.get("category"),
            "qlib_consensus_score": etf_info.get("qlib_consensus_score"),
            "qlib_consensus_percentile": etf_info.get("qlib_consensus_percentile"),
            "qlib_consensus_factor_group": etf_info.get("qlib_consensus_factor_group"),
            "qlib_high_conviction_count": etf_info.get("qlib_high_conviction_count"),
            "qlib_model_status": etf_info.get("qlib_model_status"),
            "qlib_test_top50_avg_return": etf_info.get("qlib_test_top50_avg_return"),
            "qlib_preferred_strategy": etf_info.get("qlib_preferred_strategy"),
            "qlib_predicted_buy_hold_return_pct": etf_info.get("qlib_predicted_buy_hold_return_pct"),
            "qlib_predicted_grid_return_pct": etf_info.get("qlib_predicted_grid_return_pct"),
            "qlib_predicted_grid_excess_pct": etf_info.get("qlib_predicted_grid_excess_pct"),
            "qlib_predicted_best_step_pct": etf_info.get("qlib_predicted_best_step_pct"),
            "qlib_strategy_edge_pct": etf_info.get("qlib_strategy_edge_pct"),
            "setup_state": etf_info.get("setup_state"),
            "strategy_type": etf_info.get("strategy_type"),
            "strategy_reason": etf_info.get("strategy_reason"),
            "heuristic_strategy_type": etf_info.get("heuristic_strategy_type"),
            "heuristic_grid_score": etf_info.get("heuristic_grid_score"),
            "factor_score": etf_info.get("factor_score"),
            "grid_score": etf_info.get("heuristic_grid_score"),
            "volatility_20d": etf_info.get("volatility_20d"),
            "momentum_20d": etf_info.get("momentum_20d"),
            "momentum_60d": etf_info.get("momentum_60d"),
            "max_drawdown_60d": etf_info.get("max_drawdown_60d"),
            "rotation_bucket": etf_info.get("rotation_bucket"),
            "relative_strength_4w": etf_info.get("relative_strength_4w"),
            "relative_strength_12w": etf_info.get("relative_strength_12w"),
            "grid_candidate_score": etf_info.get("grid_candidate_score"),
            "tradeability_supported": etf_info.get("tradeability_supported"),
            "tradeability_status": etf_info.get("tradeability_status"),
            "tradeability_reason": etf_info.get("tradeability_reason"),
        },
        "optimizer_summary": {
            "candidate_step_count": len(all_steps),
            "valid_step_count": len(feasible_steps),
            "rejected_step_count": max(len(all_steps) - len(feasible_steps), 0),
            "grid_available": best_step_result is not None,
            "model_rules": [
                "卖出必须被已持有份额覆盖",
                "现金余额不可为负",
                "成交必须满足100份整手",
                "现金流、份额流、盈亏流都要能对账闭合",
                "没有有效卖出回笼的步长不能进入寻优",
            ],
        },
        "all_steps": all_steps,
        "best_step": best_curve_result or best_step_result,
        "buy_hold": bh,
        "daily_prices": [{"date": row.get("date"), "close": row.get("close")} for row in price_rows],
        "multi_period": multi_period,
        "verdict": verdict,
        "recommended_strategy": recommended,
    }


def _build_verdict(info: dict, best: Optional[dict], bh: Optional[dict],
                   all_steps: list, multi_period: list) -> dict:
    """基于回测数据生成量化基金经理视角的投资结论。"""
    lines = []
    strategy = info.get("strategy_type") or "买入持有"
    name = info.get("name") or info.get("code") or "-"
    feasible_steps = [step for step in all_steps if step.get("hard_gate_passed")]
    qlib_score = _safe_float(info.get("qlib_consensus_score"))
    qlib_factor_group = info.get("qlib_consensus_factor_group") or ""
    qlib_model_status = info.get("qlib_model_status") or ""

    # 策略适配性
    if strategy == "网格交易":
        lines.append(f"{name} 只有在回测验证通过后才保留网格标签，当前属于少数真正可做网格的 ETF。")
    elif strategy == "买入持有":
        lines.append(f"{name} 当前更适合买入持有，不再把启发式网格画像直接当成交易结论。")
    elif strategy == "防守停泊":
        lines.append(f"{name} 属于低波防守资产，更适合作为资金停泊和仓位缓冲。")
    elif strategy == "暂不参与":
        lines.append(f"{name} 结构偏弱，建议回避等待趋势修复后再介入。")
    else:
        lines.append(f"{name} 当前处于 {strategy} 状态。")

    if qlib_model_status == "trained" and qlib_score is not None:
        factor_text = f"，领先因子组 {qlib_factor_group}" if qlib_factor_group else ""
        lines.append(f"ETF-only Qlib 共识 {qlib_score:.1f} 分{factor_text}。")

    if all_steps and not feasible_steps:
        lines.append("候选步长均未通过实盘硬约束或未形成有效卖出回笼，因此本轮不保留网格策略。")
    elif all_steps and feasible_steps and len(feasible_steps) < len(all_steps):
        lines.append(f"{len(feasible_steps)}/{len(all_steps)} 个候选步长通过实盘硬约束，最优步长只从可执行方案中选取。")

    audit_target = best or bh or {}
    audit = audit_target.get("audit") or {}
    if audit:
        if audit.get("audit_passed"):
            lines.append("回测使用 10 万本金、100 份整手和现金/仓位账本约束，未出现负现金或无仓卖出。")
        else:
            lines.append("账本约束校验未通过，该回测结果不能直接作为策略结论。")

    # 最优步长 vs 买入持有
    if best and bh:
        grid_ret = best.get("return_pct") or 0
        bh_ret = bh.get("return_pct") or 0
        excess = round(grid_ret - bh_ret, 2)
        if excess > 2:
            lines.append(f"最优网格步长 {best.get('step_pct')}% 回测超额收益 +{excess}%，网格策略显著优于买入持有。")
        elif excess > 0:
            lines.append(f"最优网格步长 {best.get('step_pct')}% 小幅跑赢买入持有 {excess}%，波段增强效果温和。")
        else:
            lines.append(f"网格策略回测未能跑赢买入持有（差 {excess}%），此标的更适合趋势跟随而非区间震荡。")

        grid_dd = best.get("max_drawdown_pct") or 0
        bh_dd = bh.get("max_drawdown_pct") or 0
        if bh_dd > 0 and grid_dd < bh_dd * 0.8:
            lines.append(f"网格策略最大回撤 {grid_dd}% 显著低于持有的 {bh_dd}%，风控效果好。")

        grid_sharpe = best.get("sharpe")
        bh_sharpe = bh.get("sharpe")
        if grid_sharpe is not None and bh_sharpe is not None:
            if grid_sharpe > bh_sharpe:
                lines.append(f"网格 Sharpe {grid_sharpe} > 持有 Sharpe {bh_sharpe}，风险调整后收益更优。")

    # 步长敏感性
    if len(all_steps) >= 3:
        returns = [s.get("return_pct") or 0 for s in all_steps]
        spread = max(returns) - min(returns)
        if spread < 3:
            lines.append("不同步长之间收益差异小，策略对参数不敏感，鲁棒性好。")
        elif spread > 10:
            lines.append("步长选择对收益影响大，建议严格使用最优步长，避免偏离。")

    # 多周期一致性
    consistent_windows = 0
    for period in multi_period:
        pb = period.get("best")
        pbh = period.get("buy_hold")
        if pb and pbh and (pb.get("return_pct") or 0) > (pbh.get("return_pct") or 0):
            consistent_windows += 1
    if multi_period:
        ratio = consistent_windows / len(multi_period)
        if ratio >= 0.75:
            lines.append(f"网格策略在 {consistent_windows}/{len(multi_period)} 个时间窗口跑赢持有，跨周期稳定性优秀。")
        elif ratio >= 0.5:
            lines.append(f"网格策略在 {consistent_windows}/{len(multi_period)} 个窗口跑赢，稳定性尚可。")
        else:
            lines.append(f"仅在 {consistent_windows}/{len(multi_period)} 个窗口跑赢持有，策略一致性不佳。")

    # 最终评级
    rating = "中性"
    rating_target = best if strategy == "网格交易" else (bh or best)
    if rating_target:
        score = 0
        if (rating_target.get("sharpe") or 0) > 1.0:
            score += 2
        elif (rating_target.get("sharpe") or 0) > 0.5:
            score += 1
        if (rating_target.get("return_pct") or 0) > 5:
            score += 2
        elif (rating_target.get("return_pct") or 0) > 0:
            score += 1
        if (rating_target.get("max_drawdown_pct") or 99) < 5:
            score += 2
        elif (rating_target.get("max_drawdown_pct") or 99) < 10:
            score += 1
        if strategy == "网格交易" and (rating_target.get("win_rate") or 0) > 60:
            score += 1
        if score >= 6:
            rating = "强烈推荐"
        elif score >= 4:
            rating = "推荐"
        elif score >= 2:
            rating = "中性"
        else:
            rating = "谨慎"

    return {
        "rating": rating,
        "summary": "；".join(lines) if lines else "数据不足以给出完整结论。",
        "lines": lines,
    }


def _trend_action(row: dict) -> dict:
    setup_state = row.get("setup_state") or ""
    rotation_bucket = row.get("rotation_bucket") or ""
    rel_4w = _safe_float(row.get("relative_strength_4w")) or 0.0
    rel_12w = _safe_float(row.get("relative_strength_12w")) or 0.0
    trend_status = row.get("trend_status") or ""

    action = "观察"
    reason = "等待结构和轮动进一步确认。"
    if rotation_bucket == "blacklist" or trend_status == "空头":
        action = "退出/回避"
        reason = "当前轮动处于回避区，或日线趋势已经明显转弱。"
    elif setup_state == "收敛待发" and rel_4w > 0 and rel_12w > 0:
        action = "买入观察"
        reason = "相对宽基保持领先，且日线进入收敛待发结构。"
    elif setup_state == "趋势跟随" and rel_12w > 0:
        action = "持有"
        reason = "中期相对强势仍在，适合继续持有或回踩再跟。"
    elif rotation_bucket == "leader" and rel_4w > 0:
        action = "强势观察"
        reason = "处在轮动前排，但日线仍偏松，等收敛后更合适。"
    return {
        "action": action,
        "reason": reason,
    }


def _avg_metric(items: list[dict], key: str) -> Optional[float]:
    values = [_safe_float(item.get(key)) for item in items]
    values = [value for value in values if value is not None]
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _build_etf_mining_snapshot_from_rows(rows: list[dict], factor_snapshot: Optional[dict],
                                         *, grid_topn: int = 6,
                                         trend_topn: int = 6,
                                         rotation_topn: int = 5) -> dict:
    _NON_INDUSTRY = ETF_NON_INDUSTRY_CATS
    grid_candidates = [
        row for row in rows
        if row.get("strategy_type") == "网格交易"
        and (row.get("category") or "") not in _NON_INDUSTRY
        and _safe_float(row.get("backtest_excess_pct")) is not None
        and _safe_float(row.get("backtest_excess_pct")) >= 0.0
    ]
    grid_candidates.sort(
        key=lambda row: (
            -(row.get("grid_candidate_score") or 0.0),
            -(row.get("backtest_excess_pct") or -999.0),
            -(row.get("backtest_return_pct") or -999.0),
            row.get("backtest_max_drawdown_pct") or 999.0,
            -(row.get("factor_score") or 0.0),
            row.get("code") or "",
        )
    )
    grid_results = [
        {
            "code": row.get("code"),
            "name": row.get("name"),
            "category": row.get("category"),
            "rotation_score": row.get("rotation_score"),
            "relative_strength_4w": row.get("relative_strength_4w"),
            "relative_strength_12w": row.get("relative_strength_12w"),
            "setup_state": row.get("setup_state"),
            "grid_score": row.get("heuristic_grid_score"),
            "best_step_pct": row.get("backtest_best_step_pct"),
            "backtest_return_pct": row.get("backtest_return_pct"),
            "backtest_excess_pct": row.get("backtest_excess_pct"),
            "backtest_trade_count": row.get("backtest_trade_count"),
            "backtest_max_drawdown_pct": row.get("backtest_max_drawdown_pct"),
            "grid_candidate_score": row.get("grid_candidate_score"),
            "comparison_return_pct": row.get("comparison_strategy_return_pct"),
        }
        for row in grid_candidates[:grid_topn]
    ]

    trend_candidates = [
        row for row in rows
        if row.get("strategy_type") == "买入持有"
        or (
            row.get("rotation_bucket") == "leader"
            and row.get("setup_state") in ("收敛待发", "趋势跟随", "震荡观察")
        )
    ]
    trend_candidates.sort(
        key=lambda row: (
            -(
                ((_safe_float(row.get("rotation_score")) or 0.0) * 0.45)
                + ((_safe_float(row.get("factor_score")) or 0.0) * 0.35)
                + ((_safe_float(row.get("relative_strength_12w")) or 0.0) * 0.20)
            ),
            -(row.get("rotation_score") or 0.0),
            -(_safe_float(row.get("relative_strength_12w")) or 0.0),
            -(_safe_float(row.get("relative_strength_4w")) or 0.0),
            row.get("code") or "",
        )
    )
    trend_results = []
    for row in trend_candidates[:trend_topn]:
        signal = _trend_action(row)
        trend_results.append({
            "code": row.get("code"),
            "name": row.get("name"),
            "category": row.get("category"),
            "rotation_score": row.get("rotation_score"),
            "relative_strength_4w": row.get("relative_strength_4w"),
            "relative_strength_12w": row.get("relative_strength_12w"),
            "factor_score": row.get("factor_score"),
            "setup_state": row.get("setup_state"),
            "action": signal["action"],
            "reason": signal["reason"],
        })

    next_rotation = [
        {
            "sector_name": item.get("category"),
            "next_rotation_score": item.get("next_rotation_score"),
            "avg_factor_score": item.get("avg_factor_score"),
            "avg_rotation_score": item.get("avg_rotation_score"),
            "avg_relative_strength_4w": item.get("avg_relative_strength_4w"),
            "avg_relative_strength_12w": item.get("avg_relative_strength_12w"),
            "leader_etf_count": item.get("leader_etf_count"),
            "buy_hold_count": item.get("buy_hold_count"),
            "grid_count": item.get("grid_count"),
            "rotation_reason": item.get("rotation_reason"),
            "top_etfs": item.get("top_etfs") or [],
            "top_return_etfs": item.get("top_return_etfs") or [],
        }
        for item in (factor_snapshot or {}).get("categories") or []
        if (item.get("category") or "") not in ETF_NON_INDUSTRY_CATS
    ][:rotation_topn]

    return {
        "grid_candidates": grid_results[:grid_topn],
        "trend_candidates": trend_results[:trend_topn],
        "next_rotation_watchlist": next_rotation,
        "factor_snapshot_id": ((factor_snapshot or {}).get("model") or {}).get("snapshot_id"),
    }


def _build_etf_factor_snapshot_from_rows(
    rows: list[dict],
    mkt_conn,
    *,
    leader_topn: int = 24,
    category_topn: int = 12,
) -> dict:
    if not rows:
        return {"model": None, "leaders": [], "categories": []}

    coverage = _price_coverage_summary(mkt_conn)
    top_cutoff = max(5, math.ceil(len(rows) * 0.15))
    leaders = []
    for row in rows[:leader_topn]:
        leaders.append({
            "code": row.get("code"),
            "name": row.get("name"),
            "category": row.get("category"),
            "factor_rank": row.get("factor_rank"),
            "factor_score": row.get("factor_score"),
            "strategy_type": row.get("strategy_type"),
            "strategy_reason": row.get("strategy_reason"),
            "relative_strength_4w": row.get("relative_strength_4w"),
            "relative_strength_12w": row.get("relative_strength_12w"),
            "rotation_score": row.get("rotation_score"),
            "trend_status": row.get("trend_status"),
            "setup_state": row.get("setup_state"),
            "backtest_best_step_pct": row.get("backtest_best_step_pct"),
            "backtest_return_pct": row.get("backtest_return_pct"),
            "buy_hold_return_pct": row.get("buy_hold_return_pct"),
            "backtest_excess_pct": row.get("backtest_excess_pct"),
            "grid_candidate_score": row.get("grid_candidate_score"),
        })

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row.get("category") or "其他", []).append(row)

    categories = []
    for category, items in grouped.items():
        items_sorted = sorted(items, key=lambda item: (-(item.get("factor_score") or 0.0), item.get("code") or ""))
        top_items = items_sorted[:3]
        top_return_items = sorted(
            [item for item in items if _safe_float(item.get("recommended_strategy_return_pct")) is not None],
            key=lambda item: (
                -(_safe_float(item.get("recommended_strategy_return_pct")) or -9999.0),
                -(_safe_float(item.get("strategy_edge_pct")) or -9999.0),
                item.get("code") or "",
            ),
        )[:5]
        avg_factor = _avg_metric(top_items, "factor_score") or 0.0
        avg_rotation = _avg_metric(top_items, "rotation_score")
        avg_rs4 = _avg_metric(top_items, "relative_strength_4w") or 0.0
        avg_rs12 = _avg_metric(top_items, "relative_strength_12w") or 0.0
        relative_signal = _clamp(50.0 + avg_rs4 * 2.2 + avg_rs12 * 1.4, 0.0, 100.0)
        rotation_signal = _clamp(avg_rotation if avg_rotation is not None else 50.0, 0.0, 100.0)
        next_rotation_score = round(
            _clamp(
                avg_factor * 0.45
                + rotation_signal * 0.35
                + relative_signal * 0.20,
                0.0,
                100.0,
            ),
            1,
        )
        leader_etf_count = sum(1 for item in items if (item.get("factor_rank") or 999999) <= top_cutoff)
        buy_hold_count = sum(1 for item in items if item.get("strategy_type") == "买入持有")
        grid_count = sum(1 for item in items if item.get("strategy_type") == "网格交易")
        rotation_reason = (
            f"前三只 ETF 平均因子分 {avg_factor:.1f}，"
            + (f"平均轮动分 {avg_rotation:.1f}，" if avg_rotation is not None else "")
            + f"4周相强 {avg_rs4:+.2f}% ，12周相强 {avg_rs12:+.2f}% ，"
            + f"前排 ETF {leader_etf_count} 只，买入持有 {buy_hold_count} 只，网格交易 {grid_count} 只。"
        )
        categories.append({
            "category": category,
            "avg_factor_score": round(avg_factor, 1),
            "avg_rotation_score": round(avg_rotation, 1) if avg_rotation is not None else None,
            "avg_relative_strength_4w": round(avg_rs4, 2),
            "avg_relative_strength_12w": round(avg_rs12, 2),
            "next_rotation_score": next_rotation_score,
            "leader_etf_count": leader_etf_count,
            "buy_hold_count": buy_hold_count,
            "grid_count": grid_count,
            "rotation_reason": rotation_reason,
            "top_etfs": [
                {
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "factor_score": item.get("factor_score"),
                    "strategy_type": item.get("strategy_type"),
                }
                for item in top_items
            ],
            "top_return_etfs": [
                {
                    "code": item.get("code"),
                    "name": item.get("name"),
                    "strategy_type": item.get("strategy_type"),
                    "recommended_strategy_label": item.get("recommended_strategy_label"),
                    "recommended_strategy_return_pct": item.get("recommended_strategy_return_pct"),
                    "comparison_strategy_label": item.get("comparison_strategy_label"),
                    "comparison_strategy_return_pct": item.get("comparison_strategy_return_pct"),
                    "strategy_edge_pct": item.get("strategy_edge_pct"),
                    "buy_hold_return_pct": item.get("buy_hold_return_pct"),
                    "backtest_return_pct": item.get("backtest_return_pct"),
                    "backtest_excess_pct": item.get("backtest_excess_pct"),
                    "relative_strength_4w": item.get("relative_strength_4w"),
                    "relative_strength_12w": item.get("relative_strength_12w"),
                }
                for item in top_return_items
            ],
        })

    categories.sort(
        key=lambda item: (
            -(item.get("next_rotation_score") or 0.0),
            -(item.get("avg_factor_score") or 0.0),
            item.get("category") or "",
        )
    )
    history_end = coverage.get("max_date")
    return {
        "model": {
            "snapshot_id": f"etf_factor_{(history_end or 'na').replace('-', '')}",
            "model_type": "etf_native_factor_snapshot",
            "etf_count": len(rows),
            "factor_count": 6,
            "history_start": coverage.get("min_date"),
            "history_end": history_end,
            "basis": "ETF 原生价格、相对强弱和结构因子快照。",
        },
        "leaders": leaders,
        "categories": categories[:category_topn],
    }


def build_etf_factor_snapshot(conn, mkt_conn, *, leader_topn: int = 24, category_topn: int = 12) -> dict:
    rows = enrich_etf_rows_with_strategy_validation(
        calc_etf_momentum(conn, mkt_conn),
        conn,
        mkt_conn,
    )
    return _build_etf_factor_snapshot_from_rows(
        rows,
        mkt_conn,
        leader_topn=leader_topn,
        category_topn=category_topn,
    )


def build_etf_mining_snapshot(conn, mkt_conn, *, grid_topn: int = 6,
                              trend_topn: int = 6, rotation_topn: int = 5) -> dict:
    rows = enrich_etf_rows_with_strategy_validation(
        calc_etf_momentum(conn, mkt_conn),
        conn,
        mkt_conn,
    )
    factor_snapshot = _build_etf_factor_snapshot_from_rows(
        rows,
        mkt_conn,
        leader_topn=18,
        category_topn=max(rotation_topn, 10),
    )
    return _build_etf_mining_snapshot_from_rows(
        rows,
        factor_snapshot,
        grid_topn=grid_topn,
        trend_topn=trend_topn,
        rotation_topn=rotation_topn,
    )

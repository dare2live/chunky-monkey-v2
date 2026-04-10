"""
etf_mining_engine.py — ETF 挖掘建议引擎

职责：
- 对 ETF 做确定性策略分类后的进一步建议
- 网格候选：用简单可解释的区间网格回测，给出步长建议
- 趋势持有：给出当前动作建议
- 下一轮动板块：聚合现有股票 Qlib 预测，输出行业观察名单

说明：
- 网格回测是确定性策略问题，不交给 Qlib
- 板块轮动前瞻属于未来收益问题，优先复用现有股票 Qlib 结果做行业聚合
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional

from services.etf_engine import calc_etf_momentum
from services.utils import safe_float as _safe_float, clamp as _clamp
from services.constants import ETF_NON_INDUSTRY_CATS


def _load_price_rows(mkt_conn, code: str, limit: int = 180) -> list[dict]:
    rows = mkt_conn.execute(
        """
        SELECT date, close
        FROM price_kline
        WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
        ORDER BY date DESC
        LIMIT ?
        """,
        (code, limit),
    ).fetchall()
    return list(reversed([dict(row) for row in rows]))


def _max_drawdown(values: list[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    peak = values[0]
    max_dd = 0.0
    for value in values:
        if value > peak:
            peak = value
        if peak > 0:
            dd = (peak - value) / peak * 100
            if dd > max_dd:
                max_dd = dd
    return round(max_dd, 2)


def _run_grid_backtest(price_rows: list[dict], step_pct: float,
                       tranche_count: int = 8, fee_bps: float = 5.0,
                       *, full_curve: bool = False) -> Optional[dict]:
    """固定网格回测：以起始价为中心建立对称网格，每到一个低网格买入一档，
    到高网格卖出一档。使用 FIFO 追踪买入成本，正确统计胜率。

    full_curve=True 时额外返回每日净值序列。
    """
    closes = [_safe_float(row.get("close")) for row in price_rows]
    dates = [row.get("date") for row in price_rows]
    closes = [(c, d) for c, d in zip(closes, dates) if c not in (None, 0)]
    if len(closes) < 40:
        return None

    fee = fee_bps / 10000.0
    initial_price = closes[0][0]
    step_ratio = step_pct / 100.0

    # 建立固定网格：基准价 = 起始价，向上/向下各 tranche_count 级
    grid_levels = []
    for i in range(-tranche_count, tranche_count + 1):
        grid_levels.append(initial_price * (1 + i * step_ratio))
    grid_levels.sort()

    # 初始仓位：持有 tranche_count//2 份（半仓），对应中间网格
    initial_tranches = tranche_count // 2
    tranche_value = 1.0 / tranche_count  # 每档价值占总资产比例
    cash = 1.0 - initial_tranches * tranche_value
    units = (initial_tranches * tranche_value) / initial_price
    # FIFO 买入队列：[(buy_price, units_bought), ...]
    buy_queue = [(initial_price, units)] if units > 0 else []

    # 跟踪当前持仓对应的"网格持仓层级"
    # 起始在中间层，买入降一层，卖出升一层
    center_idx = len(grid_levels) // 2
    current_level = center_idx - initial_tranches  # 下一次买入触发的网格索引
    sell_level = center_idx + 1  # 下一次卖出触发的网格索引

    trade_count = 0
    buy_count = 0
    sell_count = 0
    win_trades = 0
    lose_trades = 0
    portfolio_values = [1.0]
    curve_dates = [closes[0][1]]

    prev_close = initial_price
    for close, date in closes[1:]:
        # 买入逻辑：价格降到当前买入网格以下
        while (current_level >= 0 and
               close <= grid_levels[current_level] and
               cash >= tranche_value * 0.3):
            invest = min(tranche_value, cash)
            bought_units = (invest * (1 - fee)) / close
            units += bought_units
            cash -= invest
            buy_queue.append((close, bought_units))
            trade_count += 1
            buy_count += 1
            current_level -= 1
            sell_level -= 1

        # 卖出逻辑：价格升到当前卖出网格以上
        while (sell_level < len(grid_levels) and
               close >= grid_levels[sell_level] and
               units * close >= tranche_value * 0.3):
            sell_value = min(tranche_value, units * close)
            sell_units = sell_value / close
            units -= sell_units
            cash += sell_value * (1 - fee)
            trade_count += 1
            sell_count += 1
            # FIFO 胜率：与最早买入的成本比较
            if buy_queue:
                cost_price = buy_queue[0][0]
                if close > cost_price:
                    win_trades += 1
                else:
                    lose_trades += 1
                # 消耗 FIFO 队列
                remaining = sell_units
                while remaining > 1e-10 and buy_queue:
                    bp, bu = buy_queue[0]
                    if bu <= remaining + 1e-10:
                        remaining -= bu
                        buy_queue.pop(0)
                    else:
                        buy_queue[0] = (bp, bu - remaining)
                        remaining = 0
            current_level += 1
            sell_level += 1

        pv = cash + units * close
        portfolio_values.append(pv)
        curve_dates.append(date)
        prev_close = close

    final_value = cash + units * closes[-1][0]
    max_dd = _max_drawdown(portfolio_values)
    days = len(closes)

    # 日收益率序列
    daily_returns = []
    for i in range(1, len(portfolio_values)):
        if portfolio_values[i - 1] > 0:
            daily_returns.append(portfolio_values[i] / portfolio_values[i - 1] - 1.0)

    # 年化收益
    annual_return = None
    if days > 1:
        annual_return = round(((final_value) ** (252.0 / days) - 1.0) * 100.0, 2)

    # Sharpe ratio (假设无风险利率 2%)
    sharpe = None
    if daily_returns:
        mean_ret = sum(daily_returns) / len(daily_returns)
        rf_daily = 0.02 / 252.0
        variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
        std = math.sqrt(variance) if variance > 0 else 0
        if std > 0:
            sharpe = round((mean_ret - rf_daily) / std * math.sqrt(252), 2)

    # Calmar ratio
    calmar = None
    if annual_return is not None and max_dd and max_dd > 0:
        calmar = round(annual_return / max_dd, 2)

    # 胜率（基于实际卖出交易）
    total_completed = win_trades + lose_trades
    win_rate = round(win_trades / total_completed * 100.0, 1) if total_completed > 0 else None

    result = {
        "step_pct": round(step_pct, 1),
        "return_pct": round((final_value - 1.0) * 100.0, 2),
        "annual_return_pct": annual_return,
        "trade_count": trade_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "win_rate": win_rate,
        "max_drawdown_pct": max_dd,
        "sharpe": sharpe,
        "calmar": calmar,
        "days": days,
    }
    if full_curve:
        step = max(1, len(portfolio_values) // 60)
        result["curve"] = [
            {"date": curve_dates[i], "nav": round(portfolio_values[i], 4)}
            for i in range(0, len(portfolio_values), step)
        ]
        if len(portfolio_values) > 1:
            result["curve"].append({
                "date": curve_dates[-1],
                "nav": round(portfolio_values[-1], 4),
            })
    return result


def _optimize_grid(price_rows: list[dict]) -> Optional[dict]:
    candidates = [0.8, 1.2, 1.6, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5]
    results = []
    for step in candidates:
        backtest = _run_grid_backtest(price_rows, step)
        if backtest:
            results.append(backtest)
    if not results:
        return None
    results.sort(
        key=lambda item: (
            -(item.get("return_pct") or -999.0),
            item.get("max_drawdown_pct") or 999.0,
            -(item.get("trade_count") or 0),
        )
    )
    return results[0]


# ------------------------------------------------------------------
# 买入持有基准 + 多周期对比
# ------------------------------------------------------------------

def _buy_hold_stats(price_rows: list[dict]) -> Optional[dict]:
    """买入持有基准计算：收益率、年化、最大回撤、Sharpe。"""
    closes = [_safe_float(row.get("close")) for row in price_rows]
    dates = [row.get("date") for row in price_rows]
    pairs = [(c, d) for c, d in zip(closes, dates) if c not in (None, 0)]
    if len(pairs) < 10:
        return None

    first = pairs[0][0]
    values = [c / first for c, _ in pairs]
    final = values[-1]
    days = len(pairs)
    max_dd = _max_drawdown(values)

    annual_return = round(((final) ** (252.0 / days) - 1.0) * 100.0, 2)

    daily_returns = []
    for i in range(1, len(values)):
        daily_returns.append(values[i] / values[i - 1] - 1.0)

    sharpe = None
    if daily_returns:
        mean_ret = sum(daily_returns) / len(daily_returns)
        rf_daily = 0.02 / 252.0
        variance = sum((r - mean_ret) ** 2 for r in daily_returns) / len(daily_returns)
        std = math.sqrt(variance) if variance > 0 else 0
        if std > 0:
            sharpe = round((mean_ret - rf_daily) / std * math.sqrt(252), 2)

    calmar = None
    if max_dd and max_dd > 0:
        calmar = round(annual_return / max_dd, 2)

    # 采样净值曲线
    step = max(1, len(values) // 60)
    curve = [
        {"date": pairs[i][1], "nav": round(values[i], 4)}
        for i in range(0, len(values), step)
    ]
    if len(values) > 1:
        curve.append({"date": pairs[-1][1], "nav": round(values[-1], 4)})

    return {
        "return_pct": round((final - 1.0) * 100.0, 2),
        "annual_return_pct": annual_return,
        "max_drawdown_pct": max_dd,
        "sharpe": sharpe,
        "calmar": calmar,
        "days": days,
        "curve": curve,
    }


def _multi_period_backtest(mkt_conn, code: str) -> list[dict]:
    """对 60/120/180/250 天窗口分别做最优网格回测。"""
    windows = [
        (60, "近60天"),
        (120, "近120天"),
        (250, "近一年"),
        (500, "近两年"),
    ]
    results = []
    for limit, label in windows:
        rows = _load_price_rows(mkt_conn, code, limit)
        if len(rows) < 40:
            results.append({"window": label, "days": len(rows), "best": None, "buy_hold": None})
            continue
        best = _optimize_grid(rows)
        bh = _buy_hold_stats(rows)
        if best:
            best["window"] = label
        results.append({
            "window": label,
            "days": len(rows),
            "best": best,
            "buy_hold": bh,
        })
    return results


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
        "SELECT code, name, category FROM dim_asset_universe "
        "WHERE code = ? AND asset_type = 'etf'",
        (code,),
    ).fetchone()
    if not etf_row:
        return None

    # 加载 K 线（最多 500 天用于深度分析，覆盖更多市场周期）
    price_rows = _load_price_rows(mkt_conn, code, 500)
    if len(price_rows) < 40:
        return None

    # 计算 ETF 动量指标（单只）
    all_etfs = calc_etf_momentum(conn, mkt_conn)
    etf_info = None
    for row in all_etfs:
        if row.get("code") == code:
            etf_info = row
            break
    if not etf_info:
        etf_info = {"code": code, "name": dict(etf_row).get("name"), "category": dict(etf_row).get("category")}

    # 1) 全部 9 个步长的回测
    step_candidates = [0.8, 1.2, 1.6, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5]
    all_steps = []
    for step in step_candidates:
        bt = _run_grid_backtest(price_rows, step, full_curve=False)
        if bt:
            all_steps.append(bt)

    # 2) 找最优步长（综合排名: 收益 40% + Sharpe 30% + 最大回撤逆序 30%）
    best_step_result = None
    if all_steps:
        ranked = list(all_steps)
        n = len(ranked)
        # 分项排名
        by_return = sorted(range(n), key=lambda i: -(ranked[i].get("return_pct") or -999))
        by_sharpe = sorted(range(n), key=lambda i: -(ranked[i].get("sharpe") or -999))
        by_dd = sorted(range(n), key=lambda i: ranked[i].get("max_drawdown_pct") or 999)
        ranks = {}
        for order, indices in [(by_return, 0.4), (by_sharpe, 0.3), (by_dd, 0.3)]:
            for rank, idx in enumerate(order):
                ranks.setdefault(idx, 0.0)
                ranks[idx] += rank * indices
        best_idx = min(ranks, key=ranks.get)
        best_step_result = ranked[best_idx]

    # 3) 最优步长的完整净值曲线
    best_curve_result = None
    if best_step_result:
        best_curve_result = _run_grid_backtest(
            price_rows, best_step_result["step_pct"], full_curve=True
        )

    # 4) 买入持有基准
    bh = _buy_hold_stats(price_rows)

    # 5) 多周期对比
    multi_period = _multi_period_backtest(mkt_conn, code)

    # 6) 策略推荐：网格 vs 买入持有
    recommended = "买入持有"  # 默认
    if best_step_result and bh:
        grid_sharpe = best_step_result.get("sharpe") or 0
        bh_sharpe = bh.get("sharpe") or 0
        grid_ret = best_step_result.get("return_pct") or 0
        bh_ret = bh.get("return_pct") or 0
        grid_dd = best_step_result.get("max_drawdown_pct") or 999
        bh_dd = bh.get("max_drawdown_pct") or 999
        # 多周期中跑赢持有的窗口数
        mp_wins = sum(1 for p in multi_period
                      if p.get("best") and p.get("buy_hold")
                      and (p["best"].get("return_pct") or 0) > (p["buy_hold"].get("return_pct") or 0))
        mp_total = sum(1 for p in multi_period if p.get("best") and p.get("buy_hold"))
        # 综合判断：收益差距>30%直接判负；否则看Sharpe+多周期+回撤
        if bh_ret > 0 and grid_ret < bh_ret * 0.7:
            recommended = "买入持有"  # 网格收益差距太大
        else:
            grid_score = 0
            if grid_sharpe > bh_sharpe * 1.05:
                grid_score += 1
            if mp_total > 0 and mp_wins / mp_total > 0.5:
                grid_score += 2
            if grid_ret >= bh_ret:
                grid_score += 2
            if grid_dd < bh_dd * 0.6:
                grid_score += 1
            recommended = "网格交易" if grid_score >= 3 else "买入持有"

    # 7) 量化基金经理视角结论
    verdict = _build_verdict(etf_info, best_step_result, bh, all_steps, multi_period)

    return {
        "info": {
            "code": etf_info.get("code"),
            "name": etf_info.get("name"),
            "category": etf_info.get("category"),
            "setup_state": etf_info.get("setup_state"),
            "strategy_type": etf_info.get("strategy_type"),
            "grid_score": etf_info.get("grid_score"),
            "volatility_20d": etf_info.get("volatility_20d"),
            "momentum_20d": etf_info.get("momentum_20d"),
            "momentum_60d": etf_info.get("momentum_60d"),
            "max_drawdown_60d": etf_info.get("max_drawdown_60d"),
            "rotation_bucket": etf_info.get("rotation_bucket"),
            "relative_strength_4w": etf_info.get("relative_strength_4w"),
            "relative_strength_12w": etf_info.get("relative_strength_12w"),
        },
        "all_steps": all_steps,
        "best_step": best_curve_result or best_step_result,
        "buy_hold": bh,
        "multi_period": multi_period,
        "verdict": verdict,
        "recommended_strategy": recommended,
    }


def _build_verdict(info: dict, best: Optional[dict], bh: Optional[dict],
                   all_steps: list, multi_period: list) -> dict:
    """基于回测数据生成量化基金经理视角的投资结论。"""
    lines = []
    strategy = info.get("strategy_type") or "观察池"
    setup = info.get("setup_state") or "待补结构"
    name = info.get("name") or info.get("code") or "-"

    # 策略适配性
    if strategy == "网格候选":
        lines.append(f"{name} 波动特征适合网格交易，振幅和波动率处于网格策略甜蜜区。")
    elif strategy == "趋势持有":
        lines.append(f"{name} 处于趋势上行通道，建议持有为主，不宜频繁做网格。")
    elif strategy == "暂不参与":
        lines.append(f"{name} 结构偏弱，建议回避等待趋势修复后再介入。")
    else:
        lines.append(f"{name} 当前处于 {strategy} 状态。")

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
    if best:
        score = 0
        if (best.get("sharpe") or 0) > 1.0:
            score += 2
        elif (best.get("sharpe") or 0) > 0.5:
            score += 1
        if (best.get("return_pct") or 0) > 5:
            score += 2
        elif (best.get("return_pct") or 0) > 0:
            score += 1
        if (best.get("max_drawdown_pct") or 99) < 5:
            score += 2
        elif (best.get("max_drawdown_pct") or 99) < 10:
            score += 1
        if (best.get("win_rate") or 0) > 60:
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


def _trend_action(row: Dict) -> dict:
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


def _load_next_rotation_watchlist(conn, topn: int = 5) -> dict:
    model_row = conn.execute(
        """
        SELECT model_id
        FROM qlib_model_state
        WHERE status = 'trained'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if not model_row:
        return {"model_id": None, "data": []}
    model_id = model_row["model_id"]

    rows = conn.execute(
        """
        SELECT ctx.sw_level1 AS sector_name,
               AVG(p.qlib_percentile) AS avg_qlib_percentile,
               AVG(p.qlib_score) AS avg_qlib_score,
               SUM(CASE WHEN p.qlib_percentile >= 80 THEN 1 ELSE 0 END) AS high_conviction_count,
               COUNT(*) AS stock_count,
               msm.rotation_score,
               msm.rotation_rank_1m,
               msm.rotation_rank_3m,
               msm.rotation_bucket,
               msm.trend_state,
               msm.momentum_score
        FROM qlib_predictions p
        INNER JOIN dim_stock_industry_context_latest ctx ON ctx.stock_code = p.stock_code
        LEFT JOIN mart_sector_momentum msm ON msm.sector_name = ctx.sw_level1
        WHERE p.model_id = ?
          AND ctx.sw_level1 IS NOT NULL
          AND ctx.sw_level1 != ''
        GROUP BY ctx.sw_level1
        HAVING COUNT(*) >= 5
        """,
        (model_id,),
    ).fetchall()

    candidates = []
    for row in rows:
        item = dict(row)
        if item.get("rotation_bucket") == "leader":
            continue
        avg_q = _safe_float(item.get("avg_qlib_percentile")) or 0.0
        improve = (item.get("rotation_rank_3m") or 99) - (item.get("rotation_rank_1m") or 99)
        trend_bonus = {
            "recovering": 10,
            "bullish": 8,
            "neutral": 2,
            "weakening": -4,
            "bearish": -8,
        }.get(item.get("trend_state") or "", 0)
        next_rotation_score = (
            avg_q * 0.60
            + _clamp(improve * 2.5, -10, 15)
            + ((_safe_float(item.get("momentum_score")) or 50.0) - 50.0) * 0.20
            + trend_bonus
        )
        if item.get("rotation_bucket") == "blacklist" and avg_q < 75:
            continue
        item["next_rotation_score"] = round(_clamp(next_rotation_score, 0, 100), 1)
        candidates.append(item)

    candidates.sort(
        key=lambda item: (
            -(item.get("next_rotation_score") or 0.0),
            -(item.get("avg_qlib_percentile") or 0.0),
            -(item.get("high_conviction_count") or 0),
            item.get("sector_name") or "",
        )
    )
    return {
        "model_id": model_id,
        "data": candidates[:topn],
    }


def build_etf_mining_snapshot(conn, mkt_conn, *, grid_topn: int = 6,
                              trend_topn: int = 6, rotation_topn: int = 5) -> dict:
    rows = calc_etf_momentum(conn, mkt_conn)

    _NON_INDUSTRY = ETF_NON_INDUSTRY_CATS
    grid_candidates = [
        row for row in rows
        if row.get("strategy_type") == "网格候选"
        and (row.get("category") or "") not in _NON_INDUSTRY
    ]
    grid_candidates.sort(
        key=lambda row: (
            -(row.get("grid_score") or 0.0),
            -(row.get("rotation_score") or 0.0),
            abs(_safe_float(row.get("relative_strength_4w")) or 0.0),
            row.get("code") or "",
        )
    )
    grid_results = []
    for row in grid_candidates[: max(grid_topn * 4, 18)]:
        backtest = _optimize_grid(_load_price_rows(mkt_conn, row["code"], 180))
        if not backtest:
            continue
        grid_results.append({
            "code": row.get("code"),
            "name": row.get("name"),
            "category": row.get("category"),
            "rotation_score": row.get("rotation_score"),
            "relative_strength_4w": row.get("relative_strength_4w"),
            "relative_strength_12w": row.get("relative_strength_12w"),
            "setup_state": row.get("setup_state"),
            "grid_score": row.get("grid_score"),
            "best_step_pct": backtest.get("step_pct"),
            "backtest_return_pct": backtest.get("return_pct"),
            "backtest_trade_count": backtest.get("trade_count"),
            "backtest_max_drawdown_pct": backtest.get("max_drawdown_pct"),
        })
    grid_results.sort(
        key=lambda item: (
            -(item.get("backtest_return_pct") or -999.0),
            item.get("backtest_max_drawdown_pct") or 999.0,
            -(item.get("rotation_score") or 0.0),
        )
    )

    trend_candidates = [
        row for row in rows
        if row.get("strategy_type") == "趋势持有"
        or (
            row.get("rotation_bucket") == "leader"
            and row.get("setup_state") in ("收敛待发", "趋势跟随", "震荡观察")
        )
    ]
    trend_candidates.sort(
        key=lambda row: (
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
            "setup_state": row.get("setup_state"),
            "action": signal["action"],
            "reason": signal["reason"],
        })

    next_rotation = _load_next_rotation_watchlist(conn, topn=rotation_topn)

    return {
        "grid_candidates": grid_results[:grid_topn],
        "trend_candidates": trend_results[:trend_topn],
        "next_rotation_watchlist": next_rotation.get("data") or [],
        "qlib_model_id": next_rotation.get("model_id"),
    }

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
                       tranche_count: int = 8, fee_bps: float = 5.0) -> Optional[dict]:
    closes = [_safe_float(row.get("close")) for row in price_rows]
    closes = [value for value in closes if value not in (None, 0)]
    if len(closes) < 40:
        return None

    fee = fee_bps / 10000.0
    initial_price = closes[0]
    cash = 0.5
    units = 0.5 / initial_price
    tranche_cash = 1.0 / tranche_count
    anchor_price = initial_price
    trade_count = 0
    portfolio_values = [1.0]

    for close in closes[1:]:
        while close <= anchor_price * (1 - step_pct / 100.0) and cash >= tranche_cash * 0.5:
            invest = min(tranche_cash, cash)
            units += (invest * (1 - fee)) / close
            cash -= invest
            anchor_price *= (1 - step_pct / 100.0)
            trade_count += 1
        while close >= anchor_price * (1 + step_pct / 100.0) and units * close >= tranche_cash * 0.5:
            gross_value = min(tranche_cash, units * close)
            sell_units = gross_value / close
            units -= sell_units
            cash += gross_value * (1 - fee)
            anchor_price *= (1 + step_pct / 100.0)
            trade_count += 1
        portfolio_values.append(cash + units * close)

    final_value = cash + units * closes[-1]
    return {
        "step_pct": round(step_pct, 1),
        "return_pct": round((final_value - 1.0) * 100.0, 2),
        "trade_count": trade_count,
        "max_drawdown_pct": _max_drawdown(portfolio_values),
    }


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

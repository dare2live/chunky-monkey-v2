"""etf_grid_engine.py - ETF 网格回测与策略决策引擎。"""

from __future__ import annotations

import math
from typing import Optional

from services.utils import clamp as _clamp
from services.utils import safe_float as _safe_float


def is_supported_exchange_etf_code(code: str) -> bool:
    text = str(code or "").strip()
    if len(text) != 6 or not text.isdigit():
        return False
    if text.startswith("519"):
        return False
    return True


def _price_jump_profile(price_rows: list[dict]) -> dict:
    closes = [_safe_float(row.get("close")) for row in price_rows]
    closes = [close for close in closes if close not in (None, 0)]
    if len(closes) < 2:
        return {
            "max_abs_change_pct": None,
            "jump_days": 0,
            "price_span_ratio": None,
        }

    changes = []
    for prev_close, close in zip(closes, closes[1:]):
        if prev_close and prev_close > 0:
            changes.append(close / prev_close - 1.0)

    max_abs_change_pct = max((abs(change) * 100.0 for change in changes), default=None)
    jump_days = sum(1 for change in changes if abs(change) >= 0.35)
    min_close = min(closes)
    max_close = max(closes)
    price_span_ratio = round(max_close / min_close, 2) if min_close > 0 else None
    return {
        "max_abs_change_pct": round(max_abs_change_pct, 2) if max_abs_change_pct is not None else None,
        "jump_days": jump_days,
        "price_span_ratio": price_span_ratio,
    }


def assess_etf_tradeability(
    code: str,
    name: str,
    category: Optional[str],
    price_rows: Optional[list[dict]] = None,
) -> dict:
    text_code = str(code or "").strip()
    text_name = str(name or "").strip()
    profile = _price_jump_profile(price_rows or [])

    if text_code and not is_supported_exchange_etf_code(text_code):
        reason = f"{text_code or '该产品'} 更像场外开放式基金份额，不属于可交易 ETF 池。"
        return {
            "supported": False,
            "status": "unsupported_instrument",
            "reason": reason,
            "detail": reason,
            "profile": profile,
            "category": category,
            "name": text_name,
        }

    max_abs_change_pct = _safe_float(profile.get("max_abs_change_pct"))
    jump_days = int(profile.get("jump_days") or 0)
    price_span_ratio = _safe_float(profile.get("price_span_ratio"))
    if (
        max_abs_change_pct is not None
        and max_abs_change_pct >= 65.0
        and (
            jump_days >= 1
            or (price_span_ratio is not None and price_span_ratio >= 8.0)
        )
    ):
        reason = (
            f"日线出现异常跳变，最大单日波动 {max_abs_change_pct:.2f}%"
            + (f"，价格跨度 {price_span_ratio:.2f} 倍" if price_span_ratio is not None else "")
            + "，不能用于 ETF 网格回测。"
        )
        return {
            "supported": False,
            "status": "abnormal_price_series",
            "reason": reason,
            "detail": reason,
            "profile": profile,
            "category": category,
            "name": text_name,
        }

    return {
        "supported": True,
        "status": "ok" if text_code else "code_missing",
        "reason": "",
        "detail": "通过 ETF 代码与价格序列基础检查。" if text_code else "未提供 ETF 代码，仅基于价格序列通过基础检查。",
        "profile": profile,
        "category": category,
        "name": text_name,
    }


def _median_abs_move_pct(price_rows: list[dict]) -> Optional[float]:
    closes = [_safe_float(row.get("close")) for row in price_rows]
    closes = [close for close in closes if close not in (None, 0)]
    if len(closes) < 12:
        return None

    changes = []
    for prev_close, close in zip(closes, closes[1:]):
        if prev_close and prev_close > 0:
            changes.append(abs(close / prev_close - 1.0) * 100.0)
    if not changes:
        return None

    changes.sort()
    mid = len(changes) // 2
    if len(changes) % 2 == 1:
        return round(changes[mid], 2)
    return round((changes[mid - 1] + changes[mid]) / 2.0, 2)


def _build_grid_step_candidates(
    price_rows: list[dict],
    *,
    row: Optional[dict] = None,
) -> list[float]:
    row = row or {}
    heuristic_step = _safe_float(row.get("heuristic_grid_step_pct") or row.get("grid_step_pct"))
    amplitude_20d = _safe_float(row.get("amplitude_20d"))
    volatility_20d = _safe_float(row.get("volatility_20d"))
    median_move = _median_abs_move_pct(price_rows)

    seeds = [
        heuristic_step,
        amplitude_20d / 6.0 if amplitude_20d is not None else None,
        volatility_20d / 9.0 if volatility_20d is not None else None,
        median_move * 1.8 if median_move is not None else None,
    ]
    seeds = [value for value in seeds if value is not None]
    base_step = sum(seeds) / len(seeds) if seeds else 1.6

    trend = row.get("trend_status") or ""
    setup_state = row.get("setup_state") or ""
    if trend == "多头":
        base_step *= 1.05
    elif trend == "空头":
        base_step *= 1.10
    elif trend == "震荡":
        base_step *= 0.95

    if setup_state == "收敛待发":
        base_step *= 1.05
    elif setup_state == "结构松散":
        base_step *= 0.90

    candidates = [0.8, 1.2, 1.6, 2.0, 2.6, 3.2, 4.0, 5.0]
    candidates.extend(base_step * factor for factor in (0.72, 0.88, 1.0, 1.12, 1.28))
    if heuristic_step is not None:
        candidates.append(heuristic_step)

    unique: list[float] = []
    seen: set[float] = set()
    for value in candidates:
        step = round(_clamp(value, 0.8, 6.0), 1)
        if step not in seen:
            seen.add(step)
            unique.append(step)
    unique.sort()
    return unique


def _ledger_audit_failures(audit: dict) -> list[str]:
    checks = (
        ("cash_never_negative", "现金账本出现负值"),
        ("position_never_negative", "持仓份额出现负值"),
        ("sell_units_backed", "卖出份额未被真实持仓覆盖"),
        ("sell_cost_backed", "卖出成本未被历史买入覆盖"),
        ("open_units_match_batches", "持仓批次与剩余份额不一致"),
        ("lot_size_respected", "成交未满足整手约束"),
        ("cash_flow_reconciled", "现金流水未能对账闭合"),
        ("position_flow_reconciled", "买卖份额流水未能对账闭合"),
        ("pnl_flow_reconciled", "盈亏账本未能对账闭合"),
    )
    return [label for key, label in checks if audit.get(key) is False]


def _grid_hard_gate(backtest: dict) -> dict:
    audit = dict(backtest.get("audit") or {})
    failures = list(audit.get("failures") or _ledger_audit_failures(audit))
    sell_count = int(backtest.get("sell_count") or 0)
    trade_count = int(backtest.get("trade_count") or 0)
    sell_net_total = round(_safe_float(backtest.get("sell_net_total")) or 0.0, 2)
    initial_position_cost = round(_safe_float(backtest.get("initial_position_cost")) or 0.0, 2)

    if initial_position_cost <= 0:
        failures.append("初始底仓未能按实盘约束建成")
    if trade_count <= 0:
        failures.append("未形成有效网格成交")
    if sell_count <= 0 or sell_net_total <= 0:
        failures.append("未形成有效卖出回笼")

    unique_failures = list(dict.fromkeys(failures))
    hard_gate_passed = len(unique_failures) == 0
    return {
        "hard_gate_passed": hard_gate_passed,
        "hard_gate_reason": "通过实盘硬约束" if hard_gate_passed else "；".join(unique_failures),
        "hard_gate_failures": unique_failures,
    }


def _score_grid_backtest(
    backtest: dict,
    bh: Optional[dict],
    *,
    row: Optional[dict] = None,
) -> dict:
    row = row or {}
    bh = bh or {}
    hard_gate = _grid_hard_gate(backtest)

    grid_ret = _safe_float(backtest.get("return_pct")) or 0.0
    bh_ret = _safe_float(bh.get("return_pct")) or 0.0
    excess = round(grid_ret - bh_ret, 2)

    grid_sharpe = _safe_float(backtest.get("sharpe"))
    bh_sharpe = _safe_float(bh.get("sharpe"))
    grid_dd = _safe_float(backtest.get("max_drawdown_pct"))
    bh_dd = _safe_float(bh.get("max_drawdown_pct"))

    excess_score = _clamp(50.0 + excess * 7.0, 0.0, 100.0)
    if grid_sharpe is None and bh_sharpe is None:
        sharpe_score = 50.0
    elif grid_sharpe is not None and bh_sharpe is None:
        sharpe_score = 62.0
    else:
        sharpe_score = _clamp(50.0 + ((grid_sharpe or 0.0) - (bh_sharpe or 0.0)) * 18.0, 0.0, 100.0)

    if grid_dd is None and bh_dd is None:
        dd_score = 50.0
    elif grid_dd is not None and bh_dd is None:
        dd_score = 58.0
    else:
        dd_score = _clamp(50.0 + ((bh_dd or 0.0) - (grid_dd or 0.0)) * 4.5, 0.0, 100.0)

    sell_count = int(backtest.get("sell_count") or 0)
    win_rate = _safe_float(backtest.get("win_rate")) or 50.0
    trade_quality_score = _clamp(min(sell_count, 8) * 10.0 + win_rate * 0.25, 0.0, 100.0)

    regime_score = 50.0
    trend = row.get("trend_status") or ""
    setup_state = row.get("setup_state") or ""
    momentum_20d = abs(_safe_float(row.get("momentum_20d")) or 0.0)
    rotation_bucket = row.get("rotation_bucket") or ""
    qlib_score = _safe_float(row.get("qlib_consensus_score"))
    qlib_model_status = row.get("qlib_model_status") or ""
    qlib_factor_group = row.get("qlib_consensus_factor_group") or ""

    if trend == "震荡":
        regime_score += 10.0
    elif trend == "空头":
        regime_score -= 6.0
    elif trend == "多头":
        regime_score -= 4.0

    if setup_state in ("收敛待发", "震荡观察"):
        regime_score += 6.0
    elif setup_state == "结构松散":
        regime_score -= 12.0

    if momentum_20d <= 12:
        regime_score += 6.0
    elif momentum_20d >= 20:
        regime_score -= 6.0

    if rotation_bucket == "leader" and trend == "多头":
        regime_score -= 10.0
    elif rotation_bucket == "leader":
        regime_score += 4.0
    elif rotation_bucket == "blacklist":
        regime_score -= 6.0

    qlib_bonus = 0.0
    if qlib_model_status == "trained" and qlib_score is not None:
        qlib_bonus += _clamp((qlib_score - 55.0) * 0.18, -4.0, 8.0)
        if qlib_factor_group == "institution":
            qlib_bonus += 1.5
        elif qlib_factor_group == "financial":
            qlib_bonus += 1.0
        elif qlib_factor_group == "alpha158":
            qlib_bonus += 1.2

    raw_candidate_score = round(
        _clamp(
            excess_score * 0.34
            + sharpe_score * 0.18
            + dd_score * 0.18
            + trade_quality_score * 0.18
            + _clamp(regime_score, 0.0, 100.0) * 0.12,
            0.0,
            100.0,
        )
        + qlib_bonus,
        1,
    )
    raw_candidate_score = round(_clamp(raw_candidate_score, 0.0, 100.0), 1)
    candidate_score = raw_candidate_score if hard_gate.get("hard_gate_passed") else 0.0

    scored = dict(backtest)
    scored.update({
        "candidate_score": candidate_score,
        "raw_candidate_score": raw_candidate_score,
        "qlib_bonus": round(qlib_bonus, 1),
        "backtest_excess_pct": excess,
        "trade_quality_score": round(trade_quality_score, 1),
        "regime_score": round(_clamp(regime_score, 0.0, 100.0), 1),
        "buy_hold_return_pct": bh_ret,
        "buy_hold_sharpe": bh_sharpe,
        "buy_hold_max_drawdown_pct": bh_dd,
    })
    scored.update(hard_gate)
    return scored


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


def _trade_fee(notional: float, fee_rate: float, min_fee: float) -> float:
    if notional <= 0:
        return 0.0
    return round(max(notional * fee_rate, min_fee), 2)


def _max_affordable_units(
    budget: float,
    price: float,
    lot_size: int,
    fee_rate: float,
    min_fee: float,
) -> int:
    if budget <= 0 or price <= 0 or lot_size <= 0:
        return 0
    lot_cost = price * lot_size
    if lot_cost <= 0:
        return 0
    lots = int(budget // lot_cost)
    while lots > 0:
        units = lots * lot_size
        notional = round(price * units, 2)
        total_cost = notional + _trade_fee(notional, fee_rate, min_fee)
        if total_cost <= budget + 1e-8:
            return units
        lots -= 1
    return 0


def _open_units(batches: list[dict]) -> int:
    return sum(int(batch.get("units") or 0) for batch in batches)


def _open_cost_basis(batches: list[dict]) -> float:
    return round(sum(_safe_float(batch.get("cost_total")) or 0.0 for batch in batches), 2)


def _run_grid_backtest(
    price_rows: list[dict],
    step_pct: float,
    tranche_count: int = 8,
    fee_bps: float = 5.0,
    initial_capital: float = 100000.0,
    lot_size: int = 100,
    min_fee: float = 5.0,
    *,
    full_curve: bool = False,
    include_trades: bool = False,
) -> Optional[dict]:
    closes = [_safe_float(row.get("close")) for row in price_rows]
    dates = [row.get("date") for row in price_rows]
    closes = [(close, date) for close, date in zip(closes, dates) if close not in (None, 0)]
    if len(closes) < 40:
        return None

    fee = fee_bps / 10000.0
    initial_price = closes[0][0]
    step_ratio = step_pct / 100.0

    grid_levels = []
    for index in range(-tranche_count, tranche_count + 1):
        grid_levels.append(initial_price * (1 + index * step_ratio))
    grid_levels.sort()

    initial_tranches = tranche_count // 2
    tranche_budget = initial_capital / tranche_count
    cash = round(float(initial_capital), 2)
    open_batches: list[dict] = []

    trade_count = 0
    buy_count = 0
    sell_count = 0
    win_trades = 0
    lose_trades = 0

    buy_units_total = 0
    sell_units_total = 0
    buy_notional_total = 0.0
    buy_fee_total = 0.0
    sell_notional_total = 0.0
    sell_fee_total = 0.0
    sell_net_total = 0.0
    sell_cost_basis_total = 0.0
    realized_pnl = 0.0

    initial_position_batches = 0
    initial_position_units = 0
    initial_position_notional = 0.0
    initial_position_cost = 0.0

    cash_low_watermark = cash
    position_low_watermark = 0
    peak_deployed_capital = 0.0
    lot_size_valid = True
    trades: list[dict] = []

    def _record_trade(side: str, payload: dict) -> None:
        if not include_trades:
            return
        trade = {
            "seq": len(trades) + 1,
            "side": side,
        }
        trade.update(payload)
        trades.append(trade)

    def _buy_batch(price: float, date: str, budget: float, *, initial: bool = False) -> bool:
        nonlocal buy_count, buy_fee_total, buy_notional_total, buy_units_total, cash, cash_low_watermark
        nonlocal initial_position_batches, initial_position_cost, initial_position_notional, initial_position_units
        nonlocal lot_size_valid, peak_deployed_capital, trade_count

        budget = min(round(budget, 2), cash)
        units = _max_affordable_units(budget, price, lot_size, fee, min_fee)
        if units <= 0:
            return False

        notional = round(price * units, 2)
        fee_amt = _trade_fee(notional, fee, min_fee)
        total_cost = round(notional + fee_amt, 2)
        if total_cost > cash + 1e-8:
            return False

        cash = round(cash - total_cost, 2)
        open_batches.append({
            "date": date,
            "price": price,
            "units": units,
            "notional": notional,
            "fee": fee_amt,
            "cost_total": total_cost,
            "initial": initial,
        })

        buy_units_total += units
        buy_notional_total = round(buy_notional_total + notional, 2)
        buy_fee_total = round(buy_fee_total + fee_amt, 2)
        lot_size_valid = lot_size_valid and units % lot_size == 0

        if initial:
            initial_position_batches += 1
            initial_position_units += units
            initial_position_notional = round(initial_position_notional + notional, 2)
            initial_position_cost = round(initial_position_cost + total_cost, 2)
        else:
            trade_count += 1
            buy_count += 1

        cash_low_watermark = min(cash_low_watermark, cash)
        peak_deployed_capital = max(peak_deployed_capital, initial_capital - cash)
        _record_trade("buy", {
            "date": date,
            "price": round(price, 4),
            "units": units,
            "notional": notional,
            "fee": round(fee_amt, 2),
            "cash_total": total_cost,
            "cash_after": round(cash, 2),
            "position_units": _open_units(open_batches),
            "is_initial": initial,
            "note": "初始底仓" if initial else "网格买入",
        })
        return True

    def _sell_batch(price: float, date: str) -> bool:
        nonlocal cash, cash_low_watermark, lose_trades, lot_size_valid, peak_deployed_capital
        nonlocal realized_pnl, sell_cost_basis_total, sell_count, sell_fee_total, sell_net_total
        nonlocal sell_notional_total, sell_units_total, trade_count, win_trades

        if not open_batches:
            return False

        batch = open_batches.pop(0)
        units = int(batch.get("units") or 0)
        if units <= 0:
            return False

        notional = round(price * units, 2)
        fee_amt = _trade_fee(notional, fee, min_fee)
        net_proceeds = round(notional - fee_amt, 2)
        cost_total = round(_safe_float(batch.get("cost_total")) or 0.0, 2)
        realized = round(net_proceeds - cost_total, 2)

        cash = round(cash + net_proceeds, 2)
        trade_count += 1
        sell_count += 1
        sell_units_total += units
        sell_notional_total = round(sell_notional_total + notional, 2)
        sell_fee_total = round(sell_fee_total + fee_amt, 2)
        sell_net_total = round(sell_net_total + net_proceeds, 2)
        sell_cost_basis_total = round(sell_cost_basis_total + cost_total, 2)
        realized_pnl = round(realized_pnl + realized, 2)
        lot_size_valid = lot_size_valid and units % lot_size == 0

        if realized > 0:
            win_trades += 1
        else:
            lose_trades += 1

        cash_low_watermark = min(cash_low_watermark, cash)
        peak_deployed_capital = max(peak_deployed_capital, initial_capital - cash)
        realized_pct = round(realized / cost_total * 100.0, 2) if cost_total > 0 else None
        _record_trade("sell", {
            "date": date,
            "price": round(price, 4),
            "units": units,
            "notional": notional,
            "fee": round(fee_amt, 2),
            "net_proceeds": net_proceeds,
            "cash_after": round(cash, 2),
            "position_units": _open_units(open_batches),
            "matched_buy_date": batch.get("date"),
            "buy_cost_total": cost_total,
            "realized_pnl": realized,
            "realized_pnl_pct": realized_pct,
            "note": "网格卖出",
        })
        return True

    for _ in range(initial_tranches):
        if not _buy_batch(initial_price, closes[0][1], tranche_budget, initial=True):
            break

    center_idx = len(grid_levels) // 2
    current_level = center_idx - initial_position_batches
    sell_level = center_idx + 1

    initial_units = _open_units(open_batches)
    initial_value = round(cash + initial_units * initial_price, 2)
    portfolio_values = [initial_value]
    curve_dates = [closes[0][1]]

    for close, date in closes[1:]:
        while current_level >= 0 and close <= grid_levels[current_level]:
            if not _buy_batch(close, date, tranche_budget):
                break
            current_level -= 1
            sell_level -= 1

        while sell_level < len(grid_levels) and close >= grid_levels[sell_level] and open_batches:
            if not _sell_batch(close, date):
                break
            current_level += 1
            sell_level += 1

        open_units = _open_units(open_batches)
        open_cost = _open_cost_basis(open_batches)
        portfolio_value = round(cash + open_units * close, 2)
        portfolio_values.append(portfolio_value)
        curve_dates.append(date)
        cash_low_watermark = min(cash_low_watermark, cash)
        position_low_watermark = min(position_low_watermark, open_units)
        peak_deployed_capital = max(peak_deployed_capital, open_cost)

    final_units = _open_units(open_batches)
    final_market_value = round(final_units * closes[-1][0], 2)
    final_value = round(cash + final_market_value, 2)
    final_open_cost = _open_cost_basis(open_batches)
    unrealized_pnl = round(final_market_value - final_open_cost, 2)
    total_pnl = round(final_value - initial_capital, 2)
    max_dd = _max_drawdown(portfolio_values)
    days = len(closes)

    daily_returns = []
    for index in range(1, len(portfolio_values)):
        if portfolio_values[index - 1] > 0:
            daily_returns.append(portfolio_values[index] / portfolio_values[index - 1] - 1.0)

    annual_return = None
    if days > 1:
        annual_return = round((((final_value / initial_capital) ** (252.0 / days)) - 1.0) * 100.0, 2)

    sharpe = None
    if daily_returns:
        mean_ret = sum(daily_returns) / len(daily_returns)
        rf_daily = 0.02 / 252.0
        variance = sum((item - mean_ret) ** 2 for item in daily_returns) / len(daily_returns)
        std = math.sqrt(variance) if variance > 0 else 0
        if std > 0:
            sharpe = round((mean_ret - rf_daily) / std * math.sqrt(252), 2)

    calmar = None
    if annual_return is not None and max_dd and max_dd > 0:
        calmar = round(annual_return / max_dd, 2)

    total_completed = win_trades + lose_trades
    win_rate = round(win_trades / total_completed * 100.0, 1) if total_completed > 0 else None

    buy_cash_total = round(buy_notional_total + buy_fee_total, 2)
    expected_final_cash = round(initial_capital - buy_cash_total + sell_net_total, 2)
    expected_final_units = buy_units_total - sell_units_total
    expected_total_pnl = round(realized_pnl + unrealized_pnl, 2)
    cash_ledger_gap = round(cash - expected_final_cash, 2)
    pnl_ledger_gap = round(total_pnl - expected_total_pnl, 2)

    audit = {
        "cash_never_negative": cash_low_watermark >= -0.01,
        "position_never_negative": position_low_watermark >= 0,
        "sell_units_backed": sell_units_total <= buy_units_total,
        "sell_cost_backed": sell_cost_basis_total <= buy_cash_total + 0.01,
        "open_units_match_batches": final_units == _open_units(open_batches),
        "lot_size_respected": lot_size_valid,
        "cash_flow_reconciled": abs(cash_ledger_gap) <= 0.05,
        "position_flow_reconciled": final_units == expected_final_units,
        "pnl_flow_reconciled": abs(pnl_ledger_gap) <= 0.05,
    }
    audit_failures = _ledger_audit_failures(audit)
    audit["failures"] = audit_failures
    audit["audit_passed"] = len(audit_failures) == 0

    result = {
        "step_pct": round(step_pct, 1),
        "return_pct": round((final_value / initial_capital - 1.0) * 100.0, 2),
        "annual_return_pct": annual_return,
        "trade_count": trade_count,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "win_rate": win_rate,
        "max_drawdown_pct": max_dd,
        "sharpe": sharpe,
        "calmar": calmar,
        "days": days,
        "initial_capital": round(initial_capital, 2),
        "lot_size": lot_size,
        "fee_bps": fee_bps,
        "min_fee": round(min_fee, 2),
        "tranche_count": tranche_count,
        "tranche_budget": round(tranche_budget, 2),
        "initial_position_ratio_pct": round(_clamp(initial_position_cost / initial_capital * 100.0, 0.0, 100.0), 2),
        "initial_position_batches": initial_position_batches,
        "initial_position_units": initial_position_units,
        "initial_position_notional": round(initial_position_notional, 2),
        "initial_position_cost": round(initial_position_cost, 2),
        "initial_cash": round(initial_capital - initial_position_cost, 2),
        "buy_units_total": buy_units_total,
        "sell_units_total": sell_units_total,
        "buy_notional_total": round(buy_notional_total, 2),
        "buy_fee_total": round(buy_fee_total, 2),
        "buy_cash_total": buy_cash_total,
        "sell_notional_total": round(sell_notional_total, 2),
        "sell_fee_total": round(sell_fee_total, 2),
        "sell_net_total": round(sell_net_total, 2),
        "sell_cost_basis_total": round(sell_cost_basis_total, 2),
        "final_cash": round(cash, 2),
        "final_units": final_units,
        "final_market_value": round(final_market_value, 2),
        "open_cost_basis": round(final_open_cost, 2),
        "realized_pnl": round(realized_pnl, 2),
        "unrealized_pnl": round(unrealized_pnl, 2),
        "total_pnl": round(total_pnl, 2),
        "cash_low_watermark": round(cash_low_watermark, 2),
        "peak_deployed_capital": round(peak_deployed_capital, 2),
        "peak_deployed_pct": round(_clamp(peak_deployed_capital / initial_capital * 100.0, 0.0, 100.0), 2),
        "cash_ledger_gap": cash_ledger_gap,
        "pnl_ledger_gap": pnl_ledger_gap,
        "audit": audit,
    }
    if full_curve:
        step = max(1, len(portfolio_values) // 60)
        result["curve"] = [
            {"date": curve_dates[index], "nav": round(portfolio_values[index] / initial_capital, 4)}
            for index in range(0, len(portfolio_values), step)
        ]
        if len(portfolio_values) > 1:
            result["curve"].append({
                "date": curve_dates[-1],
                "nav": round(portfolio_values[-1] / initial_capital, 4),
            })
    if include_trades:
        result["trades"] = trades
    return result


def _optimize_grid(
    price_rows: list[dict],
    *,
    row: Optional[dict] = None,
) -> Optional[dict]:
    tradeability = assess_etf_tradeability(
        (row or {}).get("code") or "",
        (row or {}).get("name") or "",
        (row or {}).get("category"),
        price_rows,
    )
    if not tradeability.get("supported"):
        return None

    buy_hold = _buy_hold_stats(price_rows)
    candidates = _build_grid_step_candidates(price_rows, row=row)
    results = []
    feasible_results = []
    for step in candidates:
        backtest = _run_grid_backtest(price_rows, step)
        if backtest:
            scored = _score_grid_backtest(backtest, buy_hold, row=row)
            results.append(scored)
            if scored.get("hard_gate_passed"):
                feasible_results.append(scored)
    if not feasible_results:
        return None
    feasible_results.sort(
        key=lambda item: (
            -(item.get("candidate_score") or 0.0),
            -(item.get("backtest_excess_pct") or -999.0),
            -(item.get("return_pct") or -999.0),
            item.get("max_drawdown_pct") or 999.0,
            -(item.get("sell_count") or 0),
            item.get("step_pct") or 0.0,
        )
    )
    best = dict(feasible_results[0])
    best["candidate_count"] = len(results)
    best["valid_candidate_count"] = len(feasible_results)
    best["rejected_candidate_count"] = len(results) - len(feasible_results)
    return best


def _buy_hold_stats(price_rows: list[dict]) -> Optional[dict]:
    closes = [_safe_float(row.get("close")) for row in price_rows]
    dates = [row.get("date") for row in price_rows]
    pairs = [(close, date) for close, date in zip(closes, dates) if close not in (None, 0)]
    if len(pairs) < 10:
        return None

    initial_capital = 100000.0
    fee_bps = 5.0
    lot_size = 100
    min_fee = 5.0
    fee_rate = fee_bps / 10000.0

    first = pairs[0][0]
    buy_units = _max_affordable_units(initial_capital, first, lot_size, fee_rate, min_fee)
    if buy_units <= 0:
        return None

    buy_notional = round(first * buy_units, 2)
    buy_fee = _trade_fee(buy_notional, fee_rate, min_fee)
    initial_position_cost = round(buy_notional + buy_fee, 2)
    cash = round(initial_capital - initial_position_cost, 2)

    values = [round(cash + close * buy_units, 2) for close, _ in pairs]
    final_value = values[-1]
    days = len(pairs)
    max_dd = _max_drawdown(values)

    annual_return = round((((final_value / initial_capital) ** (252.0 / days)) - 1.0) * 100.0, 2)

    daily_returns = []
    for index in range(1, len(values)):
        daily_returns.append(values[index] / values[index - 1] - 1.0)

    sharpe = None
    if daily_returns:
        mean_ret = sum(daily_returns) / len(daily_returns)
        rf_daily = 0.02 / 252.0
        variance = sum((item - mean_ret) ** 2 for item in daily_returns) / len(daily_returns)
        std = math.sqrt(variance) if variance > 0 else 0
        if std > 0:
            sharpe = round((mean_ret - rf_daily) / std * math.sqrt(252), 2)

    calmar = None
    if max_dd and max_dd > 0:
        calmar = round(annual_return / max_dd, 2)

    step = max(1, len(values) // 60)
    curve = [
        {"date": pairs[index][1], "nav": round(values[index] / initial_capital, 4)}
        for index in range(0, len(values), step)
    ]
    if len(values) > 1:
        curve.append({"date": pairs[-1][1], "nav": round(values[-1] / initial_capital, 4)})

    final_market_value = round(pairs[-1][0] * buy_units, 2)
    open_cost_basis = round(initial_position_cost, 2)
    unrealized_pnl = round(final_market_value - open_cost_basis, 2)
    cash_ledger_gap = round(cash - round(initial_capital - initial_position_cost, 2), 2)
    pnl_ledger_gap = round((final_value - initial_capital) - unrealized_pnl, 2)
    audit = {
        "cash_never_negative": cash >= -0.01,
        "position_never_negative": buy_units >= 0,
        "sell_units_backed": True,
        "sell_cost_backed": True,
        "open_units_match_batches": True,
        "lot_size_respected": buy_units % lot_size == 0,
        "cash_flow_reconciled": abs(cash_ledger_gap) <= 0.05,
        "position_flow_reconciled": buy_units >= 0,
        "pnl_flow_reconciled": abs(pnl_ledger_gap) <= 0.05,
    }
    audit_failures = _ledger_audit_failures(audit)
    audit["failures"] = audit_failures
    audit["audit_passed"] = len(audit_failures) == 0

    return {
        "return_pct": round((final_value / initial_capital - 1.0) * 100.0, 2),
        "annual_return_pct": annual_return,
        "max_drawdown_pct": max_dd,
        "sharpe": sharpe,
        "calmar": calmar,
        "days": days,
        "curve": curve,
        "trade_count": 1,
        "buy_count": 1,
        "sell_count": 0,
        "win_rate": None,
        "initial_capital": round(initial_capital, 2),
        "lot_size": lot_size,
        "fee_bps": fee_bps,
        "min_fee": min_fee,
        "tranche_count": None,
        "tranche_budget": None,
        "initial_position_ratio_pct": round(_clamp(initial_position_cost / initial_capital * 100.0, 0.0, 100.0), 2),
        "initial_position_batches": 1,
        "initial_position_units": buy_units,
        "initial_position_notional": round(buy_notional, 2),
        "initial_position_cost": round(initial_position_cost, 2),
        "initial_cash": round(cash, 2),
        "buy_units_total": buy_units,
        "sell_units_total": 0,
        "buy_notional_total": round(buy_notional, 2),
        "buy_fee_total": round(buy_fee, 2),
        "buy_cash_total": round(initial_position_cost, 2),
        "sell_notional_total": 0.0,
        "sell_fee_total": 0.0,
        "sell_net_total": 0.0,
        "sell_cost_basis_total": 0.0,
        "final_cash": round(cash, 2),
        "final_units": buy_units,
        "final_market_value": round(final_market_value, 2),
        "open_cost_basis": round(open_cost_basis, 2),
        "realized_pnl": 0.0,
        "unrealized_pnl": round(unrealized_pnl, 2),
        "total_pnl": round(final_value - initial_capital, 2),
        "cash_low_watermark": round(cash, 2),
        "peak_deployed_capital": round(initial_position_cost, 2),
        "peak_deployed_pct": round(_clamp(initial_position_cost / initial_capital * 100.0, 0.0, 100.0), 2),
        "cash_ledger_gap": cash_ledger_gap,
        "pnl_ledger_gap": pnl_ledger_gap,
        "audit": audit,
    }


def _window_price_rows(price_rows: list[dict], limit: int) -> list[dict]:
    if limit <= 0 or len(price_rows) <= limit:
        return list(price_rows)
    return list(price_rows[-limit:])


def _multi_period_backtest_from_rows(
    price_rows: list[dict],
    row: Optional[dict] = None,
    windows: Optional[list[tuple[int, str]]] = None,
) -> list[dict]:
    windows = windows or [
        (60, "近60天"),
        (120, "近120天"),
        (250, "近一年"),
        (500, "近两年"),
    ]
    results = []
    for limit, label in windows:
        rows = _window_price_rows(price_rows, limit)
        if len(rows) < 40:
            results.append({"window": label, "days": len(rows), "best": None, "buy_hold": None})
            continue
        best = _optimize_grid(rows, row=row)
        buy_hold = _buy_hold_stats(rows)
        if best:
            best["window"] = label
        results.append({
            "window": label,
            "days": len(rows),
            "best": best,
            "buy_hold": buy_hold,
        })
    return results


def _build_strategy_decision(
    row: dict,
    best: Optional[dict],
    buy_hold: Optional[dict],
    multi_period: list[dict],
) -> dict:
    heuristic_type = row.get("heuristic_strategy_type") or row.get("strategy_type") or "观察池"
    category = row.get("category") or ""
    trend = row.get("trend_status") or ""
    setup_state = row.get("setup_state") or ""
    rotation_bucket = row.get("rotation_bucket") or ""
    volatility_20d = _safe_float(row.get("volatility_20d"))
    momentum_20d = _safe_float(row.get("momentum_20d")) or 0.0
    rel_12w = _safe_float(row.get("relative_strength_12w")) or 0.0
    qlib_score = _safe_float(row.get("qlib_consensus_score"))
    qlib_model_status = row.get("qlib_model_status") or ""
    qlib_factor_group = row.get("qlib_consensus_factor_group") or ""
    qlib_support = qlib_model_status == "trained" and qlib_score is not None and qlib_score >= 68.0

    if category in ("债券", "货币") and (volatility_20d is None or volatility_20d <= 12):
        return {
            "strategy_type": "防守停泊",
            "strategy_reason": "低波动 ETF 以防守停泊为主，不参与网格与趋势收益比较。",
        }

    if rotation_bucket == "blacklist" or (trend == "空头" and rel_12w < 0):
        return {
            "strategy_type": "暂不参与",
            "strategy_reason": "轮动排名处于回避区，且趋势偏弱，先不参与更稳妥。",
        }

    if not buy_hold:
        return {
            "strategy_type": "买入持有",
            "strategy_reason": "历史样本不足，暂不做网格结论，默认按买入持有观察。",
        }

    if not best:
        if heuristic_type == "网格候选":
            return {
                "strategy_type": "买入持有",
                "strategy_reason": "候选步长未通过实盘硬约束或未形成有效卖出回笼，不能把网格当成可执行策略。",
            }
        return {
            "strategy_type": "买入持有",
            "strategy_reason": "当前没有通过实盘硬约束的网格候选，先按买入持有观察。",
        }

    grid_ret = _safe_float(best.get("return_pct")) or 0.0
    buy_hold_ret = _safe_float(buy_hold.get("return_pct")) or 0.0
    grid_sharpe = _safe_float(best.get("sharpe"))
    buy_hold_sharpe = _safe_float(buy_hold.get("sharpe"))
    grid_dd = _safe_float(best.get("max_drawdown_pct"))
    buy_hold_dd = _safe_float(buy_hold.get("max_drawdown_pct"))
    grid_candidate_score = _safe_float(best.get("candidate_score")) or 0.0
    grid_excess = _safe_float(best.get("backtest_excess_pct"))
    if grid_excess is None:
        grid_excess = round(grid_ret - buy_hold_ret, 2)

    comparable_periods = [item for item in multi_period if item.get("best") and item.get("buy_hold")]
    mp_total = len(comparable_periods)
    mp_wins = sum(
        1
        for period in comparable_periods
        if (_safe_float(period["best"].get("return_pct")) or 0.0)
        > (_safe_float(period["buy_hold"].get("return_pct")) or 0.0)
    )

    trend_neutral = abs(momentum_20d) <= 12 and abs(rel_12w) <= 10
    mean_reversion_profile = (
        heuristic_type == "网格候选"
        and setup_state not in ("结构松散", "待补结构")
        and trend != "多头"
        and trend_neutral
    )
    strong_trend_profile = (
        trend == "多头"
        and rel_12w > 0
        and setup_state in ("收敛待发", "趋势跟随")
    )
    completed_grid_trades = (best.get("sell_count") or 0) >= (2 if qlib_support else 3)
    sharpe_ok = (
        grid_sharpe is not None
        and buy_hold_sharpe is not None
        and grid_sharpe >= buy_hold_sharpe * 0.95
    ) or (grid_sharpe is not None and buy_hold_sharpe is None)
    dd_ok = (
        grid_dd is not None
        and buy_hold_dd is not None
        and grid_dd <= buy_hold_dd * 0.9
    ) or (grid_dd is not None and buy_hold_dd is None)
    stability_ok = mp_total == 0 or mp_wins >= max(1, math.ceil(mp_total * (0.25 if qlib_support else (1 / 3))))
    risk_ok = sharpe_ok or dd_ok
    excess_ok = grid_excess >= 0.0

    if mean_reversion_profile and completed_grid_trades and risk_ok and stability_ok and excess_ok and grid_candidate_score >= (54 if qlib_support else 58):
        qlib_suffix = ""
        if qlib_support:
            qlib_detail = f"Qlib 共识 {qlib_score:.1f} 分"
            if qlib_factor_group:
                qlib_detail += f"，领先因子组为 {qlib_factor_group}"
            qlib_suffix = f" {qlib_detail}，允许对边界信号做更积极的网格判定。"
        return {
            "strategy_type": "网格交易",
            "strategy_reason": (
                f"近 {best.get('days') or '-'} 天网格收益 {grid_ret:.2f}% 对比持有 {buy_hold_ret:.2f}% ，"
                f"综合评分 {grid_candidate_score:.1f} 分，{mp_wins}/{mp_total} 个窗口占优，保留网格标签。{qlib_suffix}"
            ),
        }

    if completed_grid_trades and risk_ok and stability_ok and grid_candidate_score >= (62 if qlib_support else 66) and excess_ok and not strong_trend_profile:
        qlib_suffix = ""
        if qlib_support:
            qlib_suffix = f" Qlib 共识 {qlib_score:.1f} 分，对震荡区间判断提供额外支持。"
        return {
            "strategy_type": "网格交易",
            "strategy_reason": (
                f"最优步长 {best.get('step_pct')}% 的综合评分 {grid_candidate_score:.1f} 分，"
                f"回测超额 {grid_excess:.2f}%，当前更适合做区间交易。{qlib_suffix}"
            ),
        }

    if heuristic_type == "网格候选":
        return {
            "strategy_type": "买入持有",
            "strategy_reason": (
                f"启发式画像偏向网格，但综合评分仅 {grid_candidate_score:.1f} 分，"
                f"回测超额 {grid_excess:.2f}% 或跨窗口稳定性不足，因此暂不保留网格标签。"
            ),
        }

    if strong_trend_profile:
        return {
            "strategy_type": "买入持有",
            "strategy_reason": "中期相对强势仍在，买入持有比频繁网格更符合历史表现。",
        }

    if buy_hold_ret >= grid_ret:
        return {
            "strategy_type": "买入持有",
            "strategy_reason": f"近 {buy_hold.get('days') or '-'} 天持有收益 {buy_hold_ret:.2f}% 高于网格 {grid_ret:.2f}%，优先持有。",
        }

    return {
        "strategy_type": "买入持有",
        "strategy_reason": "当前没有足够证据证明网格优于持有，先按买入持有处理。",
    }
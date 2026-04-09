"""
收益计算引擎 (Phase 0 重构版)

以公告日后下一交易日开盘为锚点，计算事件后的收益和回撤。
结果直接回写 fact_institution_event（不再写 fact_event_return）。
K 线数据从 market_data.db 读取。
"""

import logging
from datetime import datetime

from typing import Optional

from services.market_db import get_market_conn, get_kline, get_kline_range
from services.utils import normalize_ymd as _normalize_ymd

logger = logging.getLogger("cm-api")

CALC_VERSION = "v2_qfq_open_anchor_dual_cost"
CALC_REF_PRICE_MODE = "next_trade_open_qfq"

# 覆盖率门槛：低于此值直接 skip，不写半成品结果
MIN_COVERAGE_RATIO = 0.80

BUY_EVENT_TYPES = {"new_entry", "increase"}


# ---------------------------------------------------------------------------
# 内部工具函数
# ---------------------------------------------------------------------------

def _next_trading_day(biz_conn, date_str: str) -> Optional[str]:
    """找公告日后的下一个交易日"""
    d = date_str.replace("-", "")
    if len(d) == 8:
        normalized = f"{d[:4]}-{d[4:6]}-{d[6:8]}"
    else:
        normalized = date_str
    row = biz_conn.execute(
        "SELECT trade_date FROM dim_trading_calendar "
        "WHERE trade_date > ? AND is_trading = 1 "
        "ORDER BY trade_date LIMIT 1",
        (normalized,)
    ).fetchone()
    return row["trade_date"] if row else None


def _resolve_cost_window(biz_conn, report_date: str) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """
    解析机构参考成本窗口。

    标准季报/年报：使用完整报告期窗口。
    非标准报告期：回退到报告日前最近 20 个交易日。
    """
    normalized = _normalize_ymd(report_date)
    if not normalized:
        return None, None, None

    year = int(normalized[:4])
    mmdd = normalized[5:]
    if mmdd == "03-31":
        return f"{year}-01-01", normalized, "q1"
    if mmdd == "06-30":
        return f"{year}-04-01", normalized, "q2"
    if mmdd == "09-30":
        return f"{year}-07-01", normalized, "q3"
    if mmdd == "12-31":
        return f"{year}-10-01", normalized, "annual"

    rows = biz_conn.execute(
        "SELECT trade_date FROM dim_trading_calendar "
        "WHERE trade_date <= ? AND is_trading = 1 "
        "ORDER BY trade_date DESC LIMIT 20",
        (normalized,),
    ).fetchall()
    if not rows:
        return normalized, normalized, "special"
    return rows[-1]["trade_date"], rows[0]["trade_date"], "special"


def _rows_vwap(rows: list[dict]) -> Optional[float]:
    """基于 amount / volume 计算 VWAP。"""
    amount_sum = 0.0
    volume_sum = 0.0
    for row in rows:
        amount = row.get("amount")
        volume = row.get("volume")
        if amount is None or volume is None or amount <= 0 or volume <= 0:
            continue
        amount_sum += float(amount)
        volume_sum += float(volume)
    if volume_sum <= 0:
        return None
    return amount_sum / volume_sum


def _rows_close_mean(rows: list[dict]) -> Optional[float]:
    """回退口径：区间收盘均价。"""
    closes = [float(row["close"]) for row in rows if row.get("close") and row["close"] > 0]
    if not closes:
        return None
    return sum(closes) / len(closes)


def _resolve_reasonable_vwap(rows: list[dict], base_method: str) -> tuple[Optional[float], Optional[str]]:
    """
    对 VWAP 做单位合理性校验。

    实际抓到的 volume 在不同源上可能有“股 / 手”差异，导致 amount / volume
    偶发放大 100 倍。这里优先选择与窗口收盘均价量级一致的口径。
    """
    raw_vwap = _rows_vwap(rows)
    if raw_vwap is None:
        return None, None

    close_mean = _rows_close_mean(rows)
    if close_mean is None or close_mean <= 0:
        return raw_vwap, base_method

    ratio = raw_vwap / close_mean
    if 0.5 <= ratio <= 1.5:
        return raw_vwap, base_method

    adjusted_vwap = raw_vwap / 100.0
    adjusted_ratio = adjusted_vwap / close_mean
    if 0.5 <= adjusted_ratio <= 1.5:
        return adjusted_vwap, f"{base_method}_volume_hand_adjusted"

    return None, None


def _estimate_inst_ref_cost(biz_conn, mkt_conn, code: str,
                            report_date: str) -> tuple[Optional[float], Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    估算机构参考成本。

    主口径：报告期窗口内日线 VWAP。
    回退：日线收盘均价 -> 月线 VWAP -> 月线收盘均价。
    """
    start_date, end_date, report_season = _resolve_cost_window(biz_conn, report_date)
    if not start_date or not end_date:
        return None, None, report_season, start_date, end_date

    daily_rows = get_kline_range(mkt_conn, code, start_date, end_date, freq="daily")
    if len(daily_rows) >= 3:
        vwap, vwap_method = _resolve_reasonable_vwap(daily_rows, "daily_vwap_qfq")
        if vwap:
            return round(vwap, 4), vwap_method, report_season, start_date, end_date
        close_mean = _rows_close_mean(daily_rows)
        if close_mean:
            return round(close_mean, 4), "daily_close_mean_qfq", report_season, start_date, end_date

    monthly_rows = get_kline_range(mkt_conn, code, start_date, end_date, freq="monthly")
    if monthly_rows:
        vwap, vwap_method = _resolve_reasonable_vwap(monthly_rows, "monthly_vwap_qfq")
        if vwap:
            return round(vwap, 4), vwap_method, report_season, start_date, end_date
        close_mean = _rows_close_mean(monthly_rows)
        if close_mean:
            return round(close_mean, 4), "monthly_close_mean_qfq", report_season, start_date, end_date

    return None, None, report_season, start_date, end_date


def _classify_premium_bucket(premium_pct: Optional[float]) -> Optional[str]:
    """按跟随溢价做粗粒度分桶。"""
    if premium_pct is None:
        return None
    if premium_pct <= -5:
        return "discount"
    if premium_pct <= 5:
        return "near_cost"
    if premium_pct <= 15:
        return "premium"
    return "high_premium"


def _suggest_follow_gate(event_type: Optional[str], premium_pct: Optional[float]) -> tuple[Optional[str], Optional[str]]:
    """
    给出最基础的跟随门槛提示。

    这里只给 hint，不直接进入正式评分。
    """
    if event_type in {"decrease", "exit"}:
        return "avoid", "sell_signal"
    if event_type == "unchanged":
        return "observe", "unchanged_signal"
    if event_type not in BUY_EVENT_TYPES:
        return None, None
    if premium_pct is None:
        return "unknown", "missing_cost"
    if premium_pct <= 5:
        return "follow", "near_cost"
    if premium_pct <= 15:
        return "watch", "premium"
    return "avoid", "high_premium"


def _get_exact_daily_field(mkt_conn, code: str, date: str, field: str) -> Optional[float]:
    """读取日K字段。精确匹配不到时，取该日期起最近的有效交易日（停牌/假期容错）。"""
    row = mkt_conn.execute(
        f"SELECT [{field}] FROM price_kline "
        "WHERE code=? AND date=? AND freq='daily' AND adjust='qfq'",
        (code, date),
    ).fetchone()
    if row and row[0]:
        return row[0]
    # 回退：取 date 起 10 天内最近的有效记录
    row = mkt_conn.execute(
        f"SELECT [{field}] FROM price_kline "
        "WHERE code=? AND date>=? AND date<=date(?,'+10 days') AND freq='daily' AND adjust='qfq' "
        "ORDER BY date LIMIT 1",
        (code, date, date),
    ).fetchone()
    if not row:
        return None
    return row[0]


def _price_after_n_days(biz_conn, mkt_conn, code: str, anchor: str, n: int) -> Optional[float]:
    """取锚点后第 n 个交易日的收盘价"""
    row = biz_conn.execute(
        "SELECT trade_date FROM dim_trading_calendar "
        "WHERE trade_date >= ? AND is_trading = 1 "
        "ORDER BY trade_date LIMIT 1 OFFSET ?",
        (anchor, n)
    ).fetchone()
    if not row:
        return None
    return _get_exact_daily_field(mkt_conn, code, row["trade_date"], "close")


def _max_drawdown(mkt_conn, code: str, anchor: str, end_date: str) -> Optional[float]:
    """计算 anchor 到 end_date 之间的最大回撤"""
    klines = get_kline_range(mkt_conn, code, anchor, end_date, freq="daily")
    closes = [k["close"] for k in klines if k.get("close")]
    if len(closes) < 2:
        return None
    peak = closes[0]
    md = 0.0
    for p in closes:
        if p > peak:
            peak = p
        dd = (peak - p) / peak if peak > 0 else 0
        if dd > md:
            md = dd
    return round(md * 100, 2)


def _get_nth_trade_date(biz_conn, anchor: str, n: int) -> Optional[str]:
    """获取锚点后第 n 个交易日的日期"""
    row = biz_conn.execute(
        "SELECT trade_date FROM dim_trading_calendar "
        "WHERE trade_date >= ? AND is_trading = 1 "
        "ORDER BY trade_date LIMIT 1 OFFSET ?",
        (anchor, n)
    ).fetchone()
    return row["trade_date"] if row else None


def _classify_path(mkt_conn, code: str, anchor: str) -> dict:
    """
    价格路径分类（从 scoring.py 移入）。
    基于公告后至今的日 K 判断路径状态。
    """
    today = datetime.now().strftime("%Y-%m-%d")
    klines = get_kline_range(mkt_conn, code, anchor, today, freq="daily")
    if len(klines) < 2:
        return {"path_state": None, "return_to_now": None,
                "max_rally_to_now": None, "max_drawdown_to_now": None}

    first_close = klines[0]["close"]
    last_close = klines[-1]["close"]
    if not first_close or first_close <= 0:
        return {"path_state": None, "return_to_now": None,
                "max_rally_to_now": None, "max_drawdown_to_now": None}

    total_gain = round((last_close - first_close) / first_close * 100, 2)

    peak = first_close
    max_gain = 0.0
    max_dd = 0.0
    for k in klines:
        c = k["close"]
        if not c:
            continue
        if c > peak:
            peak = c
        gain = (c - first_close) / first_close * 100
        if gain > max_gain:
            max_gain = gain
        dd = (peak - c) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # 路径分类
    if max_dd >= 15 and total_gain < 0:
        path_state = "失效破坏"
    elif max_gain >= 30:
        path_state = "已充分演绎"
    elif max_gain >= 10:
        path_state = "温和验证"
    elif max_gain < 10:
        path_state = "未充分演绎"
    else:
        path_state = "震荡待定"

    return {
        "path_state": path_state,
        "return_to_now": total_gain,
        "max_rally_to_now": round(max_gain, 2),
        "max_drawdown_to_now": round(max_dd, 2),
    }


# ---------------------------------------------------------------------------
# 主计算函数
# ---------------------------------------------------------------------------

def calculate_returns(biz_conn) -> int:
    """
    为所有事件计算收益，结果回写 fact_institution_event。

    Returns:
        int: 成功计算的事件数
        str: 如果覆盖率不足，返回跳过原因字符串
    """
    logger.info("[收益] 开始计算...")

    events = biz_conn.execute(
        "SELECT institution_id, stock_code, report_date, notice_date, event_type "
        "FROM fact_institution_event "
        "WHERE notice_date IS NOT NULL AND notice_date != ''"
    ).fetchall()

    if not events:
        logger.warning("[收益] 无事件数据")
        return 0

    # 覆盖率检查：批量检查待计算事件对应股票是否有日 K
    mkt_conn = get_market_conn()
    try:
        stock_codes = set(ev["stock_code"] for ev in events)
        kline_code_rows = mkt_conn.execute(
            "SELECT DISTINCT code FROM price_kline WHERE freq='daily'"
        ).fetchall()
        has_kline = stock_codes.intersection(r["code"] for r in kline_code_rows)

        coverage = len(has_kline) / len(stock_codes) if stock_codes else 0
        logger.info(f"[收益] 日K覆盖率: {len(has_kline)}/{len(stock_codes)} = {coverage:.1%}")

        if coverage < MIN_COVERAGE_RATIO:
            msg = f"日K覆盖率不足({coverage:.0%})，需先补齐行情数据"
            logger.warning(f"[收益] {msg}")
            return msg  # 返回字符串表示跳过

        # 开始计算
        now = datetime.now().isoformat()
        done = 0
        skip = 0
        updates = []
        total_events = len(events)

        for ev_idx, ev in enumerate(events):
            # C-3: 进度日志
            if ev_idx > 0 and ev_idx % 200 == 0:
                logger.info(f"[收益] 进度: {ev_idx}/{total_events} ({ev_idx*100//total_events}%)")

            inst_ref_cost, inst_cost_method, report_season, cost_window_start, cost_window_end = (
                _estimate_inst_ref_cost(biz_conn, mkt_conn, ev["stock_code"], ev["report_date"])
            )
            anchor = _next_trading_day(biz_conn, ev["notice_date"])
            if not anchor:
                updates.append((
                    report_season, cost_window_start, cost_window_end,
                    inst_ref_cost, inst_cost_method, None,
                    None, None, None,
                    None, None, None,
                    None, None, None,
                    None, None,
                    None, None,
                    None, None, None,
                    None,
                    CALC_VERSION, CALC_REF_PRICE_MODE, now,
                    ev["institution_id"], ev["stock_code"], ev["report_date"],
                ))
                skip += 1
                continue

            price = _get_exact_daily_field(mkt_conn, ev["stock_code"], anchor, "open")
            if not price or price <= 0:
                follow_gate, follow_gate_reason = _suggest_follow_gate(ev["event_type"], None)
                updates.append((
                    report_season, cost_window_start, cost_window_end,
                    inst_ref_cost, inst_cost_method, None,
                    None, follow_gate, follow_gate_reason,
                    anchor, None, 'suspended_waiting',
                    None, None, None,
                    None, None,
                    None, None,
                    None, None, None,
                    None,
                    CALC_VERSION, CALC_REF_PRICE_MODE, now,
                    ev["institution_id"], ev["stock_code"], ev["report_date"],
                ))
                skip += 1
                continue

            premium_pct = None
            if inst_ref_cost and inst_ref_cost > 0:
                premium_pct = round((price - inst_ref_cost) / inst_ref_cost * 100, 2)
            premium_bucket = _classify_premium_bucket(premium_pct)
            follow_gate, follow_gate_reason = _suggest_follow_gate(ev["event_type"], premium_pct)

            # 各期收益
            gains = {}
            for n, label in [(10, "10d"), (30, "30d"), (60, "60d"),
                              (90, "90d"), (120, "120d")]:
                ep = _price_after_n_days(biz_conn, mkt_conn,
                                          ev["stock_code"], anchor, n)
                gains[label] = round((ep - price) / price * 100, 2) if ep else None

            # 最大回撤
            end_30 = _get_nth_trade_date(biz_conn, anchor, 30)
            end_60 = _get_nth_trade_date(biz_conn, anchor, 60)
            dd30 = _max_drawdown(mkt_conn, ev["stock_code"], anchor, end_30) if end_30 else None
            dd60 = _max_drawdown(mkt_conn, ev["stock_code"], anchor, end_60) if end_60 else None

            # 路径分类 + 至今收益
            path_info = _classify_path(mkt_conn, ev["stock_code"], anchor)

            updates.append((
                report_season, cost_window_start, cost_window_end,
                inst_ref_cost, inst_cost_method, premium_pct,
                premium_bucket, follow_gate, follow_gate_reason,
                anchor, price, 'ok',
                gains.get("10d"), gains.get("30d"), gains.get("60d"),
                gains.get("90d"), gains.get("120d"),
                dd30, dd60,
                path_info["return_to_now"],
                path_info["max_rally_to_now"],
                path_info["max_drawdown_to_now"],
                path_info["path_state"],
                CALC_VERSION, CALC_REF_PRICE_MODE, now,
                ev["institution_id"], ev["stock_code"], ev["report_date"],
            ))
            done += 1

        # 批量回写 fact_institution_event
        biz_conn.execute("BEGIN IMMEDIATE")
        try:
            for i in range(0, len(updates), 500):
                batch = updates[i:i + 500]
                biz_conn.executemany("""
                    UPDATE fact_institution_event SET
                        report_season=?, cost_window_start=?, cost_window_end=?,
                        inst_ref_cost=?, inst_cost_method=?, premium_pct=?,
                        premium_bucket=?, follow_gate=?, follow_gate_reason=?,
                        tradable_date=?, price_entry=?, price_entry_status=?,
                        gain_10d=?, gain_30d=?, gain_60d=?,
                        gain_90d=?, gain_120d=?,
                        max_drawdown_30d=?, max_drawdown_60d=?,
                        return_to_now=?,
                        max_rally_to_now=?,
                        max_drawdown_to_now=?,
                        path_state=?,
                        calc_version=?, calc_ref_price_mode=?,
                        calc_completed_at=?
                    WHERE institution_id=? AND stock_code=? AND report_date=?
                """, batch)
            biz_conn.commit()
        except Exception:
            biz_conn.rollback()
            raise

        logger.info(f"[收益] 首轮完成: {done} 条, 跳过 {skip}")

        # 补救扫描：找 tradable_date ≤ 今天但无入口价、且现在有 K 线的事件
        today = datetime.now().strftime("%Y-%m-%d")
        fixable = biz_conn.execute(
            "SELECT institution_id, stock_code, report_date, notice_date, event_type "
            "FROM fact_institution_event "
            "WHERE tradable_date IS NOT NULL AND tradable_date != '' "
            "  AND tradable_date <= ? "
            "  AND (price_entry IS NULL OR price_entry = 0) "
            "  AND notice_date IS NOT NULL AND notice_date != ''",
            (today,)
        ).fetchall()

        fixed = 0
        for ev in fixable:
            anchor = _next_trading_day(biz_conn, ev["notice_date"])
            if not anchor:
                continue
            price = _get_exact_daily_field(mkt_conn, ev["stock_code"], anchor, "open")
            if not price or price <= 0:
                continue

            inst_ref_cost, inst_cost_method, report_season, cost_window_start, cost_window_end = (
                _estimate_inst_ref_cost(biz_conn, mkt_conn, ev["stock_code"], ev["report_date"])
            )
            premium_pct = round((price - inst_ref_cost) / inst_ref_cost * 100, 2) if inst_ref_cost and inst_ref_cost > 0 else None
            premium_bucket = _classify_premium_bucket(premium_pct)
            follow_gate, follow_gate_reason = _suggest_follow_gate(ev["event_type"], premium_pct)

            gains = {}
            for n, label in [(10, "10d"), (30, "30d"), (60, "60d"), (90, "90d"), (120, "120d")]:
                ep = _price_after_n_days(biz_conn, mkt_conn, ev["stock_code"], anchor, n)
                gains[label] = round((ep - price) / price * 100, 2) if ep else None

            end_30 = _get_nth_trade_date(biz_conn, anchor, 30)
            end_60 = _get_nth_trade_date(biz_conn, anchor, 60)
            dd30 = _max_drawdown(mkt_conn, ev["stock_code"], anchor, end_30) if end_30 else None
            dd60 = _max_drawdown(mkt_conn, ev["stock_code"], anchor, end_60) if end_60 else None
            path_info = _classify_path(mkt_conn, ev["stock_code"], anchor)

            biz_conn.execute("""
                UPDATE fact_institution_event SET
                    report_season=?, cost_window_start=?, cost_window_end=?,
                    inst_ref_cost=?, inst_cost_method=?, premium_pct=?,
                    premium_bucket=?, follow_gate=?, follow_gate_reason=?,
                    tradable_date=?, price_entry=?, price_entry_status=?,
                    gain_10d=?, gain_30d=?, gain_60d=?, gain_90d=?, gain_120d=?,
                    max_drawdown_30d=?, max_drawdown_60d=?,
                    return_to_now=?, max_rally_to_now=?, max_drawdown_to_now=?,
                    path_state=?, calc_version=?, calc_ref_price_mode=?, calc_completed_at=?
                WHERE institution_id=? AND stock_code=? AND report_date=?
            """, (
                report_season, cost_window_start, cost_window_end,
                inst_ref_cost, inst_cost_method, premium_pct,
                premium_bucket, follow_gate, follow_gate_reason,
                anchor, price, 'ok',
                gains.get("10d"), gains.get("30d"), gains.get("60d"),
                gains.get("90d"), gains.get("120d"),
                dd30, dd60,
                path_info["return_to_now"], path_info["max_rally_to_now"],
                path_info["max_drawdown_to_now"], path_info["path_state"],
                CALC_VERSION, CALC_REF_PRICE_MODE, now,
                ev["institution_id"], ev["stock_code"], ev["report_date"],
            ))
            fixed += 1

        if fixed:
            biz_conn.commit()
            logger.info(f"[收益] 补救扫描修复 {fixed} 条之前缺失入口价的事件")

        logger.info(f"[收益] 完成: {done + fixed} 条（首轮 {done} + 补救 {fixed}）, 跳过 {skip}")
        return done + fixed

    finally:
        mkt_conn.close()

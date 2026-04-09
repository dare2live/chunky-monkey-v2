"""
stock_stage_engine.py — 股票阶段特征中间事实层

把价格路径、趋势健康、量能过热和分类型阶段惩罚统一沉成阶段特征层，
供评分、解释页和后续验证统一复用。
"""

import logging
from datetime import date, datetime
from statistics import pstdev
from typing import Optional

from services.market_db import get_market_conn
from services.utils import safe_float as _safe_float
from services.constants import PATH_THRESHOLDS

logger = logging.getLogger("cm-api")


def _mean(values: list[float]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _clamp_score(value: Optional[float], lo: float = 0.0, hi: float = 100.0) -> float:
    if value is None:
        return lo
    return round(max(lo, min(hi, float(value))), 2)


def _days_since(value: Optional[str]) -> Optional[int]:
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            dt = datetime.strptime(str(value), fmt)
            return max((datetime.now() - dt).days, 0)
        except Exception:
            continue
    return None


def _window_return(closes: list[float], window: int) -> Optional[float]:
    if len(closes) <= window:
        return None
    prev = closes[-window - 1]
    last = closes[-1]
    if prev in (None, 0) or last is None:
        return None
    return round((last / prev - 1) * 100, 2)


def _moving_average(closes: list[float], window: int) -> Optional[float]:
    if len(closes) < window:
        return None
    return _mean(closes[-window:])


def _max_drawdown_pct(closes: list[float]) -> Optional[float]:
    if len(closes) < 2:
        return None
    peak = closes[0]
    max_dd = 0.0
    for close in closes:
        if close is None:
            continue
        if close > peak:
            peak = close
        if peak > 0:
            dd = (peak - close) / peak * 100
            if dd > max_dd:
                max_dd = dd
    return round(max_dd, 2)


def _volatility_pct(closes: list[float], window: int = 20) -> Optional[float]:
    if len(closes) < window + 1:
        return None
    returns = []
    for prev, curr in zip(closes[-window - 1:-1], closes[-window:]):
        if prev in (None, 0) or curr is None:
            continue
        returns.append((curr / prev - 1) * 100)
    if not returns:
        return None
    if len(returns) == 1:
        return 0.0
    return round(pstdev(returns), 2)


def _amplitude_pct(highs: list[float], lows: list[float], window: int = 20) -> Optional[float]:
    if len(highs) < window or len(lows) < window:
        return None
    highs_w = [v for v in highs[-window:] if v is not None]
    lows_w = [v for v in lows[-window:] if v is not None]
    if not highs_w or not lows_w:
        return None
    low = min(lows_w)
    high = max(highs_w)
    if low in (None, 0):
        return None
    return round((high - low) / low * 100, 2)


def _amount_ratio(amounts: list[Optional[float]], short: int = 20, long: int = 120) -> Optional[float]:
    if len(amounts) < long:
        return None
    short_avg = _mean([v for v in amounts[-short:] if v is not None])
    long_avg = _mean([v for v in amounts[-long:] if v is not None])
    if short_avg is None or long_avg in (None, 0):
        return None
    return round(short_avg / long_avg, 2)


def _classify_path(price_rows: list[dict], anchor_date: Optional[str]) -> tuple[str, Optional[float], Optional[float]]:
    rows = price_rows
    if anchor_date:
        rows = [row for row in price_rows if str(row["date"]) >= str(anchor_date)]
    if len(rows) < 2:
        return "未充分演绎", None, None

    first_close = _safe_float(rows[0]["close"])
    last_close = _safe_float(rows[-1]["close"])
    if first_close in (None, 0) or last_close is None:
        return "未充分演绎", None, None

    peak = first_close
    max_gain = 0.0
    max_drawdown = 0.0
    for row in rows:
        high = _safe_float(row.get("high")) or _safe_float(row.get("close")) or 0.0
        low = _safe_float(row.get("low")) or _safe_float(row.get("close")) or 0.0
        close = _safe_float(row.get("close")) or 0.0

        gain = (high - first_close) / first_close * 100
        if gain > max_gain:
            max_gain = gain

        if close > peak:
            peak = close
        if peak > 0:
            dd = (peak - low) / peak * 100
            if dd > max_drawdown:
                max_drawdown = dd

    total_gain = (last_close - first_close) / first_close * 100
    if max_drawdown >= PATH_THRESHOLDS["broken_drawdown"] and total_gain < 0:
        return "失效破坏", round(max_gain, 2), round(max_drawdown, 2)
    if max_gain >= PATH_THRESHOLDS["exhausted_min"]:
        return "已充分演绎", round(max_gain, 2), round(max_drawdown, 2)
    if max_gain >= PATH_THRESHOLDS["mild_gain_max"]:
        return "温和验证", round(max_gain, 2), round(max_drawdown, 2)
    return "未充分演绎", round(max_gain, 2), round(max_drawdown, 2)


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_stock_stage_features (
            snapshot_date                 TEXT NOT NULL,
            stock_code                    TEXT NOT NULL,
            latest_notice_date            TEXT,
            latest_report_date            TEXT,
            stock_archetype               TEXT,
            path_state                    TEXT,
            path_max_gain_pct             REAL,
            path_max_drawdown_pct         REAL,
            return_1m                     REAL,
            return_3m                     REAL,
            return_6m                     REAL,
            return_12m                    REAL,
            ma120                         REAL,
            ma250                         REAL,
            dist_ma120_pct                REAL,
            dist_ma250_pct                REAL,
            above_ma250                   INTEGER DEFAULT 0,
            max_drawdown_60d              REAL,
            amount_ratio_20_120           REAL,
            volatility_20d                REAL,
            amplitude_20d                 REAL,
            gate_follow_count             INTEGER DEFAULT 0,
            gate_watch_count              INTEGER DEFAULT 0,
            gate_observe_count            INTEGER DEFAULT 0,
            gate_avoid_count              INTEGER DEFAULT 0,
            stock_gate                    TEXT,
            generic_stage_raw             REAL,
            stage_quality_continuity_raw  REAL,
            stage_quality_trend_raw       REAL,
            stage_quality_overheat_penalty REAL,
            stage_growth_continuity_raw   REAL,
            stage_growth_slowdown_penalty REAL,
            stage_growth_stretch_penalty  REAL,
            stage_cycle_recovery_raw      REAL,
            stage_cycle_realization_penalty REAL,
            stage_cycle_uncertainty_penalty REAL,
            stage_type_adjust_raw         REAL,
            stage_score_v1                REAL,
            stage_reason                  TEXT,
            updated_at                    TEXT,
            PRIMARY KEY (snapshot_date, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_fssf_stock ON fact_stock_stage_features(stock_code);

        CREATE TABLE IF NOT EXISTS dim_stock_stage_latest (
            stock_code                    TEXT PRIMARY KEY,
            snapshot_date                 TEXT,
            latest_notice_date            TEXT,
            latest_report_date            TEXT,
            stock_archetype               TEXT,
            path_state                    TEXT,
            path_max_gain_pct             REAL,
            path_max_drawdown_pct         REAL,
            return_1m                     REAL,
            return_3m                     REAL,
            return_6m                     REAL,
            return_12m                    REAL,
            ma120                         REAL,
            ma250                         REAL,
            dist_ma120_pct                REAL,
            dist_ma250_pct                REAL,
            above_ma250                   INTEGER DEFAULT 0,
            max_drawdown_60d              REAL,
            amount_ratio_20_120           REAL,
            volatility_20d                REAL,
            amplitude_20d                 REAL,
            gate_follow_count             INTEGER DEFAULT 0,
            gate_watch_count              INTEGER DEFAULT 0,
            gate_observe_count            INTEGER DEFAULT 0,
            gate_avoid_count              INTEGER DEFAULT 0,
            stock_gate                    TEXT,
            generic_stage_raw             REAL,
            stage_quality_continuity_raw  REAL,
            stage_quality_trend_raw       REAL,
            stage_quality_overheat_penalty REAL,
            stage_growth_continuity_raw   REAL,
            stage_growth_slowdown_penalty REAL,
            stage_growth_stretch_penalty  REAL,
            stage_cycle_recovery_raw      REAL,
            stage_cycle_realization_penalty REAL,
            stage_cycle_uncertainty_penalty REAL,
            stage_type_adjust_raw         REAL,
            stage_score_v1                REAL,
            stage_reason                  TEXT,
            updated_at                    TEXT
        );
    """)
    conn.commit()


def _load_price_history(mkt_conn, codes: list[str], since_days: int = 420) -> dict[str, list[dict]]:
    history = {code: [] for code in codes}
    chunk_size = 400
    for idx in range(0, len(codes), chunk_size):
        chunk = codes[idx:idx + chunk_size]
        placeholders = ",".join("?" for _ in chunk)
        rows = mkt_conn.execute(
            f"SELECT code, date, high, low, close, amount "
            f"FROM price_kline "
            f"WHERE code IN ({placeholders}) AND freq='daily' AND adjust='qfq' "
            f"AND date >= date('now', ?) ORDER BY code, date",
            (*chunk, f"-{since_days} day"),
        ).fetchall()
        for row in rows:
            history.setdefault(row["code"], []).append(dict(row))
    return history


def build_stock_stage_features(conn, mkt_conn=None, snapshot_date: Optional[str] = None) -> int:
    ensure_tables(conn)
    snapshot_date = snapshot_date or date.today().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()
    own_conn = False
    if mkt_conn is None:
        mkt_conn = get_market_conn()
        own_conn = True

    try:
        stock_rows = conn.execute("""
            SELECT stock_code, latest_notice_date, latest_report_date,
                   price_1m_pct, price_20d_pct, price_trend
            FROM mart_stock_trend
        """).fetchall()
        if not stock_rows:
            logger.info("[阶段特征] 无股票趋势数据，跳过构建")
            return 0

        codes = [row["stock_code"] for row in stock_rows]
        price_history = _load_price_history(mkt_conn, codes)

        archetype_rows = conn.execute("""
            SELECT stock_code, stock_archetype, archetype_confidence,
                   revenue_yoy_positive_4q, profit_yoy_positive_4q,
                   operating_cashflow_positive_8q, eps_yoy_positive_4q,
                   revenue_yoy_median_4q, profit_yoy_median_4q,
                   latest_revenue_yoy, latest_profit_yoy,
                   revenue_yoy_down_streak_2q, profit_yoy_down_streak_2q,
                   net_profit_sign_switch_8q, inventory_revenue_vol_4q,
                   total_shares_growth_3y
            FROM dim_stock_archetype_latest
        """).fetchall()
        archetype_by_stock = {row["stock_code"]: dict(row) for row in archetype_rows}

        industry_rows = conn.execute("""
            SELECT stock_code, industry_tailwind_score, stage_industry_adjust_raw,
                   sector_excess_3m, sector_excess_6m
            FROM dim_stock_industry_context_latest
        """).fetchall()
        industry_by_stock = {row["stock_code"]: dict(row) for row in industry_rows}

        fin_rows = conn.execute("""
            SELECT stock_code, debt_ratio, holder_count_change_pct, ocf_to_profit
            FROM dim_financial_latest
        """).fetchall()
        fin_by_stock = {row["stock_code"]: dict(row) for row in fin_rows}

        gate_rows = conn.execute("""
            SELECT stock_code,
                   SUM(CASE WHEN follow_gate = 'follow' THEN 1 ELSE 0 END) AS follow_count,
                   SUM(CASE WHEN follow_gate = 'watch' THEN 1 ELSE 0 END) AS watch_count,
                   SUM(CASE WHEN follow_gate = 'observe' THEN 1 ELSE 0 END) AS observe_count,
                   SUM(CASE WHEN follow_gate = 'avoid' THEN 1 ELSE 0 END) AS avoid_count
            FROM mart_current_relationship
            GROUP BY stock_code
        """).fetchall()
        gate_by_stock = {row["stock_code"]: dict(row) for row in gate_rows}

        conn.execute("DELETE FROM fact_stock_stage_features WHERE snapshot_date = ?", (snapshot_date,))
        inserted = 0
        for stock in stock_rows:
            stock = dict(stock)
            stock_code = stock["stock_code"]
            prices = price_history.get(stock_code) or []
            closes = [_safe_float(row.get("close")) for row in prices if _safe_float(row.get("close")) is not None]
            highs = [_safe_float(row.get("high")) for row in prices if _safe_float(row.get("high")) is not None]
            lows = [_safe_float(row.get("low")) for row in prices if _safe_float(row.get("low")) is not None]
            amounts = [_safe_float(row.get("amount")) for row in prices]

            notice_anchor = stock.get("latest_notice_date") or stock.get("latest_report_date")
            path_state, path_max_gain_pct, path_max_drawdown_pct = _classify_path(prices, notice_anchor)
            return_1m = _window_return(closes, 20) if closes else None
            return_3m = _window_return(closes, 60) if closes else None
            return_6m = _window_return(closes, 120) if closes else None
            return_12m = _window_return(closes, 240) if closes else None
            ma120 = _moving_average(closes, 120)
            ma250 = _moving_average(closes, 250)
            last_close = closes[-1] if closes else None
            dist_ma120_pct = round((last_close / ma120 - 1) * 100, 2) if last_close not in (None, 0) and ma120 not in (None, 0) else None
            dist_ma250_pct = round((last_close / ma250 - 1) * 100, 2) if last_close not in (None, 0) and ma250 not in (None, 0) else None
            above_ma250 = 1 if last_close not in (None, 0) and ma250 not in (None, 0) and last_close > ma250 else 0
            max_drawdown_60d = _max_drawdown_pct(closes[-60:]) if len(closes) >= 2 else None
            amount_ratio_20_120 = _amount_ratio(amounts, 20, 120)
            volatility_20d = _volatility_pct(closes, 20)
            amplitude_20d = _amplitude_pct(highs, lows, 20)

            gates = gate_by_stock.get(stock_code) or {}
            follow_count = int(gates.get("follow_count") or 0)
            watch_count = int(gates.get("watch_count") or 0)
            observe_count = int(gates.get("observe_count") or 0)
            avoid_count = int(gates.get("avoid_count") or 0)
            stock_gate = (
                "follow" if follow_count > 0 else
                "watch" if watch_count > 0 else
                "observe" if observe_count > 0 else
                "avoid" if avoid_count > 0 else None
            )

            notice_age_days = _days_since(notice_anchor)
            generic_stage_raw = 45.0
            generic_stage_raw += {
                "未充分演绎": 18,
                "温和验证": 12,
                "震荡待定": 6,
                "已充分演绎": -12,
                "失效破坏": -28,
            }.get(path_state, 0)
            generic_stage_raw += {
                "连涨": 6,
                "震荡": 3,
                "连跌": -8,
            }.get(stock.get("price_trend"), 0)
            price_20d = _safe_float(stock.get("price_20d_pct"))
            price_1m = _safe_float(stock.get("price_1m_pct"))
            if price_20d is not None:
                if -12 <= price_20d <= 15:
                    generic_stage_raw += 10
                elif 15 < price_20d <= 30:
                    generic_stage_raw += 4
                elif price_20d > 30:
                    generic_stage_raw -= 12
                elif price_20d < -20:
                    generic_stage_raw -= 10
                elif price_20d < -10:
                    generic_stage_raw -= 4
            if price_1m is not None:
                if -8 <= price_1m <= 18:
                    generic_stage_raw += 6
                elif price_1m > 35:
                    generic_stage_raw -= 8
                elif price_1m < -15:
                    generic_stage_raw -= 6
            generic_stage_raw += (
                10 if notice_age_days is not None and notice_age_days <= 30 else
                6 if notice_age_days is not None and notice_age_days <= 60 else
                2 if notice_age_days is not None and notice_age_days <= 120 else
                -4
            )
            generic_stage_raw += {
                "follow": 10,
                "watch": 5,
                "observe": 0,
                "avoid": -15,
                None: 0,
            }.get(stock_gate, 0)
            generic_stage_raw += _safe_float((industry_by_stock.get(stock_code) or {}).get("stage_industry_adjust_raw")) or 0

            archetype = archetype_by_stock.get(stock_code) or {}
            fin = fin_by_stock.get(stock_code) or {}
            stock_archetype = archetype.get("stock_archetype") or "周期/事件驱动型"

            quality_continuity = quality_trend = quality_overheat = 0.0
            growth_continuity = growth_slowdown = growth_stretch = 0.0
            cycle_recovery = cycle_realization = cycle_uncertainty = 0.0
            stage_reason = "阶段结构中性"

            if stock_archetype == "高质量稳健型":
                quality_continuity += 10 if int(archetype.get("revenue_yoy_positive_4q") or 0) >= 3 else 0
                quality_continuity += 10 if int(archetype.get("profit_yoy_positive_4q") or 0) >= 3 else 0
                quality_continuity += 10 if int(archetype.get("operating_cashflow_positive_8q") or 0) >= 6 else 0
                quality_continuity += 10 if (_safe_float(fin.get("debt_ratio")) or 1) <= 0.80 else 0
                if return_12m is not None and 0 <= return_12m <= 80:
                    quality_trend += 10
                if above_ma250:
                    quality_trend += 10
                if amplitude_20d is not None and amplitude_20d <= 25 and (amount_ratio_20_120 or 0) <= 2:
                    quality_trend += 5
                if max_drawdown_60d is not None and max_drawdown_60d < 20:
                    quality_trend += 10

                if (return_12m or 0) > 120:
                    quality_overheat += 8
                if (amount_ratio_20_120 or 0) > 2 and (amplitude_20d or 0) > 25:
                    quality_overheat += 8
                if (_safe_float(fin.get("holder_count_change_pct")) or 0) > 0.10 and (return_6m or 0) > 40:
                    quality_overheat += 4
                if (_safe_float(archetype.get("latest_profit_yoy")) or 0) <= 0 and (return_3m or 0) > 15:
                    quality_overheat += 5
                stage_type_adjust_raw = round((quality_continuity + quality_trend - quality_overheat - 25) * 0.4, 2)
                if stage_type_adjust_raw >= 5:
                    stage_reason = "稳健型基本面续航与趋势健康较好"
                elif quality_overheat >= 8:
                    stage_reason = "稳健型短期存在过热迹象"
            elif stock_archetype == "成长兑现型":
                growth_continuity += 15 if (_safe_float(archetype.get("revenue_yoy_median_4q")) or 0) >= 0.20 else 8 if (_safe_float(archetype.get("revenue_yoy_median_4q")) or 0) >= 0.12 else 0
                growth_continuity += 15 if (_safe_float(archetype.get("profit_yoy_median_4q")) or 0) >= 0.20 else 8 if (_safe_float(archetype.get("profit_yoy_median_4q")) or 0) >= 0.12 else 0
                growth_continuity += 10 if int(archetype.get("eps_yoy_positive_4q") or 0) >= 3 else 4 if int(archetype.get("eps_yoy_positive_4q") or 0) >= 2 else 0

                if int(archetype.get("revenue_yoy_down_streak_2q") or 0):
                    growth_slowdown += 8
                if int(archetype.get("profit_yoy_down_streak_2q") or 0):
                    growth_slowdown += 10
                if (_safe_float(archetype.get("latest_revenue_yoy")) or 0) > 0 and (_safe_float(archetype.get("latest_profit_yoy")) or 0) < 0:
                    growth_slowdown += 7

                if (return_6m or 0) > 80:
                    growth_stretch += 10
                if (dist_ma120_pct or 0) > 35:
                    growth_stretch += 10
                if (amount_ratio_20_120 or 0) > 1.8 and (price_20d or 0) < 10:
                    growth_stretch += 8
                if (volatility_20d or 0) > 4.5:
                    growth_stretch += 7
                stage_type_adjust_raw = round((growth_continuity - growth_slowdown - growth_stretch) * 0.35, 2)
                if stage_type_adjust_raw >= 5:
                    stage_reason = "成长型增速延续尚可，阶段仍具跟踪价值"
                elif growth_slowdown >= 10 or growth_stretch >= 10:
                    stage_reason = "成长型已出现放缓或价格透支信号"
            else:
                if (_safe_float(archetype.get("latest_revenue_yoy")) or 0) > 0 and (_safe_float(archetype.get("latest_profit_yoy")) or 0) > 0:
                    cycle_recovery += 15
                if (_safe_float(fin.get("ocf_to_profit")) or 0) >= 0.8:
                    cycle_recovery += 10
                if (_safe_float((industry_by_stock.get(stock_code) or {}).get("sector_excess_6m")) or 0) > 5:
                    cycle_recovery += 10

                if (return_6m or 0) > 100:
                    cycle_realization += 10
                if int(archetype.get("profit_yoy_down_streak_2q") or 0):
                    cycle_realization += 12
                if (_safe_float((industry_by_stock.get(stock_code) or {}).get("sector_excess_3m")) or 0) < 0 and (return_3m or 0) > 30:
                    cycle_realization += 8
                if (amount_ratio_20_120 or 0) > 2 and (_safe_float(fin.get("holder_count_change_pct")) or 0) > 0.10:
                    cycle_realization += 10

                if int(archetype.get("net_profit_sign_switch_8q") or 0) >= 2:
                    cycle_uncertainty += 10
                if (_safe_float(fin.get("ocf_to_profit")) or 0) < 0.5:
                    cycle_uncertainty += 8
                if (_safe_float(archetype.get("total_shares_growth_3y")) or 0) > 0.30:
                    cycle_uncertainty += 7
                stage_type_adjust_raw = round((cycle_recovery - cycle_realization - cycle_uncertainty) * 0.3, 2)
                if stage_type_adjust_raw >= 5:
                    stage_reason = "周期/事件型处于修复展开阶段"
                elif cycle_realization >= 10 or cycle_uncertainty >= 8:
                    stage_reason = "周期/事件型兑现或不确定性压力偏大"

            stage_score_v1 = _clamp_score(generic_stage_raw + stage_type_adjust_raw)

            conn.execute("""
                INSERT OR REPLACE INTO fact_stock_stage_features (
                    snapshot_date, stock_code, latest_notice_date, latest_report_date,
                    stock_archetype, path_state, path_max_gain_pct, path_max_drawdown_pct,
                    return_1m, return_3m, return_6m, return_12m, ma120, ma250,
                    dist_ma120_pct, dist_ma250_pct, above_ma250, max_drawdown_60d,
                    amount_ratio_20_120, volatility_20d, amplitude_20d,
                    gate_follow_count, gate_watch_count, gate_observe_count, gate_avoid_count,
                    stock_gate, generic_stage_raw, stage_quality_continuity_raw,
                    stage_quality_trend_raw, stage_quality_overheat_penalty,
                    stage_growth_continuity_raw, stage_growth_slowdown_penalty,
                    stage_growth_stretch_penalty, stage_cycle_recovery_raw,
                    stage_cycle_realization_penalty, stage_cycle_uncertainty_penalty,
                    stage_type_adjust_raw, stage_score_v1, stage_reason, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_date,
                stock_code,
                stock.get("latest_notice_date"),
                stock.get("latest_report_date"),
                stock_archetype,
                path_state,
                path_max_gain_pct,
                path_max_drawdown_pct,
                return_1m,
                return_3m,
                return_6m,
                return_12m,
                ma120,
                ma250,
                dist_ma120_pct,
                dist_ma250_pct,
                above_ma250,
                max_drawdown_60d,
                amount_ratio_20_120,
                volatility_20d,
                amplitude_20d,
                follow_count,
                watch_count,
                observe_count,
                avoid_count,
                stock_gate,
                generic_stage_raw,
                quality_continuity,
                quality_trend,
                quality_overheat,
                growth_continuity,
                growth_slowdown,
                growth_stretch,
                cycle_recovery,
                cycle_realization,
                cycle_uncertainty,
                stage_type_adjust_raw,
                stage_score_v1,
                stage_reason,
                now,
            ))
            inserted += 1

        conn.execute("DELETE FROM dim_stock_stage_latest")
        conn.execute("""
            INSERT INTO dim_stock_stage_latest (
                stock_code, snapshot_date, latest_notice_date, latest_report_date,
                stock_archetype, path_state, path_max_gain_pct, path_max_drawdown_pct,
                return_1m, return_3m, return_6m, return_12m, ma120, ma250,
                dist_ma120_pct, dist_ma250_pct, above_ma250, max_drawdown_60d,
                amount_ratio_20_120, volatility_20d, amplitude_20d,
                gate_follow_count, gate_watch_count, gate_observe_count, gate_avoid_count,
                stock_gate, generic_stage_raw, stage_quality_continuity_raw,
                stage_quality_trend_raw, stage_quality_overheat_penalty,
                stage_growth_continuity_raw, stage_growth_slowdown_penalty,
                stage_growth_stretch_penalty, stage_cycle_recovery_raw,
                stage_cycle_realization_penalty, stage_cycle_uncertainty_penalty,
                stage_type_adjust_raw, stage_score_v1, stage_reason, updated_at
            )
            SELECT stock_code, snapshot_date, latest_notice_date, latest_report_date,
                   stock_archetype, path_state, path_max_gain_pct, path_max_drawdown_pct,
                   return_1m, return_3m, return_6m, return_12m, ma120, ma250,
                   dist_ma120_pct, dist_ma250_pct, above_ma250, max_drawdown_60d,
                   amount_ratio_20_120, volatility_20d, amplitude_20d,
                   gate_follow_count, gate_watch_count, gate_observe_count, gate_avoid_count,
                   stock_gate, generic_stage_raw, stage_quality_continuity_raw,
                   stage_quality_trend_raw, stage_quality_overheat_penalty,
                   stage_growth_continuity_raw, stage_growth_slowdown_penalty,
                   stage_growth_stretch_penalty, stage_cycle_recovery_raw,
                   stage_cycle_realization_penalty, stage_cycle_uncertainty_penalty,
                   stage_type_adjust_raw, stage_score_v1, stage_reason, updated_at
            FROM fact_stock_stage_features
            WHERE snapshot_date = ?
        """, (snapshot_date,))
        conn.commit()
        logger.info(f"[阶段特征] 构建完成: {inserted} 只股票, 快照 {snapshot_date}")
        return inserted
    finally:
        if own_conn:
            mkt_conn.close()

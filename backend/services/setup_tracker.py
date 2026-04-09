"""
setup_tracker.py

Setup A 前瞻验证闭环：
- 记录每个交易日的候选 setup 快照
- 自动补齐 10/30/60 交易日后验结果
- 提供可复算的研究摘要
"""

import logging
from datetime import datetime
from typing import Optional

from services.industry import load_industry_map
from services.market_db import get_market_conn
from services.utils import safe_float as _safe_float

logger = logging.getLogger("cm-api")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _resolve_snapshot_date() -> str:
    mkt_conn = get_market_conn()
    try:
        row = mkt_conn.execute(
            "SELECT MAX(date) AS max_date FROM price_kline WHERE freq='daily' AND adjust='qfq'"
        ).fetchone()
        return row["max_date"] if row and row["max_date"] else _today_str()
    finally:
        mkt_conn.close()


def _get_latest_close_on_or_before(mkt_conn, code: str, anchor_date: str):
    return mkt_conn.execute("""
        SELECT date, close
        FROM price_kline
        WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
          AND close IS NOT NULL AND date <= ?
        ORDER BY date DESC
        LIMIT 1
    """, (code, anchor_date)).fetchone()


def _get_first_close_on_or_after(mkt_conn, code: str, anchor_date: str):
    return mkt_conn.execute("""
        SELECT date, close
        FROM price_kline
        WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
          AND close IS NOT NULL AND date >= ?
        ORDER BY date ASC
        LIMIT 1
    """, (code, anchor_date)).fetchone()


def _get_nth_trade_date(conn, anchor_date: str, offset: int) -> Optional[str]:
    row = conn.execute("""
        SELECT trade_date
        FROM dim_trading_calendar
        WHERE trade_date >= ? AND is_trading = 1
        ORDER BY trade_date
        LIMIT 1 OFFSET ?
    """, (anchor_date, offset)).fetchone()
    return row["trade_date"] if row else None


def _calc_gain(entry_price: Optional[float], exit_price: Optional[float]) -> Optional[float]:
    if entry_price is None or exit_price is None or entry_price <= 0:
        return None
    return round((exit_price - entry_price) / entry_price * 100, 2)


def _calc_max_drawdown(mkt_conn, code: str, start_date: str, end_date: str) -> Optional[float]:
    rows = mkt_conn.execute("""
        SELECT close
        FROM price_kline
        WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
          AND close IS NOT NULL AND date >= ? AND date <= ?
        ORDER BY date
    """, (code, start_date, end_date)).fetchall()
    closes = [_safe_float(row["close"]) for row in rows if _safe_float(row["close"]) is not None]
    if len(closes) < 2:
        return None

    peak = closes[0]
    max_drawdown = 0.0
    for close in closes:
        if close > peak:
            peak = close
        if peak > 0:
            drawdown = (peak - close) / peak * 100
            if drawdown > max_drawdown:
                max_drawdown = drawdown
    return round(max_drawdown, 2)


def snapshot_setup_candidates(conn, snapshot_date: Optional[str] = None) -> int:
    snapshot_date = snapshot_date or _resolve_snapshot_date()
    now = datetime.now().isoformat()

    rows = conn.execute("""
        SELECT stock_code, stock_name,
               setup_tag, setup_priority, setup_reason, setup_confidence,
               setup_level, setup_inst_id, setup_inst_name, setup_event_type,
               setup_industry_name, action_score,
               discovery_score, company_quality_score, stage_score,
               forecast_score, forecast_score_effective,
               raw_composite_priority_score, composite_priority_score,
               composite_cap_score, composite_cap_reason,
               stock_archetype, priority_pool, priority_pool_reason,
               score_highlights, score_risks,
               latest_report_date, latest_notice_date, report_age_days,
               setup_score_raw, setup_execution_gate, setup_execution_reason,
               industry_skill_raw, industry_skill_grade,
               followability_grade, premium_grade, report_recency_grade,
               reliability_grade, crowding_bucket, crowding_yield_raw,
               crowding_yield_grade, crowding_stability_raw, crowding_stability_grade,
               crowding_fit_raw, crowding_fit_grade, crowding_fit_sample, crowding_fit_source
        FROM mart_stock_trend
        WHERE setup_tag IS NOT NULL
    """).fetchall()

    snapshot_columns = [
        "snapshot_date", "stock_code", "stock_name",
        "setup_tag", "setup_priority", "setup_reason", "setup_confidence",
        "setup_level", "setup_inst_id", "setup_inst_name", "setup_event_type",
        "setup_industry_name", "snapshot_sw_level1", "snapshot_sw_level2", "snapshot_sw_level3", "action_score",
        "discovery_score", "company_quality_score", "stage_score",
        "forecast_score", "forecast_score_effective",
        "raw_composite_priority_score", "composite_priority_score",
        "composite_cap_score", "composite_cap_reason",
        "stock_archetype", "priority_pool", "priority_pool_reason",
        "score_highlights", "score_risks",
        "latest_report_date", "latest_notice_date", "report_age_days",
        "setup_score_raw", "setup_execution_gate", "setup_execution_reason",
        "industry_skill_raw", "industry_skill_grade",
        "followability_grade", "premium_grade", "report_recency_grade",
        "reliability_grade", "crowding_bucket", "crowding_yield_raw",
        "crowding_yield_grade", "crowding_stability_raw", "crowding_stability_grade",
        "crowding_fit_raw", "crowding_fit_grade", "crowding_fit_sample", "crowding_fit_source",
        "entry_trade_date", "entry_price", "updated_at",
    ]
    snapshot_placeholders = ",".join(["?"] * len(snapshot_columns))

    mkt_conn = get_market_conn()
    try:
        industry_map = load_industry_map(conn)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM fact_setup_snapshot WHERE snapshot_date = ?", (snapshot_date,))
        inserted = 0
        for row in rows:
            entry = _get_latest_close_on_or_before(mkt_conn, row["stock_code"], snapshot_date)
            entry_trade_date = entry["date"] if entry else None
            entry_price = _safe_float(entry["close"]) if entry else None
            industry = industry_map.get(row["stock_code"]) or {}
            conn.execute(f"""
                INSERT OR REPLACE INTO fact_setup_snapshot (
                    {", ".join(snapshot_columns)}
                ) VALUES ({snapshot_placeholders})
            """, (
                snapshot_date,
                row["stock_code"],
                row["stock_name"],
                row["setup_tag"],
                row["setup_priority"],
                row["setup_reason"],
                row["setup_confidence"],
                row["setup_level"],
                row["setup_inst_id"],
                row["setup_inst_name"],
                row["setup_event_type"],
                row["setup_industry_name"],
                industry.get("sw_level1"),
                industry.get("sw_level2"),
                industry.get("sw_level3"),
                row["action_score"],
                row["discovery_score"],
                row["company_quality_score"],
                row["stage_score"],
                row["forecast_score"],
                row["forecast_score_effective"],
                row["raw_composite_priority_score"],
                row["composite_priority_score"],
                row["composite_cap_score"],
                row["composite_cap_reason"],
                row["stock_archetype"],
                row["priority_pool"],
                row["priority_pool_reason"],
                row["score_highlights"],
                row["score_risks"],
                row["latest_report_date"],
                row["latest_notice_date"],
                row["report_age_days"],
                row["setup_score_raw"],
                row["setup_execution_gate"],
                row["setup_execution_reason"],
                row["industry_skill_raw"],
                row["industry_skill_grade"],
                row["followability_grade"],
                row["premium_grade"],
                row["report_recency_grade"],
                row["reliability_grade"],
                row["crowding_bucket"],
                row["crowding_yield_raw"],
                row["crowding_yield_grade"],
                row["crowding_stability_raw"],
                row["crowding_stability_grade"],
                row["crowding_fit_raw"],
                row["crowding_fit_grade"],
                row["crowding_fit_sample"],
                row["crowding_fit_source"],
                entry_trade_date,
                entry_price,
                now,
            ))
            inserted += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        mkt_conn.close()

    logger.info(f"[Setup跟踪] 快照完成: {snapshot_date} · {inserted} 条")
    return inserted


def backfill_setup_snapshot_industry(conn, snapshot_date: Optional[str] = None) -> int:
    params = []
    where = """
        WHERE COALESCE(snapshot_sw_level1, '') = ''
           OR COALESCE(snapshot_sw_level2, '') = ''
           OR COALESCE(snapshot_sw_level3, '') = ''
    """
    if snapshot_date:
        where += " AND snapshot_date = ?"
        params.append(snapshot_date)

    rows = conn.execute(
        f"""
        SELECT snapshot_date, stock_code, setup_tag, setup_inst_id,
               snapshot_sw_level1, snapshot_sw_level2, snapshot_sw_level3
        FROM fact_setup_snapshot
        {where}
        """,
        params,
    ).fetchall()
    if not rows:
        return 0

    industry_map = load_industry_map(conn)
    now = datetime.now().isoformat()
    updates = []
    for row in rows:
        industry = industry_map.get(row["stock_code"]) or {}
        sw_level1 = row["snapshot_sw_level1"] or industry.get("sw_level1")
        sw_level2 = row["snapshot_sw_level2"] or industry.get("sw_level2")
        sw_level3 = row["snapshot_sw_level3"] or industry.get("sw_level3")
        if not any((sw_level1, sw_level2, sw_level3)):
            continue
        updates.append((
            sw_level1,
            sw_level2,
            sw_level3,
            now,
            row["snapshot_date"],
            row["stock_code"],
            row["setup_tag"],
            row["setup_inst_id"],
        ))

    if not updates:
        return 0

    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.executemany(
            """
            UPDATE fact_setup_snapshot
            SET snapshot_sw_level1 = ?,
                snapshot_sw_level2 = ?,
                snapshot_sw_level3 = ?,
                updated_at = ?
            WHERE snapshot_date = ? AND stock_code = ? AND setup_tag = ? AND setup_inst_id = ?
            """,
            updates,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    logger.info(f"[Setup跟踪] 行业快照回填完成: {len(updates)} 条")
    return len(updates)


def refresh_setup_snapshot_returns(conn, snapshot_date: Optional[str] = None) -> int:
    now = datetime.now().isoformat()
    today = _today_str()
    params = []
    where = "WHERE entry_trade_date IS NOT NULL"
    if snapshot_date:
        where += " AND snapshot_date = ?"
        params.append(snapshot_date)

    rows = conn.execute(f"""
        SELECT snapshot_date, stock_code, setup_tag, setup_inst_id, entry_trade_date, entry_price
        FROM fact_setup_snapshot
        {where}
    """, params).fetchall()

    mkt_conn = get_market_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        refreshed = 0
        for row in rows:
            stock_code = row["stock_code"]
            entry_trade_date = row["entry_trade_date"]
            entry_price = _safe_float(row["entry_price"])
            if not entry_trade_date or entry_price is None or entry_price <= 0:
                continue

            latest = _get_latest_close_on_or_before(mkt_conn, stock_code, today)
            current_trade_date = latest["date"] if latest else None
            current_price = _safe_float(latest["close"]) if latest else None
            gain_to_now = _calc_gain(entry_price, current_price)

            horizon_values = {}
            for horizon in (10, 30, 60):
                target_trade_date = _get_nth_trade_date(conn, entry_trade_date, horizon)
                matured = 1 if target_trade_date and target_trade_date <= today else 0
                eval_row = None
                eval_price = None
                gain = None
                max_drawdown = None
                if matured and target_trade_date:
                    eval_row = _get_first_close_on_or_after(mkt_conn, stock_code, target_trade_date)
                    eval_date = eval_row["date"] if eval_row else None
                    eval_price = _safe_float(eval_row["close"]) if eval_row else None
                    gain = _calc_gain(entry_price, eval_price)
                    if eval_date:
                        max_drawdown = _calc_max_drawdown(mkt_conn, stock_code, entry_trade_date, eval_date)
                horizon_values[horizon] = {
                    "matured": matured,
                    "gain": gain,
                    "max_drawdown": max_drawdown,
                }

            conn.execute("""
                UPDATE fact_setup_snapshot
                SET current_trade_date = ?,
                    current_price = ?,
                    gain_to_now = ?,
                    gain_10d = ?,
                    gain_30d = ?,
                    gain_60d = ?,
                    max_drawdown_10d = ?,
                    max_drawdown_30d = ?,
                    max_drawdown_60d = ?,
                    matured_10d = ?,
                    matured_30d = ?,
                    matured_60d = ?,
                    updated_at = ?
                WHERE snapshot_date = ? AND stock_code = ? AND setup_tag = ? AND setup_inst_id = ?
            """, (
                current_trade_date,
                current_price,
                gain_to_now,
                horizon_values[10]["gain"],
                horizon_values[30]["gain"],
                horizon_values[60]["gain"],
                horizon_values[10]["max_drawdown"],
                horizon_values[30]["max_drawdown"],
                horizon_values[60]["max_drawdown"],
                horizon_values[10]["matured"],
                horizon_values[30]["matured"],
                horizon_values[60]["matured"],
                now,
                row["snapshot_date"],
                stock_code,
                row["setup_tag"],
                row["setup_inst_id"],
            ))
            refreshed += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        mkt_conn.close()

    logger.info(f"[Setup跟踪] 后验刷新完成: {refreshed} 条")
    return refreshed


def refresh_setup_tracking(conn, snapshot_date: Optional[str] = None) -> dict:
    resolved = snapshot_date or _resolve_snapshot_date()
    inserted = snapshot_setup_candidates(conn, resolved)
    industry_backfilled = backfill_setup_snapshot_industry(conn)
    refreshed = refresh_setup_snapshot_returns(conn, resolved)
    return {
        "snapshot_date": resolved,
        "snapshots": inserted,
        "industry_backfilled": industry_backfilled,
        "refreshed": refreshed,
    }


def get_setup_tracking_summary(conn) -> dict:
    latest_row = conn.execute(
        "SELECT MAX(snapshot_date) AS snapshot_date FROM fact_setup_snapshot"
    ).fetchone()
    latest_snapshot_date = latest_row["snapshot_date"] if latest_row else None

    totals_row = conn.execute("""
        SELECT COUNT(*) AS total,
               SUM(CASE WHEN matured_10d = 0 THEN 1 ELSE 0 END) AS open_10d,
               SUM(CASE WHEN matured_30d = 0 THEN 1 ELSE 0 END) AS open_30d,
               SUM(CASE WHEN matured_60d = 0 THEN 1 ELSE 0 END) AS open_60d
        FROM fact_setup_snapshot
    """).fetchone()

    def _horizon_summary(horizon: int) -> dict:
        row = conn.execute(f"""
            SELECT COUNT(*) AS total,
                   AVG(gain_{horizon}d) AS avg_gain,
                   AVG(max_drawdown_{horizon}d) AS avg_drawdown,
                   AVG(CASE WHEN gain_{horizon}d > 0 THEN 1.0 ELSE 0.0 END) * 100 AS win_rate
            FROM fact_setup_snapshot
            WHERE matured_{horizon}d = 1 AND gain_{horizon}d IS NOT NULL
        """).fetchone()
        return {
            "count": int(row["total"] or 0),
            "avg_gain": round(row["avg_gain"], 2) if row["avg_gain"] is not None else None,
            "avg_drawdown": round(row["avg_drawdown"], 2) if row["avg_drawdown"] is not None else None,
            "win_rate": round(row["win_rate"], 2) if row["win_rate"] is not None else None,
        }

    by_priority = []
    rows = conn.execute("""
        SELECT setup_priority,
               COUNT(*) AS total,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN gain_30d END) AS avg_gain_30d,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_30d,
               AVG(CASE WHEN matured_30d = 1 THEN max_drawdown_30d END) AS avg_drawdown_30d
        FROM fact_setup_snapshot
        GROUP BY setup_priority
        ORDER BY setup_priority
    """).fetchall()
    for row in rows:
        by_priority.append({
            "priority": row["setup_priority"],
            "count": int(row["total"] or 0),
            "avg_gain_30d": round(row["avg_gain_30d"], 2) if row["avg_gain_30d"] is not None else None,
            "win_rate_30d": round(row["win_rate_30d"], 2) if row["win_rate_30d"] is not None else None,
            "avg_drawdown_30d": round(row["avg_drawdown_30d"], 2) if row["avg_drawdown_30d"] is not None else None,
        })

    by_pool = []
    rows = conn.execute("""
        SELECT priority_pool,
               COUNT(*) AS total,
               AVG(composite_priority_score) AS avg_composite_score,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN gain_30d END) AS avg_gain_30d,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_30d,
               AVG(CASE WHEN matured_30d = 1 THEN max_drawdown_30d END) AS avg_drawdown_30d
        FROM fact_setup_snapshot
        WHERE priority_pool IS NOT NULL AND priority_pool != ''
        GROUP BY priority_pool
        ORDER BY
            CASE priority_pool
                WHEN 'A池' THEN 0
                WHEN 'B池' THEN 1
                WHEN 'C池' THEN 2
                WHEN 'D池' THEN 3
                ELSE 9
            END
    """).fetchall()
    for row in rows:
        by_pool.append({
            "priority_pool": row["priority_pool"],
            "count": int(row["total"] or 0),
            "avg_composite_score": round(row["avg_composite_score"], 2) if row["avg_composite_score"] is not None else None,
            "avg_gain_30d": round(row["avg_gain_30d"], 2) if row["avg_gain_30d"] is not None else None,
            "win_rate_30d": round(row["win_rate_30d"], 2) if row["win_rate_30d"] is not None else None,
            "avg_drawdown_30d": round(row["avg_drawdown_30d"], 2) if row["avg_drawdown_30d"] is not None else None,
        })

    return {
        "snapshot_date": latest_snapshot_date,
        "total_snapshots": int(totals_row["total"] or 0) if totals_row else 0,
        "open_10d": int(totals_row["open_10d"] or 0) if totals_row else 0,
        "open_30d": int(totals_row["open_30d"] or 0) if totals_row else 0,
        "open_60d": int(totals_row["open_60d"] or 0) if totals_row else 0,
        "h10": _horizon_summary(10),
        "h30": _horizon_summary(30),
        "h60": _horizon_summary(60),
        "by_priority": by_priority,
        "by_pool": by_pool,
    }


def list_setup_tracking_snapshots(conn, limit: int = 200):
    rows = conn.execute("""
        SELECT snapshot_date, stock_code, stock_name,
               setup_tag, setup_priority, setup_reason, setup_confidence,
               setup_inst_name, latest_report_date,
               discovery_score, company_quality_score, stage_score,
               forecast_score, composite_priority_score, priority_pool,
               stock_archetype, score_highlights, score_risks,
               crowding_bucket, crowding_fit_raw, crowding_fit_grade,
               gain_to_now, gain_10d, gain_30d, gain_60d,
               max_drawdown_10d, max_drawdown_30d, max_drawdown_60d,
               matured_10d, matured_30d, matured_60d
        FROM fact_setup_snapshot
        ORDER BY snapshot_date DESC,
                 CASE COALESCE(priority_pool, '')
                     WHEN 'A池' THEN 0
                     WHEN 'B池' THEN 1
                     WHEN 'C池' THEN 2
                     WHEN 'D池' THEN 3
                     ELSE 9
                 END,
                 setup_priority ASC,
                 COALESCE(composite_priority_score, 0) DESC,
                 COALESCE(discovery_score, 0) DESC,
                 COALESCE(setup_score_raw, 0) DESC,
                 stock_code
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(row) for row in rows]

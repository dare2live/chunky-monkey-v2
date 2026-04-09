"""
industry_context_engine.py — 股票级行业上下文中间层

把板块动量和双重确认结果沉成股票级行业上下文，
供评分、详情页和后续解释型 UI 统一复用。
"""

import logging
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger("cm-api")


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_stock_industry_context (
            snapshot_date             TEXT NOT NULL,
            stock_code                TEXT NOT NULL,
            sw_level1                 TEXT,
            sw_level2                 TEXT,
            sector_momentum_score     REAL,
            sector_trend_state        TEXT,
            sector_macd_cross         INTEGER DEFAULT 0,
            sector_return_1m          REAL,
            sector_return_3m          REAL,
            sector_return_6m          REAL,
            sector_return_12m         REAL,
            sector_excess_1m          REAL,
            sector_excess_3m          REAL,
            sector_excess_6m          REAL,
            sector_excess_12m         REAL,
            sector_rotation_score     REAL,
            sector_rotation_rank      INTEGER,
            sector_rotation_rank_1m   INTEGER,
            sector_rotation_rank_3m   INTEGER,
            sector_rotation_bucket    TEXT,
            sector_rotation_blacklisted INTEGER DEFAULT 0,
            dual_confirm_total        INTEGER DEFAULT 0,
            dual_confirm_recent_180d  INTEGER DEFAULT 0,
            dual_confirm_new_entry    INTEGER DEFAULT 0,
            dual_confirm_increase     INTEGER DEFAULT 0,
            industry_tailwind_score   REAL,
            stage_industry_adjust_raw REAL,
            updated_at                TEXT,
            PRIMARY KEY (snapshot_date, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_fsic_stock ON fact_stock_industry_context(stock_code);

        CREATE TABLE IF NOT EXISTS dim_stock_industry_context_latest (
            stock_code                TEXT PRIMARY KEY,
            snapshot_date             TEXT,
            sw_level1                 TEXT,
            sw_level2                 TEXT,
            sector_momentum_score     REAL,
            sector_trend_state        TEXT,
            sector_macd_cross         INTEGER DEFAULT 0,
            sector_return_1m          REAL,
            sector_return_3m          REAL,
            sector_return_6m          REAL,
            sector_return_12m         REAL,
            sector_excess_1m          REAL,
            sector_excess_3m          REAL,
            sector_excess_6m          REAL,
            sector_excess_12m         REAL,
            sector_rotation_score     REAL,
            sector_rotation_rank      INTEGER,
            sector_rotation_rank_1m   INTEGER,
            sector_rotation_rank_3m   INTEGER,
            sector_rotation_bucket    TEXT,
            sector_rotation_blacklisted INTEGER DEFAULT 0,
            dual_confirm_total        INTEGER DEFAULT 0,
            dual_confirm_recent_180d  INTEGER DEFAULT 0,
            dual_confirm_new_entry    INTEGER DEFAULT 0,
            dual_confirm_increase     INTEGER DEFAULT 0,
            industry_tailwind_score   REAL,
            stage_industry_adjust_raw REAL,
            updated_at                TEXT
        );
    """)
    for col in [
        "sector_return_1m REAL", "sector_return_3m REAL", "sector_return_6m REAL", "sector_return_12m REAL",
        "sector_excess_1m REAL", "sector_excess_3m REAL", "sector_excess_6m REAL", "sector_excess_12m REAL",
        "sector_rotation_score REAL", "sector_rotation_rank INTEGER", "sector_rotation_rank_1m INTEGER",
        "sector_rotation_rank_3m INTEGER", "sector_rotation_bucket TEXT",
        "sector_rotation_blacklisted INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(f"ALTER TABLE fact_stock_industry_context ADD COLUMN {col}")
        except Exception:
            pass
        try:
            conn.execute(f"ALTER TABLE dim_stock_industry_context_latest ADD COLUMN {col}")
        except Exception:
            pass
    conn.commit()


def _score_tailwind(
    momentum_score: Optional[float],
    trend_state: Optional[str],
    macd_cross: int,
    dual_recent: int,
    excess_3m: Optional[float],
    excess_6m: Optional[float],
    rotation_score: Optional[float],
    rotation_bucket: Optional[str],
) -> tuple[float, float]:
    momentum = float(momentum_score or 0)
    score = momentum * 0.75
    score += {
        "bullish": 20,
        "recovering": 14,
        "neutral": 6,
        "weakening": -6,
        "bearish": -14,
    }.get(str(trend_state or ""), 0)
    if macd_cross:
        score += 8
    if dual_recent >= 5:
        score += 10
    elif dual_recent >= 2:
        score += 6
    elif dual_recent >= 1:
        score += 3
    ex3 = float(excess_3m or 0)
    ex6 = float(excess_6m or 0)
    score += (
        8 if ex3 >= 10 else
        5 if ex3 >= 5 else
        2 if ex3 >= 0 else
        -3 if ex3 <= -5 else 0
    )
    score += (
        6 if ex6 >= 15 else
        4 if ex6 >= 8 else
        2 if ex6 >= 0 else
        -2 if ex6 <= -8 else 0
    )
    rot_score = float(rotation_score or 0)
    if rot_score:
        score += (rot_score - 50.0) * 0.22
    if rotation_bucket == "leader":
        score += 10
    elif rotation_bucket == "blacklist":
        score -= 12

    tailwind_score = round(max(0.0, min(100.0, score)), 2)
    if tailwind_score >= 75:
        stage_adjust = 8.0
    elif tailwind_score >= 60:
        stage_adjust = 5.0
    elif tailwind_score >= 45:
        stage_adjust = 2.0
    elif tailwind_score >= 30:
        stage_adjust = -2.0
    else:
        stage_adjust = -6.0
    return tailwind_score, stage_adjust


def build_stock_industry_context(conn, snapshot_date: Optional[str] = None) -> int:
    ensure_tables(conn)
    snapshot_date = snapshot_date or date.today().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()
    recent_cutoff = (date.today() - timedelta(days=180)).strftime("%Y%m%d")

    sector_by_name = {}
    try:
        rows = conn.execute("""
            SELECT sector_name, momentum_score, trend_state, macd_cross,
                   return_1m, return_3m, return_6m, return_12m,
                   excess_1m, excess_3m, excess_6m, excess_12m,
                   rotation_score, rotation_rank, rotation_rank_1m, rotation_rank_3m,
                   rotation_bucket, rotation_blacklisted
            FROM mart_sector_momentum
        """).fetchall()
        for row in rows:
            sector_by_name[row["sector_name"]] = dict(row)
    except Exception:
        sector_by_name = {}

    dual_by_stock = {}
    try:
        rows = conn.execute("""
            SELECT stock_code,
                   COUNT(*) AS dual_confirm_total,
                   SUM(CASE WHEN report_date >= ? THEN 1 ELSE 0 END) AS dual_confirm_recent_180d,
                   SUM(CASE WHEN event_type = 'new_entry' THEN 1 ELSE 0 END) AS dual_confirm_new_entry,
                   SUM(CASE WHEN event_type = 'increase' THEN 1 ELSE 0 END) AS dual_confirm_increase
            FROM mart_dual_confirm
            WHERE dual_confirm = 1
            GROUP BY stock_code
        """, (recent_cutoff,)).fetchall()
        for row in rows:
            dual_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        dual_by_stock = {}

    stocks = conn.execute("""
        SELECT stock_code, sw_level1, sw_level2
        FROM dim_stock_industry
    """).fetchall()

    conn.execute("DELETE FROM fact_stock_industry_context WHERE snapshot_date = ?", (snapshot_date,))
    inserted = 0
    for row in stocks:
        stock_code = row["stock_code"]
        sw_level1 = row["sw_level1"]
        sw_level2 = row["sw_level2"]
        sector = sector_by_name.get(sw_level1 or "") or {}
        dual = dual_by_stock.get(stock_code) or {}
        tailwind_score, stage_adjust = _score_tailwind(
            sector.get("momentum_score"),
            sector.get("trend_state"),
            int(sector.get("macd_cross") or 0),
            int(dual.get("dual_confirm_recent_180d") or 0),
            sector.get("excess_3m"),
            sector.get("excess_6m"),
            sector.get("rotation_score"),
            sector.get("rotation_bucket"),
        )

        conn.execute("""
            INSERT OR REPLACE INTO fact_stock_industry_context
            (snapshot_date, stock_code, sw_level1, sw_level2, sector_momentum_score,
             sector_trend_state, sector_macd_cross, sector_return_1m, sector_return_3m,
             sector_return_6m, sector_return_12m, sector_excess_1m, sector_excess_3m,
             sector_excess_6m, sector_excess_12m, sector_rotation_score,
             sector_rotation_rank, sector_rotation_rank_1m, sector_rotation_rank_3m,
             sector_rotation_bucket, sector_rotation_blacklisted, dual_confirm_total,
             dual_confirm_recent_180d, dual_confirm_new_entry, dual_confirm_increase,
             industry_tailwind_score, stage_industry_adjust_raw, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_date,
            stock_code,
            sw_level1,
            sw_level2,
            sector.get("momentum_score"),
            sector.get("trend_state"),
            int(sector.get("macd_cross") or 0),
            sector.get("return_1m"),
            sector.get("return_3m"),
            sector.get("return_6m"),
            sector.get("return_12m"),
            sector.get("excess_1m"),
            sector.get("excess_3m"),
            sector.get("excess_6m"),
            sector.get("excess_12m"),
            sector.get("rotation_score"),
            sector.get("rotation_rank"),
            sector.get("rotation_rank_1m"),
            sector.get("rotation_rank_3m"),
            sector.get("rotation_bucket"),
            int(sector.get("rotation_blacklisted") or 0),
            int(dual.get("dual_confirm_total") or 0),
            int(dual.get("dual_confirm_recent_180d") or 0),
            int(dual.get("dual_confirm_new_entry") or 0),
            int(dual.get("dual_confirm_increase") or 0),
            tailwind_score,
            stage_adjust,
            now,
        ))
        inserted += 1

    conn.execute("DELETE FROM dim_stock_industry_context_latest")
    conn.execute("""
        INSERT INTO dim_stock_industry_context_latest (
            stock_code, snapshot_date, sw_level1, sw_level2, sector_momentum_score,
            sector_trend_state, sector_macd_cross, sector_return_1m, sector_return_3m,
            sector_return_6m, sector_return_12m, sector_excess_1m, sector_excess_3m,
            sector_excess_6m, sector_excess_12m, sector_rotation_score,
            sector_rotation_rank, sector_rotation_rank_1m, sector_rotation_rank_3m,
            sector_rotation_bucket, sector_rotation_blacklisted, dual_confirm_total,
            dual_confirm_recent_180d, dual_confirm_new_entry, dual_confirm_increase,
            industry_tailwind_score, stage_industry_adjust_raw, updated_at
        )
        SELECT stock_code, snapshot_date, sw_level1, sw_level2, sector_momentum_score,
               sector_trend_state, sector_macd_cross, sector_return_1m, sector_return_3m,
               sector_return_6m, sector_return_12m, sector_excess_1m, sector_excess_3m,
               sector_excess_6m, sector_excess_12m, sector_rotation_score,
               sector_rotation_rank, sector_rotation_rank_1m, sector_rotation_rank_3m,
               sector_rotation_bucket, sector_rotation_blacklisted, dual_confirm_total,
               dual_confirm_recent_180d, dual_confirm_new_entry, dual_confirm_increase,
               industry_tailwind_score, stage_industry_adjust_raw, updated_at
        FROM fact_stock_industry_context
        WHERE snapshot_date = ?
    """, (snapshot_date,))
    conn.commit()
    logger.info(f"[行业上下文] 构建完成: {inserted} 只股票, 快照 {snapshot_date}")
    return inserted

"""
选股筛选 API 路由

提供 TDX 选股公式结果、板块动量、双重确认信号的查询接口。
数据来源于 screening_engine.py / sector_momentum.py 的计算结果。
"""

from datetime import date, timedelta

from fastapi import APIRouter, Query
from services.db import get_conn
from services.industry import industry_join_clause

router = APIRouter()


@router.get("/sector-momentum")
async def get_sector_momentum():
    """板块动量状态"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM mart_sector_momentum ORDER BY momentum_score DESC"
        ).fetchall()
        return {"ok": True, "count": len(rows), "data": [dict(r) for r in rows]}
    except Exception:
        return {"ok": True, "count": 0, "data": []}
    finally:
        conn.close()


@router.get("/dual-confirm")
async def get_dual_confirm(hits_only: bool = Query(True)):
    """双重确认信号"""
    conn = get_conn()
    try:
        where = "WHERE dual_confirm = 1" if hits_only else ""
        rows = conn.execute(
            f"SELECT * FROM mart_dual_confirm {where} ORDER BY report_date DESC LIMIT 500"
        ).fetchall()
        return {"ok": True, "count": len(rows), "data": [dict(r) for r in rows]}
    except Exception:
        return {"ok": True, "count": 0, "data": []}
    finally:
        conn.close()


@router.get("/results")
async def get_results(
    formula: str = Query(None, description="按公式过滤: f1/f3/f5"),
    hits_only: bool = Query(False, description="只返回命中的"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0),
):
    """选股结果列表"""
    conn = get_conn()
    try:
        conditions = []
        params = []

        if formula in ("f1", "f3", "f5"):
            conditions.append(f"{formula}_hit = 1")

        if hits_only:
            conditions.append("hit_count > 0")

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        rows = conn.execute(
            f"SELECT * FROM mart_stock_screening {where} "
            f"ORDER BY hit_count DESC, stock_code LIMIT ? OFFSET ?",
            params + [limit, offset]
        ).fetchall()

        total = conn.execute(
            f"SELECT COUNT(*) FROM mart_stock_screening {where}",
            params
        ).fetchone()[0]

        return {
            "ok": True,
            "total": total,
            "count": len(rows),
            "data": [dict(r) for r in rows],
        }
    finally:
        conn.close()


@router.get("/detail/{stock_code}")
async def get_detail(stock_code: str):
    """单股选股详细分解"""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM mart_stock_screening WHERE stock_code = ?",
            (stock_code,)
        ).fetchone()
        if not row:
            return {"ok": False, "message": "无数据"}
        return {"ok": True, "data": dict(row)}
    finally:
        conn.close()


@router.get("/industry-overview")
async def get_industry_overview(topn: int = Query(3, ge=1, le=10)):
    """行业研究背景视图。

    面向前端行业页，统一输出：
    - 行业动量 / 相对强弱
    - 当前机构活跃度
    - 最近新进入与买入信号
    - 候选股票质量 / 阶段 / 综合分分布
    - 每个行业前排候选股票
    """
    conn = get_conn()
    try:
        sector_rows = conn.execute("""
            SELECT sector_name, sector_code, trend_state, macd_cross, momentum_score,
                   return_1m, return_3m, return_6m, return_12m,
                   excess_1m, excess_3m, excess_6m, excess_12m,
                   rotation_score, rotation_rank, rotation_rank_1m, rotation_rank_3m,
                   rotation_bucket, rotation_blacklisted
            FROM mart_sector_momentum
            ORDER BY momentum_score DESC, sector_name
        """).fetchall()
        sector_map = {row["sector_name"]: dict(row) for row in sector_rows}

        active_map = {}
        try:
            rows = conn.execute("""
                SELECT sw_level1 AS sector_name,
                       COUNT(DISTINCT institution_id) AS active_institution_count,
                       COUNT(DISTINCT stock_code) AS current_stock_count
                FROM mart_current_relationship
                WHERE sw_level1 IS NOT NULL AND sw_level1 != ''
                GROUP BY sw_level1
            """).fetchall()
            active_map = {row["sector_name"]: dict(row) for row in rows}
        except Exception:
            active_map = {}

        candidate_map = {}
        try:
            rows = conn.execute("""
                SELECT ctx.sw_level1 AS sector_name,
                       COUNT(*) AS candidate_count,
                       AVG(t.discovery_score) AS avg_discovery_score,
                       AVG(t.company_quality_score) AS avg_quality_score,
                       AVG(t.stage_score) AS avg_stage_score,
                       AVG(t.composite_priority_score) AS avg_composite_score,
                       SUM(CASE WHEN t.price_20d_pct IS NOT NULL THEN 1 ELSE 0 END) AS feedback_20d_count,
                       AVG(CASE WHEN t.price_20d_pct IS NOT NULL THEN t.price_20d_pct END) AS avg_price_20d_pct,
                       AVG(CASE WHEN t.price_20d_pct IS NOT NULL AND t.price_20d_pct > 0 THEN 1.0
                                WHEN t.price_20d_pct IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS win_rate_20d,
                       SUM(CASE WHEN t.priority_pool = 'A池' THEN 1 ELSE 0 END) AS a_pool_count,
                       SUM(CASE WHEN t.priority_pool = 'B池' THEN 1 ELSE 0 END) AS b_pool_count,
                       SUM(CASE WHEN t.priority_pool = 'C池' THEN 1 ELSE 0 END) AS c_pool_count,
                       SUM(CASE WHEN t.priority_pool = 'D池' THEN 1 ELSE 0 END) AS d_pool_count,
                       SUM(CASE WHEN t.priority_pool IN ('A池', 'B池') AND t.price_20d_pct IS NOT NULL THEN 1 ELSE 0 END) AS ab_feedback_20d_count,
                       AVG(CASE WHEN t.priority_pool IN ('A池', 'B池') AND t.price_20d_pct IS NOT NULL THEN t.price_20d_pct END) AS ab_avg_price_20d_pct,
                       AVG(CASE WHEN t.priority_pool IN ('A池', 'B池') AND t.price_20d_pct IS NOT NULL AND t.price_20d_pct > 0 THEN 1.0
                                WHEN t.priority_pool IN ('A池', 'B池') AND t.price_20d_pct IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS ab_win_rate_20d,
                       SUM(CASE WHEN t.priority_pool = 'A池' AND t.price_20d_pct IS NOT NULL THEN 1 ELSE 0 END) AS a_feedback_20d_count,
                       AVG(CASE WHEN t.priority_pool = 'A池' AND t.price_20d_pct IS NOT NULL THEN t.price_20d_pct END) AS a_avg_price_20d_pct,
                       AVG(CASE WHEN t.priority_pool = 'A池' AND t.price_20d_pct IS NOT NULL AND t.price_20d_pct > 0 THEN 1.0
                                WHEN t.priority_pool = 'A池' AND t.price_20d_pct IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS a_win_rate_20d,
                       SUM(CASE WHEN t.setup_tag IS NOT NULL THEN 1 ELSE 0 END) AS setup_candidate_count,
                       SUM(CASE WHEN t.company_quality_score >= 80 THEN 1 ELSE 0 END) AS quality_strong_count,
                       SUM(CASE WHEN t.stage_score >= 80 THEN 1 ELSE 0 END) AS stage_strong_count,
                       SUM(CASE WHEN COALESCE(t.company_quality_score, -1) >= 80 THEN 1 ELSE 0 END) AS quality_band_80_plus,
                       SUM(CASE WHEN COALESCE(t.company_quality_score, -1) >= 65 AND COALESCE(t.company_quality_score, -1) < 80 THEN 1 ELSE 0 END) AS quality_band_65_80,
                       SUM(CASE WHEN COALESCE(t.company_quality_score, -1) >= 50 AND COALESCE(t.company_quality_score, -1) < 65 THEN 1 ELSE 0 END) AS quality_band_50_65,
                       SUM(CASE WHEN COALESCE(t.company_quality_score, -1) < 50 THEN 1 ELSE 0 END) AS quality_band_below_50,
                       SUM(CASE WHEN COALESCE(t.stage_score, -1) >= 80 THEN 1 ELSE 0 END) AS stage_band_80_plus,
                       SUM(CASE WHEN COALESCE(t.stage_score, -1) >= 60 AND COALESCE(t.stage_score, -1) < 80 THEN 1 ELSE 0 END) AS stage_band_60_80,
                       SUM(CASE WHEN COALESCE(t.stage_score, -1) >= 40 AND COALESCE(t.stage_score, -1) < 60 THEN 1 ELSE 0 END) AS stage_band_40_60,
                       SUM(CASE WHEN COALESCE(t.stage_score, -1) < 40 THEN 1 ELSE 0 END) AS stage_band_below_40,
                       SUM(CASE WHEN COALESCE(t.composite_priority_score, -1) >= 75 THEN 1 ELSE 0 END) AS composite_band_75_plus,
                       SUM(CASE WHEN COALESCE(t.composite_priority_score, -1) >= 60 AND COALESCE(t.composite_priority_score, -1) < 75 THEN 1 ELSE 0 END) AS composite_band_60_75,
                       SUM(CASE WHEN COALESCE(t.composite_priority_score, -1) >= 45 AND COALESCE(t.composite_priority_score, -1) < 60 THEN 1 ELSE 0 END) AS composite_band_45_60,
                       SUM(CASE WHEN COALESCE(t.composite_priority_score, -1) < 45 THEN 1 ELSE 0 END) AS composite_band_below_45
                FROM mart_stock_trend t
                INNER JOIN dim_stock_industry_context_latest ctx ON ctx.stock_code = t.stock_code
                WHERE ctx.sw_level1 IS NOT NULL AND ctx.sw_level1 != ''
                GROUP BY ctx.sw_level1
            """).fetchall()
            candidate_map = {row["sector_name"]: dict(row) for row in rows}
        except Exception:
            candidate_map = {}

        snapshot_feedback_map = {}
        try:
            rows = conn.execute("""
                SELECT snapshot_sw_level1 AS sector_name,
                       COUNT(*) AS snapshot_total_count,
                       COUNT(DISTINCT snapshot_date) AS snapshot_date_count,
                       MIN(snapshot_date) AS snapshot_first_date,
                       MAX(snapshot_date) AS snapshot_last_date,
                       SUM(CASE WHEN priority_pool IS NOT NULL AND priority_pool != '' THEN 1 ELSE 0 END) AS snapshot_scored_count,
                       COUNT(DISTINCT CASE WHEN priority_pool IS NOT NULL AND priority_pool != '' THEN snapshot_date END) AS snapshot_scored_date_count,
                       SUM(CASE WHEN matured_10d = 1 AND gain_10d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_feedback_10d_count,
                       AVG(CASE WHEN matured_10d = 1 THEN gain_10d END) AS snapshot_avg_gain_10d,
                       AVG(CASE WHEN matured_10d = 1 AND gain_10d > 0 THEN 1.0
                                WHEN matured_10d = 1 AND gain_10d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_win_rate_10d,
                       SUM(CASE WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_feedback_30d_count,
                       AVG(CASE WHEN matured_30d = 1 THEN gain_30d END) AS snapshot_avg_gain_30d,
                       AVG(CASE WHEN matured_30d = 1 AND gain_30d > 0 THEN 1.0
                                WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_win_rate_30d,
                       SUM(CASE WHEN matured_60d = 1 AND gain_60d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_feedback_60d_count,
                       AVG(CASE WHEN matured_60d = 1 THEN gain_60d END) AS snapshot_avg_gain_60d,
                       AVG(CASE WHEN matured_60d = 1 AND gain_60d > 0 THEN 1.0
                                WHEN matured_60d = 1 AND gain_60d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_win_rate_60d,
                       SUM(CASE WHEN priority_pool = 'A池' AND matured_10d = 1 AND gain_10d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_a_feedback_10d_count,
                       AVG(CASE WHEN priority_pool = 'A池' AND matured_10d = 1 THEN gain_10d END) AS snapshot_a_avg_gain_10d,
                       AVG(CASE WHEN priority_pool = 'A池' AND matured_10d = 1 AND gain_10d > 0 THEN 1.0
                                WHEN priority_pool = 'A池' AND matured_10d = 1 AND gain_10d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_a_win_rate_10d,
                       SUM(CASE WHEN priority_pool IN ('A池', 'B池') AND matured_30d = 1 AND gain_30d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_ab_feedback_30d_count,
                       AVG(CASE WHEN priority_pool IN ('A池', 'B池') AND matured_30d = 1 THEN gain_30d END) AS snapshot_ab_avg_gain_30d,
                       AVG(CASE WHEN priority_pool IN ('A池', 'B池') AND matured_30d = 1 AND gain_30d > 0 THEN 1.0
                                WHEN priority_pool IN ('A池', 'B池') AND matured_30d = 1 AND gain_30d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_ab_win_rate_30d,
                       SUM(CASE WHEN priority_pool = 'A池' AND matured_30d = 1 AND gain_30d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_a_feedback_30d_count,
                       AVG(CASE WHEN priority_pool = 'A池' AND matured_30d = 1 THEN gain_30d END) AS snapshot_a_avg_gain_30d,
                       AVG(CASE WHEN priority_pool = 'A池' AND matured_30d = 1 AND gain_30d > 0 THEN 1.0
                                WHEN priority_pool = 'A池' AND matured_30d = 1 AND gain_30d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_a_win_rate_30d,
                       SUM(CASE WHEN priority_pool = 'A池' AND matured_60d = 1 AND gain_60d IS NOT NULL THEN 1 ELSE 0 END) AS snapshot_a_feedback_60d_count,
                       AVG(CASE WHEN priority_pool = 'A池' AND matured_60d = 1 THEN gain_60d END) AS snapshot_a_avg_gain_60d,
                       AVG(CASE WHEN priority_pool = 'A池' AND matured_60d = 1 AND gain_60d > 0 THEN 1.0
                                WHEN priority_pool = 'A池' AND matured_60d = 1 AND gain_60d IS NOT NULL THEN 0.0
                                ELSE NULL END) * 100 AS snapshot_a_win_rate_60d
                FROM fact_setup_snapshot
                WHERE snapshot_sw_level1 IS NOT NULL AND snapshot_sw_level1 != ''
                GROUP BY snapshot_sw_level1
            """).fetchall()
            snapshot_feedback_map = {row["sector_name"]: dict(row) for row in rows}
        except Exception:
            snapshot_feedback_map = {}

        context_map = {}
        try:
            rows = conn.execute("""
                SELECT sw_level1 AS sector_name,
                       AVG(industry_tailwind_score) AS avg_tailwind_score,
                       SUM(CASE WHEN dual_confirm_recent_180d > 0 THEN 1 ELSE 0 END) AS dual_confirm_stock_count,
                       SUM(dual_confirm_recent_180d) AS dual_confirm_signal_count
                FROM dim_stock_industry_context_latest
                WHERE sw_level1 IS NOT NULL AND sw_level1 != ''
                GROUP BY sw_level1
            """).fetchall()
            context_map = {row["sector_name"]: dict(row) for row in rows}
        except Exception:
            context_map = {}

        cutoff = (date.today() - timedelta(days=120)).strftime("%Y%m%d")
        recent_event_map = {}
        try:
            rows = conn.execute(f"""
                SELECT industry_dim.sw_level1 AS sector_name,
                       SUM(CASE WHEN e.event_type = 'new_entry' THEN 1 ELSE 0 END) AS recent_new_entry_count,
                       COUNT(DISTINCT CASE WHEN e.event_type = 'new_entry' THEN e.stock_code END) AS recent_new_entry_stock_count,
                       SUM(CASE WHEN e.event_type IN ('new_entry', 'increase') THEN 1 ELSE 0 END) AS recent_buy_signal_count,
                       COUNT(DISTINCT CASE WHEN e.event_type IN ('new_entry', 'increase') THEN e.stock_code END) AS recent_buy_signal_stock_count
                FROM fact_institution_event e
                {industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER")}
                WHERE industry_dim.sw_level1 IS NOT NULL
                  AND industry_dim.sw_level1 != ''
                  AND COALESCE(NULLIF(REPLACE(e.notice_date, '-', ''), ''), REPLACE(e.report_date, '-', '')) >= ?
                GROUP BY industry_dim.sw_level1
            """, (cutoff,)).fetchall()
            recent_event_map = {row["sector_name"]: dict(row) for row in rows}
        except Exception:
            recent_event_map = {}

        top_stock_map = {}
        try:
            rows = conn.execute("""
                SELECT sector_name, stock_code, stock_name, stock_archetype, priority_pool,
                       composite_priority_score, company_quality_score, stage_score, setup_tag
                FROM (
                    SELECT ctx.sw_level1 AS sector_name,
                           t.stock_code,
                           t.stock_name,
                           t.stock_archetype,
                           t.priority_pool,
                           t.composite_priority_score,
                           t.company_quality_score,
                           t.stage_score,
                           t.setup_tag,
                           ROW_NUMBER() OVER (
                               PARTITION BY ctx.sw_level1
                               ORDER BY
                                   CASE COALESCE(t.priority_pool, '')
                                       WHEN 'A池' THEN 0
                                       WHEN 'B池' THEN 1
                                       WHEN 'C池' THEN 2
                                       WHEN 'D池' THEN 3
                                       ELSE 9
                                   END,
                                   COALESCE(t.composite_priority_score, 0) DESC,
                                   t.stock_code
                           ) AS rn
                    FROM mart_stock_trend t
                    INNER JOIN dim_stock_industry_context_latest ctx ON ctx.stock_code = t.stock_code
                    WHERE ctx.sw_level1 IS NOT NULL AND ctx.sw_level1 != ''
                )
                WHERE rn <= ?
                ORDER BY sector_name, rn
            """, (topn,)).fetchall()
            for row in rows:
                top_stock_map.setdefault(row["sector_name"], []).append(dict(row))
        except Exception:
            top_stock_map = {}

        sectors = sorted(
            set(sector_map.keys())
            | set(active_map.keys())
            | set(candidate_map.keys())
            | set(snapshot_feedback_map.keys())
            | set(context_map.keys())
            | set(recent_event_map.keys())
        )

        data = []
        strongest_sector = None
        strongest_momentum = None
        for sector_name in sectors:
            sector = sector_map.get(sector_name) or {}
            active = active_map.get(sector_name) or {}
            candidate = candidate_map.get(sector_name) or {}
            snapshot_feedback = snapshot_feedback_map.get(sector_name) or {}
            context = context_map.get(sector_name) or {}
            recent = recent_event_map.get(sector_name) or {}

            item = {
                "sector_name": sector_name,
                "sector_code": sector.get("sector_code"),
                "trend_state": sector.get("trend_state"),
                "macd_cross": sector.get("macd_cross"),
                "momentum_score": sector.get("momentum_score"),
                "return_1m": sector.get("return_1m"),
                "return_3m": sector.get("return_3m"),
                "return_6m": sector.get("return_6m"),
                "return_12m": sector.get("return_12m"),
                "excess_1m": sector.get("excess_1m"),
                "excess_3m": sector.get("excess_3m"),
                "excess_6m": sector.get("excess_6m"),
                "excess_12m": sector.get("excess_12m"),
                "rotation_score": sector.get("rotation_score"),
                "rotation_rank": sector.get("rotation_rank"),
                "rotation_rank_1m": sector.get("rotation_rank_1m"),
                "rotation_rank_3m": sector.get("rotation_rank_3m"),
                "rotation_bucket": sector.get("rotation_bucket"),
                "rotation_blacklisted": sector.get("rotation_blacklisted", 0),
                "active_institution_count": active.get("active_institution_count", 0),
                "current_stock_count": active.get("current_stock_count", 0),
                "recent_new_entry_count": recent.get("recent_new_entry_count", 0),
                "recent_new_entry_stock_count": recent.get("recent_new_entry_stock_count", 0),
                "recent_buy_signal_count": recent.get("recent_buy_signal_count", 0),
                "recent_buy_signal_stock_count": recent.get("recent_buy_signal_stock_count", 0),
                "candidate_count": candidate.get("candidate_count", 0),
                "feedback_20d_count": candidate.get("feedback_20d_count", 0),
                "avg_price_20d_pct": candidate.get("avg_price_20d_pct"),
                "win_rate_20d": candidate.get("win_rate_20d"),
                "snapshot_total_count": snapshot_feedback.get("snapshot_total_count", 0),
                "snapshot_date_count": snapshot_feedback.get("snapshot_date_count", 0),
                "snapshot_first_date": snapshot_feedback.get("snapshot_first_date"),
                "snapshot_last_date": snapshot_feedback.get("snapshot_last_date"),
                "snapshot_scored_count": snapshot_feedback.get("snapshot_scored_count", 0),
                "snapshot_scored_date_count": snapshot_feedback.get("snapshot_scored_date_count", 0),
                "snapshot_feedback_10d_count": snapshot_feedback.get("snapshot_feedback_10d_count", 0),
                "snapshot_avg_gain_10d": snapshot_feedback.get("snapshot_avg_gain_10d"),
                "snapshot_win_rate_10d": snapshot_feedback.get("snapshot_win_rate_10d"),
                "snapshot_feedback_30d_count": snapshot_feedback.get("snapshot_feedback_30d_count", 0),
                "snapshot_avg_gain_30d": snapshot_feedback.get("snapshot_avg_gain_30d"),
                "snapshot_win_rate_30d": snapshot_feedback.get("snapshot_win_rate_30d"),
                "snapshot_feedback_60d_count": snapshot_feedback.get("snapshot_feedback_60d_count", 0),
                "snapshot_avg_gain_60d": snapshot_feedback.get("snapshot_avg_gain_60d"),
                "snapshot_win_rate_60d": snapshot_feedback.get("snapshot_win_rate_60d"),
                "snapshot_a_feedback_10d_count": snapshot_feedback.get("snapshot_a_feedback_10d_count", 0),
                "snapshot_a_avg_gain_10d": snapshot_feedback.get("snapshot_a_avg_gain_10d"),
                "snapshot_a_win_rate_10d": snapshot_feedback.get("snapshot_a_win_rate_10d"),
                "snapshot_ab_feedback_30d_count": snapshot_feedback.get("snapshot_ab_feedback_30d_count", 0),
                "snapshot_ab_avg_gain_30d": snapshot_feedback.get("snapshot_ab_avg_gain_30d"),
                "snapshot_ab_win_rate_30d": snapshot_feedback.get("snapshot_ab_win_rate_30d"),
                "snapshot_a_feedback_30d_count": snapshot_feedback.get("snapshot_a_feedback_30d_count", 0),
                "snapshot_a_avg_gain_30d": snapshot_feedback.get("snapshot_a_avg_gain_30d"),
                "snapshot_a_win_rate_30d": snapshot_feedback.get("snapshot_a_win_rate_30d"),
                "snapshot_a_feedback_60d_count": snapshot_feedback.get("snapshot_a_feedback_60d_count", 0),
                "snapshot_a_avg_gain_60d": snapshot_feedback.get("snapshot_a_avg_gain_60d"),
                "snapshot_a_win_rate_60d": snapshot_feedback.get("snapshot_a_win_rate_60d"),
                "setup_candidate_count": candidate.get("setup_candidate_count", 0),
                "a_pool_count": candidate.get("a_pool_count", 0),
                "b_pool_count": candidate.get("b_pool_count", 0),
                "c_pool_count": candidate.get("c_pool_count", 0),
                "d_pool_count": candidate.get("d_pool_count", 0),
                "ab_feedback_20d_count": candidate.get("ab_feedback_20d_count", 0),
                "ab_avg_price_20d_pct": candidate.get("ab_avg_price_20d_pct"),
                "ab_win_rate_20d": candidate.get("ab_win_rate_20d"),
                "a_feedback_20d_count": candidate.get("a_feedback_20d_count", 0),
                "a_avg_price_20d_pct": candidate.get("a_avg_price_20d_pct"),
                "a_win_rate_20d": candidate.get("a_win_rate_20d"),
                "quality_strong_count": candidate.get("quality_strong_count", 0),
                "stage_strong_count": candidate.get("stage_strong_count", 0),
                "avg_discovery_score": candidate.get("avg_discovery_score"),
                "avg_quality_score": candidate.get("avg_quality_score"),
                "avg_stage_score": candidate.get("avg_stage_score"),
                "avg_composite_score": candidate.get("avg_composite_score"),
                "quality_band_80_plus": candidate.get("quality_band_80_plus", 0),
                "quality_band_65_80": candidate.get("quality_band_65_80", 0),
                "quality_band_50_65": candidate.get("quality_band_50_65", 0),
                "quality_band_below_50": candidate.get("quality_band_below_50", 0),
                "stage_band_80_plus": candidate.get("stage_band_80_plus", 0),
                "stage_band_60_80": candidate.get("stage_band_60_80", 0),
                "stage_band_40_60": candidate.get("stage_band_40_60", 0),
                "stage_band_below_40": candidate.get("stage_band_below_40", 0),
                "composite_band_75_plus": candidate.get("composite_band_75_plus", 0),
                "composite_band_60_75": candidate.get("composite_band_60_75", 0),
                "composite_band_45_60": candidate.get("composite_band_45_60", 0),
                "composite_band_below_45": candidate.get("composite_band_below_45", 0),
                "avg_tailwind_score": context.get("avg_tailwind_score"),
                "dual_confirm_stock_count": context.get("dual_confirm_stock_count", 0),
                "dual_confirm_signal_count": context.get("dual_confirm_signal_count", 0),
                "top_stocks": top_stock_map.get(sector_name, []),
            }
            data.append(item)

            momentum = item.get("momentum_score")
            if momentum is not None and (strongest_momentum is None or momentum > strongest_momentum):
                strongest_momentum = momentum
                strongest_sector = sector_name

        data.sort(
            key=lambda item: (
                -(item.get("a_pool_count") or 0),
                -(item.get("avg_composite_score") or 0),
                -(item.get("momentum_score") or 0),
                item.get("sector_name") or "",
            )
        )

        summary = {
            "sector_count": len(data),
            "strongest_sector": strongest_sector,
            "a_pool_total": sum(item.get("a_pool_count") or 0 for item in data),
            "setup_total": sum(item.get("setup_candidate_count") or 0 for item in data),
            "dual_confirm_total": sum(item.get("dual_confirm_stock_count") or 0 for item in data),
            "positive_trend_count": sum(1 for item in data if item.get("trend_state") in ("bullish", "recovering")),
            "feedback_ready_total": sum(item.get("feedback_20d_count") or 0 for item in data),
            "snapshot_feedback_ready_10d_total": sum(item.get("snapshot_feedback_10d_count") or 0 for item in data),
            "snapshot_feedback_ready_total": sum(item.get("snapshot_feedback_30d_count") or 0 for item in data),
            "snapshot_feedback_ready_60d_total": sum(item.get("snapshot_feedback_60d_count") or 0 for item in data),
            "snapshot_feedback_sector_count": sum(1 for item in data if (item.get("snapshot_feedback_30d_count") or 0) > 0),
        }
        return {"ok": True, "count": len(data), "summary": summary, "data": data}
    finally:
        conn.close()


@router.get("/summary")
async def get_summary():
    """命中统计汇总"""
    conn = get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM mart_stock_screening").fetchone()[0]
        f1 = conn.execute("SELECT COUNT(*) FROM mart_stock_screening WHERE f1_hit = 1").fetchone()[0]
        f3 = conn.execute("SELECT COUNT(*) FROM mart_stock_screening WHERE f3_hit = 1").fetchone()[0]
        f5 = conn.execute("SELECT COUNT(*) FROM mart_stock_screening WHERE f5_hit = 1").fetchone()[0]
        any_hit = conn.execute("SELECT COUNT(*) FROM mart_stock_screening WHERE hit_count > 0").fetchone()[0]

        screen_date = None
        row = conn.execute(
            "SELECT screen_date FROM mart_stock_screening LIMIT 1"
        ).fetchone()
        if row:
            screen_date = row["screen_date"]

        return {
            "ok": True,
            "screen_date": screen_date,
            "total_stocks": total,
            "f1_hits": f1,
            "f3_hits": f3,
            "f5_hits": f5,
            "any_hit": any_hit,
        }
    finally:
        conn.close()

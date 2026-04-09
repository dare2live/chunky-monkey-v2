"""
stock_validation.py

四层股票评分体系的验证报告：
- 当前分池结构与近20日反馈
- 新旧排序对比
- 股票级异常项
- 数据审计摘要
"""

from __future__ import annotations

from datetime import datetime

from services.audit import run_quality_audit
from services.qlib_full_engine import get_model_summary


def _safe_round(value, digits: int = 2):
    if value is None:
        return None
    return round(float(value), digits)


def _pool_order(value: str) -> int:
    return {
        "A池": 0,
        "B池": 1,
        "C池": 2,
        "D池": 3,
    }.get(value or "", 9)


def _serialize_rows(rows, fields: list[str]) -> list[dict]:
    result = []
    for row in rows:
        item = {}
        for field in fields:
            value = row[field]
            if isinstance(value, float):
                value = _safe_round(value)
            item[field] = value
        result.append(item)
    return result


def _normalize_sector(sector: str | None) -> str | None:
    value = str(sector or "").strip()
    return value or None


def _sector_exists_clause(alias: str, sector: str | None, *, snapshot_level1_col: str | None = None) -> tuple[str, tuple]:
    normalized = _normalize_sector(sector)
    if not normalized:
        return "", ()
    if snapshot_level1_col:
        return (
            f"""
              AND (
                  (COALESCE({alias}.{snapshot_level1_col}, '') != '' AND {alias}.{snapshot_level1_col} = ?)
                  OR (
                      COALESCE({alias}.{snapshot_level1_col}, '') = ''
                      AND EXISTS (
                          SELECT 1
                          FROM dim_stock_industry sector_ctx
                          WHERE sector_ctx.stock_code = {alias}.stock_code
                            AND sector_ctx.sw_level1 = ?
                      )
                  )
              )
            """,
            (normalized, normalized),
        )
    return (
        f"""
          AND EXISTS (
              SELECT 1
              FROM dim_stock_industry_context_latest sector_ctx
              WHERE sector_ctx.stock_code = {alias}.stock_code
                AND sector_ctx.sw_level1 = ?
          )
        """,
        (normalized,),
    )


def _load_pool_feedback(conn, sector: str | None = None) -> list[dict]:
    sector_clause, sector_params = _sector_exists_clause("t", sector)
    rows = conn.execute(
        f"""
        SELECT COALESCE(priority_pool, '未分池') AS priority_pool,
               COUNT(*) AS total,
               SUM(CASE WHEN setup_tag IS NOT NULL THEN 1 ELSE 0 END) AS setup_count,
               SUM(CASE WHEN composite_cap_reason IS NOT NULL AND composite_cap_reason != '' THEN 1 ELSE 0 END) AS capped_count,
               AVG(discovery_score) AS avg_discovery_score,
               AVG(company_quality_score) AS avg_quality_score,
               AVG(stage_score) AS avg_stage_score,
               AVG(forecast_score) AS avg_forecast_score,
               AVG(composite_priority_score) AS avg_composite_score,
               AVG(price_20d_pct) AS avg_price_20d_pct
        FROM mart_stock_trend t
        WHERE 1 = 1
        {sector_clause}
        GROUP BY COALESCE(priority_pool, '未分池')
        ORDER BY
            CASE COALESCE(priority_pool, '未分池')
                WHEN 'A池' THEN 0
                WHEN 'B池' THEN 1
                WHEN 'C池' THEN 2
                WHEN 'D池' THEN 3
                ELSE 9
            END
        """,
        sector_params,
    ).fetchall()
    return _serialize_rows(
        rows,
        [
            "priority_pool",
            "total",
            "setup_count",
            "capped_count",
            "avg_discovery_score",
            "avg_quality_score",
            "avg_stage_score",
            "avg_forecast_score",
            "avg_composite_score",
            "avg_price_20d_pct",
        ],
    )


def _load_snapshot_pool_replay(conn, sector: str | None = None) -> dict:
    sector_clause, sector_params = _sector_exists_clause("s", sector, snapshot_level1_col="snapshot_sw_level1")
    coverage_row = conn.execute(
        f"""
        SELECT COUNT(*) AS total_rows,
               SUM(CASE WHEN priority_pool IS NOT NULL AND priority_pool != '' THEN 1 ELSE 0 END) AS scored_rows,
               COUNT(DISTINCT snapshot_date) AS snapshot_dates,
               COUNT(DISTINCT CASE WHEN priority_pool IS NOT NULL AND priority_pool != '' THEN snapshot_date END) AS scored_snapshot_dates,
               MIN(CASE WHEN priority_pool IS NOT NULL AND priority_pool != '' THEN snapshot_date END) AS first_scored_snapshot_date,
               MAX(CASE WHEN priority_pool IS NOT NULL AND priority_pool != '' THEN snapshot_date END) AS last_scored_snapshot_date
        FROM fact_setup_snapshot s
        WHERE 1 = 1
        {sector_clause}
        """,
        sector_params,
    ).fetchone()

    baseline_row = conn.execute(
        f"""
        SELECT SUM(CASE WHEN matured_10d = 1 AND gain_10d IS NOT NULL THEN 1 ELSE 0 END) AS matured_10d_count,
               AVG(CASE WHEN matured_10d = 1 THEN gain_10d END) AS avg_gain_10d,
               AVG(CASE WHEN matured_10d = 1 AND gain_10d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_10d,
               AVG(CASE WHEN matured_10d = 1 THEN max_drawdown_10d END) AS avg_drawdown_10d,
               SUM(CASE WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN 1 ELSE 0 END) AS matured_30d_count,
               AVG(CASE WHEN matured_30d = 1 THEN gain_30d END) AS avg_gain_30d,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_30d,
               AVG(CASE WHEN matured_30d = 1 THEN max_drawdown_30d END) AS avg_drawdown_30d,
               SUM(CASE WHEN matured_60d = 1 AND gain_60d IS NOT NULL THEN 1 ELSE 0 END) AS matured_60d_count,
               AVG(CASE WHEN matured_60d = 1 THEN gain_60d END) AS avg_gain_60d,
               AVG(CASE WHEN matured_60d = 1 AND gain_60d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_60d,
               AVG(CASE WHEN matured_60d = 1 THEN max_drawdown_60d END) AS avg_drawdown_60d
        FROM fact_setup_snapshot s
        WHERE priority_pool IS NOT NULL AND priority_pool != ''
        {sector_clause}
        """,
        sector_params,
    ).fetchone()

    rows = conn.execute(
        f"""
        SELECT priority_pool,
               COUNT(*) AS total,
               COUNT(DISTINCT snapshot_date) AS snapshot_days,
               AVG(composite_priority_score) AS avg_composite_score,
               SUM(CASE WHEN matured_10d = 1 AND gain_10d IS NOT NULL THEN 1 ELSE 0 END) AS matured_10d_count,
               AVG(CASE WHEN matured_10d = 1 THEN gain_10d END) AS avg_gain_10d,
               AVG(CASE WHEN matured_10d = 1 AND gain_10d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_10d,
               AVG(CASE WHEN matured_10d = 1 THEN max_drawdown_10d END) AS avg_drawdown_10d,
               SUM(CASE WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN 1 ELSE 0 END) AS matured_30d_count,
               AVG(CASE WHEN matured_30d = 1 THEN gain_30d END) AS avg_gain_30d,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_30d,
               AVG(CASE WHEN matured_30d = 1 THEN max_drawdown_30d END) AS avg_drawdown_30d,
               SUM(CASE WHEN matured_60d = 1 AND gain_60d IS NOT NULL THEN 1 ELSE 0 END) AS matured_60d_count,
               AVG(CASE WHEN matured_60d = 1 THEN gain_60d END) AS avg_gain_60d,
               AVG(CASE WHEN matured_60d = 1 AND gain_60d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_60d,
               AVG(CASE WHEN matured_60d = 1 THEN max_drawdown_60d END) AS avg_drawdown_60d
        FROM fact_setup_snapshot s
        WHERE priority_pool IS NOT NULL AND priority_pool != ''
        {sector_clause}
        GROUP BY priority_pool
        ORDER BY
            CASE priority_pool
                WHEN 'A池' THEN 0
                WHEN 'B池' THEN 1
                WHEN 'C池' THEN 2
                WHEN 'D池' THEN 3
                ELSE 9
            END
        """,
        sector_params,
    ).fetchall()

    history_rows = conn.execute(
        f"""
        SELECT snapshot_date,
               priority_pool,
               COUNT(*) AS total,
               AVG(composite_priority_score) AS avg_composite_score,
               SUM(CASE WHEN matured_30d = 1 AND gain_30d IS NOT NULL THEN 1 ELSE 0 END) AS matured_30d_count,
               AVG(CASE WHEN matured_30d = 1 THEN gain_30d END) AS avg_gain_30d,
               AVG(CASE WHEN matured_30d = 1 AND gain_30d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_30d
        FROM fact_setup_snapshot s
        WHERE priority_pool IS NOT NULL AND priority_pool != ''
        {sector_clause}
        GROUP BY snapshot_date, priority_pool
        ORDER BY snapshot_date DESC,
                 CASE priority_pool
                     WHEN 'A池' THEN 0
                     WHEN 'B池' THEN 1
                     WHEN 'C池' THEN 2
                     WHEN 'D池' THEN 3
                     ELSE 9
                 END
        LIMIT 48
        """,
        sector_params,
    ).fetchall()

    fields = [
        "priority_pool",
        "total",
        "snapshot_days",
        "avg_composite_score",
        "matured_10d_count",
        "avg_gain_10d",
        "win_rate_10d",
        "avg_drawdown_10d",
        "matured_30d_count",
        "avg_gain_30d",
        "win_rate_30d",
        "avg_drawdown_30d",
        "matured_60d_count",
        "avg_gain_60d",
        "win_rate_60d",
        "avg_drawdown_60d",
        "uplift_vs_baseline_30d",
        "uplift_vs_baseline_60d",
    ]
    history_fields = [
        "snapshot_date",
        "priority_pool",
        "total",
        "avg_composite_score",
        "matured_30d_count",
        "avg_gain_30d",
        "win_rate_30d",
    ]
    coverage = {
        "total_rows": int(coverage_row["total_rows"] or 0),
        "scored_rows": int(coverage_row["scored_rows"] or 0),
        "snapshot_dates": int(coverage_row["snapshot_dates"] or 0),
        "scored_snapshot_dates": int(coverage_row["scored_snapshot_dates"] or 0),
        "first_scored_snapshot_date": coverage_row["first_scored_snapshot_date"],
        "last_scored_snapshot_date": coverage_row["last_scored_snapshot_date"],
    }
    baseline = {
        "matured_10d_count": int(baseline_row["matured_10d_count"] or 0),
        "avg_gain_10d": _safe_round(baseline_row["avg_gain_10d"]),
        "win_rate_10d": _safe_round(baseline_row["win_rate_10d"]),
        "avg_drawdown_10d": _safe_round(baseline_row["avg_drawdown_10d"]),
        "matured_30d_count": int(baseline_row["matured_30d_count"] or 0),
        "avg_gain_30d": _safe_round(baseline_row["avg_gain_30d"]),
        "win_rate_30d": _safe_round(baseline_row["win_rate_30d"]),
        "avg_drawdown_30d": _safe_round(baseline_row["avg_drawdown_30d"]),
        "matured_60d_count": int(baseline_row["matured_60d_count"] or 0),
        "avg_gain_60d": _safe_round(baseline_row["avg_gain_60d"]),
        "win_rate_60d": _safe_round(baseline_row["win_rate_60d"]),
        "avg_drawdown_60d": _safe_round(baseline_row["avg_drawdown_60d"]),
    }
    by_pool = _serialize_rows(rows, fields[:-2])
    for item in by_pool:
        item["uplift_vs_baseline_30d"] = (
            _safe_round(item["avg_gain_30d"] - baseline["avg_gain_30d"])
            if item.get("avg_gain_30d") is not None and baseline.get("avg_gain_30d") is not None
            else None
        )
        item["uplift_vs_baseline_60d"] = (
            _safe_round(item["avg_gain_60d"] - baseline["avg_gain_60d"])
            if item.get("avg_gain_60d") is not None and baseline.get("avg_gain_60d") is not None
            else None
        )
    return {
        "coverage": coverage,
        "baseline": baseline,
        "by_pool": by_pool,
        "history": _serialize_rows(history_rows, history_fields),
    }


def _load_snapshot_rank_compare(conn, sector: str | None = None) -> dict:
    sector_clause, sector_params = _sector_exists_clause("fact_setup_snapshot", sector, snapshot_level1_col="snapshot_sw_level1")
    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT snapshot_date,
                   stock_code,
                   stock_name,
                   gain_30d,
                   max_drawdown_30d,
                   ROW_NUMBER() OVER (
                       PARTITION BY snapshot_date
                       ORDER BY
                           CASE COALESCE(priority_pool, '')
                               WHEN 'A池' THEN 0
                               WHEN 'B池' THEN 1
                               WHEN 'C池' THEN 2
                               WHEN 'D池' THEN 3
                               ELSE 9
                           END,
                           COALESCE(composite_priority_score, 0) DESC,
                           stock_code
                   ) AS composite_rank,
                   ROW_NUMBER() OVER (
                       PARTITION BY snapshot_date
                       ORDER BY COALESCE(action_score, 0) DESC, stock_code
                   ) AS legacy_rank
            FROM fact_setup_snapshot
            WHERE composite_priority_score IS NOT NULL
              AND action_score IS NOT NULL
              AND matured_30d = 1
              AND gain_30d IS NOT NULL
              {sector_clause}
        )
        SELECT *
        FROM ranked
        ORDER BY snapshot_date DESC, composite_rank, legacy_rank
        """,
        sector_params,
    ).fetchall()

    if not rows:
        return {
            "summary": [],
            "history": [],
            "matured_snapshot_dates": 0,
        }

    topns = (10, 20, 50)
    method_aggs = {
        topn: {
            "composite": {"count": 0, "sum_gain": 0.0, "sum_win": 0, "sum_dd": 0.0, "snapshot_dates": set()},
            "legacy": {"count": 0, "sum_gain": 0.0, "sum_win": 0, "sum_dd": 0.0, "snapshot_dates": set()},
        }
        for topn in topns
    }
    overlap_sets = {
        topn: {}
        for topn in topns
    }
    history_aggs = {}

    for row in rows:
        item = dict(row)
        snapshot_date = item["snapshot_date"]
        history_aggs.setdefault(snapshot_date, {
            "snapshot_date": snapshot_date,
            "composite": {"count": 0, "sum_gain": 0.0, "sum_win": 0, "sum_dd": 0.0},
            "legacy": {"count": 0, "sum_gain": 0.0, "sum_win": 0, "sum_dd": 0.0},
            "top20_overlap": set(),
            "top20_composite": set(),
            "top20_legacy": set(),
        })

        gain = float(item["gain_30d"])
        drawdown = float(item["max_drawdown_30d"]) if item["max_drawdown_30d"] is not None else 0.0
        for topn in topns:
            if item["composite_rank"] <= topn:
                agg = method_aggs[topn]["composite"]
                agg["count"] += 1
                agg["sum_gain"] += gain
                agg["sum_win"] += 1 if gain > 0 else 0
                agg["sum_dd"] += drawdown
                agg["snapshot_dates"].add(snapshot_date)
                overlap_sets[topn].setdefault(snapshot_date, {"composite": set(), "legacy": set()})
                overlap_sets[topn][snapshot_date]["composite"].add(item["stock_code"])
            if item["legacy_rank"] <= topn:
                agg = method_aggs[topn]["legacy"]
                agg["count"] += 1
                agg["sum_gain"] += gain
                agg["sum_win"] += 1 if gain > 0 else 0
                agg["sum_dd"] += drawdown
                agg["snapshot_dates"].add(snapshot_date)
                overlap_sets[topn].setdefault(snapshot_date, {"composite": set(), "legacy": set()})
                overlap_sets[topn][snapshot_date]["legacy"].add(item["stock_code"])

        if item["composite_rank"] <= 20:
            agg = history_aggs[snapshot_date]["composite"]
            agg["count"] += 1
            agg["sum_gain"] += gain
            agg["sum_win"] += 1 if gain > 0 else 0
            agg["sum_dd"] += drawdown
            history_aggs[snapshot_date]["top20_composite"].add(item["stock_code"])
        if item["legacy_rank"] <= 20:
            agg = history_aggs[snapshot_date]["legacy"]
            agg["count"] += 1
            agg["sum_gain"] += gain
            agg["sum_win"] += 1 if gain > 0 else 0
            agg["sum_dd"] += drawdown
            history_aggs[snapshot_date]["top20_legacy"].add(item["stock_code"])

    summary = []
    for topn in topns:
        per_date = overlap_sets[topn]
        overlap_total = sum(
            len(values["composite"] & values["legacy"])
            for values in per_date.values()
        )
        for method in ("composite", "legacy"):
            agg = method_aggs[topn][method]
            count = agg["count"]
            summary.append({
                "topn": topn,
                "method": method,
                "sample_count": count,
                "snapshot_days": len(agg["snapshot_dates"]),
                "avg_gain_30d": _safe_round(agg["sum_gain"] / count) if count else None,
                "win_rate_30d": _safe_round(agg["sum_win"] * 100.0 / count) if count else None,
                "avg_drawdown_30d": _safe_round(agg["sum_dd"] / count) if count else None,
                "overlap_count": overlap_total if method == "composite" else None,
            })

    history = []
    for snapshot_date in sorted(history_aggs.keys(), reverse=True)[:12]:
        item = history_aggs[snapshot_date]
        composite = item["composite"]
        legacy = item["legacy"]
        history.append({
            "snapshot_date": snapshot_date,
            "composite_count": composite["count"],
            "composite_avg_gain_30d": _safe_round(composite["sum_gain"] / composite["count"]) if composite["count"] else None,
            "composite_win_rate_30d": _safe_round(composite["sum_win"] * 100.0 / composite["count"]) if composite["count"] else None,
            "legacy_count": legacy["count"],
            "legacy_avg_gain_30d": _safe_round(legacy["sum_gain"] / legacy["count"]) if legacy["count"] else None,
            "legacy_win_rate_30d": _safe_round(legacy["sum_win"] * 100.0 / legacy["count"]) if legacy["count"] else None,
            "top20_overlap": len(item["top20_composite"] & item["top20_legacy"]),
        })

    return {
        "summary": summary,
        "history": history,
        "matured_snapshot_dates": len(history_aggs),
    }


def _load_rank_compare(conn, limit: int = 120, sector: str | None = None) -> dict:
    sector_clause, sector_params = _sector_exists_clause("mart_stock_trend", sector)
    rows = conn.execute(
        f"""
        WITH ranked AS (
            SELECT stock_code,
                   stock_name,
                   priority_pool,
                   stock_archetype,
                   action_score,
                   composite_priority_score,
                   ROW_NUMBER() OVER (
                       ORDER BY
                           CASE COALESCE(priority_pool, '')
                               WHEN 'A池' THEN 0
                               WHEN 'B池' THEN 1
                               WHEN 'C池' THEN 2
                               WHEN 'D池' THEN 3
                               ELSE 9
                           END,
                           COALESCE(composite_priority_score, 0) DESC,
                           stock_code
                   ) AS composite_rank,
                   ROW_NUMBER() OVER (
                       ORDER BY COALESCE(action_score, 0) DESC, stock_code
                   ) AS legacy_rank
            FROM mart_stock_trend
            WHERE action_score IS NOT NULL OR composite_priority_score IS NOT NULL
              {sector_clause}
        )
        SELECT *
        FROM ranked
        WHERE composite_rank <= ? OR legacy_rank <= ?
        ORDER BY composite_rank, legacy_rank
        """,
        sector_params + (limit, limit),
    ).fetchall()

    items = []
    top_sets = {20: {"composite": set(), "legacy": set()}, 50: {"composite": set(), "legacy": set()}, 100: {"composite": set(), "legacy": set()}}
    for row in rows:
        item = dict(row)
        item["action_score"] = _safe_round(item.get("action_score"))
        item["composite_priority_score"] = _safe_round(item.get("composite_priority_score"))
        item["rank_delta"] = int(item["legacy_rank"] - item["composite_rank"])
        items.append(item)
        for topn in (20, 50, 100):
            if item["composite_rank"] <= topn:
                top_sets[topn]["composite"].add(item["stock_code"])
            if item["legacy_rank"] <= topn:
                top_sets[topn]["legacy"].add(item["stock_code"])

    overlap = {
        f"top{topn}": len(top_sets[topn]["composite"] & top_sets[topn]["legacy"])
        for topn in (20, 50, 100)
    }

    promoted = [
        item for item in items
        if item.get("rank_delta", 0) >= 10
        and item.get("action_score") is not None
        and item.get("composite_priority_score") is not None
        and (item["composite_rank"] <= limit or item["legacy_rank"] <= limit)
    ]
    promoted.sort(key=lambda item: (-item["rank_delta"], item["composite_rank"], item["stock_code"]))

    demoted = [
        item for item in items
        if item.get("rank_delta", 0) <= -10
        and item.get("action_score") is not None
        and item.get("composite_priority_score") is not None
        and (item["composite_rank"] <= limit or item["legacy_rank"] <= limit)
    ]
    demoted.sort(key=lambda item: (item["rank_delta"], item["legacy_rank"], item["stock_code"]))

    return {
        "overlap": overlap,
        "promoted": promoted[:12],
        "demoted": demoted[:12],
    }


def _load_anomalies(conn, sector: str | None = None) -> dict:
    sector_clause, sector_params = _sector_exists_clause("mart_stock_trend", sector)
    common_fields = [
        "stock_code",
        "stock_name",
        "priority_pool",
        "stock_archetype",
        "action_score",
        "discovery_score",
        "company_quality_score",
        "stage_score",
        "forecast_score",
        "raw_composite_priority_score",
        "composite_priority_score",
        "priority_pool_reason",
        "composite_cap_reason",
    ]

    capped_rows = conn.execute(
        f"""
        SELECT stock_code, stock_name, priority_pool, stock_archetype,
               action_score, discovery_score, company_quality_score, stage_score, forecast_score,
               raw_composite_priority_score, composite_priority_score, priority_pool_reason, composite_cap_reason
        FROM mart_stock_trend
        WHERE raw_composite_priority_score >= 75
          AND (COALESCE(priority_pool, '') != 'A池' OR composite_cap_reason IS NOT NULL)
          {sector_clause}
        ORDER BY COALESCE(raw_composite_priority_score, 0) DESC, stock_code
        LIMIT 12
        """,
        sector_params,
    ).fetchall()

    forecast_rows = conn.execute(
        f"""
        SELECT stock_code, stock_name, priority_pool, stock_archetype,
               action_score, discovery_score, company_quality_score, stage_score, forecast_score,
               raw_composite_priority_score, composite_priority_score, priority_pool_reason, composite_cap_reason
        FROM mart_stock_trend
        WHERE forecast_score >= 70
          AND stage_score < 40
          {sector_clause}
        ORDER BY forecast_score DESC, stage_score ASC, stock_code
        LIMIT 12
        """,
        sector_params,
    ).fetchall()

    quality_rows = conn.execute(
        f"""
        SELECT stock_code, stock_name, priority_pool, stock_archetype,
               action_score, discovery_score, company_quality_score, stage_score, forecast_score,
               raw_composite_priority_score, composite_priority_score, priority_pool_reason, composite_cap_reason
        FROM mart_stock_trend
        WHERE company_quality_score < 45
          AND COALESCE(stock_archetype, '') != '周期/事件驱动型'
          AND composite_priority_score >= 60
          {sector_clause}
        ORDER BY composite_priority_score DESC, stock_code
        LIMIT 12
        """,
        sector_params,
    ).fetchall()

    return {
        "capped_high_raw": _serialize_rows(capped_rows, common_fields),
        "forecast_stage_conflict": _serialize_rows(forecast_rows, common_fields),
        "quality_gate_conflict": _serialize_rows(quality_rows, common_fields),
        "counts": {
            "capped_high_raw": conn.execute(
                f"""
                SELECT COUNT(*)
                FROM mart_stock_trend
                WHERE raw_composite_priority_score >= 75
                  AND (COALESCE(priority_pool, '') != 'A池' OR composite_cap_reason IS NOT NULL)
                  {sector_clause}
                """,
                sector_params,
            ).fetchone()[0],
            "forecast_stage_conflict": conn.execute(
                f"""
                SELECT COUNT(*)
                FROM mart_stock_trend
                WHERE forecast_score >= 70
                  AND stage_score < 40
                  {sector_clause}
                """,
                sector_params,
            ).fetchone()[0],
            "quality_gate_conflict": conn.execute(
                f"""
                SELECT COUNT(*)
                FROM mart_stock_trend
                WHERE company_quality_score < 45
                  AND COALESCE(stock_archetype, '') != '周期/事件驱动型'
                  AND composite_priority_score >= 60
                  {sector_clause}
                """,
                sector_params,
            ).fetchone()[0],
        },
    }


def _load_audit_snapshot(conn) -> dict:
    audit = run_quality_audit(conn)
    layers = audit.get("layers") or {}
    financial = layers.get("financial") or {}
    trends = layers.get("trends") or {}
    sector = layers.get("sector_momentum") or {}
    current_rel = layers.get("current_relationship") or {}
    return {
        "audit_score": audit.get("score"),
        "latest_notice": (layers.get("raw") or {}).get("latest_notice"),
        "trend_count": trends.get("count"),
        "trend_scored": trends.get("scored"),
        "financial_research_ready": financial.get("research_history_ready"),
        "financial_research_gap": financial.get("research_history_gap"),
        "indicator_research_ready": financial.get("indicator_research_ready"),
        "indicator_research_gap": financial.get("indicator_research_gap"),
        "stage_feature_count": sector.get("stage_feature_count"),
        "forecast_feature_count": sector.get("forecast_feature_count"),
        "industry_context_count": sector.get("industry_context_count"),
        "industry_missing_current": current_rel.get("industry_missing_stocks"),
    }


def _load_archetype_distribution(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT COALESCE(stock_archetype, '待分类') AS stock_archetype,
               COUNT(*) AS total,
               SUM(CASE WHEN priority_pool = 'A池' THEN 1 ELSE 0 END) AS a_pool_count,
               AVG(company_quality_score) AS avg_quality_score,
               AVG(stage_score) AS avg_stage_score,
               AVG(composite_priority_score) AS avg_composite_score
        FROM mart_stock_trend
        GROUP BY COALESCE(stock_archetype, '待分类')
        ORDER BY COUNT(*) DESC, stock_archetype
        """
    ).fetchall()
    return _serialize_rows(
        rows,
        [
            "stock_archetype",
            "total",
            "a_pool_count",
            "avg_quality_score",
            "avg_stage_score",
            "avg_composite_score",
        ],
    )


def _load_qlib_summary(conn) -> dict:
    summary = get_model_summary(conn)
    if not summary:
        return {}
    factor_groups = summary.get("factor_groups") or []
    top_factors = summary.get("top_factors") or []
    train_params = summary.get("train_params") or {}
    enabled_parts = []
    if train_params.get("use_alpha158"):
        enabled_parts.append("Alpha158")
    if train_params.get("use_financial"):
        enabled_parts.append("financial")
    if train_params.get("use_institution"):
        enabled_parts.append("institution")
    summary["feature_stack_label"] = " + ".join(enabled_parts) if enabled_parts else "未标注"
    summary["factor_group_top"] = factor_groups[:3]
    summary["top_factors"] = top_factors[:5]
    return summary


def get_stock_scorecard_stats(conn) -> dict:
    pools = _load_pool_feedback(conn)
    snapshot_replay = _load_snapshot_pool_replay(conn)
    archetypes = _load_archetype_distribution(conn)
    qlib_summary = _load_qlib_summary(conn)

    pool_map = {item.get("priority_pool"): item for item in pools}
    coverage = snapshot_replay.get("coverage") or {}
    baseline = snapshot_replay.get("baseline") or {}
    return {
        "summary": {
            "stock_count": sum(int(item.get("total") or 0) for item in pools),
            "setup_count": sum(int(item.get("setup_count") or 0) for item in pools),
            "capped_count": sum(int(item.get("capped_count") or 0) for item in pools),
            "a_pool_count": int((pool_map.get("A池") or {}).get("total") or 0),
            "b_pool_count": int((pool_map.get("B池") or {}).get("total") or 0),
            "c_pool_count": int((pool_map.get("C池") or {}).get("total") or 0),
            "d_pool_count": int((pool_map.get("D池") or {}).get("total") or 0),
            "snapshot_scored_rows": int(coverage.get("scored_rows") or 0),
            "snapshot_scored_dates": int(coverage.get("scored_snapshot_dates") or 0),
            "first_scored_snapshot_date": coverage.get("first_scored_snapshot_date"),
            "last_scored_snapshot_date": coverage.get("last_scored_snapshot_date"),
        },
        "current_pools": pools,
        "archetypes": archetypes,
        "qlib_summary": qlib_summary,
        "snapshot_replay": {
            "coverage": coverage,
            "baseline": baseline,
            "by_pool": snapshot_replay.get("by_pool") or [],
        },
    }


def get_stock_validation_report(conn, sector: str | None = None) -> dict:
    normalized_sector = _normalize_sector(sector)
    pools = _load_pool_feedback(conn, normalized_sector)
    snapshot_replay = _load_snapshot_pool_replay(conn, normalized_sector)
    snapshot_rank_compare = _load_snapshot_rank_compare(conn, normalized_sector)
    compare = _load_rank_compare(conn, sector=normalized_sector)
    anomalies = _load_anomalies(conn, normalized_sector)
    audit = _load_audit_snapshot(conn)
    qlib_summary = _load_qlib_summary(conn)

    total = sum(int(item.get("total") or 0) for item in pools)
    pool_map = {item.get("priority_pool"): item for item in pools}
    capped_total = sum(int(item.get("capped_count") or 0) for item in pools)

    summary = {
        "generated_at": datetime.now().isoformat(),
        "stock_count": total,
        "a_pool_count": int((pool_map.get("A池") or {}).get("total") or 0),
        "b_pool_count": int((pool_map.get("B池") or {}).get("total") or 0),
        "c_pool_count": int((pool_map.get("C池") or {}).get("total") or 0),
        "d_pool_count": int((pool_map.get("D池") or {}).get("total") or 0),
        "capped_total": capped_total,
        "overlap_top20": compare["overlap"].get("top20", 0),
        "overlap_top50": compare["overlap"].get("top50", 0),
        "overlap_top100": compare["overlap"].get("top100", 0),
        "anomaly_total": sum(int(value or 0) for value in (anomalies.get("counts") or {}).values()),
        "audit_score": audit.get("audit_score"),
        "snapshot_scored_rows": snapshot_replay["coverage"].get("scored_rows", 0),
        "snapshot_scored_dates": snapshot_replay["coverage"].get("scored_snapshot_dates", 0),
        "snapshot_rank_matured_dates": snapshot_rank_compare.get("matured_snapshot_dates", 0),
        "qlib_prediction_count": int(qlib_summary.get("prediction_count") or 0),
        "qlib_model_id": qlib_summary.get("model_id"),
        "qlib_predict_date": qlib_summary.get("predict_date"),
    }

    return {
        "scope": {
            "sector": normalized_sector,
            "mode": "sector" if normalized_sector else "all",
            "audit_scope": "global",
        },
        "summary": summary,
        "pool_feedback": pools,
        "snapshot_pool_replay": snapshot_replay,
        "snapshot_rank_compare": snapshot_rank_compare,
        "legacy_compare": compare,
        "anomalies": anomalies,
        "audit": audit,
        "qlib_summary": qlib_summary,
    }

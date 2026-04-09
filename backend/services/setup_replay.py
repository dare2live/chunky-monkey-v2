"""
setup_replay.py

历史事件回放引擎：
- 按“当时可见”的口径重放所有 buy_event
- 生成事件级 Setup 结果
- 生成按优先级/维度的研究摘要
"""

import logging
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from services.industry import industry_join_clause, industry_select_clause
from services.scoring import (
    SETUP_LEVEL_THRESHOLDS,
    _build_setup_reason,
    _crowding_bucket,
    _crowding_fit_grade,
    _crowding_fit_raw_from_stats,
    _crowding_stability_grade,
    _crowding_stability_raw_from_stats,
    _crowding_yield_grade,
    _crowding_yield_raw_from_stats,
    _followability_grade,
    _industry_edge_raw,
    _industry_skill_grade,
    _premium_bucket_label,
    _premium_grade,
    _report_recency_grade,
    _reliability_grade,
    _setup_confidence_text,
    _setup_execution_from_grades,
    _setup_priority_from_grades,
)
from services.utils import safe_float as _safe_float, clamp as _clamp

logger = logging.getLogger("cm-api")


def _parse_any_date(value) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _iso_date(value) -> Optional[str]:
    dt = _parse_any_date(value)
    return dt.strftime("%Y-%m-%d") if dt else None


def _avg(sum_value: float, count: int) -> Optional[float]:
    if not count:
        return None
    return sum_value / count


def _pct(part: int, total: int) -> Optional[float]:
    if not total:
        return None
    return part * 100.0 / total


def _init_buy_agg():
    return {
        "count": 0,
        "sum_gain_30d": 0.0,
        "sum_win_30d": 0,
        "sum_dd_30d": 0.0,
    }


def _init_safe_agg():
    return {
        "buy_count": 0,
        "sum_premium": 0.0,
        "safe_count": 0,
        "safe_sum_gain_30d": 0.0,
        "safe_sum_dd_30d": 0.0,
        "safe_sum_win_30d": 0,
    }


def _init_stat_agg():
    return {
        "count": 0,
        "sum_gain_30d": 0.0,
        "sum_win_30d": 0,
        "sum_dd_30d": 0.0,
    }


def _build_inst_baseline(buy_agg: dict, safe_agg: dict) -> dict:
    buy_count = int(buy_agg.get("count") or 0)
    buy_avg30 = _avg(buy_agg.get("sum_gain_30d") or 0.0, buy_count)
    buy_wr30 = _pct(int(buy_agg.get("sum_win_30d") or 0), buy_count)
    buy_dd30 = _avg(buy_agg.get("sum_dd_30d") or 0.0, buy_count)

    safe_count = int(safe_agg.get("safe_count") or 0)
    avg_premium = _avg(safe_agg.get("sum_premium") or 0.0, int(safe_agg.get("buy_count") or 0))
    safe_avg30 = _avg(safe_agg.get("safe_sum_gain_30d") or 0.0, safe_count)
    safe_dd30 = _avg(safe_agg.get("safe_sum_dd_30d") or 0.0, safe_count)
    safe_wr30 = _pct(int(safe_agg.get("safe_sum_win_30d") or 0), safe_count)

    quality_score = None
    if buy_count >= 1 and buy_avg30 is not None and buy_wr30 is not None:
        quality_score = round(_clamp(
            40.0
            + buy_avg30 * 3.0
            + (buy_wr30 - 50.0) * 0.9
            + max(0.0, 18.0 - (buy_dd30 if buy_dd30 is not None else 18.0)) * 1.0
            + min(buy_count, 40) / 40.0 * 8.0,
            0.0, 100.0
        ), 2)

    transfer_eff = None
    if buy_avg30 is not None and buy_avg30 > 0 and safe_avg30 is not None:
        transfer_eff = round(safe_avg30 / buy_avg30 * 100.0, 2)

    followability_score = None
    if safe_count > 0:
        followability_score = round(_clamp(
            35.0
            + ((safe_wr30 or 50.0) - 50.0) * 0.9
            + (safe_avg30 or 0.0) * 2.2
            + max(0.0, 18.0 - (safe_dd30 if safe_dd30 is not None else 18.0)) * 0.9
            + ((transfer_eff or 50.0) - 50.0) * 0.18
            - max(avg_premium or 0.0, 0.0) * 0.22
            + min(safe_count, 20) / 20.0 * 10.0,
            0.0, 100.0
        ), 2)

    return {
        "buy_count": buy_count,
        "buy_avg_gain_30d": buy_avg30,
        "buy_win_rate_30d": buy_wr30,
        "buy_median_max_drawdown_30d": buy_dd30,
        "quality_score": quality_score,
        "followability_score": followability_score,
        "avg_premium_pct": avg_premium,
        "safe_follow_event_count": safe_count,
        "safe_follow_win_rate_30d": safe_wr30,
        "safe_follow_avg_gain_30d": safe_avg30,
        "safe_follow_avg_drawdown_30d": safe_dd30,
        "signal_transfer_efficiency_30d": transfer_eff,
    }


def _build_industry_stat(agg: dict) -> dict:
    count = int(agg.get("count") or 0)
    return {
        "sample_events": count,
        "avg_gain_30d": _avg(agg.get("sum_gain_30d") or 0.0, count),
        "win_rate_30d": _pct(int(agg.get("sum_win_30d") or 0), count),
        "max_drawdown_30d": _avg(agg.get("sum_dd_30d") or 0.0, count),
    }


def _build_crowding_stat(agg: Optional[dict]) -> Optional[dict]:
    if not agg:
        return None
    count = int(agg.get("count") or 0)
    if count <= 0:
        return None
    return {
        "n": count,
        "avg30": _avg(agg.get("sum_gain_30d") or 0.0, count),
        "wr30": _pct(int(agg.get("sum_win_30d") or 0), count),
        "dd30": _avg(agg.get("sum_dd_30d") or 0.0, count),
    }


def _evaluate_replay_candidate(
    event_row: dict,
    inst_baseline: dict,
    industry_aggs: dict,
    crowd_count: int,
    crowd_full_aggs: dict,
    crowd_l3_aggs: dict,
) -> Optional[dict]:
    event_type = event_row.get("event_type")
    if event_type not in ("new_entry", "increase"):
        return None

    follow_gate = event_row.get("follow_gate")
    if follow_gate not in ("follow", "watch"):
        return None

    premium_pct = _safe_float(event_row.get("premium_pct"))
    premium_grade = _premium_grade(premium_pct)
    if premium_grade >= 5:
        return None
    premium_bucket = _premium_bucket_label(premium_pct)

    replay_dt = _parse_any_date(event_row.get("replay_date"))
    report_dt = _parse_any_date(event_row.get("report_date"))
    report_age_days = None
    if replay_dt and report_dt:
        report_age_days = max((replay_dt - report_dt).days, 0)
    report_grade = _report_recency_grade(report_age_days)

    follow_grade = _followability_grade(
        _safe_float(inst_baseline.get("followability_score")),
        follow_gate,
    )

    crowd_bucket = _crowding_bucket(crowd_count)

    for level, industry_name in (
        ("level3", event_row.get("sw_level3")),
        ("level2", event_row.get("sw_level2")),
        ("level1", event_row.get("sw_level1")),
    ):
        if not industry_name:
            continue
        stat = industry_aggs.get((level, industry_name))
        if not stat:
            continue

        stat_view = _build_industry_stat(stat)
        sample_events = int(stat_view.get("sample_events") or 0)
        threshold = SETUP_LEVEL_THRESHOLDS[level]
        edge_raw = _industry_edge_raw(stat_view, inst_baseline)
        if sample_events < threshold["min_samples"] or edge_raw < threshold["min_edge_raw"]:
            continue

        industry_grade = _industry_skill_grade(edge_raw)
        reliability_grade = _reliability_grade(sample_events)

        full_key = (event_type, crowd_bucket, premium_bucket)
        full_stats = _build_crowding_stat(crowd_full_aggs.get(full_key))
        l3_stats = _build_crowding_stat(crowd_l3_aggs.get(full_key))
        crowding_source = "full_sample"
        crowding_stats = full_stats
        if level == "level3" and l3_stats and int(l3_stats.get("n") or 0) >= 20:
            crowding_source = "l3_expert"
            crowding_stats = l3_stats

        crowding_yield_raw = _crowding_yield_raw_from_stats(
            crowding_stats,
            event_type,
            crowd_bucket,
            premium_bucket,
        )
        crowding_yield_grade = _crowding_yield_grade(crowding_yield_raw)
        crowding_stability_raw = _crowding_stability_raw_from_stats(
            crowding_stats,
            event_type,
            crowd_bucket,
            premium_bucket,
        )
        crowding_stability_grade = _crowding_stability_grade(crowding_stability_raw)
        crowding_fit_raw = _crowding_fit_raw_from_stats(
            crowding_stats,
            event_type,
            crowd_bucket,
            premium_bucket,
        )
        crowding_fit_grade = _crowding_fit_grade(crowding_fit_raw)
        crowding_fit_sample = int((crowding_stats or {}).get("n") or 0)
        setup_confidence = _setup_confidence_text(sample_events, edge_raw)
        setup_execution_gate, setup_execution_reason = _setup_execution_from_grades(
            follow_gate,
            event_row.get("follow_gate_reason"),
            premium_grade,
            follow_grade,
            crowding_stability_grade,
            reliability_grade,
            report_grade,
        )
        setup_priority = _setup_priority_from_grades(
            level,
            event_type,
            report_grade,
            premium_grade,
            follow_grade,
            reliability_grade,
            crowding_yield_grade,
        )
        setup_score_raw = round(
            edge_raw * 8
            + (6 - premium_grade) * 10
            + (6 - report_grade) * 7
            + (6 - follow_grade) * 7
            + (6 - reliability_grade) * 6
            + (6 - crowding_yield_grade) * 4
            + (6 - crowding_stability_grade) * 2
            + min(max(crowding_yield_raw, 0.0), 60.0) * 0.12
            + min(max(crowding_stability_raw, 0.0), 60.0) * 0.06
            + (_safe_float(inst_baseline.get("followability_score")) or 50.0) * 0.25
            + (_safe_float(inst_baseline.get("quality_score")) or 50.0) * 0.15
            + (6 if event_type == "new_entry" else 3),
            2,
        )

        return {
            "setup_tag": "industry_expert_entry",
            "setup_priority": setup_priority,
            "setup_reason": _build_setup_reason(level, event_type, report_grade, premium_grade),
            "setup_confidence": setup_confidence,
            "matched_level": level,
            "matched_industry_name": industry_name,
            "setup_score_raw": setup_score_raw,
            "setup_execution_gate": setup_execution_gate,
            "setup_execution_reason": setup_execution_reason,
            "industry_skill_raw": edge_raw,
            "industry_skill_grade": industry_grade,
            "quality_proxy_score": _safe_float(inst_baseline.get("quality_score")),
            "followability_proxy_score": _safe_float(inst_baseline.get("followability_score")),
            "followability_grade": follow_grade,
            "premium_grade": premium_grade,
            "report_recency_grade": report_grade,
            "reliability_grade": reliability_grade,
            "crowding_bucket": crowd_bucket,
            "crowding_yield_raw": crowding_yield_raw,
            "crowding_yield_grade": crowding_yield_grade,
            "crowding_stability_raw": crowding_stability_raw,
            "crowding_stability_grade": crowding_stability_grade,
            "crowding_fit_raw": crowding_fit_raw,
            "crowding_fit_grade": crowding_fit_grade,
            "crowding_fit_sample": crowding_fit_sample,
            "crowding_fit_source": crowding_source,
            "report_age_days": report_age_days,
        }

    return None


def _iter_date_batches(rows: List[dict]) -> Iterable[Tuple[str, List[dict]]]:
    current_key = None
    bucket = []
    for row in rows:
        key = row["replay_date"]
        if current_key is None:
            current_key = key
        if key != current_key:
            yield current_key, bucket
            current_key = key
            bucket = []
        bucket.append(row)
    if bucket:
        yield current_key, bucket


def _create_replay_tables(conn):
    conn.execute("DROP TABLE IF EXISTS research_setup_replay_event")
    conn.execute("""
        CREATE TABLE research_setup_replay_event (
            replay_date TEXT,
            institution_id TEXT,
            inst_name TEXT,
            inst_type TEXT,
            stock_code TEXT,
            stock_name TEXT,
            report_date TEXT,
            notice_date TEXT,
            event_type TEXT,
            follow_gate TEXT,
            follow_gate_reason TEXT,
            premium_pct REAL,
            premium_bucket TEXT,
            crowd_count INTEGER,
            crowding_bucket TEXT,
            sw_level1 TEXT,
            sw_level2 TEXT,
            sw_level3 TEXT,
            prior_inst_buy_count INTEGER,
            prior_safe_follow_count INTEGER,
            matched_level TEXT,
            matched_industry_name TEXT,
            setup_tag TEXT,
            setup_priority INTEGER,
            setup_reason TEXT,
            setup_confidence TEXT,
            setup_score_raw REAL,
            setup_execution_gate TEXT,
            setup_execution_reason TEXT,
            industry_skill_raw REAL,
            industry_skill_grade INTEGER,
            quality_proxy_score REAL,
            followability_proxy_score REAL,
            followability_grade INTEGER,
            premium_grade INTEGER,
            report_recency_grade INTEGER,
            reliability_grade INTEGER,
            crowding_yield_raw REAL,
            crowding_yield_grade INTEGER,
            crowding_stability_raw REAL,
            crowding_stability_grade INTEGER,
            crowding_fit_raw REAL,
            crowding_fit_grade INTEGER,
            crowding_fit_sample INTEGER,
            crowding_fit_source TEXT,
            report_age_days INTEGER,
            gain_10d REAL,
            gain_30d REAL,
            gain_60d REAL,
            gain_120d REAL,
            max_drawdown_30d REAL,
            max_drawdown_60d REAL,
            PRIMARY KEY (institution_id, stock_code, report_date)
        )
    """)
    conn.execute("DROP TABLE IF EXISTS research_setup_replay_summary")
    conn.execute("""
        CREATE TABLE research_setup_replay_summary (
            group_name TEXT PRIMARY KEY,
            sample_count INTEGER,
            avg_gain_10d REAL,
            avg_gain_30d REAL,
            avg_gain_60d REAL,
            avg_gain_120d REAL,
            win_rate_10d REAL,
            win_rate_30d REAL,
            win_rate_60d REAL,
            win_rate_120d REAL,
            avg_drawdown_30d REAL,
            avg_drawdown_60d REAL,
            uplift_vs_baseline_30d REAL
        )
    """)
    conn.execute("DROP TABLE IF EXISTS research_setup_replay_factor")
    conn.execute("""
        CREATE TABLE research_setup_replay_factor (
            factor_name TEXT,
            factor_value TEXT,
            sample_count INTEGER,
            avg_gain_30d REAL,
            avg_gain_60d REAL,
            avg_gain_120d REAL,
            win_rate_30d REAL,
            win_rate_60d REAL,
            win_rate_120d REAL,
            avg_drawdown_30d REAL,
            uplift_vs_baseline_30d REAL
        )
    """)


def _write_replay_summary(conn):
    baseline = conn.execute("""
        SELECT COUNT(*) AS n,
               AVG(gain_10d) AS g10,
               AVG(gain_30d) AS g30,
               AVG(gain_60d) AS g60,
               AVG(gain_120d) AS g120,
               AVG(CASE WHEN gain_10d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr10,
               AVG(CASE WHEN gain_30d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr30,
               AVG(CASE WHEN gain_60d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr60,
               AVG(CASE WHEN gain_120d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr120,
               AVG(max_drawdown_30d) AS dd30,
               AVG(max_drawdown_60d) AS dd60
        FROM research_setup_replay_event
    """).fetchone()
    baseline_g30 = _safe_float(baseline["g30"]) or 0.0

    groups = [
        ("baseline_all_buy", "SELECT * FROM research_setup_replay_event"),
        ("setup_hit_all", "SELECT * FROM research_setup_replay_event WHERE setup_tag IS NOT NULL"),
    ]
    for priority in range(1, 6):
        groups.append((
            f"priority_{priority}",
            f"SELECT * FROM research_setup_replay_event WHERE setup_priority = {priority}",
        ))

    for name, sql in groups:
        row = conn.execute(f"""
            SELECT COUNT(*) AS n,
                   AVG(gain_10d) AS g10,
                   AVG(gain_30d) AS g30,
                   AVG(gain_60d) AS g60,
                   AVG(gain_120d) AS g120,
                   AVG(CASE WHEN gain_10d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr10,
                   AVG(CASE WHEN gain_30d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr30,
                   AVG(CASE WHEN gain_60d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr60,
                   AVG(CASE WHEN gain_120d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr120,
                   AVG(max_drawdown_30d) AS dd30,
                   AVG(max_drawdown_60d) AS dd60
            FROM ({sql})
        """).fetchone()
        conn.execute("""
            INSERT INTO research_setup_replay_summary
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name,
            int(row["n"] or 0),
            row["g10"], row["g30"], row["g60"], row["g120"],
            row["wr10"], row["wr30"], row["wr60"], row["wr120"],
            row["dd30"], row["dd60"],
            (_safe_float(row["g30"]) or 0.0) - baseline_g30,
        ))


def _write_replay_factors(conn):
    baseline = conn.execute("""
        SELECT AVG(gain_30d) AS g30
        FROM research_setup_replay_event
    """).fetchone()
    baseline_g30 = _safe_float(baseline["g30"]) or 0.0

    factors = [
        "setup_priority",
        "matched_level",
        "event_type",
        "inst_type",
        "industry_skill_grade",
        "followability_grade",
        "premium_grade",
        "report_recency_grade",
        "reliability_grade",
        "setup_execution_gate",
        "crowding_yield_grade",
        "crowding_stability_grade",
        "crowding_fit_grade",
        "crowding_bucket",
    ]
    for factor in factors:
        rows = conn.execute(f"""
            SELECT {factor} AS factor_value,
                   COUNT(*) AS n,
                   AVG(gain_30d) AS g30,
                   AVG(gain_60d) AS g60,
                   AVG(gain_120d) AS g120,
                   AVG(CASE WHEN gain_30d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr30,
                   AVG(CASE WHEN gain_60d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr60,
                   AVG(CASE WHEN gain_120d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS wr120,
                   AVG(max_drawdown_30d) AS dd30
            FROM research_setup_replay_event
            WHERE setup_tag IS NOT NULL AND {factor} IS NOT NULL
            GROUP BY {factor}
            HAVING n >= 20
            ORDER BY g30 DESC
        """).fetchall()
        for row in rows:
            conn.execute("""
                INSERT INTO research_setup_replay_factor
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                factor,
                str(row["factor_value"]),
                int(row["n"] or 0),
                row["g30"],
                row["g60"],
                row["g120"],
                row["wr30"],
                row["wr60"],
                row["wr120"],
                row["dd30"],
                (_safe_float(row["g30"]) or 0.0) - baseline_g30,
            ))


def build_setup_replay(conn) -> dict:
    """研究表：历史事件 Setup 回放。"""
    logger.info("[回测-表3] Setup 历史回放...")

    _create_replay_tables(conn)

    rows = conn.execute("""
        SELECT e.institution_id,
               COALESCE(NULLIF(i.display_name, ''), i.name) AS inst_name,
               i.type AS inst_type,
               e.stock_code,
               COALESCE(NULLIF(e.stock_name, ''), ds.stock_name, e.stock_code) AS stock_name,
               e.report_date,
               e.notice_date,
               e.event_type,
               e.follow_gate,
               e.follow_gate_reason,
               e.premium_pct,
               e.premium_bucket,
               e.gain_10d,
               e.gain_30d,
               e.gain_60d,
               e.gain_120d,
               e.max_drawdown_30d,
               e.max_drawdown_60d,
               {industry_columns}
        FROM fact_institution_event e
        JOIN inst_institutions i ON e.institution_id = i.id
        {industry_join}
        LEFT JOIN dim_active_a_stock ds ON e.stock_code = ds.stock_code
        WHERE e.event_type IN ('new_entry', 'increase')
          AND e.gain_30d IS NOT NULL
    """.format(
        industry_columns=industry_select_clause(alias="industry_dim"),
        industry_join=industry_join_clause("e.stock_code", alias="industry_dim", join_type="LEFT"),
    )).fetchall()

    events = []
    for row in rows:
        item = dict(row)
        replay_date = _iso_date(item.get("notice_date") or item.get("report_date"))
        if not replay_date:
            continue
        item["replay_date"] = replay_date
        events.append(item)
    events.sort(key=lambda r: (r["replay_date"], r["stock_code"], r["institution_id"]))

    inst_buy_aggs = defaultdict(_init_buy_agg)
    inst_safe_aggs = defaultdict(_init_safe_agg)
    inst_industry_aggs = defaultdict(_init_stat_agg)
    crowd_full_aggs = defaultdict(_init_stat_agg)
    crowd_l3_aggs = defaultdict(_init_stat_agg)
    cohort_counts = Counter()

    insert_rows = []
    replayed = 0
    setup_hits = 0

    for replay_date, batch in _iter_date_batches(events):
        batch_cohorts = Counter((row["stock_code"], row["report_date"]) for row in batch)
        evaluated_batch = []

        for row in batch:
            inst_id = row["institution_id"]
            inst_baseline = _build_inst_baseline(inst_buy_aggs[inst_id], inst_safe_aggs[inst_id])
            industry_lookup = {}
            for level, industry_name in (
                ("level1", row.get("sw_level1")),
                ("level2", row.get("sw_level2")),
                ("level3", row.get("sw_level3")),
            ):
                if industry_name:
                    agg = inst_industry_aggs.get((inst_id, level, industry_name))
                    if agg:
                        industry_lookup[(level, industry_name)] = agg

            crowd_count = cohort_counts[(row["stock_code"], row["report_date"])] + batch_cohorts[(row["stock_code"], row["report_date"])]
            candidate = _evaluate_replay_candidate(
                row,
                inst_baseline,
                industry_lookup,
                crowd_count,
                crowd_full_aggs,
                crowd_l3_aggs,
            )
            evaluated_batch.append((row, candidate, crowd_count, inst_baseline))

            insert_rows.append((
                replay_date,
                row["institution_id"],
                row["inst_name"],
                row["inst_type"],
                row["stock_code"],
                row["stock_name"],
                row["report_date"],
                row["notice_date"],
                row["event_type"],
                row.get("follow_gate"),
                row.get("follow_gate_reason"),
                row.get("premium_pct"),
                row.get("premium_bucket"),
                crowd_count,
                candidate.get("crowding_bucket") if candidate else _crowding_bucket(crowd_count),
                row.get("sw_level1"),
                row.get("sw_level2"),
                row.get("sw_level3"),
                int(inst_baseline.get("buy_count") or 0),
                int(inst_baseline.get("safe_follow_event_count") or 0),
                candidate.get("matched_level") if candidate else None,
                candidate.get("matched_industry_name") if candidate else None,
                candidate.get("setup_tag") if candidate else None,
                candidate.get("setup_priority") if candidate else None,
                candidate.get("setup_reason") if candidate else None,
                candidate.get("setup_confidence") if candidate else None,
                candidate.get("setup_score_raw") if candidate else None,
                candidate.get("setup_execution_gate") if candidate else None,
                candidate.get("setup_execution_reason") if candidate else None,
                candidate.get("industry_skill_raw") if candidate else None,
                candidate.get("industry_skill_grade") if candidate else None,
                candidate.get("quality_proxy_score") if candidate else _safe_float(inst_baseline.get("quality_score")),
                candidate.get("followability_proxy_score") if candidate else _safe_float(inst_baseline.get("followability_score")),
                candidate.get("followability_grade") if candidate else None,
                candidate.get("premium_grade") if candidate else _premium_grade(_safe_float(row.get("premium_pct"))),
                candidate.get("report_recency_grade") if candidate else _report_recency_grade(
                    max(((_parse_any_date(replay_date) - _parse_any_date(row.get("report_date"))).days), 0)
                    if _parse_any_date(replay_date) and _parse_any_date(row.get("report_date"))
                    else None
                ),
                candidate.get("reliability_grade") if candidate else None,
                candidate.get("crowding_yield_raw") if candidate else None,
                candidate.get("crowding_yield_grade") if candidate else None,
                candidate.get("crowding_stability_raw") if candidate else None,
                candidate.get("crowding_stability_grade") if candidate else None,
                candidate.get("crowding_fit_raw") if candidate else None,
                candidate.get("crowding_fit_grade") if candidate else None,
                candidate.get("crowding_fit_sample") if candidate else None,
                candidate.get("crowding_fit_source") if candidate else None,
                candidate.get("report_age_days") if candidate else None,
                row.get("gain_10d"),
                row.get("gain_30d"),
                row.get("gain_60d"),
                row.get("gain_120d"),
                row.get("max_drawdown_30d"),
                row.get("max_drawdown_60d"),
            ))
            replayed += 1
            if candidate:
                setup_hits += 1

        for row, candidate, crowd_count, _ in evaluated_batch:
            inst_id = row["institution_id"]

            buy_agg = inst_buy_aggs[inst_id]
            buy_agg["count"] += 1
            buy_agg["sum_gain_30d"] += _safe_float(row.get("gain_30d")) or 0.0
            buy_agg["sum_win_30d"] += 1 if (_safe_float(row.get("gain_30d")) or 0.0) > 0 else 0
            buy_agg["sum_dd_30d"] += _safe_float(row.get("max_drawdown_30d")) or 0.0

            safe_agg = inst_safe_aggs[inst_id]
            safe_agg["buy_count"] += 1
            safe_agg["sum_premium"] += _safe_float(row.get("premium_pct")) or 0.0
            if (_safe_float(row.get("premium_pct")) or 999.0) <= 5:
                safe_agg["safe_count"] += 1
                safe_agg["safe_sum_gain_30d"] += _safe_float(row.get("gain_30d")) or 0.0
                safe_agg["safe_sum_dd_30d"] += _safe_float(row.get("max_drawdown_30d")) or 0.0
                safe_agg["safe_sum_win_30d"] += 1 if (_safe_float(row.get("gain_30d")) or 0.0) > 0 else 0

            for level, industry_name in (
                ("level1", row.get("sw_level1")),
                ("level2", row.get("sw_level2")),
                ("level3", row.get("sw_level3")),
            ):
                if not industry_name:
                    continue
                agg = inst_industry_aggs[(inst_id, level, industry_name)]
                agg["count"] += 1
                agg["sum_gain_30d"] += _safe_float(row.get("gain_30d")) or 0.0
                agg["sum_win_30d"] += 1 if (_safe_float(row.get("gain_30d")) or 0.0) > 0 else 0
                agg["sum_dd_30d"] += _safe_float(row.get("max_drawdown_30d")) or 0.0

            cohort_counts[(row["stock_code"], row["report_date"])] += 1
            crowd_key = (
                row["event_type"],
                _crowding_bucket(crowd_count),
                _premium_bucket_label(_safe_float(row.get("premium_pct"))),
            )
            full_agg = crowd_full_aggs[crowd_key]
            full_agg["count"] += 1
            full_agg["sum_gain_30d"] += _safe_float(row.get("gain_30d")) or 0.0
            full_agg["sum_win_30d"] += 1 if (_safe_float(row.get("gain_30d")) or 0.0) > 0 else 0
            full_agg["sum_dd_30d"] += _safe_float(row.get("max_drawdown_30d")) or 0.0

            if candidate and candidate.get("matched_level") == "level3":
                l3_agg = crowd_l3_aggs[crowd_key]
                l3_agg["count"] += 1
                l3_agg["sum_gain_30d"] += _safe_float(row.get("gain_30d")) or 0.0
                l3_agg["sum_win_30d"] += 1 if (_safe_float(row.get("gain_30d")) or 0.0) > 0 else 0
                l3_agg["sum_dd_30d"] += _safe_float(row.get("max_drawdown_30d")) or 0.0

    replay_columns = [
        "replay_date", "institution_id", "inst_name", "inst_type", "stock_code", "stock_name",
        "report_date", "notice_date", "event_type", "follow_gate", "follow_gate_reason",
        "premium_pct", "premium_bucket", "crowd_count", "crowding_bucket",
        "sw_level1", "sw_level2", "sw_level3", "prior_inst_buy_count", "prior_safe_follow_count",
        "matched_level", "matched_industry_name", "setup_tag", "setup_priority", "setup_reason",
        "setup_confidence", "setup_score_raw", "setup_execution_gate", "setup_execution_reason",
        "industry_skill_raw", "industry_skill_grade",
        "quality_proxy_score", "followability_proxy_score", "followability_grade",
        "premium_grade", "report_recency_grade", "reliability_grade",
        "crowding_yield_raw", "crowding_yield_grade", "crowding_stability_raw", "crowding_stability_grade",
        "crowding_fit_raw", "crowding_fit_grade", "crowding_fit_sample", "crowding_fit_source",
        "report_age_days", "gain_10d", "gain_30d", "gain_60d", "gain_120d",
        "max_drawdown_30d", "max_drawdown_60d",
    ]
    replay_placeholders = ",".join(["?"] * len(replay_columns))
    conn.executemany(f"""
        INSERT OR REPLACE INTO research_setup_replay_event
        ({", ".join(replay_columns)})
        VALUES ({replay_placeholders})
    """, insert_rows)

    _write_replay_summary(conn)
    _write_replay_factors(conn)
    conn.commit()

    logger.info(f"[回测-表3] Setup 回放完成: {replayed} 条事件, {setup_hits} 条命中")
    return {"rows": replayed, "setup_hits": setup_hits}


def get_setup_replay_summary(conn) -> dict:
    summary_rows = conn.execute("""
        SELECT *
        FROM research_setup_replay_summary
        ORDER BY CASE
            WHEN group_name = 'baseline_all_buy' THEN 0
            WHEN group_name = 'setup_hit_all' THEN 1
            WHEN group_name LIKE 'priority_%' THEN 2
            ELSE 9
        END, group_name
    """).fetchall()
    factor_overview = conn.execute("""
        SELECT factor_name, COUNT(*) AS groups
        FROM research_setup_replay_factor
        GROUP BY factor_name
        ORDER BY factor_name
    """).fetchall()
    return {
        "summary": [dict(r) for r in summary_rows],
        "factors": [dict(r) for r in factor_overview],
    }


def list_setup_replay_factors(conn, factor_name: Optional[str] = None, limit: int = 200):
    if factor_name:
        rows = conn.execute("""
            SELECT *
            FROM research_setup_replay_factor
            WHERE factor_name = ?
            ORDER BY avg_gain_30d DESC, sample_count DESC
            LIMIT ?
        """, (factor_name, limit)).fetchall()
    else:
        rows = conn.execute("""
            SELECT *
            FROM research_setup_replay_factor
            ORDER BY factor_name, avg_gain_30d DESC, sample_count DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def list_setup_replay_events(conn, limit: int = 200, setup_only: bool = True):
    where = "WHERE setup_tag IS NOT NULL" if setup_only else ""
    rows = conn.execute(f"""
        SELECT replay_date, stock_code, stock_name, institution_id, inst_name, inst_type,
               report_date, notice_date, event_type, setup_tag, setup_priority,
               setup_reason, setup_confidence, setup_execution_gate, setup_execution_reason,
               matched_level, matched_industry_name,
               industry_skill_grade, followability_grade, premium_grade,
               report_recency_grade, reliability_grade, crowding_yield_grade,
               crowding_stability_grade, crowding_fit_grade,
               gain_30d, gain_60d, gain_120d, max_drawdown_30d
        FROM research_setup_replay_event
        {where}
        ORDER BY replay_date DESC, COALESCE(setup_priority, 9), gain_30d DESC
        LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in rows]

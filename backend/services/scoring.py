"""
评分引擎 — Chunky Monkey v2

机构质量评分 + 股票行动评分 + 路径分类 + 时机标签

纯计算模块，不含 FastAPI 路由。
"""

import logging
from datetime import datetime
from typing import Optional, Tuple

from services.industry import industry_join_clause
from services.utils import safe_float as _safe_float, percentile_ranks as _percentile_ranks
from services.constants import PATH_THRESHOLDS

logger = logging.getLogger("cm-api")

# ============================================================
# 默认评分配置
# ============================================================

# 机构评分卡默认权重
INST_SCORE_DEFAULTS = {
    "sample_weight": 10,        # 样本充足度
    "gain_30d_weight": 15,      # 30日平均收益
    "gain_60d_weight": 15,      # 60日平均收益
    "gain_120d_weight": 10,     # 120日平均收益
    "win_rate_30d_weight": 15,  # 30日胜率
    "win_rate_60d_weight": 10,  # 60日胜率
    "win_rate_90d_weight": 5,   # 90日胜率
    "drawdown_weight": 10,      # 回撤控制
    "stability_weight": 10,     # 收益稳定性
}

FOLLOW_SCORE_DEFAULTS = {
    "safe_sample_weight": 20,          # 安全跟随样本充足度
    "safe_win_rate_30d_weight": 25,    # 安全跟随30日胜率
    "safe_gain_30d_weight": 15,        # 安全跟随30日平均收益
    "safe_drawdown_weight": 10,        # 安全跟随平均回撤（越小越好）
    "transfer_efficiency_weight": 20,  # 信号传递效率
    "avg_premium_weight": 10,          # 平均跟随溢价（越低越好）
}

# 股票评分卡默认权重
STOCK_SCORE_DEFAULTS = {
    "leader_quality_weight": 30,    # 龙头机构质量
    "industry_match_weight": 25,    # 行业命中
    "event_type_weight": 15,        # 事件类型
    "consensus_weight": 10,         # 共识度
    "timeliness_weight": 10,        # 时效性
    "data_confidence_weight": 10,   # 数据可信度
    # 扣分项
    "overheated_penalty": 20,       # 过热扣分
    "conflict_penalty": 15,         # 冲突扣分
    "path_exhausted_penalty": 15,   # 已充分演绎扣分
}

# 事件类型得分
EVENT_TYPE_SCORES = {
    "new_entry": 100,
    "increase": 70,
    "unchanged": 30,
    "decrease": 10,
    "exit": 0,
}

SETUP_LEVEL_THRESHOLDS = {
    "level3": {"min_samples": 5, "min_edge_raw": 2.5},
    "level2": {"min_samples": 8, "min_edge_raw": 2.0},
    "level1": {"min_samples": 12, "min_edge_raw": 1.5},
}


# ============================================================
# 配置读写
# ============================================================

def load_scoring_config(conn, prefix: str) -> dict:
    """
    从 app_settings 加载评分权重配置。

    prefix: "scoring.institution" | "scoring.followability"
    找不到时回退到内置默认值。
    """
    defaults_map = {
        "scoring.institution": INST_SCORE_DEFAULTS,
        "scoring.followability": FOLLOW_SCORE_DEFAULTS,
    }
    defaults = defaults_map.get(prefix, {})

    config = dict(defaults)
    rows = conn.execute(
        "SELECT key, value FROM app_settings WHERE key LIKE ?",
        (f"{prefix}.%",)
    ).fetchall()

    for row in rows:
        short_key = row["key"][len(prefix) + 1:]  # strip "prefix."
        try:
            config[short_key] = float(row["value"])
        except (ValueError, TypeError):
            logger.warning(f"[评分] 无法解析配置 {row['key']}={row['value']}, 使用默认值")

    return config


def save_scoring_config(conn, prefix: str, config: dict):
    """
    将评分权重配置写入 app_settings。
    """
    now = datetime.now().isoformat()
    for key, value in config.items():
        full_key = f"{prefix}.{key}"
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            (full_key, str(value), now)
        )
    conn.commit()
    logger.info(f"[评分] 保存配置 prefix={prefix}, {len(config)} 项")


# ============================================================
# 辅助函数
# ============================================================

from services.utils import parse_any_date as _parse_any_date


def _days_since(value) -> Optional[int]:
    dt = _parse_any_date(value)
    if not dt:
        return None
    return max((datetime.now() - dt).days, 0)


def _report_recency_grade(days: Optional[int]) -> int:
    """
    披露时效等级（1 最优，5 最弱）。

    这里不再假设"越新越强"，而是根据历史 replay 结果使用
    "最佳披露窗口" 口径：
    - 46-60 天最强
    - 0-30 天次强
    - 61-90 天仍可用
    - 31-45 / 91-120 天偏弱
    - >120 天明显衰减
    """
    if days is None:
        return 5
    if days <= 30:
        return 2
    if days <= 45:
        return 4
    if days <= 60:
        return 1
    if days <= 90:
        return 3
    if days <= 120:
        return 4
    return 5


def _premium_grade(premium_pct: Optional[float]) -> int:
    if premium_pct is None:
        return 5
    if premium_pct <= 0:
        return 1
    if premium_pct <= 5:
        return 2
    if premium_pct <= 10:
        return 3
    if premium_pct <= 20:
        return 4
    return 5


def _premium_bucket_label(premium_pct: Optional[float]) -> str:
    if premium_pct is None:
        return "unknown"
    if premium_pct <= 0:
        return "neg"
    if premium_pct <= 10:
        return "0_10"
    return "gt10"


def _crowding_bucket(signal_count: Optional[int]) -> str:
    n = int(signal_count or 0)
    if n <= 1:
        return "solo"
    if n == 2:
        return "pair"
    return "crowded"


def _followability_grade(score: Optional[float], gate: Optional[str] = None) -> int:
    if score is None:
        grade = 3
    elif score >= 75:
        grade = 1
    elif score >= 65:
        grade = 2
    elif score >= 55:
        grade = 3
    elif score >= 45:
        grade = 4
    else:
        grade = 5

    if gate == "avoid":
        return 5
    if gate == "observe":
        return max(grade, 4)
    if gate == "watch":
        return max(grade, 3)
    return grade


def _reliability_grade(sample_events: Optional[int]) -> int:
    n = int(sample_events or 0)
    if n >= 60:
        return 1
    if n >= 25:
        return 2
    if n >= 10:
        return 3
    if n >= 5:
        return 4
    return 5


def _grade_from_raw(raw: Optional[float], thresholds: Tuple[float, float, float, float]) -> int:
    score = _safe_float(raw)
    if score is None:
        return 3
    if score >= thresholds[0]:
        return 1
    if score >= thresholds[1]:
        return 2
    if score >= thresholds[2]:
        return 3
    if score >= thresholds[3]:
        return 4
    return 5


def _crowding_sample_bonus(sample_count: int, max_bonus: float = 10.0) -> float:
    return min(int(sample_count or 0), 300) / 300.0 * max_bonus


def _crowding_fit_grade(raw: Optional[float]) -> int:
    return _grade_from_raw(raw, (50.0, 34.0, 20.0, 8.0))


def _crowding_yield_grade(raw: Optional[float]) -> int:
    return _grade_from_raw(raw, (38.0, 28.0, 18.0, 8.0))


def _crowding_stability_grade(raw: Optional[float]) -> int:
    return _grade_from_raw(raw, (34.0, 24.0, 14.0, 6.0))


def _crowding_yield_raw_from_stats(
    stats: Optional[dict], event_type: str, crowd_bucket: str, premium_bucket: str
) -> float:
    if stats:
        avg30 = _safe_float(stats.get("avg30")) or 0.0
        wr30 = _safe_float(stats.get("wr30")) or 50.0
        sample_bonus = _crowding_sample_bonus(int(stats.get("n") or 0), 8.0)
        raw = avg30 * 5.4 + max(wr30 - 50.0, 0.0) * 0.55 + sample_bonus
        return round(raw, 2)

    base = {
        "solo": 24.0,
        "pair": 21.0,
        "crowded": 15.0,
    }.get(crowd_bucket, 18.0)
    if premium_bucket == "neg":
        base += 4.0
    elif premium_bucket == "gt10":
        base -= 4.0
    if event_type == "new_entry":
        base += 2.0
    return round(base, 2)


def _crowding_stability_raw_from_stats(
    stats: Optional[dict], event_type: str, crowd_bucket: str, premium_bucket: str
) -> float:
    if stats:
        wr30 = _safe_float(stats.get("wr30")) or 50.0
        dd30 = _safe_float(stats.get("dd30"))
        if dd30 is None:
            dd30 = 15.0
        sample_bonus = _crowding_sample_bonus(int(stats.get("n") or 0), 8.0)
        raw = (wr30 - 50.0) * 1.55 + max(0.0, 18.0 - dd30) * 1.7 + sample_bonus
        return round(raw, 2)

    base = {
        "solo": 25.0,
        "pair": 22.0,
        "crowded": 16.0,
    }.get(crowd_bucket, 19.0)
    if premium_bucket == "neg":
        base += 2.0
    elif premium_bucket == "gt10":
        base -= 2.0
    if event_type == "new_entry":
        base += 1.5
    return round(base, 2)


def _crowding_fit_raw_from_stats(stats: Optional[dict], event_type: str,
                                 crowd_bucket: str, premium_bucket: str) -> float:
    if stats:
        avg30 = _safe_float(stats.get("avg30")) or 0.0
        wr30 = _safe_float(stats.get("wr30")) or 50.0
        dd30 = _safe_float(stats.get("dd30"))
        if dd30 is None:
            dd30 = 15.0
        sample_count = int(stats.get("n") or 0)
        sample_bonus = min(sample_count, 300) / 300.0 * 10.0
        raw = avg30 * 4.0 + (wr30 - 50.0) * 1.4 + max(0.0, 18.0 - dd30) * 1.2 + sample_bonus
        return round(raw, 2)

    base = {
        "solo": 26.0,
        "pair": 22.0,
        "crowded": 14.0,
    }.get(crowd_bucket, 18.0)
    if premium_bucket == "neg":
        base += 4.0
    elif premium_bucket == "gt10":
        base -= 4.0
    if event_type == "new_entry":
        base += 2.0
    return round(base, 2)


def _load_crowding_fit_lookup(conn) -> dict:
    lookup = {"full": {}, "skilled_l3": {}}

    rows = conn.execute("""
        WITH buy_events AS (
            SELECT stock_code, report_date, event_type, premium_pct, gain_30d, max_drawdown_30d
            FROM fact_institution_event
            WHERE event_type IN ('new_entry', 'increase') AND gain_30d IS NOT NULL
        ),
        crowd AS (
            SELECT stock_code, report_date, COUNT(*) AS signal_count
            FROM buy_events
            GROUP BY stock_code, report_date
        )
        SELECT b.event_type,
               CASE
                   WHEN c.signal_count <= 1 THEN 'solo'
                   WHEN c.signal_count = 2 THEN 'pair'
                   ELSE 'crowded'
               END AS crowd_bucket,
               CASE
                   WHEN b.premium_pct IS NULL THEN 'unknown'
                   WHEN b.premium_pct <= 0 THEN 'neg'
                   WHEN b.premium_pct <= 10 THEN '0_10'
                   ELSE 'gt10'
               END AS premium_bucket,
               COUNT(*) AS sample_count,
               AVG(b.gain_30d) AS avg_gain_30d,
               AVG(CASE WHEN b.gain_30d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_30d,
               AVG(b.max_drawdown_30d) AS avg_drawdown_30d
        FROM buy_events b
        JOIN crowd c USING (stock_code, report_date)
        GROUP BY b.event_type, crowd_bucket, premium_bucket
    """).fetchall()
    for row in rows:
        lookup["full"][(row["event_type"], row["crowd_bucket"], row["premium_bucket"])] = {
            "n": int(row["sample_count"] or 0),
            "avg30": _safe_float(row["avg_gain_30d"]),
            "wr30": _safe_float(row["win_rate_30d"]),
            "dd30": _safe_float(row["avg_drawdown_30d"]),
        }

    try:
        rows = conn.execute("""
            WITH perf AS (
                SELECT institution_id, industry_name
                FROM research_inst_industry_performance
                WHERE industry_level = 'L3'
                  AND buy_event_count >= 5
                  AND win_rate_30d >= 60
            ),
            buy_events AS (
                SELECT e.stock_code, e.report_date, e.event_type, e.premium_pct,
                       e.gain_30d, e.max_drawdown_30d, e.institution_id,
                       industry_dim.sw_level3
                FROM fact_institution_event e
                {industry_join}
                WHERE e.event_type IN ('new_entry', 'increase') AND e.gain_30d IS NOT NULL
            ),
            skilled AS (
                SELECT b.*
                FROM buy_events b
                JOIN perf p
                  ON p.institution_id = b.institution_id
                 AND p.industry_name = b.sw_level3
            ),
            crowd AS (
                SELECT stock_code, report_date, COUNT(*) AS signal_count
                FROM skilled
                GROUP BY stock_code, report_date
            )
            SELECT s.event_type,
                   CASE
                       WHEN c.signal_count <= 1 THEN 'solo'
                       WHEN c.signal_count = 2 THEN 'pair'
                       ELSE 'crowded'
                   END AS crowd_bucket,
                   CASE
                       WHEN s.premium_pct IS NULL THEN 'unknown'
                       WHEN s.premium_pct <= 0 THEN 'neg'
                       WHEN s.premium_pct <= 10 THEN '0_10'
                       ELSE 'gt10'
                   END AS premium_bucket,
                   COUNT(*) AS sample_count,
                   AVG(s.gain_30d) AS avg_gain_30d,
                   AVG(CASE WHEN s.gain_30d > 0 THEN 1.0 ELSE 0.0 END) * 100.0 AS win_rate_30d,
                   AVG(s.max_drawdown_30d) AS avg_drawdown_30d
            FROM skilled s
            JOIN crowd c USING (stock_code, report_date)
            GROUP BY s.event_type, crowd_bucket, premium_bucket
        """.format(industry_join=industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER"))).fetchall()
        for row in rows:
            lookup["skilled_l3"][(row["event_type"], row["crowd_bucket"], row["premium_bucket"])] = {
                "n": int(row["sample_count"] or 0),
                "avg30": _safe_float(row["avg_gain_30d"]),
                "wr30": _safe_float(row["win_rate_30d"]),
                "dd30": _safe_float(row["avg_drawdown_30d"]),
            }
    except Exception:
        logger.info("[评分] research_inst_industry_performance 不可用，crowding_fit 回退到全样本统计")

    return lookup


def _industry_edge_raw(stat: dict, baseline: dict) -> float:
    gain = _safe_float(stat.get("avg_gain_30d")) or 0.0
    win = _safe_float(stat.get("win_rate_30d")) or 0.0
    dd = _safe_float(stat.get("max_drawdown_30d")) or 0.0

    base_gain = _safe_float(baseline.get("buy_avg_gain_30d")) or 0.0
    base_win = _safe_float(baseline.get("buy_win_rate_30d")) or 0.0
    base_dd = _safe_float(baseline.get("buy_median_max_drawdown_30d"))
    if base_dd is None:
        base_dd = dd

    return round((gain - base_gain) * 2.5 + (win - base_win) * 0.35 + (base_dd - dd) * 0.8, 2)


def _industry_skill_grade(edge_raw: Optional[float]) -> int:
    if edge_raw is None:
        return 5
    if edge_raw >= 9:
        return 1
    if edge_raw >= 6:
        return 2
    if edge_raw >= 3:
        return 3
    if edge_raw >= 0:
        return 4
    return 5


def _setup_confidence_text(sample_events: Optional[int], edge_raw: Optional[float]) -> str:
    n = int(sample_events or 0)
    e = _safe_float(edge_raw) or 0.0
    if n >= 25 and e >= 6:
        return "高"
    if n >= 10 and e >= 3:
        return "中"
    return "低"


def _setup_priority_from_grades(level: str, event_type: str, report_grade: int,
                                premium_grade: int, follow_grade: int,
                                reliability_grade: int, crowding_yield_grade: int) -> int:
    priority_score = {
        "level3": 0.0,
        "level2": 0.8,
        "level1": 1.6,
    }.get(level, 2.0)
    priority_score += 0.0 if event_type == "new_entry" else 0.5
    priority_score += {1: 0.0, 2: 0.3, 3: 0.9, 4: 1.8, 5: 3.0}.get(premium_grade, 3.0)
    priority_score += {1: 0.0, 2: 0.4, 3: 0.9, 4: 1.6, 5: 2.5}.get(report_grade, 2.5)
    priority_score += {1: 0.0, 2: 0.4, 3: 0.8, 4: 1.4, 5: 2.2}.get(follow_grade, 2.2)
    priority_score += {1: 0.0, 2: 0.3, 3: 0.7, 4: 1.2, 5: 1.8}.get(reliability_grade, 1.8)
    priority_score += {1: 0.0, 2: 0.35, 3: 0.8, 4: 1.35, 5: 1.9}.get(crowding_yield_grade, 0.8)

    if priority_score <= 1.5:
        return 1
    if priority_score <= 3.0:
        return 2
    if priority_score <= 4.5:
        return 3
    if priority_score <= 6.0:
        return 4
    return 5


def _setup_execution_from_grades(
    source_gate: Optional[str],
    source_reason: Optional[str],
    premium_grade: int,
    follow_grade: int,
    stability_grade: int,
    reliability_grade: int,
    report_grade: int,
) -> Tuple[str, str]:
    source_rank = {
        "follow": 1,
        "watch": 2,
        "observe": 3,
        "avoid": 4,
    }.get(source_gate or "", 3)

    target_rank = 3
    target_reason = "稳健适配一般，先观察"
    if (
        stability_grade <= 2
        and follow_grade <= 2
        and premium_grade <= 2
        and reliability_grade <= 3
        and report_grade <= 3
    ):
        target_rank = 1
        target_reason = "稳健适配较强，执行条件较完整"
    elif (
        stability_grade <= 3
        and follow_grade <= 3
        and premium_grade <= 3
        and report_grade <= 4
    ):
        target_rank = 2
        target_reason = "稳健性尚可，可重点跟踪"

    final_rank = max(source_rank, target_rank)
    if premium_grade >= 4:
        final_rank = max(final_rank, 3)
        target_reason = "溢价偏高，先观察"
    elif report_grade >= 5:
        final_rank = max(final_rank, 3)
        target_reason = "披露过慢，先观察"
    elif source_rank > target_rank and source_gate:
        reason_map = {
            "watch": "来源机构门槛偏谨慎，先关注",
            "observe": "来源机构仅建议观察",
            "avoid": "来源机构建议回避",
        }
        target_reason = reason_map.get(source_gate, target_reason)

    gate = {
        1: "follow",
        2: "watch",
        3: "observe",
        4: "avoid",
    }.get(final_rank, "observe")
    if source_reason and gate == source_gate:
        return gate, source_reason
    return gate, target_reason


def _build_setup_reason(level: str, event_type: str, report_grade: int, premium_grade: int) -> str:
    level_label = {"level1": "L1有效命中", "level2": "L2有效命中", "level3": "L3有效命中"}.get(level, "行业命中")
    event_label = {"new_entry": "新进", "increase": "增持"}.get(event_type, event_type or "信号")
    report_label = {
        1: "披露时效佳",
        2: "披露较快",
        3: "披露适中",
        4: "披露偏慢",
        5: "披露过慢",
    }.get(report_grade, "披露适中")
    premium_label = {1: "低溢价", 2: "近成本", 3: "小幅溢价", 4: "中等溢价", 5: "高溢价"}.get(premium_grade, "溢价未知")
    return " · ".join([level_label, event_label, report_label, premium_label])


def _setup_sort_key(candidate: dict):
    return (
        candidate.get("setup_priority", 9),
        {"高": 0, "中": 1, "低": 2}.get(candidate.get("setup_confidence"), 3),
        candidate.get("premium_grade", 9),
        candidate.get("report_recency_grade", 9),
        -(candidate.get("setup_score_raw") or 0),
        -(candidate.get("hold_market_cap") or 0),
    )


def _evaluate_setup_candidate(holder: dict, profile: dict, stock_industry: dict,
                              industry_stats: dict, buy_signal_count: int,
                              crowding_lookup: dict):
    event_type = holder.get("event_type")
    if event_type not in ("new_entry", "increase"):
        return None

    follow_gate = holder.get("follow_gate")
    if follow_gate not in ("follow", "watch"):
        return None

    premium_pct = _safe_float(holder.get("premium_pct"))
    premium_grade = _premium_grade(premium_pct)
    if premium_grade >= 5:
        return None
    premium_bucket = _premium_bucket_label(premium_pct)
    crowd_bucket = _crowding_bucket(buy_signal_count)

    report_age_days = _days_since(holder.get("report_date"))
    report_grade = _report_recency_grade(report_age_days)
    follow_grade = _followability_grade(_safe_float(profile.get("followability_score")), follow_gate)

    for level, industry_name in (
        ("level3", stock_industry.get("sw_level3")),
        ("level2", stock_industry.get("sw_level2")),
        ("level1", stock_industry.get("sw_level1")),
    ):
        if not industry_name:
            continue
        stat = industry_stats.get((holder.get("institution_id"), level, industry_name))
        if not stat:
            continue
        sample_events = int(stat.get("sample_events") or 0)
        threshold = SETUP_LEVEL_THRESHOLDS[level]
        edge_raw = _industry_edge_raw(stat, profile)
        if sample_events < threshold["min_samples"] or edge_raw < threshold["min_edge_raw"]:
            continue

        industry_grade = _industry_skill_grade(edge_raw)
        reliability_grade = _reliability_grade(sample_events)
        skilled_stats = crowding_lookup.get("skilled_l3", {}).get((event_type, crowd_bucket, premium_bucket))
        full_stats = crowding_lookup.get("full", {}).get((event_type, crowd_bucket, premium_bucket))
        crowding_source = "full_sample"
        crowding_stats = full_stats
        if level == "level3" and skilled_stats and int(skilled_stats.get("n") or 0) >= 20:
            crowding_source = "l3_expert"
            crowding_stats = skilled_stats
        crowding_yield_raw = _crowding_yield_raw_from_stats(
            crowding_stats, event_type, crowd_bucket, premium_bucket
        )
        crowding_yield_grade = _crowding_yield_grade(crowding_yield_raw)
        crowding_stability_raw = _crowding_stability_raw_from_stats(
            crowding_stats, event_type, crowd_bucket, premium_bucket
        )
        crowding_stability_grade = _crowding_stability_grade(crowding_stability_raw)
        crowding_fit_raw = _crowding_fit_raw_from_stats(crowding_stats, event_type, crowd_bucket, premium_bucket)
        crowding_fit_grade = _crowding_fit_grade(crowding_fit_raw)
        crowding_fit_sample = int((crowding_stats or {}).get("n") or 0)
        setup_confidence = _setup_confidence_text(sample_events, edge_raw)
        setup_execution_gate, setup_execution_reason = _setup_execution_from_grades(
            follow_gate,
            holder.get("follow_gate_reason"),
            premium_grade,
            follow_grade,
            crowding_stability_grade,
            reliability_grade,
            report_grade,
        )
        setup_priority = _setup_priority_from_grades(
            level, event_type, report_grade, premium_grade, follow_grade, reliability_grade,
            crowding_yield_grade
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
            + (_safe_float(profile.get("followability_score")) or 50) * 0.25
            + (_safe_float(profile.get("quality_score")) or 50) * 0.15
            + (6 if event_type == "new_entry" else 3),
            2,
        )
        return {
            "setup_tag": "industry_expert_entry",
            "setup_priority": setup_priority,
            "setup_reason": _build_setup_reason(level, event_type, report_grade, premium_grade),
            "setup_confidence": setup_confidence,
            "setup_level": level,
            "setup_inst_id": holder.get("institution_id"),
            "setup_inst_name": holder.get("display_name") or holder.get("institution_id"),
            "setup_event_type": event_type,
            "setup_industry_name": industry_name,
            "setup_score_raw": setup_score_raw,
            "setup_execution_gate": setup_execution_gate,
            "setup_execution_reason": setup_execution_reason,
            "industry_skill_raw": edge_raw,
            "industry_skill_grade": industry_grade,
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
            "hold_market_cap": _safe_float(holder.get("hold_market_cap")) or 0.0,
        }

    return None


# ============================================================
# 机构评分
# ============================================================

def calculate_institution_scores(conn) -> int:
    """
    计算所有机构的 quality_score 并写入 mart_institution_profile。

    1. 加载配置
    2. 对每项指标做百分位排名归一化 (0-100)
    3. 加权求和
    4. 写回 quality_score

    返回评分机构数。
    """
    config = load_scoring_config(conn, "scoring.institution")
    follow_config = load_scoring_config(conn, "scoring.followability")
    logger.info(f"[评分] 机构评分开始, 权重: {config}")

    # Phase 1: 优先使用买入类指标（buy_event_count, buy_win_rate_*, buy_avg_gain_*）
    # 如果买入类字段不存在或全为空，回退到全事件指标
    profiles = conn.execute("""
        SELECT institution_id, total_events,
               avg_gain_30d, avg_gain_60d, avg_gain_120d,
               win_rate_30d, win_rate_60d, win_rate_90d,
               median_max_drawdown_30d, median_gain_30d,
               buy_event_count, buy_avg_gain_30d, buy_avg_gain_60d, buy_avg_gain_120d,
               buy_win_rate_30d, buy_win_rate_60d, buy_win_rate_120d,
               buy_median_max_drawdown_30d,
               avg_premium_pct, safe_follow_event_count, safe_follow_win_rate_30d,
               safe_follow_avg_gain_30d, safe_follow_avg_drawdown_30d,
               signal_transfer_efficiency_30d
        FROM mart_institution_profile
    """).fetchall()

    if not profiles:
        logger.warning("[评分] 无机构数据")
        return 0

    profiles = [dict(p) for p in profiles]
    n = len(profiles)

    # 判断是否有买入类数据
    has_buy_data = any(p.get("buy_event_count") and p["buy_event_count"] > 0 for p in profiles)
    if has_buy_data:
        logger.info("[评分] 使用买入类指标（new_entry + increase）")
    else:
        logger.info("[评分] 买入类指标为空，回退到全事件指标")

    # 提取各指标列（优先买入类）
    def _pick(p, buy_key, all_key):
        if has_buy_data and p.get(buy_key) is not None:
            return _safe_float(p[buy_key])
        return _safe_float(p.get(all_key))

    sample_vals = [_safe_float(p.get("buy_event_count") if has_buy_data else p["total_events"]) for p in profiles]
    gain_30d_vals = [_pick(p, "buy_avg_gain_30d", "avg_gain_30d") for p in profiles]
    gain_60d_vals = [_pick(p, "buy_avg_gain_60d", "avg_gain_60d") for p in profiles]
    gain_120d_vals = [_pick(p, "buy_avg_gain_120d", "avg_gain_120d") for p in profiles]
    wr_30d_vals = [_pick(p, "buy_win_rate_30d", "win_rate_30d") for p in profiles]
    wr_60d_vals = [_pick(p, "buy_win_rate_60d", "win_rate_60d") for p in profiles]
    wr_90d_vals = [_pick(p, "buy_win_rate_120d", "win_rate_90d") for p in profiles]
    # 回撤越小越好，取负值做排名
    dd_vals = []
    for p in profiles:
        dd_raw = _pick(p, "buy_median_max_drawdown_30d", "median_max_drawdown_30d")
        dd_vals.append(-dd_raw if dd_raw is not None else None)
    # 稳定性：中位数/均值比
    stability_vals = []
    for p in profiles:
        med = _safe_float(p.get("median_gain_30d"))
        avg = _pick(p, "buy_avg_gain_30d", "avg_gain_30d")
        if med is not None and avg is not None and avg != 0:
            stability_vals.append(1.0 - abs(med - avg) / (abs(avg) + 1e-9))
        else:
            stability_vals.append(None)

    # 百分位排名
    sample_ranks = _percentile_ranks(sample_vals)
    gain_30d_ranks = _percentile_ranks(gain_30d_vals)
    gain_60d_ranks = _percentile_ranks(gain_60d_vals)
    gain_120d_ranks = _percentile_ranks(gain_120d_vals)
    wr_30d_ranks = _percentile_ranks(wr_30d_vals)
    wr_60d_ranks = _percentile_ranks(wr_60d_vals)
    wr_90d_ranks = _percentile_ranks(wr_90d_vals)
    dd_ranks = _percentile_ranks(dd_vals)
    stability_ranks = _percentile_ranks(stability_vals)

    safe_sample_vals = [_safe_float(p.get("safe_follow_event_count")) for p in profiles]
    safe_wr30_vals = [_safe_float(p.get("safe_follow_win_rate_30d")) for p in profiles]
    safe_gain30_vals = [_safe_float(p.get("safe_follow_avg_gain_30d")) for p in profiles]
    safe_dd_vals = []
    for p in profiles:
        dd_raw = _safe_float(p.get("safe_follow_avg_drawdown_30d"))
        safe_dd_vals.append(-dd_raw if dd_raw is not None else None)
    transfer_eff_vals = [_safe_float(p.get("signal_transfer_efficiency_30d")) for p in profiles]
    avg_premium_vals = []
    for p in profiles:
        premium = _safe_float(p.get("avg_premium_pct"))
        avg_premium_vals.append(-premium if premium is not None else None)

    safe_sample_ranks = _percentile_ranks(safe_sample_vals)
    safe_wr30_ranks = _percentile_ranks(safe_wr30_vals)
    safe_gain30_ranks = _percentile_ranks(safe_gain30_vals)
    safe_dd_ranks = _percentile_ranks(safe_dd_vals)
    transfer_eff_ranks = _percentile_ranks(transfer_eff_vals)
    avg_premium_ranks = _percentile_ranks(avg_premium_vals)

    # 加权求和
    now = datetime.now().isoformat()
    updates = []
    for i in range(n):
        weighted_parts = [
            (sample_ranks[i], config.get("sample_weight", 0)),
            (gain_30d_ranks[i], config.get("gain_30d_weight", 0)),
            (gain_60d_ranks[i], config.get("gain_60d_weight", 0)),
            (gain_120d_ranks[i], config.get("gain_120d_weight", 0)),
            (wr_30d_ranks[i], config.get("win_rate_30d_weight", 0)),
            (wr_60d_ranks[i], config.get("win_rate_60d_weight", 0)),
            (wr_90d_ranks[i], config.get("win_rate_90d_weight", 0)),
            (dd_ranks[i], config.get("drawdown_weight", 0)),
            (stability_ranks[i], config.get("stability_weight", 0)),
        ]

        total_weight = 0
        total_score = 0.0
        for rank, weight in weighted_parts:
            if rank is not None and weight > 0:
                total_score += rank * weight
                total_weight += weight

        raw_score = round(total_score / total_weight, 2) if total_weight > 0 else None

        # Phase 1 fix: 低样本降权 — confidence_factor = min(1, sqrt(sample / 10))
        buy_cnt = profiles[i].get("buy_event_count") or profiles[i].get("total_events") or 0
        import math
        confidence_factor = min(1.0, math.sqrt(buy_cnt / 10.0)) if buy_cnt > 0 else 0
        score = round(raw_score * confidence_factor, 2) if raw_score is not None else None

        # 评分来源标注
        score_basis = "buy" if has_buy_data and (profiles[i].get("buy_event_count") or 0) > 0 else "fallback_all"
        if buy_cnt >= 10:
            score_confidence = "high"
        elif buy_cnt >= 3:
            score_confidence = "medium"
        else:
            score_confidence = "low"

        follow_weighted_parts = [
            (safe_sample_ranks[i], follow_config.get("safe_sample_weight", 0)),
            (safe_wr30_ranks[i], follow_config.get("safe_win_rate_30d_weight", 0)),
            (safe_gain30_ranks[i], follow_config.get("safe_gain_30d_weight", 0)),
            (safe_dd_ranks[i], follow_config.get("safe_drawdown_weight", 0)),
            (transfer_eff_ranks[i], follow_config.get("transfer_efficiency_weight", 0)),
            (avg_premium_ranks[i], follow_config.get("avg_premium_weight", 0)),
        ]
        follow_total_weight = 0
        follow_total_score = 0.0
        for rank, weight in follow_weighted_parts:
            if rank is not None and weight > 0:
                follow_total_score += rank * weight
                follow_total_weight += weight

        follow_raw_score = round(follow_total_score / follow_total_weight, 2) if follow_total_weight > 0 else None
        safe_cnt = profiles[i].get("safe_follow_event_count") or 0
        follow_conf_factor = min(1.0, math.sqrt(safe_cnt / 10.0)) if safe_cnt > 0 else 0
        follow_score = round(follow_raw_score * follow_conf_factor, 2) if follow_raw_score is not None else None
        if safe_cnt >= 10:
            follow_confidence = "high"
        elif safe_cnt >= 3:
            follow_confidence = "medium"
        else:
            follow_confidence = "low"

        updates.append((score, follow_score, score_basis, score_confidence, follow_confidence, now, profiles[i]["institution_id"]))

    conn.executemany(
        "UPDATE mart_institution_profile SET quality_score = ?, followability_score = ?, score_basis = ?, "
        "score_confidence = ?, followability_confidence = ?, updated_at = ? WHERE institution_id = ?",
        updates
    )
    conn.commit()

    scored = sum(1 for u in updates if u[0] is not None)

    # Phase 1: 填充 top_industry 字段
    # main_industry: 当前持仓中频次最高的行业（暴露）
    # best_industry: 历史表现最好的行业（能力）
    _fill_top_industries(conn)

    logger.info(f"[评分] 机构评分完成: {scored}/{n}")
    return scored


def _fill_top_industries(conn):
    """
    填充 mart_institution_profile 的 main_industry_1/2/3 和 best_industry_1/2/3。
    main = 按当前持仓行业频次排序
    best = 按历史 buy_event 表现排序（avg_gain_30d * win_rate_30d, 最低样本 >=3）
    """
    now = datetime.now().isoformat()
    institutions = conn.execute(
        "SELECT institution_id FROM mart_institution_profile"
    ).fetchall()

    for inst in institutions:
        iid = inst["institution_id"]

        # main_industry: 从 mart_current_relationship 按行业频次
        main_rows = conn.execute("""
            SELECT sw_level2, COUNT(*) as cnt
            FROM mart_current_relationship
            WHERE institution_id = ? AND sw_level2 IS NOT NULL AND sw_level2 != ''
            GROUP BY sw_level2 ORDER BY cnt DESC LIMIT 3
        """, (iid,)).fetchall()
        main = [r["sw_level2"] for r in main_rows]

        # best_industry: 从 mart_institution_industry_stat 按表现排序
        best_rows = conn.execute("""
            SELECT industry_name,
                   COALESCE(avg_gain_30d, 0) * COALESCE(win_rate_30d, 0) / 100.0 as perf
            FROM mart_institution_industry_stat
            WHERE institution_id = ? AND sw_level = 'level2' AND sample_events >= 3
            ORDER BY perf DESC LIMIT 3
        """, (iid,)).fetchall()
        best = [r["industry_name"] for r in best_rows]

        # 计算持仓集中度（top1 行业占比）
        total = conn.execute(
            "SELECT COUNT(*) FROM mart_current_relationship WHERE institution_id = ?",
            (iid,)
        ).fetchone()[0]
        concentration = round(main_rows[0]["cnt"] / total * 100, 1) if main_rows and total > 0 else None

        conn.execute("""
            UPDATE mart_institution_profile SET
                main_industry_1 = ?, main_industry_2 = ?, main_industry_3 = ?,
                best_industry_1 = ?, best_industry_2 = ?, best_industry_3 = ?,
                concentration = ?, updated_at = ?
            WHERE institution_id = ?
        """, (
            main[0] if len(main) > 0 else None,
            main[1] if len(main) > 1 else None,
            main[2] if len(main) > 2 else None,
            best[0] if len(best) > 0 else None,
            best[1] if len(best) > 1 else None,
            best[2] if len(best) > 2 else None,
            concentration, now, iid
        ))

    conn.commit()
    logger.info(f"[评分] 行业字段填充完成: {len(institutions)} 个机构")


# ============================================================
# 路径分类
# ============================================================

def classify_price_path(conn, stock_code: str, notice_date: str) -> str:
    """
    分类公告日以来的价格路径。

    根据 K线数据计算总涨幅、最大涨幅、最大回撤，
    返回: "未充分演绎" | "温和验证" | "震荡待定" | "已充分演绎" | "失效破坏"
    """
    thresholds = PATH_THRESHOLDS

    from services.market_db import get_market_conn, get_kline_range
    from datetime import datetime as _dt
    mkt_conn = get_market_conn()
    try:
        today = _dt.now().strftime("%Y-%m-%d")
        klines = get_kline_range(mkt_conn, stock_code, notice_date, today, freq="daily")
    finally:
        mkt_conn.close()

    if not klines or len(klines) < 2:
        return "未充分演绎"

    first_close = _safe_float(klines[0]["close"])
    if not first_close or first_close <= 0:
        return "未充分演绎"

    last_close = _safe_float(klines[-1]["close"])
    total_gain = ((last_close - first_close) / first_close * 100) if last_close else 0

    # 最大涨幅和最大回撤
    peak = first_close
    max_gain = 0.0
    max_drawdown = 0.0

    for k in klines:
        high = _safe_float(k["high"]) or _safe_float(k["close"]) or 0
        low = _safe_float(k["low"]) or _safe_float(k["close"]) or 0
        close = _safe_float(k["close"]) or 0

        gain_from_start = (high - first_close) / first_close * 100
        if gain_from_start > max_gain:
            max_gain = gain_from_start

        if close > peak:
            peak = close
        if peak > 0:
            dd = (peak - low) / peak * 100
            if dd > max_drawdown:
                max_drawdown = dd

    broken_dd = thresholds.get("broken_drawdown", 15)
    mild_max = thresholds.get("mild_gain_max", 10)
    warm_max = thresholds.get("warm_gain_max", 30)
    exhausted_min = thresholds.get("exhausted_min", 30)

    # 失效破坏：大幅回撤
    if max_drawdown >= broken_dd and total_gain < 0:
        return "失效破坏"

    # 已充分演绎：涨幅已经很大
    if max_gain >= exhausted_min:
        return "已充分演绎"

    # 温和验证：有一定涨幅
    if max_gain >= mild_max:
        return "温和验证"

    # 未充分演绎：涨幅很小
    if max_gain < mild_max:
        return "未充分演绎"

    return "震荡待定"

# ============================================================
# 四层评分辅助
# ============================================================

def _clamp_score(value: Optional[float], low: float = 0.0, high: float = 100.0) -> float:
    """本模块大量使用 low/high 参数名，保持兼容。"""
    if value is None:
        return low
    return round(max(min(float(value), high), low), 2)


def _quantile(values: list, q: float) -> Optional[float]:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    pos = max(min(q, 1.0), 0.0) * (len(vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def _score_ge(value: Optional[float], rules: Tuple[Tuple[float, float], ...], default: float = 0.0) -> float:
    if value is None:
        return default
    for threshold, score in rules:
        if value >= threshold:
            return score
    return default


def _score_le(value: Optional[float], rules: Tuple[Tuple[float, float], ...], default: float = 0.0) -> float:
    if value is None:
        return default
    for threshold, score in rules:
        if value <= threshold:
            return score
    return default


def _rank_score(rank: Optional[float], max_score: float) -> float:
    if rank is None:
        return max_score * 0.5
    return round(max_score * max(min(rank, 100.0), 0.0) / 100.0, 2)


def _prefer_numeric(primary, fallback):
    return fallback if primary is None else primary


def _top_reasons(items: list, limit: int = 3) -> str:
    picked = []
    for item in items:
        text = str(item or "").strip()
        if text and text not in picked:
            picked.append(text)
        if len(picked) >= limit:
            break
    return "；".join(picked)


def _pool_sort_key(pool: Optional[str]) -> int:
    return {
        "A池": 0,
        "B池": 1,
        "C池": 2,
        "D池": 3,
    }.get(pool or "", 9)


def _attention_participation_pct(value: Optional[float]) -> Optional[float]:
    score = _safe_float(value)
    if score is None:
        return None
    if -1.5 <= score <= 1.5:
        score *= 100.0
    return _clamp_score(score)


def _attention_survey_activity_score(
    survey_count_30d: int,
    survey_count_90d: int,
    survey_org_total_30d: int,
    survey_org_total_90d: int,
) -> Optional[float]:
    if max(survey_count_30d, survey_count_90d, survey_org_total_30d, survey_org_total_90d) <= 0:
        return None

    raw = 26.0
    raw += min(survey_count_30d, 4) * 10.0
    raw += min(max(survey_count_90d - survey_count_30d, 0), 8) * 3.0
    raw += min(survey_org_total_30d, 40) * 0.55
    raw += min(survey_org_total_90d, 100) * 0.10
    return _clamp_score(raw)


def _external_attention_score(attention_row: Optional[dict]) -> Optional[float]:
    if not attention_row:
        return None

    comment_available = int(attention_row.get("comment_available") or 0)
    survey_available = int(attention_row.get("survey_available") or 0)
    focus_index = _safe_float(attention_row.get("focus_index"))
    composite_score = _safe_float(attention_row.get("composite_score"))
    participation = _attention_participation_pct(attention_row.get("institution_participation"))
    survey_count_30d = int(attention_row.get("survey_count_30d") or 0)
    survey_count_90d = int(attention_row.get("survey_count_90d") or 0)
    survey_org_total_30d = int(attention_row.get("survey_org_total_30d") or 0)
    survey_org_total_90d = int(attention_row.get("survey_org_total_90d") or 0)
    survey_score = _attention_survey_activity_score(
        survey_count_30d,
        survey_count_90d,
        survey_org_total_30d,
        survey_org_total_90d,
    )

    weighted = []
    if comment_available or any(value is not None for value in (focus_index, composite_score, participation)):
        if composite_score is not None:
            weighted.append((0.42, _clamp_score(composite_score)))
        if focus_index is not None:
            weighted.append((0.30, _clamp_score(focus_index)))
        if participation is not None:
            weighted.append((0.28, participation))
    if survey_available and survey_score is not None:
        weighted.append((0.18, survey_score))
    if not weighted:
        return None
    total_weight = sum(weight for weight, _ in weighted)
    return round(sum(weight * value for weight, value in weighted) / total_weight, 2)


def _external_attention_boost(attention_score: Optional[float], survey_count_30d: int) -> float:
    if attention_score is None:
        return 0.0
    boost = max(attention_score - 55.0, 0.0) * 0.18
    if survey_count_30d >= 2:
        boost += 0.8
    elif survey_count_30d >= 1:
        boost += 0.4
    return round(min(boost, 8.0), 2)


def _external_crowding_penalty(
    attention_row: Optional[dict],
    stage_score: Optional[float],
    price_20d: Optional[float],
    price_1m: Optional[float],
) -> float:
    if not attention_row:
        return 0.0

    focus_index = _safe_float(attention_row.get("focus_index"))
    turnover_rate = _safe_float(attention_row.get("turnover_rate"))
    participation = _attention_participation_pct(attention_row.get("institution_participation"))
    rank_change = _safe_float(attention_row.get("rank_change"))
    survey_count_30d = int(attention_row.get("survey_count_30d") or 0)

    penalty = 0.0
    penalty += _score_ge(focus_index, ((90, 3.5), (85, 2.0), (80, 1.0)), 0.0)
    penalty += _score_ge(turnover_rate, ((10, 2.5), (6, 1.5), (4, 0.5)), 0.0)
    penalty += _score_ge(participation, ((45, 2.0), (35, 1.0)), 0.0)
    penalty += _score_ge(rank_change, ((1200, 2.0), (500, 1.2), (200, 0.6)), 0.0)
    penalty += _score_ge(float(survey_count_30d), ((3, 1.2), (1, 0.4)), 0.0)
    penalty += _score_ge(stage_score, ((68, 1.5), (60, 0.8)), 0.0)
    penalty += _score_ge(price_20d, ((25, 1.5), (15, 0.8)), 0.0)
    penalty += _score_ge(price_1m, ((30, 1.2), (18, 0.6)), 0.0)
    return round(min(penalty, 10.0), 2)


def _external_attention_signal(
    attention_score: Optional[float],
    crowding_penalty: float,
    survey_count_30d: int,
    survey_count_90d: int,
) -> Optional[str]:
    if crowding_penalty >= 6 and (attention_score or 0) >= 60:
        return "热度拥挤"
    if attention_score is not None:
        if attention_score >= 72:
            return "外部确认增强"
        if attention_score >= 60:
            return "关注度抬升"
    if survey_count_30d >= 2 or survey_count_90d >= 4:
        return "调研活跃"
    return None


# ============================================================
# 股票评分
# ============================================================

def calculate_stock_scores(conn) -> int:
    """
    计算股票行动评分。

    1. 加载配置
    2. 对每只股票计算：龙头机构质量、行业命中、事件类型、共识度、时效性、数据可信度
    3. 应用扣分项
    4. 确定时机标签
    5. 返回评分结果列表（由调用方决定存储位置）

    返回评分股票数。
    """
    # legacy action_score 仅保留给验证/兼容链路，权重已冻结到内置默认值
    config = dict(STOCK_SCORE_DEFAULTS)
    path_thresholds = PATH_THRESHOLDS
    logger.info(f"[评分] 股票评分开始")

    for column_def in [
        "attention_comment_trade_date TEXT",
        "attention_focus_index REAL",
        "attention_composite_score REAL",
        "attention_institution_participation REAL",
        "attention_turnover_rate REAL",
        "attention_rank_change REAL",
        "attention_survey_count_30d INTEGER",
        "attention_survey_count_90d INTEGER",
        "attention_survey_org_total_30d INTEGER",
        "attention_survey_org_total_90d INTEGER",
        "external_attention_score REAL",
        "external_crowding_penalty REAL",
        "external_attention_signal TEXT",
    ]:
        try:
            conn.execute(f"ALTER TABLE mart_stock_trend ADD COLUMN {column_def}")
        except Exception:
            pass

    # 加载股票趋势数据
    stocks = conn.execute("""
        SELECT stock_code, stock_name, latest_notice_date, latest_events,
               latest_report_date, price_1m_pct, price_20d_pct, price_trend,
               qlib_rank, qlib_score, qlib_percentile
        FROM mart_stock_trend
    """).fetchall()

    if not stocks:
        logger.warning("[评分] 无股票趋势数据")
        return 0

    # 预加载机构分数
    inst_scores = {}
    inst_profiles = {}
    for row in conn.execute("""
        SELECT institution_id, quality_score, followability_score,
               total_events, buy_event_count,
               buy_avg_gain_30d, buy_win_rate_30d, buy_median_max_drawdown_30d
        FROM mart_institution_profile
    """).fetchall():
        d = dict(row)
        inst_profiles[d["institution_id"]] = d
        if d.get("quality_score") is not None:
            inst_scores[d["institution_id"]] = _safe_float(d["quality_score"])

    industry_stats = {}
    for row in conn.execute("""
        SELECT institution_id, sw_level, industry_name, sample_events,
               avg_gain_30d, win_rate_30d, max_drawdown_30d
        FROM mart_institution_industry_stat
    """).fetchall():
        d = dict(row)
        industry_stats[(d["institution_id"], d["sw_level"], d["industry_name"])] = d

    crowding_lookup = _load_crowding_fit_lookup(conn)

    # Phase 1: 预加载机构最佳行业 — 改为 sw_level2，按表现排序（非样本数）
    inst_best_industry = {}
    for row in conn.execute("""
        SELECT institution_id, industry_name,
               ROW_NUMBER() OVER (
                   PARTITION BY institution_id
                   ORDER BY (COALESCE(avg_gain_30d, 0) * COALESCE(win_rate_30d, 0)) DESC
               ) as rn
        FROM mart_institution_industry_stat
        WHERE sw_level = 'level2' AND sample_events >= 3
    """).fetchall():
        if row["rn"] == 1:
            inst_best_industry[row["institution_id"]] = row["industry_name"]
    # 如果 level2 数据不足，回退到 level1
    if not inst_best_industry:
        for row in conn.execute("""
            SELECT institution_id, industry_name,
                   ROW_NUMBER() OVER (PARTITION BY institution_id ORDER BY sample_events DESC) as rn
            FROM mart_institution_industry_stat
            WHERE sw_level = 'level1'
        """).fetchall():
            if row["rn"] == 1:
                inst_best_industry[row["institution_id"]] = row["industry_name"]
        logger.info("[评分] sw_level2 数据不足，回退到 sw_level1 行业匹配")

    # Phase 1: 预加载股票行业 — 改为 sw_level2 主导
    from services.industry import load_industry_map
    _ind_map = load_industry_map(conn)
    stock_industry = {}
    stock_industry_name = {}
    for code, ind in _ind_map.items():
        stock_industry[code] = ind
        stock_industry_name[code] = ind.get("sw_level2") or ind.get("sw_level1", "")

    # 财务快照：v1 质量分使用最新财务快照 + 行业相对分位
    financial_by_stock = {}
    fin_groups = {("all", "all"): []}
    fin_pct_map = {}
    fin_group_sizes = {}
    fin_rows = conn.execute("""
        SELECT f.stock_code, f.latest_report_date, f.roe, f.debt_ratio, f.current_ratio,
               f.gross_margin, f.ocf_to_profit, f.contract_to_revenue,
               f.holder_count, f.holder_count_change_pct, f.float_shares, f.total_shares,
               i.sw_level1, i.sw_level2
        FROM dim_financial_latest f
        LEFT JOIN dim_stock_industry i ON i.stock_code = f.stock_code
    """).fetchall()
    for row in fin_rows:
        d = dict(row)
        financial_by_stock[d["stock_code"]] = d
        fin_groups[("all", "all")].append(d)
        if d.get("sw_level2"):
            fin_groups.setdefault(("l2", d["sw_level2"]), []).append(d)
        if d.get("sw_level1"):
            fin_groups.setdefault(("l1", d["sw_level1"]), []).append(d)

    fin_metrics = {
        "roe": False,
        "gross_margin": False,
        "ocf_to_profit": False,
        "debt_ratio": True,
        "current_ratio": False,
        "contract_to_revenue": True,
    }
    for (level, group_name), rows in fin_groups.items():
        fin_group_sizes[(level, group_name)] = len(rows)
        for metric, reverse in fin_metrics.items():
            values = [_safe_float(r.get(metric)) for r in rows]
            if reverse:
                values = [(-v if v is not None else None) for v in values]
            ranks = _percentile_ranks(values)
            for r, rank in zip(rows, ranks):
                if rank is not None:
                    fin_pct_map[(level, group_name, metric, r["stock_code"])] = rank

    def _fin_rank(stock_code: str, metric: str, sw_level2: Optional[str], sw_level1: Optional[str]) -> Optional[float]:
        if sw_level2 and fin_group_sizes.get(("l2", sw_level2), 0) >= 15:
            rank = fin_pct_map.get(("l2", sw_level2, metric, stock_code))
            if rank is not None:
                return rank
        if sw_level1 and fin_group_sizes.get(("l1", sw_level1), 0) >= 20:
            rank = fin_pct_map.get(("l1", sw_level1, metric, stock_code))
            if rank is not None:
                return rank
        return fin_pct_map.get(("all", "all", metric, stock_code))

    capital_by_stock = {}
    try:
        cap_rows = conn.execute("""
            SELECT stock_code, listed_days, cumulative_dividend, avg_annual_dividend,
                   dividend_count, financing_total, financing_count, dividend_financing_ratio,
                   repurchase_count_3y, repurchase_amount_3y, repurchase_ratio_sum_3y,
                   active_repurchase_count, future_unlock_count_180d, future_unlock_ratio_180d,
                   future_unlock_count_365d, future_unlock_ratio_365d,
                   last_dividend_notice_date, dividend_cash_sum_5y, dividend_event_count_5y,
                   dividend_implemented_count_5y, last_allotment_notice_date,
                   allotment_count_5y, allotment_ratio_sum_5y, allotment_raised_funds_5y
            FROM dim_capital_behavior_latest
        """).fetchall()
        for row in cap_rows:
            d = dict(row)
            capital_by_stock[d["stock_code"]] = d
    except Exception:
        capital_by_stock = {}

    indicator_by_stock = {}
    indicator_groups = {("all", "all"): []}
    indicator_pct_map = {}
    indicator_group_sizes = {}
    try:
        indicator_rows = conn.execute("""
            SELECT f.stock_code, f.latest_report_date, f.roe_ak, f.roa_ak, f.gross_margin_ak,
                   f.net_margin_ak, f.current_ratio_ak, f.quick_ratio_ak, f.debt_ratio_ak,
                   f.asset_turnover_ak, f.inventory_turnover_ak, f.receivables_turnover_ak,
                   f.revenue_growth_yoy_ak, f.net_profit_growth_yoy_ak, i.sw_level1, i.sw_level2
            FROM dim_financial_indicator_latest f
            LEFT JOIN dim_stock_industry i ON i.stock_code = f.stock_code
        """).fetchall()
        for row in indicator_rows:
            d = dict(row)
            indicator_by_stock[d["stock_code"]] = d
            indicator_groups[("all", "all")].append(d)
            if d.get("sw_level2"):
                indicator_groups.setdefault(("l2", d["sw_level2"]), []).append(d)
            if d.get("sw_level1"):
                indicator_groups.setdefault(("l1", d["sw_level1"]), []).append(d)
        indicator_metrics = {
            "roa_ak": False,
            "asset_turnover_ak": False,
            "inventory_turnover_ak": False,
            "receivables_turnover_ak": False,
            "quick_ratio_ak": False,
            "debt_ratio_ak": True,
        }
        for (level, group_name), rows in indicator_groups.items():
            indicator_group_sizes[(level, group_name)] = len(rows)
            for metric, reverse in indicator_metrics.items():
                values = [_safe_float(r.get(metric)) for r in rows]
                if reverse:
                    values = [(-v if v is not None else None) for v in values]
                ranks = _percentile_ranks(values)
                for r, rank in zip(rows, ranks):
                    if rank is not None:
                        indicator_pct_map[(level, group_name, metric, r["stock_code"])] = rank
    except Exception:
        indicator_by_stock = {}
        indicator_groups = {("all", "all"): []}
        indicator_pct_map = {}
        indicator_group_sizes = {}

    def _indicator_rank(stock_code: str, metric: str, sw_level2: Optional[str], sw_level1: Optional[str]) -> Optional[float]:
        if sw_level2 and indicator_group_sizes.get(("l2", sw_level2), 0) >= 15:
            rank = indicator_pct_map.get(("l2", sw_level2, metric, stock_code))
            if rank is not None:
                return rank
        if sw_level1 and indicator_group_sizes.get(("l1", sw_level1), 0) >= 20:
            rank = indicator_pct_map.get(("l1", sw_level1, metric, stock_code))
            if rank is not None:
                return rank
        return indicator_pct_map.get(("all", "all", metric, stock_code))

    quality_feature_by_stock = {}
    try:
        qf_rows = conn.execute("""
            SELECT stock_code, quality_profit_raw, quality_cash_raw, quality_balance_raw,
                   quality_margin_raw, quality_contract_raw, quality_freshness_raw,
                   quality_capital_raw, quality_efficiency_raw, quality_growth_raw,
                   quality_score_v1
            FROM dim_stock_quality_latest
        """).fetchall()
        for row in qf_rows:
            quality_feature_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        quality_feature_by_stock = {}

    archetype_by_stock = {}
    try:
        archetype_rows = conn.execute("""
            SELECT stock_code, stock_archetype, archetype_confidence, archetype_reason,
                   high_quality_hits, growth_hits, cycle_flags, financial_history_rows,
                   yoy_history_rows, net_profit_positive_8q, operating_cashflow_positive_8q,
                   revenue_yoy_positive_4q, profit_yoy_positive_4q, eps_yoy_positive_4q,
                   revenue_yoy_median_4q, profit_yoy_median_4q, revenue_yoy_std_4q,
                   profit_yoy_std_4q, latest_revenue_yoy, latest_profit_yoy,
                   revenue_yoy_down_streak_2q, profit_yoy_down_streak_2q,
                   net_profit_sign_switch_8q, inventory_revenue_vol_4q,
                   total_shares_growth_3y
            FROM dim_stock_archetype_latest
        """).fetchall()
        for row in archetype_rows:
            archetype_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        archetype_by_stock = {}

    stage_feature_by_stock = {}
    try:
        stage_rows = conn.execute("""
            SELECT stock_code, path_state, path_max_gain_pct, path_max_drawdown_pct,
                   return_1m, return_3m, return_6m, return_12m,
                   dist_ma120_pct, dist_ma250_pct, above_ma250, max_drawdown_60d,
                   amount_ratio_20_120, volatility_20d, amplitude_20d,
                   stock_gate, generic_stage_raw, stage_type_adjust_raw,
                   stage_quality_overheat_penalty, stage_growth_slowdown_penalty,
                   stage_growth_stretch_penalty, stage_cycle_realization_penalty,
                   stage_cycle_uncertainty_penalty, stage_score_v1, stage_reason
            FROM dim_stock_stage_latest
        """).fetchall()
        for row in stage_rows:
            stage_feature_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        stage_feature_by_stock = {}

    forecast_feature_by_stock = {}
    try:
        forecast_rows = conn.execute("""
            SELECT stock_code, model_id, qlib_score, qlib_rank, qlib_percentile,
                   industry_qlib_percentile, forecast_20d_score,
                   forecast_60d_excess_score, forecast_risk_adjusted_score,
                   forecast_score_v1, forecast_reason
            FROM dim_stock_forecast_latest
        """).fetchall()
        for row in forecast_rows:
            forecast_feature_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        forecast_feature_by_stock = {}

    industry_context_by_stock = {}
    try:
        ctx_rows = conn.execute("""
            SELECT stock_code, sector_momentum_score, sector_trend_state, sector_macd_cross,
                   sector_return_1m, sector_return_3m, sector_return_6m, sector_return_12m,
                   sector_excess_1m, sector_excess_3m, sector_excess_6m, sector_excess_12m,
                   dual_confirm_total, dual_confirm_recent_180d, industry_tailwind_score,
                   stage_industry_adjust_raw
            FROM dim_stock_industry_context_latest
        """).fetchall()
        for row in ctx_rows:
            industry_context_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        industry_context_by_stock = {}

    attention_by_stock = {}
    try:
        attention_rows = conn.execute("""
            SELECT stock_code, comment_trade_date, turnover_rate,
                   institution_participation, composite_score, rank_change,
                   focus_index, survey_count_30d, survey_count_90d,
                   survey_org_total_30d, survey_org_total_90d,
                   comment_available, survey_available
            FROM dim_stock_attention_latest
        """).fetchall()
        for row in attention_rows:
            attention_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        attention_by_stock = {}

    # Phase 1: 机构-股票持仓改从 mart_current_relationship 读取（单一真相源）
    stock_institutions = {}
    all_hold_ratios = []
    all_hold_caps = []
    for row in conn.execute("""
        SELECT stock_code, institution_id, display_name, event_type, notice_date,
               report_date, holder_rank, hold_ratio, hold_market_cap, change_pct,
               premium_pct, follow_gate
        FROM mart_current_relationship
        ORDER BY hold_market_cap DESC
    """).fetchall():
        sc = row["stock_code"]
        if sc not in stock_institutions:
            stock_institutions[sc] = []
        item = dict(row)
        stock_institutions[sc].append(item)
        all_hold_ratios.append(_safe_float(item.get("hold_ratio")))
        all_hold_caps.append(_safe_float(item.get("hold_market_cap")))

    hold_ratio_q40 = _quantile(all_hold_ratios, 0.40)
    hold_ratio_q60 = _quantile(all_hold_ratios, 0.60)
    hold_ratio_q80 = _quantile(all_hold_ratios, 0.80)
    hold_cap_q40 = _quantile(all_hold_caps, 0.40)
    hold_cap_q60 = _quantile(all_hold_caps, 0.60)
    hold_cap_q80 = _quantile(all_hold_caps, 0.80)

    # 高质量机构阈值（前 25%）
    all_scores = [s for s in inst_scores.values() if s is not None]
    quality_threshold = 0
    if all_scores:
        all_scores_sorted = sorted(all_scores)
        idx = int(len(all_scores_sorted) * 0.75)
        quality_threshold = all_scores_sorted[min(idx, len(all_scores_sorted) - 1)]

    now = datetime.now().isoformat()
    max_qlib_rank = max((_safe_float(s["qlib_rank"]) or 0 for s in stocks), default=0)
    results = []
    scored = 0

    for stock in stocks:
        stock = dict(stock)
        sc = stock["stock_code"]
        holders = stock_institutions.get(sc, [])

        if not holders:
            continue
        buy_signal_count = sum(
            1 for h in holders if h.get("event_type") in ("new_entry", "increase")
        )

        # --- 龙头机构 ---
        leader_inst = None
        leader_score = 0
        for h in holders:
            isc = inst_scores.get(h["institution_id"])
            if isc is not None and isc > leader_score:
                leader_score = isc
                leader_inst = h["institution_id"]

        leader_quality_norm = min(leader_score, 100)

        # --- 行业命中 / 发现层主背景 ---
        stock_ind = stock_industry.get(sc, {})
        stock_sw1 = stock_ind.get("sw_level1")
        stock_sw2 = stock_ind.get("sw_level2")
        industry_match_score = 0
        if leader_inst:
            stock_ind_name = stock_industry_name.get(sc)
            leader_ind = inst_best_industry.get(leader_inst)
            if stock_ind_name and leader_ind and stock_ind_name == leader_ind:
                industry_match_score = 100

        # --- 事件类型 ---
        best_event_score = 0
        for h in holders:
            et = h.get("event_type", "unchanged")
            es = EVENT_TYPE_SCORES.get(et, 0)
            if es > best_event_score:
                best_event_score = es

        # --- 共识度 ---
        consensus_count = sum(
            1 for h in holders
            if inst_scores.get(h["institution_id"], 0) and
            inst_scores[h["institution_id"]] >= quality_threshold
        )
        # 归一化：3个以上高质量机构持仓即满分
        consensus_norm = min(consensus_count / 3.0 * 100, 100) if consensus_count > 0 else 0

        # --- 时效性 ---
        notice_date = stock["latest_notice_date"]
        notice_age_days = _days_since(notice_date)
        if notice_age_days is None:
            notice_age_days = _days_since(stock["latest_report_date"])
        if notice_age_days is None:
            notice_age_days = 999

        # 越新越好：30天内满分，线性衰减到180天归零
        if notice_age_days <= 30:
            timeliness_norm = 100
        elif notice_age_days >= 180:
            timeliness_norm = 0
        else:
            timeliness_norm = round((180 - notice_age_days) / 150 * 100, 2)

        # --- 数据可信度 ---
        # 基于持仓机构数和事件数据完整度
        holder_count = len(holders)
        confidence_norm = min(holder_count / 5.0 * 100, 100) if holder_count > 0 else 0

        # --- 旧行动分：作为 Setup / Qlib 因子兼容层继续保留 ---
        parts = [
            (leader_quality_norm, config.get("leader_quality_weight", 0)),
            (industry_match_score, config.get("industry_match_weight", 0)),
            (best_event_score, config.get("event_type_weight", 0)),
            (consensus_norm, config.get("consensus_weight", 0)),
            (timeliness_norm, config.get("timeliness_weight", 0)),
            (confidence_norm, config.get("data_confidence_weight", 0)),
        ]
        total_weight = sum(w for _, w in parts if w > 0)
        raw_score = sum(v * w for v, w in parts if w > 0)
        base_score = round(raw_score / total_weight, 2) if total_weight > 0 else 0

        # --- 路径分类与扣分 ---
        stage_row = stage_feature_by_stock.get(sc) or {}
        path_state = stage_row.get("path_state") or "未充分演绎"
        if not stage_row and notice_date:
            ndt = _parse_any_date(notice_date)
            if ndt:
                path_state = classify_price_path(conn, sc, ndt.strftime("%Y-%m-%d"))

        penalty = 0

        # 过热扣分：多机构短期内扎堆
        if consensus_count >= 5 and notice_age_days <= 30:
            penalty += config.get("overheated_penalty", 0)

        # 冲突扣分：同时有增持和减持
        event_types_set = set(h.get("event_type") for h in holders)
        has_conflict = ("increase" in event_types_set or "new_entry" in event_types_set) and \
                       ("decrease" in event_types_set or "exit" in event_types_set)
        if has_conflict:
            penalty += config.get("conflict_penalty", 0)

        # 已充分演绎扣分
        if path_state == "已充分演绎":
            penalty += config.get("path_exhausted_penalty", 0)

        action_score = round(max(base_score - penalty, 0), 2)

        # --- Setup A 叠加层 ---
        best_setup = None
        for h in holders:
            profile = inst_profiles.get(h["institution_id"])
            if not profile:
                continue
            candidate = _evaluate_setup_candidate(
                h, profile, stock_ind, industry_stats, buy_signal_count, crowding_lookup
            )
            if candidate and (best_setup is None or _setup_sort_key(candidate) < _setup_sort_key(best_setup)):
                best_setup = candidate

        # ============================================================
        # 新四层评分：Discovery / Quality / Stage / Forecast
        # ============================================================

        # 1) Discovery Score
        industry_profile = None
        if leader_inst and stock_sw2:
            industry_profile = industry_stats.get((leader_inst, "level2", stock_sw2))
        if not industry_profile and leader_inst and stock_sw1:
            industry_profile = industry_stats.get((leader_inst, "level1", stock_sw1))

        profile = inst_profiles.get(leader_inst) if leader_inst else None
        ref_sample = int((industry_profile or {}).get("sample_events") or (profile or {}).get("buy_event_count") or 0)
        ref_win = _safe_float((industry_profile or {}).get("win_rate_30d"))
        if ref_win is None:
            ref_win = _safe_float((profile or {}).get("buy_win_rate_30d"))
        ref_gain = _safe_float((industry_profile or {}).get("avg_gain_30d"))
        if ref_gain is None:
            ref_gain = _safe_float((profile or {}).get("buy_avg_gain_30d"))

        discovery_skill = (
            _score_ge(ref_win, ((60.0, 20), (50.0, 16), (40.0, 10)), 4)
            + _score_ge(ref_gain, ((15.0, 10), (10.0, 8), (5.0, 6), (0.0, 3)), 1)
            + _score_ge(ref_sample, ((20, 10), (10, 7), (5, 4)), 1)
        )
        if ref_sample < 5:
            discovery_skill = min(discovery_skill, 24)

        discovery_fresh = (
            20 if notice_age_days <= 15 else
            16 if notice_age_days <= 30 else
            12 if notice_age_days <= 45 else
            8 if notice_age_days <= 60 else
            4
        )
        if stock["latest_notice_date"] in (None, "", "-") and stock["latest_report_date"]:
            discovery_fresh = round(discovery_fresh * 0.85, 2)

        max_hold_ratio = max((_safe_float(h.get("hold_ratio")) for h in holders), default=None)
        max_hold_cap = max((_safe_float(h.get("hold_market_cap")) for h in holders), default=None)
        best_rank = min((int(h.get("holder_rank")) for h in holders if h.get("holder_rank") not in (None, "")), default=None)
        hold_ratio_score = (
            8 if hold_ratio_q80 is not None and max_hold_ratio is not None and max_hold_ratio >= hold_ratio_q80 else
            6 if hold_ratio_q60 is not None and max_hold_ratio is not None and max_hold_ratio >= hold_ratio_q60 else
            4 if hold_ratio_q40 is not None and max_hold_ratio is not None and max_hold_ratio >= hold_ratio_q40 else
            2 if max_hold_ratio is not None else 0
        )
        hold_cap_score = (
            6 if hold_cap_q80 is not None and max_hold_cap is not None and max_hold_cap >= hold_cap_q80 else
            4 if hold_cap_q60 is not None and max_hold_cap is not None and max_hold_cap >= hold_cap_q60 else
            2 if hold_cap_q40 is not None and max_hold_cap is not None and max_hold_cap >= hold_cap_q40 else
            1 if max_hold_cap is not None else 0
        )
        holder_rank_score = 0
        if best_rank is not None:
            if best_rank <= 3:
                holder_rank_score = 6
            elif best_rank <= 6:
                holder_rank_score = 4
            elif best_rank <= 10:
                holder_rank_score = 2
        discovery_strength = hold_ratio_score + hold_cap_score + holder_rank_score

        positive_change = max((_safe_float(h.get("change_pct")) for h in holders if _safe_float(h.get("change_pct")) is not None), default=None)
        event_types = {h.get("event_type") for h in holders}
        if "new_entry" in event_types or (positive_change is not None and positive_change >= 10):
            discovery_direction = 20
        elif "increase" in event_types or (positive_change is not None and positive_change >= 3):
            discovery_direction = 14
        elif "unchanged" in event_types:
            discovery_direction = 8
        elif "decrease" in event_types:
            discovery_direction = 4
        else:
            discovery_direction = 0

        discovery_score = _clamp_score(
            discovery_skill + discovery_fresh + discovery_strength + discovery_direction
        )

        # 2) Quality Score
        fin = financial_by_stock.get(sc) or {}
        fin_report_days = _days_since(fin.get("latest_report_date"))
        roe = _safe_float(fin.get("roe"))
        debt_ratio = _safe_float(fin.get("debt_ratio"))
        current_ratio = _safe_float(fin.get("current_ratio"))
        gross_margin = _safe_float(fin.get("gross_margin"))
        ocf_to_profit = _safe_float(fin.get("ocf_to_profit"))
        contract_to_revenue = _safe_float(fin.get("contract_to_revenue"))
        capital = capital_by_stock.get(sc) or {}
        dividend_count = _safe_float(capital.get("dividend_count"))
        financing_count = _safe_float(capital.get("financing_count"))
        dividend_financing_ratio = _safe_float(capital.get("dividend_financing_ratio"))
        repurchase_count_3y = _safe_float(capital.get("repurchase_count_3y"))
        repurchase_amount_3y = _safe_float(capital.get("repurchase_amount_3y"))
        active_repurchase_count = _safe_float(capital.get("active_repurchase_count"))
        unlock_ratio_180d = _safe_float(capital.get("future_unlock_ratio_180d"))
        unlock_count_180d = _safe_float(capital.get("future_unlock_count_180d"))
        dividend_implemented_count_5y = _safe_float(capital.get("dividend_implemented_count_5y"))
        allotment_count_5y = _safe_float(capital.get("allotment_count_5y"))
        allotment_ratio_sum_5y = _safe_float(capital.get("allotment_ratio_sum_5y"))
        holder_count_change_pct = _safe_float(fin.get("holder_count_change_pct"))
        total_shares_growth_3y = _safe_float((archetype_by_stock.get(sc) or {}).get("total_shares_growth_3y"))
        indicator = indicator_by_stock.get(sc) or {}
        roa_ak = _safe_float(indicator.get("roa_ak"))
        asset_turnover_ak = _safe_float(indicator.get("asset_turnover_ak"))
        inventory_turnover_ak = _safe_float(indicator.get("inventory_turnover_ak"))
        receivables_turnover_ak = _safe_float(indicator.get("receivables_turnover_ak"))
        revenue_growth_yoy_ak = _safe_float(indicator.get("revenue_growth_yoy_ak"))
        net_profit_growth_yoy_ak = _safe_float(indicator.get("net_profit_growth_yoy_ak"))

        roe_rank = _fin_rank(sc, "roe", stock_sw2, stock_sw1)
        gm_rank = _fin_rank(sc, "gross_margin", stock_sw2, stock_sw1)
        ocf_rank = _fin_rank(sc, "ocf_to_profit", stock_sw2, stock_sw1)
        debt_rank = _fin_rank(sc, "debt_ratio", stock_sw2, stock_sw1)
        current_rank = _fin_rank(sc, "current_ratio", stock_sw2, stock_sw1)
        contract_rank = _fin_rank(sc, "contract_to_revenue", stock_sw2, stock_sw1)
        roa_rank = _indicator_rank(sc, "roa_ak", stock_sw2, stock_sw1)
        asset_turnover_rank = _indicator_rank(sc, "asset_turnover_ak", stock_sw2, stock_sw1)
        inventory_turnover_rank = _indicator_rank(sc, "inventory_turnover_ak", stock_sw2, stock_sw1)
        receivables_turnover_rank = _indicator_rank(sc, "receivables_turnover_ak", stock_sw2, stock_sw1)

        quality_profit = (
            _rank_score(roe_rank, 18)
            + _score_ge(roe, ((0.18, 12), (0.10, 9), (0.05, 6), (0.0, 3)), 0)
        )
        quality_cash = (
            _rank_score(ocf_rank, 12)
            + _score_ge(ocf_to_profit, ((1.2, 13), (0.9, 10), (0.7, 7), (0.4, 4), (0.0, 2)), 0)
        )
        quality_balance = (
            _rank_score(debt_rank, 10)
            + _score_le(debt_ratio, ((0.30, 5), (0.50, 4), (0.70, 2)), 0)
            + _score_ge(current_ratio, ((2.0, 10), (1.5, 8), (1.2, 6), (1.0, 3)), 0)
        )
        quality_margin = _rank_score(gm_rank, 10)
        quality_contract = (
            _rank_score(contract_rank, 3)
            + _score_le(contract_to_revenue, ((0.10, 2), (0.20, 1)), 0)
        ) if contract_to_revenue is not None else 2.5
        quality_freshness = _score_le(
            fin_report_days, ((120, 5), (210, 4), (330, 2)), 0
        )
        quality_capital = 0.0
        if dividend_count is not None:
            quality_capital += (
                2.0 if dividend_count >= 10 else
                1.0 if dividend_count >= 5 else
                0.5 if dividend_count >= 2 else 0.0
            )
        if dividend_implemented_count_5y is not None:
            quality_capital += (
                1.5 if dividend_implemented_count_5y >= 5 else
                1.0 if dividend_implemented_count_5y >= 3 else
                0.5 if dividend_implemented_count_5y >= 1 else 0.0
            )
        if dividend_financing_ratio is None:
            if (financing_count or 0) == 0 and (dividend_count or 0) >= 3:
                quality_capital += 2.0
        else:
            quality_capital += (
                3.0 if dividend_financing_ratio >= 1.0 else
                2.0 if dividend_financing_ratio >= 0.5 else
                1.0 if dividend_financing_ratio >= 0.2 else
                -1.5
            )
        if (repurchase_count_3y or 0) >= 2 or (repurchase_amount_3y or 0) >= 1e8:
            quality_capital += 2.5
        elif (repurchase_count_3y or 0) >= 1 or (active_repurchase_count or 0) >= 1:
            quality_capital += 1.0
        if unlock_ratio_180d is not None:
            quality_capital += (
                -4.0 if unlock_ratio_180d > 0.10 else
                -2.0 if unlock_ratio_180d > 0.05 else
                -1.0 if unlock_ratio_180d > 0.02 else
                0.5
            )
        elif (unlock_count_180d or 0) == 0:
            quality_capital += 0.5
        if allotment_count_5y is not None and allotment_count_5y > 0:
            quality_capital += (
                -2.0 if (allotment_ratio_sum_5y or 0) >= 1.5 else
                -1.0
            )
        if total_shares_growth_3y is not None:
            quality_capital += (
                1.5 if total_shares_growth_3y <= 0.05 else
                0.5 if total_shares_growth_3y <= 0.15 else
                -1.0 if total_shares_growth_3y <= 0.30 else
                -2.5
            )
        if holder_count_change_pct is not None:
            quality_capital += (
                1.0 if holder_count_change_pct <= -0.05 else
                0.5 if holder_count_change_pct <= 0.02 else
                -0.5 if holder_count_change_pct <= 0.10 else
                -1.5
            )
        quality_efficiency = (
            _rank_score(roa_rank, 8)
            + _rank_score(asset_turnover_rank, 4)
            + (
                _rank_score(inventory_turnover_rank, 2)
                if inventory_turnover_ak is not None else 1.0
            )
            + (
                _rank_score(receivables_turnover_rank, 2)
                if receivables_turnover_ak is not None else 1.0
            )
        )
        quality_growth = (
            _score_ge(revenue_growth_yoy_ak, ((20.0, 4), (10.0, 3), (0.0, 1)), 0)
            + _score_ge(net_profit_growth_yoy_ak, ((20.0, 4), (10.0, 3), (0.0, 1)), 0)
        )
        company_quality_score = _clamp_score(
            quality_profit + quality_cash + quality_balance
            + quality_margin + quality_contract + quality_freshness
            + quality_capital
            + quality_efficiency
            + quality_growth
        )
        if sc in quality_feature_by_stock:
            qf = quality_feature_by_stock[sc]
            quality_profit = _prefer_numeric(_safe_float(qf.get("quality_profit_raw")), quality_profit)
            quality_cash = _prefer_numeric(_safe_float(qf.get("quality_cash_raw")), quality_cash)
            quality_balance = _prefer_numeric(_safe_float(qf.get("quality_balance_raw")), quality_balance)
            quality_margin = _prefer_numeric(_safe_float(qf.get("quality_margin_raw")), quality_margin)
            quality_contract = _prefer_numeric(_safe_float(qf.get("quality_contract_raw")), quality_contract)
            quality_freshness = _prefer_numeric(_safe_float(qf.get("quality_freshness_raw")), quality_freshness)
            quality_capital = _prefer_numeric(_safe_float(qf.get("quality_capital_raw")), quality_capital)
            quality_efficiency = _prefer_numeric(_safe_float(qf.get("quality_efficiency_raw")), quality_efficiency)
            quality_growth = _prefer_numeric(_safe_float(qf.get("quality_growth_raw")), quality_growth)
            company_quality_score = _prefer_numeric(_safe_float(qf.get("quality_score_v1")), company_quality_score)

        # 3) Archetype
        archetype_row = archetype_by_stock.get(sc) or {}
        stock_archetype = archetype_row.get("stock_archetype")
        if not stock_archetype:
            if (
                company_quality_score >= 70
                and (roe or 0) > 0
                and (ocf_to_profit or 0) >= 0.8
                and (debt_rank or 0) >= 50
            ):
                stock_archetype = "高质量稳健型"
            elif (
                company_quality_score >= 55
                and (roe or 0) > 0
                and ((gm_rank or 0) >= 60 or stock.get("price_trend") == "连涨")
            ):
                stock_archetype = "成长兑现型"
            else:
                stock_archetype = "周期/事件驱动型"

        # 4) Stage Score
        industry_ctx = industry_context_by_stock.get(sc) or {}
        follow_count = sum(1 for h in holders if h.get("follow_gate") == "follow")
        watch_count = sum(1 for h in holders if h.get("follow_gate") == "watch")
        observe_count = sum(1 for h in holders if h.get("follow_gate") == "observe")
        avoid_count = sum(1 for h in holders if h.get("follow_gate") == "avoid")
        stock_gate = (
            "follow" if follow_count > 0 else
            "watch" if watch_count > 0 else
            "observe" if observe_count > 0 else
            "avoid" if avoid_count > 0 else None
        )

        price_20d = _safe_float(stock.get("price_20d_pct"))
        price_1m = _safe_float(stock.get("price_1m_pct"))
        if stage_row:
            path_state = stage_row.get("path_state") or path_state
            stock_gate = stage_row.get("stock_gate") or stock_gate
            stage_score = _safe_float(stage_row.get("stage_score_v1"))
            if stage_score is None:
                stage_score = _safe_float(stage_row.get("generic_stage_raw"))
            stage_score = _clamp_score(stage_score)
            if has_conflict:
                stage_score = _clamp_score(stage_score - 8)
        else:
            stage_score = 45.0
            stage_score += {
                "未充分演绎": 18,
                "温和验证": 12,
                "震荡待定": 6,
                "已充分演绎": -12,
                "失效破坏": -28,
            }.get(path_state, 0)
            stage_score += {
                "连涨": 6,
                "震荡": 3,
                "连跌": -8,
            }.get(stock.get("price_trend"), 0)

            if price_20d is not None:
                if -12 <= price_20d <= 15:
                    stage_score += 10
                elif 15 < price_20d <= 30:
                    stage_score += 4
                elif price_20d > 30:
                    stage_score -= 12
                elif price_20d < -20:
                    stage_score -= 10
                elif price_20d < -10:
                    stage_score -= 4
            if price_1m is not None:
                if -8 <= price_1m <= 18:
                    stage_score += 6
                elif price_1m > 35:
                    stage_score -= 8
                elif price_1m < -15:
                    stage_score -= 6

            stage_score += (
                10 if notice_age_days <= 30 else
                6 if notice_age_days <= 60 else
                2 if notice_age_days <= 120 else
                -4
            )
            stage_score += {
                "follow": 10,
                "watch": 5,
                "observe": 0,
                "avoid": -15,
                None: 0,
            }.get(stock_gate, 0)
            stage_score += _safe_float(industry_ctx.get("stage_industry_adjust_raw")) or 0
            if has_conflict:
                stage_score -= 8
            stage_score = _clamp_score(stage_score)

        # 5) Forecast Score（Qlib 只作为排序增强）
        forecast_row = forecast_feature_by_stock.get(sc) or {}
        qlib_percentile = _safe_float(forecast_row.get("qlib_percentile"))
        qlib_rank = _safe_float(forecast_row.get("qlib_rank"))
        if qlib_percentile is None:
            qlib_percentile = _safe_float(stock.get("qlib_percentile"))
            qlib_rank = _safe_float(stock.get("qlib_rank"))
        if qlib_percentile is None and qlib_rank is not None and max_qlib_rank and max_qlib_rank > 1:
            qlib_percentile = round((1 - (qlib_rank - 1) / (max_qlib_rank - 1)) * 100, 2)
        forecast_score = _safe_float(forecast_row.get("forecast_score_v1"))
        if forecast_score is None:
            forecast_score = _clamp_score(qlib_percentile if qlib_percentile is not None else 50.0)
        else:
            forecast_score = _clamp_score(forecast_score)
        forecast_score_effective = _clamp_score(
            forecast_score * max(stage_score / 60.0, 0.5)
        )

        attention_row = attention_by_stock.get(sc) or {}
        attention_comment_trade_date = attention_row.get("comment_trade_date")
        attention_focus_index = _safe_float(attention_row.get("focus_index"))
        attention_composite_score = _safe_float(attention_row.get("composite_score"))
        attention_institution_participation = _attention_participation_pct(
            attention_row.get("institution_participation")
        )
        attention_turnover_rate = _safe_float(attention_row.get("turnover_rate"))
        attention_rank_change = _safe_float(attention_row.get("rank_change"))
        attention_survey_count_30d = int(attention_row.get("survey_count_30d") or 0)
        attention_survey_count_90d = int(attention_row.get("survey_count_90d") or 0)
        attention_survey_org_total_30d = int(attention_row.get("survey_org_total_30d") or 0)
        attention_survey_org_total_90d = int(attention_row.get("survey_org_total_90d") or 0)
        external_attention_score = _external_attention_score(attention_row)
        external_attention_boost = _external_attention_boost(
            external_attention_score,
            attention_survey_count_30d,
        )
        external_crowding_penalty = _external_crowding_penalty(
            attention_row,
            stage_score,
            price_20d,
            price_1m,
        )
        external_attention_signal = _external_attention_signal(
            external_attention_score,
            external_crowding_penalty,
            attention_survey_count_30d,
            attention_survey_count_90d,
        )

        raw_composite_priority_score = _clamp_score(
            discovery_score * 0.35
            + company_quality_score * 0.30
            + stage_score * 0.20
            + forecast_score_effective * 0.15
        )
        composite_priority_score = _clamp_score(
            raw_composite_priority_score + external_attention_boost - external_crowding_penalty
        )
        promoted_by_external = (
            raw_composite_priority_score < 75 <= composite_priority_score
        )
        demoted_by_crowding = (
            raw_composite_priority_score >= 75
            and composite_priority_score < 75
            and external_crowding_penalty >= 6
        )
        composite_cap_score = None
        composite_cap_reasons = []
        composite_ceiling = None
        if stage_score < 40:
            composite_ceiling = 69.0 if composite_ceiling is None else min(composite_ceiling, 69.0)
            composite_cap_reasons.append("阶段分低于40，综合分封顶69")
        if company_quality_score < 45 and stock_archetype != "周期/事件驱动型":
            composite_ceiling = 64.0 if composite_ceiling is None else min(composite_ceiling, 64.0)
            composite_cap_reasons.append("质量分低于45，非周期/事件型综合分封顶64")
        if external_crowding_penalty >= 8:
            composite_ceiling = 69.0 if composite_ceiling is None else min(composite_ceiling, 69.0)
            composite_cap_reasons.append("外部热度拥挤，综合分封顶69")
        elif external_crowding_penalty >= 6 and stage_score < 60:
            composite_ceiling = 74.0 if composite_ceiling is None else min(composite_ceiling, 74.0)
            composite_cap_reasons.append("外部热度偏拥挤且阶段分不足60，综合分封顶74")
        if composite_ceiling is not None and composite_priority_score > composite_ceiling:
            composite_priority_score = min(composite_priority_score, composite_ceiling)
            composite_cap_score = composite_ceiling
            composite_cap_reason = "；".join(composite_cap_reasons[:2]) if composite_cap_reasons else None
        else:
            composite_cap_reason = None

        priority_pool_reason = None
        if stage_score < 40 or composite_priority_score < 45:
            priority_pool = "D池"
            if stage_score < 40:
                priority_pool_reason = "阶段分低于40，进入D池"
            else:
                priority_pool_reason = "综合优先分低于45，进入D池"
        elif (
            composite_priority_score >= 75
            and stage_score >= 50
            and company_quality_score >= 55
            and discovery_score >= 50
        ):
            priority_pool = "A池"
            if promoted_by_external:
                priority_pool_reason = "内部分接近A池，外部确认增强后进入A池"
            else:
                priority_pool_reason = "综合分达75且通过发现/质量/阶段门槛，进入A池"
        elif composite_priority_score >= 60:
            priority_pool = "B池"
            blockers = []
            if composite_priority_score >= 75 and discovery_score < 50:
                blockers.append("发现分不足50")
            if composite_priority_score >= 75 and company_quality_score < 55:
                blockers.append("质量分不足55")
            if composite_priority_score >= 75 and stage_score < 50:
                blockers.append("阶段分不足50")
            if blockers:
                priority_pool_reason = "综合分虽高，但未满足A池门槛：" + "、".join(blockers[:2])
            elif demoted_by_crowding:
                priority_pool_reason = "内部分已达A池，但外部热度拥挤导致降至B池"
            elif raw_composite_priority_score < 60 <= composite_priority_score and (external_attention_score or 0) >= 70:
                priority_pool_reason = "外部确认增强后进入B池"
            else:
                priority_pool_reason = "综合优先分介于60-75，进入B池"
        else:
            priority_pool = "C池"
            priority_pool_reason = "综合优先分介于45-60，进入C池"

        if priority_pool_reason and external_crowding_penalty >= 6 and not demoted_by_crowding:
            priority_pool_reason += "；外部热度拥挤"
        elif priority_pool_reason and (external_attention_score or 0) >= 72 and not promoted_by_external:
            priority_pool_reason += "；外部确认增强"

        highlight_reasons = []
        risk_reasons = []
        if discovery_skill >= 28:
            highlight_reasons.append("机构行业能力较强")
        if discovery_fresh >= 16:
            highlight_reasons.append("披露较新")
        if discovery_strength >= 14:
            highlight_reasons.append("持仓强度较高")
        if company_quality_score >= 70:
            highlight_reasons.append("公司质量稳健")
        elif company_quality_score >= 55:
            highlight_reasons.append("财务体质中上")
        if (_safe_float(archetype_row.get("archetype_confidence")) or 0) >= 70:
            highlight_reasons.append(f"{stock_archetype}特征清晰")
        if (_safe_float(stage_row.get("stage_type_adjust_raw")) or 0) >= 5:
            highlight_reasons.append("阶段结构较优")
        if quality_capital >= 4:
            highlight_reasons.append("资本纪律较好")
        if quality_efficiency >= 10:
            highlight_reasons.append("经营效率较强")
        if (_safe_float(industry_ctx.get("industry_tailwind_score")) or 0) >= 75:
            highlight_reasons.append("行业背景顺风")
        elif (_safe_float(industry_ctx.get("sector_excess_3m")) or 0) >= 8:
            highlight_reasons.append("行业近3月相对走强")
        elif (_safe_float(industry_ctx.get("dual_confirm_recent_180d")) or 0) >= 2:
            highlight_reasons.append("行业双重确认活跃")
        if external_attention_signal == "外部确认增强":
            highlight_reasons.append("外部确认增强")
        elif external_attention_signal == "关注度抬升":
            highlight_reasons.append("市场关注度抬升")
        elif external_attention_signal == "调研活跃":
            highlight_reasons.append("近期机构调研活跃")
        if attention_survey_count_30d >= 2:
            highlight_reasons.append("近30天机构调研活跃")
        elif attention_survey_count_90d >= 4:
            highlight_reasons.append("近90天持续有机构调研")
        if stage_score >= 65:
            highlight_reasons.append("阶段位置友好")
        if (_safe_float(forecast_row.get("forecast_20d_score")) or 0) >= 75:
            highlight_reasons.append("Qlib 20日预测较强")
        elif (_safe_float(forecast_row.get("forecast_60d_excess_score")) or 0) >= 70:
            highlight_reasons.append("行业相对预测较强")
        elif (_safe_float(forecast_row.get("forecast_risk_adjusted_score")) or 0) >= 70:
            highlight_reasons.append("波动收益性价比较好")
        elif qlib_percentile is not None and qlib_percentile >= 75:
            highlight_reasons.append("Qlib 排名靠前")

        if company_quality_score < 45:
            risk_reasons.append("公司质量偏弱")
        elif company_quality_score < 55 and composite_priority_score >= 75:
            risk_reasons.append("质量分未达A池门槛")
        if path_state == "已充分演绎":
            risk_reasons.append("价格已充分演绎")
        elif path_state == "失效破坏":
            risk_reasons.append("价格路径转坏")
        if stage_score < 40:
            risk_reasons.append("阶段分低于D池阈值")
        elif stage_score < 50 and composite_priority_score >= 75:
            risk_reasons.append("阶段分未达A池门槛")
        if external_crowding_penalty >= 7:
            risk_reasons.append("外部热度拥挤")
        elif external_crowding_penalty >= 4.5:
            risk_reasons.append("短期外部热度偏高")
        if composite_priority_score >= 75 and external_attention_score is not None and external_attention_score < 45:
            risk_reasons.append("外部确认偏弱")
        if stock_gate == "avoid":
            risk_reasons.append("持仓机构给出回避")
        if has_conflict:
            risk_reasons.append("同股存在方向冲突")
        if not forecast_row and qlib_percentile is None:
            risk_reasons.append("Qlib 结果未覆盖")
        elif (_safe_float(forecast_row.get("forecast_risk_adjusted_score")) or 100) <= 35:
            risk_reasons.append("预测性价比偏弱")
        if unlock_ratio_180d is not None and unlock_ratio_180d > 0.05:
            risk_reasons.append("近180天解禁压力偏大")
        if dividend_financing_ratio is not None and dividend_financing_ratio < 0.2 and (financing_count or 0) >= 2:
            risk_reasons.append("融资约束偏强")
        if net_profit_growth_yoy_ak is not None and net_profit_growth_yoy_ak < 0:
            risk_reasons.append("利润增速偏弱")
        if (_safe_float(industry_ctx.get("industry_tailwind_score")) or 0) <= 30 and industry_ctx:
            risk_reasons.append("行业背景偏弱")
        elif (_safe_float(industry_ctx.get("sector_excess_3m")) or 0) <= -8 and industry_ctx:
            risk_reasons.append("行业近3月相对偏弱")
        if archetype_row and (_safe_float(archetype_row.get("archetype_confidence")) or 0) < 40:
            risk_reasons.append("股票类型置信偏低")
        if (_safe_float(stage_row.get("stage_type_adjust_raw")) or 0) <= -6:
            risk_reasons.append("阶段惩罚项偏重")
        if discovery_score < 50:
            risk_reasons.append("发现分不足A池门槛")
        if composite_cap_reason:
            risk_reasons.append(composite_cap_reason)
        if stock_archetype == "成长兑现型" and (
            (price_20d is not None and price_20d > 20) or (price_1m is not None and price_1m > 25)
        ):
            risk_reasons.append("短期走势偏热")

        highlights_text = _top_reasons(highlight_reasons)
        risks_text = _top_reasons(risk_reasons)

        results.append((
            action_score, leader_inst, leader_score,
            consensus_count, path_state,
            best_setup.get("setup_tag") if best_setup else None,
            best_setup.get("setup_priority") if best_setup else None,
            best_setup.get("setup_reason") if best_setup else None,
            best_setup.get("setup_confidence") if best_setup else None,
            best_setup.get("setup_level") if best_setup else None,
            best_setup.get("setup_inst_id") if best_setup else None,
            best_setup.get("setup_inst_name") if best_setup else None,
            best_setup.get("setup_event_type") if best_setup else None,
            best_setup.get("setup_industry_name") if best_setup else None,
            best_setup.get("setup_score_raw") if best_setup else None,
            best_setup.get("setup_execution_gate") if best_setup else None,
            best_setup.get("setup_execution_reason") if best_setup else None,
            best_setup.get("industry_skill_raw") if best_setup else None,
            best_setup.get("industry_skill_grade") if best_setup else None,
            best_setup.get("followability_grade") if best_setup else None,
            best_setup.get("premium_grade") if best_setup else None,
            best_setup.get("report_recency_grade") if best_setup else None,
            best_setup.get("reliability_grade") if best_setup else None,
            best_setup.get("crowding_bucket") if best_setup else None,
            best_setup.get("crowding_yield_raw") if best_setup else None,
            best_setup.get("crowding_yield_grade") if best_setup else None,
            best_setup.get("crowding_stability_raw") if best_setup else None,
            best_setup.get("crowding_stability_grade") if best_setup else None,
            best_setup.get("crowding_fit_raw") if best_setup else None,
            best_setup.get("crowding_fit_grade") if best_setup else None,
            best_setup.get("crowding_fit_sample") if best_setup else None,
            best_setup.get("crowding_fit_source") if best_setup else None,
            best_setup.get("report_age_days") if best_setup else None,
            discovery_score, company_quality_score, stage_score,
            forecast_score, forecast_score_effective, raw_composite_priority_score,
            composite_priority_score, composite_cap_score, composite_cap_reason,
            stock_archetype, priority_pool, priority_pool_reason,
            attention_comment_trade_date, attention_focus_index, attention_composite_score,
            attention_institution_participation, attention_turnover_rate, attention_rank_change,
            attention_survey_count_30d, attention_survey_count_90d,
            attention_survey_org_total_30d, attention_survey_org_total_90d,
            external_attention_score, external_crowding_penalty, external_attention_signal,
            highlights_text, risks_text,
            now, sc
        ))
        scored += 1

    # 写入 mart_stock_trend（增量添加列由调用方确保存在）
    if results:
        conn.executemany("""
            UPDATE mart_stock_trend
            SET action_score = ?, leader_inst = ?,
                leader_score = ?, consensus_count = ?, path_state = ?,
                setup_tag = ?, setup_priority = ?, setup_reason = ?,
                setup_confidence = ?, setup_level = ?, setup_inst_id = ?,
                setup_inst_name = ?, setup_event_type = ?, setup_industry_name = ?,
                setup_score_raw = ?, setup_execution_gate = ?, setup_execution_reason = ?,
                industry_skill_raw = ?, industry_skill_grade = ?,
                followability_grade = ?, premium_grade = ?, report_recency_grade = ?,
                reliability_grade = ?, crowding_bucket = ?, crowding_yield_raw = ?,
                crowding_yield_grade = ?, crowding_stability_raw = ?, crowding_stability_grade = ?,
                crowding_fit_raw = ?, crowding_fit_grade = ?, crowding_fit_sample = ?,
                crowding_fit_source = ?, report_age_days = ?,
                discovery_score = ?, company_quality_score = ?, stage_score = ?,
                forecast_score = ?, forecast_score_effective = ?, raw_composite_priority_score = ?,
                composite_priority_score = ?, composite_cap_score = ?, composite_cap_reason = ?,
                stock_archetype = ?, priority_pool = ?, priority_pool_reason = ?,
                attention_comment_trade_date = ?, attention_focus_index = ?, attention_composite_score = ?,
                attention_institution_participation = ?, attention_turnover_rate = ?, attention_rank_change = ?,
                attention_survey_count_30d = ?, attention_survey_count_90d = ?,
                attention_survey_org_total_30d = ?, attention_survey_org_total_90d = ?,
                external_attention_score = ?, external_crowding_penalty = ?, external_attention_signal = ?,
                score_highlights = ?, score_risks = ?,
                updated_at = ?
            WHERE stock_code = ?
        """, results)
        conn.commit()

    logger.info(f"[评分] 股票评分完成: {scored} 只")
    return scored

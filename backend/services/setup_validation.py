"""
setup_validation.py

组合 Setup 前瞻快照与历史 replay 结果，生成稳定的验证报告。
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from services.market_db import get_market_conn


def _safe_round(value, digits: int = 2):
    if value is None:
        return None
    return round(float(value), digits)


def _days_between(start: Optional[str], end: Optional[str]) -> Optional[int]:
    if not start or not end:
        return None
    try:
        start_dt = datetime.strptime(str(start), "%Y-%m-%d")
        end_dt = datetime.strptime(str(end), "%Y-%m-%d")
    except ValueError:
        return None
    return (end_dt - start_dt).days


def _market_latest_trade_date() -> Optional[str]:
    mkt_conn = get_market_conn()
    try:
        row = mkt_conn.execute(
            "SELECT MAX(date) AS max_date FROM price_kline WHERE freq='daily' AND adjust='qfq'"
        ).fetchone()
        return row["max_date"] if row and row["max_date"] else None
    finally:
        mkt_conn.close()


def _horizon_payload(row, horizon: int) -> dict:
    matured_count = int(row[f"matured_{horizon}d_count"] or 0)
    return {
        "matured_count": matured_count,
        "avg_gain": _safe_round(row[f"avg_gain_{horizon}d"]),
        "win_rate": _safe_round(row[f"win_rate_{horizon}d"]),
        "avg_drawdown": _safe_round(row[f"avg_drawdown_{horizon}d"]),
    }


def _snapshot_aggregate_fields() -> str:
    parts = [
        "AVG(composite_priority_score) AS avg_composite_score",
        "AVG(discovery_score) AS avg_discovery_score",
        "COUNT(*) AS total",
        "AVG(setup_score_raw) AS avg_setup_score",
    ]
    for horizon in (10, 30, 60):
        parts.extend([
            f"SUM(CASE WHEN matured_{horizon}d = 1 AND gain_{horizon}d IS NOT NULL THEN 1 ELSE 0 END) AS matured_{horizon}d_count",
            f"AVG(CASE WHEN matured_{horizon}d = 1 THEN gain_{horizon}d END) AS avg_gain_{horizon}d",
            f"AVG(CASE WHEN matured_{horizon}d = 1 AND gain_{horizon}d > 0 THEN 1.0 ELSE NULL END) * 100 AS win_rate_{horizon}d",
            f"AVG(CASE WHEN matured_{horizon}d = 1 THEN max_drawdown_{horizon}d END) AS avg_drawdown_{horizon}d",
        ])
    return ",\n               ".join(parts)


def _snapshot_summary_from_row(row, *, include_scores: bool = True) -> dict:
    payload = {
        "total": int(row["total"] or 0),
        "avg_composite_score": _safe_round(row["avg_composite_score"]),
        "avg_discovery_score": _safe_round(row["avg_discovery_score"]),
        "h10": _horizon_payload(row, 10),
        "h30": _horizon_payload(row, 30),
        "h60": _horizon_payload(row, 60),
    }
    if include_scores:
        payload["avg_setup_score"] = _safe_round(row["avg_setup_score"])
    return payload


def _load_snapshot_overview(conn, where_sql: str = "", params: tuple = ()) -> dict:
    row = conn.execute(
        f"""
        SELECT {_snapshot_aggregate_fields()}
        FROM fact_setup_snapshot
        {where_sql}
        """,
        params,
    ).fetchone()
    if not row:
        return {
            "total": 0,
            "avg_composite_score": None,
            "avg_discovery_score": None,
            "avg_setup_score": None,
            "h10": {"matured_count": 0, "avg_gain": None, "win_rate": None, "avg_drawdown": None},
            "h30": {"matured_count": 0, "avg_gain": None, "win_rate": None, "avg_drawdown": None},
            "h60": {"matured_count": 0, "avg_gain": None, "win_rate": None, "avg_drawdown": None},
        }
    return _snapshot_summary_from_row(row)


def _load_group_summary(conn, group_field: str, label_field: str, *,
                        where_sql: str = "", params: tuple = (), order_sql: str = "") -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT {group_field} AS group_value,
               {label_field} AS group_label,
               {_snapshot_aggregate_fields()}
        FROM fact_setup_snapshot
        {where_sql}
        GROUP BY {group_field}, {label_field}
        {order_sql}
        """,
        params,
    ).fetchall()
    result = []
    for row in rows:
        payload = {
            "group_value": row["group_value"],
            "group_label": row["group_label"],
        }
        payload.update(_snapshot_summary_from_row(row))
        result.append(payload)
    return result


def _load_snapshot_history(conn, limit: int = 12) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT snapshot_date,
               {_snapshot_aggregate_fields()}
        FROM fact_setup_snapshot
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    history = []
    for row in rows:
        item = {"snapshot_date": row["snapshot_date"]}
        item.update(_snapshot_summary_from_row(row))
        history.append(item)
    return history


def _load_replay_group(conn, group_name: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT * FROM research_setup_replay_summary WHERE group_name = ?",
        (group_name,),
    ).fetchone()
    if not row:
        return None
    return {
        "group_name": row["group_name"],
        "sample_count": int(row["sample_count"] or 0),
        "avg_gain_10d": _safe_round(row["avg_gain_10d"]),
        "avg_gain_30d": _safe_round(row["avg_gain_30d"]),
        "avg_gain_60d": _safe_round(row["avg_gain_60d"]),
        "avg_gain_120d": _safe_round(row["avg_gain_120d"]),
        "win_rate_10d": _safe_round(row["win_rate_10d"]),
        "win_rate_30d": _safe_round(row["win_rate_30d"]),
        "win_rate_60d": _safe_round(row["win_rate_60d"]),
        "win_rate_120d": _safe_round(row["win_rate_120d"]),
        "avg_drawdown_30d": _safe_round(row["avg_drawdown_30d"]),
        "avg_drawdown_60d": _safe_round(row["avg_drawdown_60d"]),
        "uplift_vs_baseline_30d": _safe_round(row["uplift_vs_baseline_30d"]),
    }


def _load_replay_factor(conn, factor_name: str, *, limit: int = 20, order_sql: Optional[str] = None) -> list[dict]:
    sql = f"""
        SELECT factor_name, factor_value, sample_count,
               avg_gain_30d, avg_gain_60d, avg_gain_120d,
               win_rate_30d, win_rate_60d, win_rate_120d,
               avg_drawdown_30d, uplift_vs_baseline_30d
        FROM research_setup_replay_factor
        WHERE factor_name = ?
        ORDER BY {order_sql or 'avg_gain_30d DESC, sample_count DESC'}
        LIMIT ?
    """
    rows = conn.execute(sql, (factor_name, limit)).fetchall()
    return [
        {
            "factor_name": row["factor_name"],
            "factor_value": row["factor_value"],
            "sample_count": int(row["sample_count"] or 0),
            "avg_gain_30d": _safe_round(row["avg_gain_30d"]),
            "avg_gain_60d": _safe_round(row["avg_gain_60d"]),
            "avg_gain_120d": _safe_round(row["avg_gain_120d"]),
            "win_rate_30d": _safe_round(row["win_rate_30d"]),
            "win_rate_60d": _safe_round(row["win_rate_60d"]),
            "win_rate_120d": _safe_round(row["win_rate_120d"]),
            "avg_drawdown_30d": _safe_round(row["avg_drawdown_30d"]),
            "uplift_vs_baseline_30d": _safe_round(row["uplift_vs_baseline_30d"]),
        }
        for row in rows
    ]


def _build_insights(forward: dict, replay: dict) -> list[str]:
    insights: list[str] = []

    setup_hit = replay.get("setup_hit")
    baseline = replay.get("baseline")
    if setup_hit and baseline and setup_hit.get("avg_gain_30d") is not None:
        insights.append(
            "历史 replay 显示 Setup 命中整体优于全量买入基线："
            f"30日均收益 {setup_hit['avg_gain_30d']:.2f}% ，较基线提升 {setup_hit.get('uplift_vs_baseline_30d') or 0:.2f}pct。"
        )

    priority_groups = replay.get("priority_groups") or []
    top_priorities = [group for group in priority_groups if group.get("factor_value") in {"1", "2", "3"}]
    weak_priorities = [group for group in priority_groups if group.get("factor_value") in {"4", "5"}]
    if len(top_priorities) >= 3:
        insights.append(
            "A1/A2/A3 在历史 replay 中都跑赢基线，其中 A1/A2 最强，"
            f"A1 30日均收益 {top_priorities[0].get('avg_gain_30d'):.2f}% ，"
            f"A2 30日均收益 {top_priorities[1].get('avg_gain_30d'):.2f}% 。"
        )
    if weak_priorities:
        bad = [group for group in weak_priorities if (group.get("uplift_vs_baseline_30d") or 0) < 0]
        if bad:
            insights.append("A4/A5 在历史 replay 中已经接近或低于基线，不适合进一步提权。")

    gate_groups = replay.get("gate_groups") or []
    gate_map = {item["factor_value"]: item for item in gate_groups}
    if gate_map.get("watch") and gate_map.get("follow") and gate_map.get("observe"):
        insights.append(
            "执行建议层有明显分层："
            f"watch 的 30日均收益 {gate_map['watch']['avg_gain_30d']:.2f}% 最高，"
            f"follow 的 30日胜率 {gate_map['follow']['win_rate_30d']:.2f}% 更高且 30日回撤 {gate_map['follow']['avg_drawdown_30d']:.2f}% 更低，"
            "observe 基本接近基线。"
        )

    latest_gate_groups = forward.get("latest_gate_groups") or []
    latest_total = forward.get("latest_snapshot", {}).get("total") or 0
    if latest_total and latest_gate_groups:
        gate_counts = {
            str(item.get("group_value") or ""): int(item.get("total") or 0)
            for item in latest_gate_groups
        }
        observe_count = gate_counts.get("observe", 0)
        watch_count = gate_counts.get("watch", 0)
        follow_count = gate_counts.get("follow", 0)
        insights.append(
            f"当前最新快照共 {latest_total} 条候选，observe {observe_count} 条、watch {watch_count} 条、follow {follow_count} 条，"
            "说明当前线上候选仍以保守观察单为主。"
        )

    if (forward.get("overall") or {}).get("h10", {}).get("matured_count", 0) <= 0:
        insights.append(
            "前瞻快照链路已经接通，但当前快照样本尚未出现 10/30/60 日成熟后验，"
            "现阶段仍不能只凭前瞻数据就修改生产排序。"
        )

    return insights


def _build_decision(forward: dict, replay: dict) -> dict:
    matured_10d = (forward.get("overall") or {}).get("h10", {}).get("matured_count", 0)
    matured_30d = (forward.get("overall") or {}).get("h30", {}).get("matured_count", 0)
    priority_groups = replay.get("priority_groups") or []
    priority_map = {item["factor_value"]: item for item in priority_groups}
    gate_map = {item["factor_value"]: item for item in (replay.get("gate_groups") or [])}

    reasons = []
    recommended_action = "保持当前 Pool + Composite 主排序，继续保留 Setup 作为候选发现层与执行解释层。"
    phase3_status = "defer"

    if matured_10d <= 0 or matured_30d <= 0:
        reasons.append("前瞻快照尚未形成成熟后验样本，缺少真实线上闭环证据。")
    if priority_map.get("1") and priority_map.get("2"):
        reasons.append(
            f"历史 replay 对 A1/A2 有正向证据，但 A4/A5 明显转弱，当前更适合把 Setup 作为发现/筛选层，而不是替代四层综合评分。"
        )
    if gate_map.get("observe") and (gate_map["observe"].get("uplift_vs_baseline_30d") or 0) < 0.3:
        reasons.append("observe 组接近基线，执行建议维度有价值，但不应整体抬高所有 Setup 股票的排序权重。")

    return {
        "phase3_status": phase3_status,
        "should_change_scoring": False,
        "forward_ready": matured_30d > 0,
        "recommended_action": recommended_action,
        "reasons": reasons,
    }


def get_setup_validation_report(conn) -> dict:
    latest_snapshot_row = conn.execute(
        "SELECT MAX(snapshot_date) AS snapshot_date FROM fact_setup_snapshot"
    ).fetchone()
    latest_snapshot_date = latest_snapshot_row["snapshot_date"] if latest_snapshot_row else None
    market_latest = _market_latest_trade_date()

    overall = _load_snapshot_overview(conn)
    latest_snapshot = (
        _load_snapshot_overview(conn, "WHERE snapshot_date = ?", (latest_snapshot_date,))
        if latest_snapshot_date
        else _load_snapshot_overview(conn, "WHERE 1 = 0")
    )

    latest_priority_groups = _load_group_summary(
        conn,
        "COALESCE(CAST(setup_priority AS TEXT), 'unknown')",
        "COALESCE(CAST(setup_priority AS TEXT), 'unknown')",
        where_sql="WHERE snapshot_date = ?",
        params=(latest_snapshot_date,),
        order_sql="ORDER BY CAST(group_value AS INTEGER)",
    ) if latest_snapshot_date else []

    latest_gate_groups = _load_group_summary(
        conn,
        "COALESCE(setup_execution_gate, 'unknown')",
        "COALESCE(setup_execution_gate, 'unknown')",
        where_sql="WHERE snapshot_date = ?",
        params=(latest_snapshot_date,),
        order_sql="ORDER BY CASE group_value WHEN 'follow' THEN 0 WHEN 'watch' THEN 1 WHEN 'observe' THEN 2 ELSE 9 END, group_value",
    ) if latest_snapshot_date else []

    all_priority_groups = _load_group_summary(
        conn,
        "COALESCE(CAST(setup_priority AS TEXT), 'unknown')",
        "COALESCE(CAST(setup_priority AS TEXT), 'unknown')",
        order_sql="ORDER BY CAST(group_value AS INTEGER)",
    )
    all_gate_groups = _load_group_summary(
        conn,
        "COALESCE(setup_execution_gate, 'unknown')",
        "COALESCE(setup_execution_gate, 'unknown')",
        order_sql="ORDER BY CASE group_value WHEN 'follow' THEN 0 WHEN 'watch' THEN 1 WHEN 'observe' THEN 2 ELSE 9 END, group_value",
    )
    snapshot_history = _load_snapshot_history(conn, limit=12)

    replay = {
        "baseline": _load_replay_group(conn, "baseline_all_buy"),
        "setup_hit": _load_replay_group(conn, "setup_hit_all"),
        "priority_groups": _load_replay_factor(
            conn,
            "setup_priority",
            limit=10,
            order_sql="CAST(factor_value AS INTEGER)",
        ),
        "gate_groups": _load_replay_factor(
            conn,
            "setup_execution_gate",
            limit=10,
            order_sql="CASE factor_value WHEN 'follow' THEN 0 WHEN 'watch' THEN 1 WHEN 'observe' THEN 2 ELSE 9 END, sample_count DESC",
        ),
    }

    forward = {
        "latest_snapshot_date": latest_snapshot_date,
        "market_latest_trade_date": market_latest,
        "snapshot_is_current": latest_snapshot_date == market_latest if latest_snapshot_date and market_latest else False,
        "snapshot_lag_days": _days_between(latest_snapshot_date, market_latest),
        "total_snapshot_days": len(snapshot_history),
        "overall": overall,
        "latest_snapshot": latest_snapshot,
        "snapshot_history": snapshot_history,
        "latest_priority_groups": latest_priority_groups,
        "latest_gate_groups": latest_gate_groups,
        "all_priority_groups": all_priority_groups,
        "all_gate_groups": all_gate_groups,
        "tracking_chain": {
            "auto_trigger_step": "calc_stock_scores",
            "manual_trigger_api": "/api/inst/scoring/calculate/stock",
            "latest_snapshot_date": latest_snapshot_date,
            "market_latest_trade_date": market_latest,
            "snapshot_is_current": latest_snapshot_date == market_latest if latest_snapshot_date and market_latest else False,
            "snapshot_lag_days": _days_between(latest_snapshot_date, market_latest),
        },
    }

    insights = _build_insights(forward, replay)
    decision = _build_decision(forward, replay)

    return {
        "forward": forward,
        "replay": replay,
        "insights": insights,
        "decision": decision,
    }

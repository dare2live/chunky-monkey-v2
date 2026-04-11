from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from services.etf_engine import calc_etf_momentum, calc_etf_overview
from services.etf_grid_engine import is_supported_exchange_etf_code


ETF_SNAPSHOT_SCHEMA_VERSION = 6
_REQUIRED_SNAPSHOT_ROW_FIELDS = (
    "qlib_consensus_score",
    "qlib_model_status",
    "qlib_consensus_factor_group",
    "qlib_preferred_strategy",
    "qlib_predicted_best_step_pct",
    "qlib_predicted_buy_hold_return_pct",
    "qlib_predicted_grid_return_pct",
    "strategy_reason",
    "backtest_hard_gate_passed",
    "tradeability_status",
    "tradeability_reason",
)
_ETF_SNAPSHOT_MEMORY_CACHE: dict[str, Optional[dict]] = {
    "snapshot_id": None,
    "bundle": None,
}


def _json_dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False)


def _json_loads(value, fallback):
    if not value:
        return fallback
    try:
        return json.loads(value)
    except Exception:
        return fallback


def _store_snapshot_bundle_in_memory(bundle: dict) -> dict:
    _ETF_SNAPSHOT_MEMORY_CACHE["snapshot_id"] = bundle.get("snapshot_id")
    _ETF_SNAPSHOT_MEMORY_CACHE["bundle"] = bundle
    return bundle


def invalidate_etf_snapshot_cache() -> None:
    _ETF_SNAPSHOT_MEMORY_CACHE["snapshot_id"] = None
    _ETF_SNAPSHOT_MEMORY_CACHE["bundle"] = None


def _snapshot_row_has_current_fields(payload: Optional[dict]) -> bool:
    if not isinstance(payload, dict):
        return False
    return all(field in payload for field in _REQUIRED_SNAPSHOT_ROW_FIELDS)


def _snapshot_rows_have_current_fields(rows: list[dict]) -> bool:
    if not rows:
        return False
    return all(_snapshot_row_has_current_fields(row) for row in rows)


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _minutes_between(older: Optional[str], newer: Optional[str]) -> Optional[int]:
    older_dt = _parse_dt(older)
    newer_dt = _parse_dt(newer)
    if not older_dt or not newer_dt:
        return None
    return max(int((newer_dt - older_dt).total_seconds() // 60), 0)


def _source_status_refresh_fingerprint(source_status: Optional[dict]) -> tuple:
    payload = source_status or {}
    source_breakdown = tuple(
        (
            str(item.get("source") or ""),
            int(item.get("count") or 0),
        )
        for item in (payload.get("source_breakdown") or [])
    )
    return (
        payload.get("universe_updated_at"),
        payload.get("latest_kline_success_at"),
        payload.get("latest_kline_attempt_at"),
        payload.get("history_start"),
        payload.get("history_end"),
        int(payload.get("universe_count") or 0),
        int(payload.get("kline_etf_count") or 0),
        int(payload.get("coverage_2023_count") or 0),
        int(payload.get("recent_only_count") or 0),
        int(payload.get("no_kline_count") or 0),
        source_breakdown,
    )


def _price_coverage_summary(mkt_conn) -> dict:
    row = mkt_conn.execute(
        """
        SELECT MIN(date) AS min_date,
               MAX(date) AS max_date,
               COUNT(DISTINCT code) AS etf_count
        FROM etf_price_kline
        WHERE freq = 'daily'
          AND adjust = 'qfq'
        """
    ).fetchone()
    return dict(row) if row else {"min_date": None, "max_date": None, "etf_count": 0}


def _build_etf_source_status(conn, mkt_conn, *, computed_at: Optional[str] = None,
                             connectivity: Optional[dict] = None) -> dict:
    etf_rows = conn.execute(
        """
        SELECT code, updated_at
        FROM etf_asset_universe
        WHERE is_active = 1
        """
    ).fetchall()
    supported_etf_rows = [row for row in etf_rows if is_supported_exchange_etf_code(row["code"] or "")]
    etf_codes = {row["code"] for row in supported_etf_rows if row["code"]}
    universe_updated_at = None
    for row in supported_etf_rows:
        updated_at = row["updated_at"]
        if updated_at and (universe_updated_at is None or updated_at > universe_updated_at):
            universe_updated_at = updated_at

    state_rows = mkt_conn.execute(
        """
        SELECT code, source, min_date, max_date, row_count,
               last_success_at, last_attempt_at, last_error
        FROM etf_sync_state
        WHERE dataset = 'price_kline' AND freq = 'daily' AND adjust = 'qfq'
        """
    ).fetchall()
    filtered_states = [dict(row) for row in state_rows if row["code"] in etf_codes]
    with_data = [row for row in filtered_states if int(row.get("row_count") or 0) > 0]
    with_data_codes = {row["code"] for row in with_data}
    missing_codes = sorted(etf_codes - with_data_codes)

    source_breakdown: dict[str, int] = {}
    latest_success_at = None
    latest_attempt_at = None
    for row in with_data:
        source = row.get("source") or "未知"
        source_breakdown[source] = source_breakdown.get(source, 0) + 1
        success_at = row.get("last_success_at")
        attempt_at = row.get("last_attempt_at")
        if success_at and (latest_success_at is None or success_at > latest_success_at):
            latest_success_at = success_at
        if attempt_at and (latest_attempt_at is None or attempt_at > latest_attempt_at):
            latest_attempt_at = attempt_at

    coverage_2023_count = sum(
        1 for row in with_data
        if row.get("min_date") and row["min_date"] <= "2023-12-31"
    )
    recent_only_count = sum(
        1 for row in with_data
        if row.get("min_date") and row["min_date"] >= "2025-08-01"
    )
    coverage = _price_coverage_summary(mkt_conn)
    snapshot_lag_minutes = _minutes_between(computed_at, latest_success_at)

    return {
        "universe_count": len(etf_codes),
        "universe_updated_at": universe_updated_at,
        "kline_etf_count": len(with_data),
        "kline_coverage_ratio": round(len(with_data) * 100.0 / len(etf_codes), 2) if etf_codes else None,
        "history_start": coverage.get("min_date"),
        "history_end": coverage.get("max_date"),
        "coverage_2023_count": coverage_2023_count,
        "recent_only_count": recent_only_count,
        "no_kline_count": len(missing_codes),
        "no_kline_examples": missing_codes[:10],
        "latest_kline_success_at": latest_success_at,
        "latest_kline_attempt_at": latest_attempt_at,
        "last_error_count": sum(1 for row in filtered_states if row.get("last_error")),
        "source_breakdown": [
            {"source": source, "count": count}
            for source, count in sorted(source_breakdown.items(), key=lambda item: (-item[1], item[0]))
        ],
        "snapshot_is_stale": snapshot_lag_minutes is not None and snapshot_lag_minutes > 0,
        "snapshot_lag_minutes": snapshot_lag_minutes,
        "connectivity": connectivity or {},
    }


def _load_cached_etf_rows(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT payload_json
        FROM mart_etf_snapshot_latest
        ORDER BY COALESCE(factor_rank, 999999), code
        """
    ).fetchall()
    return [_json_loads(row["payload_json"], {}) for row in rows if row["payload_json"]]


def load_cached_etf_row(conn, code: str) -> Optional[dict]:
    row = conn.execute(
        "SELECT payload_json FROM mart_etf_snapshot_latest WHERE code = ?",
        (code,),
    ).fetchone()
    if not row:
        return None
    payload = _json_loads(row["payload_json"], None)
    return payload if _snapshot_row_has_current_fields(payload) else None


def _load_cached_etf_state(conn) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT snapshot_id, schema_version, computed_at, etf_count,
               history_start, history_end, overview_json,
               factor_snapshot_json, mining_snapshot_json, source_status_json
        FROM mart_etf_snapshot_state
        WHERE state_key = 'latest'
        """
    ).fetchone()
    if not row or not row["snapshot_id"]:
        return None
    return {
        "snapshot_id": row["snapshot_id"],
        "schema_version": row["schema_version"],
        "computed_at": row["computed_at"],
        "etf_count": int(row["etf_count"] or 0),
        "history_start": row["history_start"],
        "history_end": row["history_end"],
        "overview": _json_loads(row["overview_json"], {}),
        "factor_snapshot": _json_loads(row["factor_snapshot_json"], {}),
        "mining_snapshot": _json_loads(row["mining_snapshot_json"], {}),
        "source_status": _json_loads(row["source_status_json"], {}),
    }


def _load_live_source_status(conn, mkt_conn, state: dict, connectivity: Optional[dict]) -> dict:
    return _build_etf_source_status(
        conn,
        mkt_conn,
        computed_at=state.get("computed_at"),
        connectivity=connectivity,
    )


def persist_latest_etf_snapshot(conn, mkt_conn, *, connectivity: Optional[dict] = None) -> dict:
    from services.etf_mining_engine import (
        _build_etf_factor_snapshot_from_rows,
        _build_etf_mining_snapshot_from_rows,
        enrich_etf_rows_with_strategy_validation,
    )

    rows = enrich_etf_rows_with_strategy_validation(
        calc_etf_momentum(conn, mkt_conn),
        conn,
        mkt_conn,
    )
    overview = calc_etf_overview(rows)
    factor_snapshot = _build_etf_factor_snapshot_from_rows(rows, mkt_conn)
    mining_snapshot = _build_etf_mining_snapshot_from_rows(rows, factor_snapshot)
    computed_at = datetime.now().isoformat(timespec="seconds")
    history_end = ((factor_snapshot.get("model") or {}).get("history_end") or "na").replace("-", "")
    snapshot_id = f"etf_snapshot_{history_end}_{computed_at[11:19].replace(':', '')}"
    source_status = _build_etf_source_status(
        conn,
        mkt_conn,
        computed_at=computed_at,
        connectivity=connectivity,
    )

    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM mart_etf_snapshot_latest")
        for row in rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO mart_etf_snapshot_latest (
                    code, snapshot_id, category, factor_rank, factor_score,
                    rotation_score, strategy_type, payload_json, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row.get("code"),
                    snapshot_id,
                    row.get("category"),
                    row.get("factor_rank"),
                    row.get("factor_score"),
                    row.get("rotation_score"),
                    row.get("strategy_type"),
                    _json_dumps(row),
                    computed_at,
                ),
            )
        conn.execute(
            """
            INSERT OR REPLACE INTO mart_etf_snapshot_state (
                state_key, snapshot_id, schema_version, computed_at, etf_count,
                history_start, history_end, overview_json,
                factor_snapshot_json, mining_snapshot_json, source_status_json
            ) VALUES ('latest', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                ETF_SNAPSHOT_SCHEMA_VERSION,
                computed_at,
                len(rows),
                source_status.get("history_start"),
                source_status.get("history_end"),
                _json_dumps(overview),
                _json_dumps(factor_snapshot),
                _json_dumps(mining_snapshot),
                _json_dumps(source_status),
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return _store_snapshot_bundle_in_memory({
        "snapshot_id": snapshot_id,
        "computed_at": computed_at,
        "etf_count": len(rows),
        "rows": rows,
        "overview": overview,
        "factor_snapshot": factor_snapshot,
        "mining_snapshot": mining_snapshot,
        "source_status": source_status,
        "is_stale": source_status.get("snapshot_is_stale", False),
    })


def get_latest_etf_snapshot_bundle(conn, mkt_conn, *, force_refresh: bool = False,
                                   connectivity: Optional[dict] = None) -> dict:
    state = _load_cached_etf_state(conn)
    if force_refresh or not state or state.get("schema_version") != ETF_SNAPSHOT_SCHEMA_VERSION:
        return persist_latest_etf_snapshot(conn, mkt_conn, connectivity=connectivity)

    stored_source_status = state.get("source_status") or {}
    live_source_status = _load_live_source_status(conn, mkt_conn, state, connectivity)
    if stored_source_status and (
        _source_status_refresh_fingerprint(live_source_status)
        != _source_status_refresh_fingerprint(stored_source_status)
    ):
        invalidate_etf_snapshot_cache()
        return persist_latest_etf_snapshot(conn, mkt_conn, connectivity=connectivity)

    cached_bundle = _ETF_SNAPSHOT_MEMORY_CACHE.get("bundle")
    if cached_bundle and _ETF_SNAPSHOT_MEMORY_CACHE.get("snapshot_id") == state.get("snapshot_id"):
        if not _snapshot_rows_have_current_fields(cached_bundle.get("rows") or []):
            return persist_latest_etf_snapshot(conn, mkt_conn, connectivity=connectivity)
        source_status = dict(live_source_status or cached_bundle.get("source_status") or stored_source_status)
        if connectivity is not None:
            source_status["connectivity"] = connectivity
        bundle = dict(cached_bundle)
        bundle["source_status"] = source_status
        bundle["is_stale"] = source_status.get("snapshot_is_stale", False)
        return bundle

    rows = _load_cached_etf_rows(conn)
    if not rows or not _snapshot_rows_have_current_fields(rows):
        return persist_latest_etf_snapshot(conn, mkt_conn, connectivity=connectivity)

    source_status = dict(live_source_status or stored_source_status)
    if not source_status:
        source_status = _load_live_source_status(conn, mkt_conn, state, connectivity)
    elif connectivity is not None:
        source_status["connectivity"] = connectivity
    return _store_snapshot_bundle_in_memory({
        "snapshot_id": state.get("snapshot_id"),
        "computed_at": state.get("computed_at"),
        "etf_count": state.get("etf_count") or len(rows),
        "rows": rows,
        "overview": state.get("overview") or {},
        "factor_snapshot": state.get("factor_snapshot") or {},
        "mining_snapshot": state.get("mining_snapshot") or {},
        "source_status": source_status,
        "is_stale": source_status.get("snapshot_is_stale", False),
    })
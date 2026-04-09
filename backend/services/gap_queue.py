"""
gap_queue.py

更新工作台缺口/失败队列。
"""

from datetime import datetime
from typing import Iterable, Optional

from services.industry import industry_complete_condition, industry_join_clause
from services.market_db import get_market_conn

DATASET_LABELS = {
    "daily_kline": "日K缺口",
    "monthly_kline": "月K缺口",
    "industry": "行业缺口",
}

UNRESOLVED_STATUSES = {"pending", "retrying", "blocked"}


def _now_iso() -> str:
    return datetime.now().isoformat()


def load_tracked_stock_names(conn) -> dict[str, Optional[str]]:
    """加载跟踪的股票列表（排除已被排除规则过滤的退市/ST等股票）"""
    rows = conn.execute(
        """
        SELECT h.stock_code, MAX(COALESCE(h.stock_name, '')) AS stock_name
        FROM inst_holdings h
        WHERE h.stock_code IS NOT NULL AND h.stock_code != ''
          AND h.stock_code NOT IN (SELECT stock_code FROM excluded_stocks)
        GROUP BY h.stock_code
        """
    ).fetchall()
    return {row["stock_code"]: (row["stock_name"] or None) for row in rows}


def _compute_missing_codes(conn, dataset: str, *,
                           stock_names: Optional[dict[str, Optional[str]]] = None,
                           mkt_conn=None) -> set[str]:
    stock_names = stock_names or load_tracked_stock_names(conn)
    tracked_codes = set(stock_names.keys())
    if not tracked_codes:
        return set()

    if dataset in {"daily_kline", "monthly_kline"}:
        own_conn = False
        if mkt_conn is None:
            mkt_conn = get_market_conn()
            own_conn = True
        try:
            freq = "daily" if dataset == "daily_kline" else "monthly"
            rows = mkt_conn.execute(
                "SELECT DISTINCT code FROM price_kline WHERE freq=? AND adjust='qfq'",
                (freq,),
            ).fetchall()
            present_codes = {row["code"] for row in rows}
            return tracked_codes - present_codes
        finally:
            if own_conn:
                mkt_conn.close()

    if dataset == "industry":
        join_clause = industry_join_clause("t.stock_code", alias="industry_dim", join_type="LEFT")
        complete_condition = industry_complete_condition(alias="industry_dim")
        rows = conn.execute(
            f"""
            WITH tracked AS (
                SELECT DISTINCT stock_code
                FROM inst_holdings
                WHERE stock_code IS NOT NULL AND stock_code != ''
            )
            SELECT t.stock_code
            FROM tracked t
            {join_clause}
            WHERE NOT ({complete_condition})
            """
        ).fetchall()
        return {row["stock_code"] for row in rows}

    raise ValueError(f"Unsupported gap dataset: {dataset}")


def _classify_gap_state(last_error: Optional[str], fallback_reason: Optional[str] = None) -> tuple[str, str]:
    text = str(last_error or "").strip()
    low = text.lower()
    if any(token in text for token in ("不可用", "未执行同步")):
        return "blocked", fallback_reason or "数据源不可用，当前未执行同步"
    if any(token in low for token in (
        "all_sources_empty",
        "empty",
        "not found",
        "unsupported",
        "unknown code",
        "no data",
    )):
        return "blocked", fallback_reason or "所有来源无有效数据，可能是历史残留或特殊代码"
    if any(token in low for token in (
        "timeout",
        "connect",
        "connection",
        "network",
        "proxy",
        "temporarily unavailable",
        "ssl",
        "read timed out",
        "reset by peer",
    )):
        return "retrying", fallback_reason or "在线补数失败，等待后续重试"
    return "retrying", fallback_reason or "补数失败，等待后续重试"


def upsert_gap_state(conn, dataset: str, stock_code: str, *,
                     stock_name: Optional[str] = None,
                     status: str = "pending",
                     reason: Optional[str] = None,
                     last_error: Optional[str] = None,
                     attempt_delta: int = 0,
                     touched_attempt: bool = False,
                     resolved: bool = False,
                     commit: bool = False):
    now = _now_iso()
    existing = conn.execute(
        """
        SELECT stock_name, source_attempts, first_seen_at
        FROM market_gap_queue
        WHERE dataset = ? AND stock_code = ?
        """,
        (dataset, stock_code),
    ).fetchone()
    prev_attempts = (existing["source_attempts"] if existing and existing["source_attempts"] is not None else 0)
    first_seen_at = (existing["first_seen_at"] if existing and existing["first_seen_at"] else now)
    name = stock_name or (existing["stock_name"] if existing and existing["stock_name"] else None)
    attempts = prev_attempts + max(attempt_delta, 0)
    last_attempt_at = now if (touched_attempt or attempt_delta > 0 or status in {"retrying", "blocked"}) else None
    resolved_at = now if resolved or status == "resolved" else None
    payload_status = "resolved" if resolved else status
    payload_reason = reason or ("已补齐" if payload_status == "resolved" else "当前仍缺失，待下一轮补齐")
    payload_error = None if payload_status == "resolved" else last_error

    conn.execute(
        """
        INSERT INTO market_gap_queue (
            dataset, stock_code, stock_name, status, reason, last_error,
            source_attempts, first_seen_at, last_attempt_at, resolved_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset, stock_code) DO UPDATE SET
            stock_name = COALESCE(excluded.stock_name, market_gap_queue.stock_name),
            status = excluded.status,
            reason = excluded.reason,
            last_error = excluded.last_error,
            source_attempts = excluded.source_attempts,
            first_seen_at = market_gap_queue.first_seen_at,
            last_attempt_at = COALESCE(excluded.last_attempt_at, market_gap_queue.last_attempt_at),
            resolved_at = excluded.resolved_at,
            updated_at = excluded.updated_at
        """,
        (
            dataset,
            stock_code,
            name,
            payload_status,
            payload_reason,
            payload_error,
            attempts,
            first_seen_at,
            last_attempt_at,
            resolved_at,
            now,
        ),
    )
    if commit:
        conn.commit()


def mark_gap_failed(conn, dataset: str, stock_code: str, *,
                    stock_name: Optional[str] = None,
                    last_error: Optional[str] = None,
                    reason: Optional[str] = None,
                    touched_attempt: bool = True,
                    commit: bool = False):
    status, classified_reason = _classify_gap_state(last_error, reason)
    upsert_gap_state(
        conn,
        dataset,
        stock_code,
        stock_name=stock_name,
        status=status,
        reason=classified_reason,
        last_error=last_error,
        attempt_delta=1 if touched_attempt else 0,
        touched_attempt=touched_attempt,
        commit=commit,
    )


def mark_gap_pending(conn, dataset: str, stock_code: str, *,
                     stock_name: Optional[str] = None,
                     reason: Optional[str] = None,
                     commit: bool = False):
    upsert_gap_state(
        conn,
        dataset,
        stock_code,
        stock_name=stock_name,
        status="pending",
        reason=reason or "当前仍缺失，待下一轮补齐",
        commit=commit,
    )


def mark_gap_retrying(conn, dataset: str, stock_code: str, *,
                      stock_name: Optional[str] = None,
                      reason: Optional[str] = None,
                      commit: bool = False):
    upsert_gap_state(
        conn,
        dataset,
        stock_code,
        stock_name=stock_name,
        status="retrying",
        reason=reason or "正在尝试补齐",
        attempt_delta=1,
        touched_attempt=True,
        commit=commit,
    )


def mark_gap_resolved(conn, dataset: str, stock_code: str, *,
                      stock_name: Optional[str] = None,
                      reason: Optional[str] = None,
                      commit: bool = False):
    upsert_gap_state(
        conn,
        dataset,
        stock_code,
        stock_name=stock_name,
        status="resolved",
        reason=reason or "已补齐",
        resolved=True,
        commit=commit,
    )


def mark_current_missing_as(conn, dataset: str, *,
                            status: str,
                            reason: str,
                            last_error: Optional[str] = None,
                            stock_names: Optional[dict[str, Optional[str]]] = None,
                            mkt_conn=None,
                            commit: bool = False) -> int:
    stock_names = stock_names or load_tracked_stock_names(conn)
    missing_codes = _compute_missing_codes(conn, dataset, stock_names=stock_names, mkt_conn=mkt_conn)
    for code in missing_codes:
        upsert_gap_state(
            conn,
            dataset,
            code,
            stock_name=stock_names.get(code),
            status=status,
            reason=reason,
            last_error=last_error,
            touched_attempt=status in {"retrying", "blocked"},
            commit=False,
        )
    if commit:
        conn.commit()
    return len(missing_codes)


def reconcile_gap_queue_snapshot(conn, *,
                                 stock_names: Optional[dict[str, Optional[str]]] = None,
                                 datasets: Optional[Iterable[str]] = None,
                                 mkt_conn=None,
                                 commit: bool = False):
    stock_names = stock_names or load_tracked_stock_names(conn)
    tracked_codes = set(stock_names.keys())
    datasets = list(datasets or ("daily_kline", "monthly_kline", "industry"))

    own_mkt = False
    if any(ds in {"daily_kline", "monthly_kline"} for ds in datasets) and mkt_conn is None:
        mkt_conn = get_market_conn()
        own_mkt = True

    try:
        for dataset in datasets:
            missing_codes = _compute_missing_codes(conn, dataset, stock_names=stock_names, mkt_conn=mkt_conn)
            existing_rows = conn.execute(
                """
                SELECT stock_code, status
                FROM market_gap_queue
                WHERE dataset = ?
                """,
                (dataset,),
            ).fetchall()
            existing = {row["stock_code"]: row["status"] for row in existing_rows}

            for code in missing_codes:
                current_status = existing.get(code)
                if current_status in UNRESOLVED_STATUSES:
                    if stock_names.get(code):
                        upsert_gap_state(
                            conn,
                            dataset,
                            code,
                            stock_name=stock_names.get(code),
                            status=current_status,
                            reason=None,
                            commit=False,
                        )
                    continue
                mark_gap_pending(conn, dataset, code, stock_name=stock_names.get(code), commit=False)

            to_resolve = {
                code for code, status in existing.items()
                if status in UNRESOLVED_STATUSES and (code not in missing_codes or code not in tracked_codes)
            }
            for code in to_resolve:
                mark_gap_resolved(
                    conn,
                    dataset,
                    code,
                    stock_name=stock_names.get(code),
                    reason="已补齐或已不在当前跟踪范围",
                    commit=False,
                )
        if commit:
            conn.commit()
    finally:
        if own_mkt:
            mkt_conn.close()


def summarize_gap_queue(conn, *, datasets: Optional[Iterable[str]] = None, limit_per_dataset: int = 8) -> dict:
    datasets = list(datasets or ("daily_kline", "monthly_kline", "industry"))
    payload = {
        "total_unresolved": 0,
        "datasets": [],
    }
    for dataset in datasets:
        counts = conn.execute(
            """
            SELECT
                COUNT(*) AS unresolved,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) AS retrying,
                SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) AS blocked
            FROM market_gap_queue
            WHERE dataset = ? AND status != 'resolved'
            """,
            (dataset,),
        ).fetchone()
        unresolved = (counts["unresolved"] if counts and counts["unresolved"] is not None else 0)
        items = conn.execute(
            """
            SELECT stock_code, stock_name, status, reason, last_error,
                   source_attempts, first_seen_at, last_attempt_at, updated_at
            FROM market_gap_queue
            WHERE dataset = ? AND status != 'resolved'
            ORDER BY
                CASE status
                    WHEN 'blocked' THEN 0
                    WHEN 'retrying' THEN 1
                    ELSE 2
                END,
                COALESCE(last_attempt_at, updated_at, first_seen_at) DESC,
                stock_code
            LIMIT ?
            """,
            (dataset, limit_per_dataset),
        ).fetchall()
        dataset_payload = {
            "dataset": dataset,
            "label": DATASET_LABELS.get(dataset, dataset),
            "unresolved": unresolved,
            "pending": (counts["pending"] if counts and counts["pending"] is not None else 0),
            "retrying": (counts["retrying"] if counts and counts["retrying"] is not None else 0),
            "blocked": (counts["blocked"] if counts and counts["blocked"] is not None else 0),
            "items": [dict(row) for row in items],
        }
        payload["datasets"].append(dataset_payload)
        payload["total_unresolved"] += unresolved
    return payload

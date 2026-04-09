"""
当前可交易 A 股主数据服务

职责：
- 拉取当前可交易 A 股代码清单
- 缓存到本地表 dim_active_a_stock
- 为更新链路提供统一的“有效股票宇宙”入口

设计原则：
- 历史公告 market_raw_holdings 只代表“曾经出现过的代码”
- 可跟踪/可同步行情的股票，必须再经过“当前有效 A 股主数据”校验
- 退市、失效、非股票代码不应进入 inst_holdings / K 线同步主链路
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Dict, Set

logger = logging.getLogger("cm-api")

ACTIVE_STOCK_CACHE_HOURS = 24
ACTIVE_STOCK_MIN_ROWS = 3000


def _disable_proxy_env() -> None:
    for key in (
        "http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
        "all_proxy", "ALL_PROXY",
    ):
        os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"


def _market_from_code(code: str) -> str:
    text = str(code or "").strip()
    if text.startswith(("600", "601", "603", "605", "688", "689")):
        return "SH"
    return "SZ"


def _parse_iso(ts: str):
    if not ts:
        return None
    text = str(ts).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _load_cached_codes(conn) -> Set[str]:
    rows = conn.execute(
        "SELECT stock_code FROM dim_active_a_stock WHERE stock_code IS NOT NULL"
    ).fetchall()
    return {str(r["stock_code"]).strip() for r in rows if r["stock_code"]}


def _cache_is_fresh(conn, max_age_hours: int = ACTIVE_STOCK_CACHE_HOURS) -> bool:
    row = conn.execute(
        "SELECT COUNT(*) AS cnt, MAX(updated_at) AS latest FROM dim_active_a_stock"
    ).fetchone()
    if not row or (row["cnt"] or 0) < ACTIVE_STOCK_MIN_ROWS or not row["latest"]:
        return False
    latest = _parse_iso(row["latest"])
    if latest is None:
        return False
    return latest >= datetime.now() - timedelta(hours=max_age_hours)


def refresh_active_a_stock_master(conn) -> int:
    """刷新当前可交易 A 股主数据缓存。"""
    _disable_proxy_env()
    import akshare as ak

    df = ak.stock_info_a_code_name()
    if df is None or df.empty:
        raise RuntimeError("stock_info_a_code_name returned empty result")

    if "code" not in df.columns or "name" not in df.columns:
        raise RuntimeError(f"unexpected columns from stock_info_a_code_name: {list(df.columns)}")

    frame = df[["code", "name"]].copy()
    frame["code"] = frame["code"].astype(str).str.strip().str.zfill(6)
    frame["name"] = frame["name"].astype(str).str.strip()
    frame = frame[frame["code"].str.fullmatch(r"\d{6}", na=False)]
    frame = frame.drop_duplicates(subset=["code"]).reset_index(drop=True)

    now = datetime.now().isoformat()
    rows = [
        (row["code"], row["name"], _market_from_code(row["code"]), "akshare_stock_info_a_code_name", now)
        for _, row in frame.iterrows()
    ]

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM dim_active_a_stock")
        conn.executemany(
            """
            INSERT INTO dim_active_a_stock
            (stock_code, stock_name, market, source, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    logger.info(f"[主数据] 刷新当前A股主数据: {len(rows)} 只")
    return len(rows)


def get_active_a_stock_codes(conn, max_age_hours: int = ACTIVE_STOCK_CACHE_HOURS) -> Set[str]:
    """返回当前可交易 A 股代码集合；优先用缓存，必要时刷新。"""
    if _cache_is_fresh(conn, max_age_hours=max_age_hours):
        return _load_cached_codes(conn)

    cached = _load_cached_codes(conn)
    try:
        refresh_active_a_stock_master(conn)
        return _load_cached_codes(conn)
    except Exception as e:
        if cached:
            logger.warning(f"[主数据] 刷新当前A股主数据失败，回退旧缓存: {e}")
            return cached
        raise


def get_active_a_stock_map(conn, max_age_hours: int = ACTIVE_STOCK_CACHE_HOURS) -> Dict[str, str]:
    """返回 {stock_code: stock_name} 映射。"""
    if not _cache_is_fresh(conn, max_age_hours=max_age_hours):
        cached = conn.execute("SELECT COUNT(*) FROM dim_active_a_stock").fetchone()[0]
        try:
            refresh_active_a_stock_master(conn)
        except Exception:
            if not cached:
                raise
    rows = conn.execute(
        "SELECT stock_code, stock_name FROM dim_active_a_stock WHERE stock_code IS NOT NULL"
    ).fetchall()
    return {str(r["stock_code"]).strip(): (r["stock_name"] or "") for r in rows if r["stock_code"]}

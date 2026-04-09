"""
market_db.py — 独立行情数据库 (market_data.db)

职责：K 线存储、同步状态、导入批次管理。
与业务库 smartmoney.db 完全解耦，业务层只通过本模块读写行情数据。
"""

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

_DB_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_DB_PATH = _DB_DIR / "market_data.db"

# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_market_conn(timeout: int = 30) -> sqlite3.Connection:
    """获取 market_data.db 连接"""
    conn = sqlite3.connect(str(_DB_PATH), timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def init_market_db():
    """创建行情数据库表结构（仅建表，不做迁移）"""
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = get_market_conn()
    try:
        conn.executescript("""
        -- K 线数据主表
        CREATE TABLE IF NOT EXISTS price_kline (
            code        TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            freq        TEXT    NOT NULL DEFAULT 'daily',
            adjust      TEXT    NOT NULL DEFAULT 'qfq',
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            amount      REAL,
            source      TEXT,
            batch_id    TEXT,
            ingested_at TEXT,
            PRIMARY KEY (code, date, freq, adjust)
        );
        CREATE INDEX IF NOT EXISTS idx_pk_code_freq
            ON price_kline(code, freq);
        CREATE INDEX IF NOT EXISTS idx_pk_date
            ON price_kline(date);

        -- 同步状态表（覆盖状态交给审计层推导，不在此表堆字段）
        CREATE TABLE IF NOT EXISTS market_sync_state (
            dataset         TEXT NOT NULL DEFAULT 'price_kline',
            code            TEXT NOT NULL,
            freq            TEXT NOT NULL DEFAULT 'daily',
            adjust          TEXT NOT NULL DEFAULT 'qfq',
            source          TEXT,
            min_date        TEXT,
            max_date        TEXT,
            row_count       INTEGER DEFAULT 0,
            last_success_at TEXT,
            last_attempt_at TEXT,
            last_error      TEXT,
            PRIMARY KEY (dataset, code, freq, adjust)
        );

        -- 导入批次记录
        CREATE TABLE IF NOT EXISTS price_import_batch (
            batch_id        TEXT PRIMARY KEY,
            source_type     TEXT,
            source_name     TEXT,
            freq            TEXT,
            adjust          TEXT,
            rows_imported   INTEGER DEFAULT 0,
            min_date        TEXT,
            max_date        TEXT,
            started_at      TEXT,
            finished_at     TEXT,
            status          TEXT DEFAULT 'running',
            error           TEXT,
            detail          TEXT
        );
        """)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Read Functions
# ---------------------------------------------------------------------------

def get_kline(conn, code: str, date: str, freq: str = "daily",
              field: str = "open") -> Optional[float]:
    """单点价格查询：取指定日期的指定字段值"""
    row = conn.execute(
        f"SELECT [{field}] FROM price_kline "
        "WHERE code=? AND date=? AND freq=? AND adjust='qfq'",
        (code, date, freq)
    ).fetchone()
    if row:
        return row[0]
    # daily 回退到 monthly close
    if freq == "daily":
        row = conn.execute(
            "SELECT [close] FROM price_kline "
            "WHERE code=? AND date<=? AND freq='monthly' AND adjust='qfq' "
            "ORDER BY date DESC LIMIT 1",
            (code, date)
        ).fetchone()
        return row[0] if row else None
    return None


def get_kline_range(conn, code: str, start: str, end: str,
                    freq: str = "daily") -> "list[dict]":
    """区间查询：返回 [{date, open, high, low, close, volume, amount}]"""
    rows = conn.execute(
        "SELECT date, open, high, low, close, volume, amount "
        "FROM price_kline "
        "WHERE code=? AND freq=? AND adjust='qfq' AND date>=? AND date<=? "
        "ORDER BY date",
        (code, freq, start, end)
    ).fetchall()
    return [dict(r) for r in rows]


def get_sync_state(conn, code: str, freq: str = "daily") -> Optional[dict]:
    """查询某只股票的同步状态"""
    row = conn.execute(
        "SELECT * FROM market_sync_state "
        "WHERE dataset='price_kline' AND code=? AND freq=? AND adjust='qfq'",
        (code, freq)
    ).fetchone()
    return dict(row) if row else None


def get_all_sync_states(conn, freq: str = "daily") -> "list[dict]":
    """查询所有股票的同步状态"""
    rows = conn.execute(
        "SELECT * FROM market_sync_state "
        "WHERE dataset='price_kline' AND freq=? AND adjust='qfq'",
        (freq,)
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Write Functions
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def upsert_price_rows(conn, rows: list[dict], source: str,
                       batch_id: str = None) -> int:
    """
    批量写入/更新 K 线数据。
    rows: [{code, date, freq, adjust, open, high, low, close, volume, amount}]
    返回实际写入行数。
    """
    if not rows:
        return 0
    now = _now_iso()
    conn.executemany(
        "INSERT OR REPLACE INTO price_kline "
        "(code, date, freq, adjust, open, high, low, close, volume, amount, "
        " source, batch_id, ingested_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                r["code"], r["date"], r.get("freq", "daily"),
                r.get("adjust", "qfq"),
                r.get("open"), r.get("high"), r.get("low"), r.get("close"),
                r.get("volume"), r.get("amount"),
                source, batch_id, now,
            )
            for r in rows
        ],
    )
    conn.commit()
    return len(rows)


def update_sync_state(conn, code: str, freq: str, *,
                       source: str = None,
                       min_date: str = None,
                       max_date: str = None,
                       row_count: int = None,
                       error: str = None):
    """更新同步状态（UPSERT 语义）"""
    now = _now_iso()
    conn.execute(
        "INSERT INTO market_sync_state "
        "(dataset, code, freq, adjust, source, min_date, max_date, "
        " row_count, last_success_at, last_attempt_at, last_error) "
        "VALUES ('price_kline',?,?,'qfq',?,?,?,?,?,?,?) "
        "ON CONFLICT(dataset, code, freq, adjust) DO UPDATE SET "
        " source=COALESCE(excluded.source, source), "
        " min_date=COALESCE(excluded.min_date, min_date), "
        " max_date=COALESCE(excluded.max_date, max_date), "
        " row_count=COALESCE(excluded.row_count, row_count), "
        " last_success_at=CASE WHEN excluded.last_error IS NULL "
        "   THEN excluded.last_success_at ELSE last_success_at END, "
        " last_attempt_at=excluded.last_attempt_at, "
        " last_error=excluded.last_error",
        (
            code, freq, source, min_date, max_date, row_count,
            now if error is None else None,  # last_success_at
            now,                              # last_attempt_at
            error,                            # last_error
        ),
    )
    conn.commit()


def start_import_batch(conn, source_type: str, source_name: str,
                        freq: str, adjust: str = "qfq") -> str:
    """创建导入批次，返回 batch_id"""
    now = _now_iso()
    batch_id = f"{source_type}_{now.replace(' ', '_').replace(':', '')}"
    conn.execute(
        "INSERT INTO price_import_batch "
        "(batch_id, source_type, source_name, freq, adjust, started_at, status) "
        "VALUES (?,?,?,?,?,?,?)",
        (batch_id, source_type, source_name, freq, adjust, now, "running"),
    )
    conn.commit()
    return batch_id


def finish_import_batch(conn, batch_id: str, *,
                         rows_imported: int = 0,
                         min_date: str = None,
                         max_date: str = None,
                         status: str = "completed",
                         error: str = None,
                         detail: str = None):
    """完成导入批次"""
    now = _now_iso()
    conn.execute(
        "UPDATE price_import_batch SET "
        " rows_imported=?, min_date=?, max_date=?, "
        " finished_at=?, status=?, error=?, detail=? "
        "WHERE batch_id=?",
        (rows_imported, min_date, max_date, now, status, error, detail,
         batch_id),
    )
    conn.commit()

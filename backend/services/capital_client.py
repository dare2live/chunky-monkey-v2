"""
capital_client.py — 资本行为增强数据同步与聚合

当前版本聚焦三类可稳定获取且直接服务质量分的数据：
1. 历史分红摘要
2. 股票回购数据
3. 未来限售解禁压力

数据流：
    AKShare
        -> raw_capital_dividend_summary
        -> raw_capital_repurchase
        -> raw_capital_unlock
        -> dim_capital_behavior_latest
"""

import asyncio
import logging
from datetime import date, datetime, timedelta
from typing import Optional

logger = logging.getLogger("cm-api")

UNLOCK_LOOKAHEAD_DAYS = 365
CAPITAL_DETAIL_BATCH_SIZE = 12
CAPITAL_DETAIL_LOOKBACK_YEARS = 5


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def _ensure_columns(conn, table_name: str, columns: dict[str, str]) -> None:
    existing = _table_columns(conn, table_name)
    for col, ddl in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {ddl}")


def _parse_date(value) -> Optional[str]:
    if value is None:
        return None
    try:
        if value != value:
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "nat"} or text in {"--", "-"}:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        digits = digits[:8]
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    if len(text) == 10 and "-" in text:
        return text
    return None


def _parse_float(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        try:
            return None if value != value else float(value)
        except Exception:
            return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"} or text in {"--", "-"}:
        return None
    text = text.replace(",", "").replace("%", "").replace("元", "").replace("股", "").replace(" ", "")
    try:
        return float(text)
    except Exception:
        return None


def _parse_int(value):
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _normalize_stock_code(value) -> Optional[str]:
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    return digits[-6:].zfill(6)


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS raw_capital_dividend_summary (
            snapshot_date        TEXT NOT NULL,
            stock_code           TEXT NOT NULL,
            stock_name           TEXT,
            listed_date          TEXT,
            cumulative_dividend  REAL,
            avg_annual_dividend  REAL,
            dividend_count       INTEGER,
            financing_total      REAL,
            financing_count      INTEGER,
            created_at           TEXT,
            PRIMARY KEY (snapshot_date, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_rcds_stock ON raw_capital_dividend_summary(stock_code);

        CREATE TABLE IF NOT EXISTS raw_capital_repurchase (
            event_id               TEXT PRIMARY KEY,
            snapshot_date          TEXT NOT NULL,
            stock_code             TEXT NOT NULL,
            stock_name             TEXT,
            latest_price           REAL,
            planned_price_ceiling  REAL,
            planned_amount_low     REAL,
            planned_amount_high    REAL,
            planned_ratio_low      REAL,
            planned_ratio_high     REAL,
            repurchase_start_date  TEXT,
            progress               TEXT,
            repurchased_shares     REAL,
            repurchased_amount     REAL,
            latest_notice_date     TEXT,
            created_at             TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rcr_stock ON raw_capital_repurchase(stock_code);
        CREATE INDEX IF NOT EXISTS idx_rcr_notice ON raw_capital_repurchase(latest_notice_date);

        CREATE TABLE IF NOT EXISTS raw_capital_unlock (
            event_id                TEXT PRIMARY KEY,
            snapshot_date           TEXT NOT NULL,
            stock_code              TEXT NOT NULL,
            stock_name              TEXT,
            unlock_date             TEXT,
            unlock_type             TEXT,
            unlock_shares           REAL,
            actual_unlock_shares    REAL,
            actual_unlock_value     REAL,
            unlock_ratio_float_mkt  REAL,
            preclose_price          REAL,
            pre20d_pct              REAL,
            post20d_pct             REAL,
            created_at              TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_rcu_stock ON raw_capital_unlock(stock_code);
        CREATE INDEX IF NOT EXISTS idx_rcu_date ON raw_capital_unlock(unlock_date);

        CREATE TABLE IF NOT EXISTS raw_capital_dividend_detail (
            stock_code           TEXT NOT NULL,
            notice_date          TEXT NOT NULL,
            progress             TEXT,
            send_shares          REAL,
            transfer_shares      REAL,
            cash_dividend        REAL,
            ex_dividend_date     TEXT,
            record_date          TEXT,
            listing_date         TEXT,
            source               TEXT,
            updated_at           TEXT,
            PRIMARY KEY (stock_code, notice_date)
        );
        CREATE INDEX IF NOT EXISTS idx_rcdd_stock ON raw_capital_dividend_detail(stock_code);
        CREATE INDEX IF NOT EXISTS idx_rcdd_notice ON raw_capital_dividend_detail(notice_date);

        CREATE TABLE IF NOT EXISTS raw_capital_allotment_detail (
            stock_code             TEXT NOT NULL,
            notice_date            TEXT NOT NULL,
            allotment_plan         REAL,
            allotment_price        REAL,
            base_shares            REAL,
            ex_rights_date         TEXT,
            record_date            TEXT,
            payment_start_date     TEXT,
            payment_end_date       TEXT,
            listing_date           TEXT,
            raised_funds_total     REAL,
            source                 TEXT,
            updated_at             TEXT,
            PRIMARY KEY (stock_code, notice_date)
        );
        CREATE INDEX IF NOT EXISTS idx_rcad_stock ON raw_capital_allotment_detail(stock_code);
        CREATE INDEX IF NOT EXISTS idx_rcad_notice ON raw_capital_allotment_detail(notice_date);

        CREATE TABLE IF NOT EXISTS capital_detail_sync_state (
            stock_code                   TEXT PRIMARY KEY,
            dividend_rows                INTEGER DEFAULT 0,
            allotment_rows               INTEGER DEFAULT 0,
            last_dividend_notice_date    TEXT,
            last_allotment_notice_date   TEXT,
            last_synced_at               TEXT,
            status                       TEXT DEFAULT 'pending',
            error                        TEXT,
            updated_at                   TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_cdss_status ON capital_detail_sync_state(status);

        CREATE TABLE IF NOT EXISTS dim_capital_behavior_latest (
            stock_code                 TEXT PRIMARY KEY,
            stock_name                 TEXT,
            listed_date                TEXT,
            listed_days                INTEGER,
            cumulative_dividend        REAL,
            avg_annual_dividend        REAL,
            dividend_count             INTEGER,
            financing_total            REAL,
            financing_count            INTEGER,
            dividend_financing_ratio   REAL,
            repurchase_count_3y        INTEGER,
            repurchase_amount_3y       REAL,
            repurchase_ratio_sum_3y    REAL,
            active_repurchase_count    INTEGER,
            future_unlock_count_180d   INTEGER,
            future_unlock_amount_180d  REAL,
            future_unlock_value_180d   REAL,
            future_unlock_ratio_180d   REAL,
            future_unlock_count_365d   INTEGER,
            future_unlock_ratio_365d   REAL,
            last_dividend_notice_date  TEXT,
            dividend_cash_sum_5y       REAL,
            dividend_event_count_5y    INTEGER,
            dividend_implemented_count_5y INTEGER,
            last_allotment_notice_date TEXT,
            allotment_count_5y         INTEGER,
            allotment_ratio_sum_5y     REAL,
            allotment_raised_funds_5y  REAL,
            updated_at                 TEXT
        );
    """)
    _ensure_columns(conn, "dim_capital_behavior_latest", {
        "last_dividend_notice_date": "TEXT",
        "dividend_cash_sum_5y": "REAL",
        "dividend_event_count_5y": "INTEGER DEFAULT 0",
        "dividend_implemented_count_5y": "INTEGER DEFAULT 0",
        "last_allotment_notice_date": "TEXT",
        "allotment_count_5y": "INTEGER DEFAULT 0",
        "allotment_ratio_sum_5y": "REAL",
        "allotment_raised_funds_5y": "REAL",
    })
    conn.commit()


def _latest_snapshot_date(conn, table_name: str) -> Optional[str]:
    row = conn.execute(
        f"SELECT MAX(snapshot_date) AS snapshot_date FROM {table_name}"
    ).fetchone()
    return row["snapshot_date"] if row and row["snapshot_date"] else None


def _need_refresh(conn, table_name: str, snapshot_date: str) -> bool:
    return _latest_snapshot_date(conn, table_name) != snapshot_date


def _fetch_dividend_summary():
    import akshare as ak
    return ak.stock_history_dividend()


def _fetch_repurchase():
    import akshare as ak
    return ak.stock_repurchase_em()


def _fetch_unlock_detail(start_date: str, end_date: str):
    import akshare as ak
    return ak.stock_restricted_release_detail_em(start_date=start_date, end_date=end_date)


def _fetch_dividend_detail(symbol: str):
    import akshare as ak
    return ak.stock_history_dividend_detail(symbol=symbol, indicator="分红")


def _fetch_allotment_detail(symbol: str):
    import akshare as ak
    return ak.stock_history_dividend_detail(symbol=symbol, indicator="配股")


def _store_dividend_summary(conn, snapshot_date: str, created_at: str, df) -> int:
    inserted = 0
    for row in df.to_dict("records"):
        conn.execute("""
            INSERT OR REPLACE INTO raw_capital_dividend_summary
            (snapshot_date, stock_code, stock_name, listed_date, cumulative_dividend,
             avg_annual_dividend, dividend_count, financing_total, financing_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_date,
            str(row.get("代码") or "").zfill(6),
            row.get("名称"),
            _parse_date(row.get("上市日期")),
            _parse_float(row.get("累计股息")),
            _parse_float(row.get("年均股息")),
            _parse_int(row.get("分红次数")),
            _parse_float(row.get("融资总额")),
            _parse_int(row.get("融资次数")),
            created_at,
        ))
        inserted += 1
    conn.commit()
    return inserted


def _repurchase_event_id(stock_code: str, notice_date: Optional[str], start_date: Optional[str], progress: Optional[str]) -> str:
    return "|".join([
        stock_code or "",
        notice_date or "",
        start_date or "",
        str(progress or "").strip(),
    ])


def _store_repurchase(conn, snapshot_date: str, created_at: str, df) -> int:
    inserted = 0
    for row in df.to_dict("records"):
        stock_code = str(row.get("股票代码") or "").zfill(6)
        notice_date = _parse_date(row.get("最新公告日期"))
        start_date = _parse_date(row.get("回购起始时间"))
        event_id = _repurchase_event_id(stock_code, notice_date, start_date, row.get("实施进度"))
        conn.execute("""
            INSERT OR REPLACE INTO raw_capital_repurchase
            (event_id, snapshot_date, stock_code, stock_name, latest_price, planned_price_ceiling,
             planned_amount_low, planned_amount_high, planned_ratio_low, planned_ratio_high,
             repurchase_start_date, progress, repurchased_shares, repurchased_amount,
             latest_notice_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            snapshot_date,
            stock_code,
            row.get("股票简称"),
            _parse_float(row.get("最新价")),
            _parse_float(row.get("计划回购价格区间")),
            _parse_float(row.get("计划回购金额区间-下限")),
            _parse_float(row.get("计划回购金额区间-上限")),
            _parse_float(row.get("占公告前一日总股本比例-下限")),
            _parse_float(row.get("占公告前一日总股本比例-上限")),
            start_date,
            row.get("实施进度"),
            _parse_float(row.get("已回购股份数量")),
            _parse_float(row.get("已回购金额")),
            notice_date,
            created_at,
        ))
        inserted += 1
    conn.commit()
    return inserted


def _unlock_event_id(stock_code: str, unlock_date: Optional[str], unlock_type: Optional[str]) -> str:
    return "|".join([stock_code or "", unlock_date or "", str(unlock_type or "").strip()])


def _store_unlock(conn, snapshot_date: str, created_at: str, df) -> int:
    inserted = 0
    for row in df.to_dict("records"):
        stock_code = str(row.get("股票代码") or "").zfill(6)
        unlock_date = _parse_date(row.get("解禁时间"))
        unlock_type = row.get("限售股类型")
        event_id = _unlock_event_id(stock_code, unlock_date, unlock_type)
        conn.execute("""
            INSERT OR REPLACE INTO raw_capital_unlock
            (event_id, snapshot_date, stock_code, stock_name, unlock_date, unlock_type,
             unlock_shares, actual_unlock_shares, actual_unlock_value, unlock_ratio_float_mkt,
             preclose_price, pre20d_pct, post20d_pct, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id,
            snapshot_date,
            stock_code,
            row.get("股票简称"),
            unlock_date,
            unlock_type,
            _parse_float(row.get("解禁数量")),
            _parse_float(row.get("实际解禁数量")),
            _parse_float(row.get("实际解禁市值")),
            _parse_float(row.get("占解禁前流通市值比例")),
            _parse_float(row.get("解禁前一交易日收盘价")),
            _parse_float(row.get("解禁前20日涨跌幅")),
            _parse_float(row.get("解禁后20日涨跌幅")),
            created_at,
        ))
        inserted += 1
    conn.commit()
    return inserted


def _store_dividend_detail(conn, stock_code: str, created_at: str, df) -> int:
    inserted = 0
    for row in (df.to_dict("records") if df is not None and not df.empty else []):
        notice_date = _parse_date(row.get("公告日期"))
        if not notice_date:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO raw_capital_dividend_detail
            (stock_code, notice_date, progress, send_shares, transfer_shares, cash_dividend,
             ex_dividend_date, record_date, listing_date, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_code,
            notice_date,
            row.get("进度"),
            _parse_float(row.get("送股")),
            _parse_float(row.get("转增")),
            _parse_float(row.get("派息")),
            _parse_date(row.get("除权除息日")),
            _parse_date(row.get("股权登记日")),
            _parse_date(row.get("红股上市日")),
            "akshare_stock_history_dividend_detail_dividend",
            created_at,
        ))
        inserted += 1
    conn.commit()
    return inserted


def _store_allotment_detail(conn, stock_code: str, created_at: str, df) -> int:
    inserted = 0
    for row in (df.to_dict("records") if df is not None and not df.empty else []):
        notice_date = _parse_date(row.get("公告日期"))
        if not notice_date:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO raw_capital_allotment_detail
            (stock_code, notice_date, allotment_plan, allotment_price, base_shares,
             ex_rights_date, record_date, payment_start_date, payment_end_date,
             listing_date, raised_funds_total, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_code,
            notice_date,
            _parse_float(row.get("配股方案")),
            _parse_float(row.get("配股价格")),
            _parse_float(row.get("基准股本")),
            _parse_date(row.get("除权日")),
            _parse_date(row.get("股权登记日")),
            _parse_date(row.get("缴款起始日")),
            _parse_date(row.get("缴款终止日")),
            _parse_date(row.get("配股上市日")),
            _parse_float(row.get("募集资金合计")),
            "akshare_stock_history_dividend_detail_allotment",
            created_at,
        ))
        inserted += 1
    conn.commit()
    return inserted


def _select_capital_detail_candidates(conn, snapshot_date: str, stock_codes: Optional[list] = None,
                                      limit: int = CAPITAL_DETAIL_BATCH_SIZE) -> list[str]:
    params = [snapshot_date]
    in_clause = ""
    if stock_codes:
        normalized = [_normalize_stock_code(code) for code in stock_codes if _normalize_stock_code(code)]
        if not normalized:
            return []
        placeholders = ",".join("?" for _ in normalized)
        in_clause = f" AND a.stock_code IN ({placeholders}) "
        params.extend(normalized)
    params.append(limit)

    rows = conn.execute(f"""
        SELECT a.stock_code
        FROM dim_active_a_stock a
        LEFT JOIN excluded_stocks e ON e.stock_code = a.stock_code
        LEFT JOIN mart_stock_trend t ON t.stock_code = a.stock_code
        LEFT JOIN capital_detail_sync_state s ON s.stock_code = a.stock_code
        WHERE e.stock_code IS NULL
          AND (
                s.stock_code IS NULL
             OR COALESCE(substr(s.last_synced_at, 1, 10), '') <> ?
             OR COALESCE(s.status, '') IN ('failed', 'partial', 'pending')
          )
          {in_clause}
        ORDER BY
            CASE WHEN t.stock_code IS NOT NULL THEN 0 ELSE 1 END,
            CASE
                WHEN s.stock_code IS NULL THEN 0
                WHEN COALESCE(s.status, '') = 'failed' THEN 1
                WHEN COALESCE(s.status, '') = 'partial' THEN 2
                WHEN COALESCE(s.status, '') = 'pending' THEN 3
                WHEN COALESCE(s.status, '') = 'empty' THEN 4
                ELSE 5
            END,
            COALESCE(s.last_synced_at, ''),
            a.stock_code
        LIMIT ?
    """, params).fetchall()
    return [row["stock_code"] for row in rows]


def _update_capital_detail_state(conn, stock_code: str, synced_at: str, error: Optional[str] = None) -> None:
    dividend_row = conn.execute("""
        SELECT COUNT(*) AS cnt, MAX(notice_date) AS last_notice_date
        FROM raw_capital_dividend_detail
        WHERE stock_code = ?
    """, (stock_code,)).fetchone()
    allotment_row = conn.execute("""
        SELECT COUNT(*) AS cnt, MAX(notice_date) AS last_notice_date
        FROM raw_capital_allotment_detail
        WHERE stock_code = ?
    """, (stock_code,)).fetchone()

    dividend_rows = dividend_row["cnt"] if dividend_row else 0
    allotment_rows = allotment_row["cnt"] if allotment_row else 0
    last_dividend_notice_date = dividend_row["last_notice_date"] if dividend_row else None
    last_allotment_notice_date = allotment_row["last_notice_date"] if allotment_row else None

    if error and dividend_rows == 0 and allotment_rows == 0:
        status = "failed"
    elif error:
        status = "partial"
    elif dividend_rows == 0 and allotment_rows == 0:
        status = "empty"
    else:
        status = "ok"

    conn.execute("""
        INSERT INTO capital_detail_sync_state
        (stock_code, dividend_rows, allotment_rows, last_dividend_notice_date,
         last_allotment_notice_date, last_synced_at, status, error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
            dividend_rows = excluded.dividend_rows,
            allotment_rows = excluded.allotment_rows,
            last_dividend_notice_date = excluded.last_dividend_notice_date,
            last_allotment_notice_date = excluded.last_allotment_notice_date,
            last_synced_at = excluded.last_synced_at,
            status = excluded.status,
            error = excluded.error,
            updated_at = excluded.updated_at
    """, (
        stock_code,
        dividend_rows,
        allotment_rows,
        last_dividend_notice_date,
        last_allotment_notice_date,
        synced_at,
        status,
        error,
        synced_at,
    ))


def build_capital_behavior_latest(conn, as_of_date: Optional[str] = None) -> int:
    ensure_tables(conn)
    snapshot_date = as_of_date or date.today().strftime("%Y-%m-%d")
    repurchase_snapshot = _latest_snapshot_date(conn, "raw_capital_repurchase")
    unlock_snapshot = _latest_snapshot_date(conn, "raw_capital_unlock")
    dividend_snapshot = _latest_snapshot_date(conn, "raw_capital_dividend_summary")
    if not any([repurchase_snapshot, unlock_snapshot, dividend_snapshot]):
        return 0

    now = datetime.now().isoformat()
    as_of = date.fromisoformat(snapshot_date)
    cutoff_3y = (as_of - timedelta(days=365 * 3)).strftime("%Y-%m-%d")
    cutoff_5y = (as_of - timedelta(days=365 * CAPITAL_DETAIL_LOOKBACK_YEARS)).strftime("%Y-%m-%d")
    horizon_180 = (as_of + timedelta(days=180)).strftime("%Y-%m-%d")
    horizon_365 = (as_of + timedelta(days=365)).strftime("%Y-%m-%d")

    conn.execute("DELETE FROM dim_capital_behavior_latest")
    conn.execute(f"""
        INSERT INTO dim_capital_behavior_latest
        (stock_code, stock_name, listed_date, listed_days, cumulative_dividend, avg_annual_dividend,
         dividend_count, financing_total, financing_count, dividend_financing_ratio,
         repurchase_count_3y, repurchase_amount_3y, repurchase_ratio_sum_3y, active_repurchase_count,
         future_unlock_count_180d, future_unlock_amount_180d, future_unlock_value_180d, future_unlock_ratio_180d,
         future_unlock_count_365d, future_unlock_ratio_365d, last_dividend_notice_date, dividend_cash_sum_5y,
         dividend_event_count_5y, dividend_implemented_count_5y, last_allotment_notice_date,
         allotment_count_5y, allotment_ratio_sum_5y, allotment_raised_funds_5y, updated_at)
        WITH base AS (
            SELECT stock_code, stock_name, listed_date, cumulative_dividend, avg_annual_dividend,
                   dividend_count, financing_total, financing_count
            FROM raw_capital_dividend_summary
            WHERE snapshot_date = ?
        ),
        rep AS (
            SELECT stock_code,
                   COUNT(*) AS repurchase_count_3y,
                   SUM(COALESCE(repurchased_amount, planned_amount_low, 0)) AS repurchase_amount_3y,
                   SUM(COALESCE(planned_ratio_high, planned_ratio_low, 0)) AS repurchase_ratio_sum_3y,
                   SUM(CASE WHEN progress IN ('董事会预案', '股东大会通过', '实施中') THEN 1 ELSE 0 END) AS active_repurchase_count
            FROM raw_capital_repurchase
            WHERE snapshot_date = ?
              AND COALESCE(latest_notice_date, repurchase_start_date, ?) >= ?
            GROUP BY stock_code
        ),
        unl AS (
            SELECT stock_code,
                   SUM(CASE WHEN unlock_date <= ? THEN 1 ELSE 0 END) AS future_unlock_count_180d,
                   SUM(CASE WHEN unlock_date <= ? THEN COALESCE(actual_unlock_shares, unlock_shares, 0) ELSE 0 END) AS future_unlock_amount_180d,
                   SUM(CASE WHEN unlock_date <= ? THEN COALESCE(actual_unlock_value, 0) ELSE 0 END) AS future_unlock_value_180d,
                   SUM(CASE WHEN unlock_date <= ? THEN COALESCE(unlock_ratio_float_mkt, 0) ELSE 0 END) AS future_unlock_ratio_180d,
                   COUNT(*) AS future_unlock_count_365d,
                   SUM(COALESCE(unlock_ratio_float_mkt, 0)) AS future_unlock_ratio_365d
            FROM raw_capital_unlock
            WHERE snapshot_date = ?
              AND unlock_date >= ?
              AND unlock_date <= ?
            GROUP BY stock_code
        ),
        dvd AS (
            SELECT stock_code,
                   MAX(notice_date) AS last_dividend_notice_date,
                   SUM(CASE WHEN notice_date >= ? THEN COALESCE(cash_dividend, 0) ELSE 0 END) AS dividend_cash_sum_5y,
                   SUM(CASE WHEN notice_date >= ? THEN 1 ELSE 0 END) AS dividend_event_count_5y,
                   SUM(CASE WHEN notice_date >= ? AND progress = '实施' THEN 1 ELSE 0 END) AS dividend_implemented_count_5y
            FROM raw_capital_dividend_detail
            GROUP BY stock_code
        ),
        allm AS (
            SELECT stock_code,
                   MAX(notice_date) AS last_allotment_notice_date,
                   SUM(CASE WHEN notice_date >= ? THEN 1 ELSE 0 END) AS allotment_count_5y,
                   SUM(CASE WHEN notice_date >= ? THEN COALESCE(allotment_plan, 0) ELSE 0 END) AS allotment_ratio_sum_5y,
                   SUM(
                       CASE WHEN notice_date >= ? THEN
                           COALESCE(
                               raised_funds_total,
                               (COALESCE(base_shares, 0) * COALESCE(allotment_plan, 0) / 10.0 * COALESCE(allotment_price, 0))
                           )
                       ELSE 0 END
                   ) AS allotment_raised_funds_5y
            FROM raw_capital_allotment_detail
            GROUP BY stock_code
        )
        SELECT
            b.stock_code,
            b.stock_name,
            b.listed_date,
            CAST(julianday(?) - julianday(b.listed_date) AS INTEGER) AS listed_days,
            b.cumulative_dividend,
            b.avg_annual_dividend,
            b.dividend_count,
            b.financing_total,
            b.financing_count,
            CASE
                WHEN COALESCE(b.financing_total, 0) <= 0 THEN NULL
                WHEN COALESCE(b.cumulative_dividend, 0) <= 0 THEN 0
                ELSE b.cumulative_dividend / b.financing_total
            END AS dividend_financing_ratio,
            COALESCE(r.repurchase_count_3y, 0),
            COALESCE(r.repurchase_amount_3y, 0),
            COALESCE(r.repurchase_ratio_sum_3y, 0),
            COALESCE(r.active_repurchase_count, 0),
            COALESCE(u.future_unlock_count_180d, 0),
            COALESCE(u.future_unlock_amount_180d, 0),
            COALESCE(u.future_unlock_value_180d, 0),
            COALESCE(u.future_unlock_ratio_180d, 0),
            COALESCE(u.future_unlock_count_365d, 0),
            COALESCE(u.future_unlock_ratio_365d, 0),
            d.last_dividend_notice_date,
            COALESCE(d.dividend_cash_sum_5y, 0),
            COALESCE(d.dividend_event_count_5y, 0),
            COALESCE(d.dividend_implemented_count_5y, 0),
            a.last_allotment_notice_date,
            COALESCE(a.allotment_count_5y, 0),
            COALESCE(a.allotment_ratio_sum_5y, 0),
            COALESCE(a.allotment_raised_funds_5y, 0),
            ?
        FROM base b
        LEFT JOIN rep r ON r.stock_code = b.stock_code
        LEFT JOIN unl u ON u.stock_code = b.stock_code
        LEFT JOIN dvd d ON d.stock_code = b.stock_code
        LEFT JOIN allm a ON a.stock_code = b.stock_code
    """, (
        dividend_snapshot,
        repurchase_snapshot or "",
        snapshot_date,
        cutoff_3y,
        horizon_180,
        horizon_180,
        horizon_180,
        horizon_180,
        unlock_snapshot or "",
        snapshot_date,
        horizon_365,
        cutoff_5y,
        cutoff_5y,
        cutoff_5y,
        cutoff_5y,
        cutoff_5y,
        cutoff_5y,
        snapshot_date,
        now,
    ))
    conn.commit()
    return conn.execute("SELECT COUNT(*) FROM dim_capital_behavior_latest").fetchone()[0]


async def sync_capital_behavior_data(conn, snapshot_date: Optional[str] = None,
                                     stock_codes: Optional[list] = None) -> int:
    ensure_tables(conn)
    snapshot_date = snapshot_date or date.today().strftime("%Y-%m-%d")
    created_at = datetime.now().isoformat()
    loop = asyncio.get_running_loop()

    total = 0

    if _need_refresh(conn, "raw_capital_dividend_summary", snapshot_date):
        logger.info("[资本] 开始同步历史分红摘要")
        df = await loop.run_in_executor(None, _fetch_dividend_summary)
        inserted = _store_dividend_summary(conn, snapshot_date, created_at, df)
        logger.info(f"[资本] 历史分红摘要同步完成: {inserted} 条")
        total += inserted
    else:
        logger.info("[资本] 今日历史分红摘要已同步，跳过")

    if _need_refresh(conn, "raw_capital_repurchase", snapshot_date):
        logger.info("[资本] 开始同步股票回购数据")
        df = await loop.run_in_executor(None, _fetch_repurchase)
        inserted = _store_repurchase(conn, snapshot_date, created_at, df)
        logger.info(f"[资本] 股票回购同步完成: {inserted} 条")
        total += inserted
    else:
        logger.info("[资本] 今日股票回购数据已同步，跳过")

    if _need_refresh(conn, "raw_capital_unlock", snapshot_date):
        logger.info(f"[资本] 开始同步未来 {UNLOCK_LOOKAHEAD_DAYS} 天限售解禁")
        start = date.fromisoformat(snapshot_date)
        end = start + timedelta(days=UNLOCK_LOOKAHEAD_DAYS)
        df = await loop.run_in_executor(
            None,
            _fetch_unlock_detail,
            start.strftime("%Y%m%d"),
            end.strftime("%Y%m%d"),
        )
        inserted = _store_unlock(conn, snapshot_date, created_at, df)
        logger.info(f"[资本] 限售解禁同步完成: {inserted} 条")
        total += inserted
    else:
        logger.info("[资本] 今日限售解禁数据已同步，跳过")

    detail_candidates = _select_capital_detail_candidates(
        conn, snapshot_date=snapshot_date, stock_codes=stock_codes, limit=CAPITAL_DETAIL_BATCH_SIZE
    )
    if detail_candidates:
        logger.info(f"[资本] 开始同步 {len(detail_candidates)} 只股票的分红/配股明细")
        for code in detail_candidates:
            errors = []
            try:
                dividend_df = await loop.run_in_executor(None, _fetch_dividend_detail, code)
                total += _store_dividend_detail(conn, code, created_at, dividend_df)
            except Exception as exc:
                errors.append(f"dividend:{str(exc)[:160]}")
            try:
                allotment_df = await loop.run_in_executor(None, _fetch_allotment_detail, code)
                total += _store_allotment_detail(conn, code, created_at, allotment_df)
            except Exception as exc:
                errors.append(f"allotment:{str(exc)[:160]}")
            _update_capital_detail_state(
                conn,
                code,
                synced_at=created_at,
                error=" | ".join(errors) if errors else None,
            )
        conn.commit()
        logger.info("[资本] 分红/配股明细同步完成")
    else:
        logger.info("[资本] 今日资本明细覆盖已达到当前批次目标，跳过")

    dim_count = build_capital_behavior_latest(conn, snapshot_date)
    logger.info(f"[资本] 最新资本行为聚合完成: {dim_count} 只股票")
    return total

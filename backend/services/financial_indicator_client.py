"""
financial_indicator_client.py — AKShare 财务分析指标增强

使用 stock_financial_abstract() 增量回填质量分所需的扩展指标。

当前聚焦指标：
- ROE / ROA
- 毛利率 / 销售净利率
- 流动比率 / 速动比率 / 资产负债率
- 总资产周转率 / 存货周转率 / 应收账款周转率
- 营业总收入增长率 / 归母净利润增长率
"""

import asyncio
import logging
import re
from datetime import datetime
from typing import Optional

logger = logging.getLogger("cm-api")

INDICATOR_HISTORY_TARGET_ROWS = 8
INDICATOR_FETCH_LIMIT = 12
INDICATOR_BATCH_SIZE = 12

_DATE_COL_RE = re.compile(r"^\d{8}$")

_ROW_KEY_MAP = {
    "roe_ak": ["净资产收益率(ROE)", "净资产收益率_平均", "净资产收益率"],
    "roa_ak": ["总资产报酬率(ROA)", "总资产报酬率", "总资产净利率_平均"],
    "gross_margin_ak": ["毛利率"],
    "net_margin_ak": ["销售净利率"],
    "current_ratio_ak": ["流动比率"],
    "quick_ratio_ak": ["速动比率"],
    "debt_ratio_ak": ["资产负债率"],
    "asset_turnover_ak": ["总资产周转率"],
    "inventory_turnover_ak": ["存货周转率"],
    "receivables_turnover_ak": ["应收账款周转率"],
    "revenue_growth_yoy_ak": ["营业总收入增长率"],
    "net_profit_growth_yoy_ak": ["归属母公司净利润增长率"],
}


def _parse_date(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"} or text in {"--", "-"}:
        return None
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
    text = text.replace(",", "").replace("%", "").replace(" ", "")
    try:
        return float(text)
    except Exception:
        return None


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_financial_indicator_ak (
            stock_code                    TEXT NOT NULL,
            report_date                   TEXT NOT NULL,
            roe_ak                        REAL,
            roa_ak                        REAL,
            gross_margin_ak               REAL,
            net_margin_ak                 REAL,
            current_ratio_ak              REAL,
            quick_ratio_ak                REAL,
            debt_ratio_ak                 REAL,
            asset_turnover_ak             REAL,
            inventory_turnover_ak         REAL,
            receivables_turnover_ak       REAL,
            revenue_growth_yoy_ak         REAL,
            net_profit_growth_yoy_ak      REAL,
            source                        TEXT,
            updated_at                    TEXT,
            PRIMARY KEY (stock_code, report_date)
        );
        CREATE INDEX IF NOT EXISTS idx_ffia_report ON fact_financial_indicator_ak(report_date);

        CREATE TABLE IF NOT EXISTS dim_financial_indicator_latest (
            stock_code                    TEXT PRIMARY KEY,
            latest_report_date            TEXT,
            roe_ak                        REAL,
            roa_ak                        REAL,
            gross_margin_ak               REAL,
            net_margin_ak                 REAL,
            current_ratio_ak              REAL,
            quick_ratio_ak                REAL,
            debt_ratio_ak                 REAL,
            asset_turnover_ak             REAL,
            inventory_turnover_ak         REAL,
            receivables_turnover_ak       REAL,
            revenue_growth_yoy_ak         REAL,
            net_profit_growth_yoy_ak      REAL,
            history_rows                  INTEGER DEFAULT 0,
            updated_at                    TEXT
        );

        CREATE TABLE IF NOT EXISTS financial_indicator_sync_state (
            stock_code        TEXT PRIMARY KEY,
            history_rows      INTEGER DEFAULT 0,
            last_report_date  TEXT,
            last_synced_at    TEXT,
            status            TEXT DEFAULT 'pending',
            error             TEXT,
            updated_at        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fiss_status ON financial_indicator_sync_state(status);
    """)
    conn.commit()


def _select_indicator_candidates(conn, stock_codes: Optional[list] = None, limit: int = INDICATOR_BATCH_SIZE) -> list[str]:
    params = [INDICATOR_HISTORY_TARGET_ROWS]
    in_clause = ""
    if stock_codes:
        normalized = [str(code).strip() for code in stock_codes if str(code).strip()]
        if not normalized:
            return []
        placeholders = ",".join("?" for _ in normalized)
        in_clause = f" AND a.stock_code IN ({placeholders}) "
        params.extend(normalized)
    params.append(limit)

    rows = conn.execute(
        f"""
        WITH hist AS (
            SELECT stock_code, COUNT(*) AS history_rows, MAX(report_date) AS latest_report_date
            FROM fact_financial_indicator_ak
            GROUP BY stock_code
        )
        SELECT a.stock_code
        FROM dim_active_a_stock a
        LEFT JOIN excluded_stocks e ON e.stock_code = a.stock_code
        LEFT JOIN mart_stock_trend t ON t.stock_code = a.stock_code
        LEFT JOIN hist h ON h.stock_code = a.stock_code
        LEFT JOIN financial_indicator_sync_state s ON s.stock_code = a.stock_code
        WHERE e.stock_code IS NULL
          AND (
                COALESCE(h.history_rows, 0) < ?
             OR COALESCE(s.status, '') IN ('failed', 'empty', 'partial')
          )
          {in_clause}
        ORDER BY
            CASE WHEN t.stock_code IS NOT NULL THEN 0 ELSE 1 END,
            COALESCE(h.history_rows, 0) ASC,
            CASE WHEN s.last_synced_at IS NULL THEN 0 ELSE 1 END,
            COALESCE(s.last_synced_at, ''),
            a.stock_code
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [row["stock_code"] for row in rows]


def _fetch_financial_abstract(symbol: str):
    import akshare as ak
    return ak.stock_financial_abstract(symbol=symbol)


def _parse_indicator_rows(stock_code: str, df) -> list[dict]:
    if df is None or df.empty:
        return []

    date_cols = [col for col in df.columns if _DATE_COL_RE.match(str(col))]
    if not date_cols:
        return []
    date_cols = sorted(date_cols, reverse=True)[:INDICATOR_FETCH_LIMIT]

    metric_rows = {}
    indexed_rows = {}
    for row in df.to_dict("records"):
        metric_name = str(row.get("指标") or "").strip()
        if metric_name and metric_name not in indexed_rows:
            indexed_rows[metric_name] = row

    for target_key, aliases in _ROW_KEY_MAP.items():
        for alias in aliases:
            if alias in indexed_rows:
                metric_rows[target_key] = indexed_rows[alias]
                break

    records = []
    for date_col in date_cols:
        report_date = _parse_date(date_col)
        if not report_date:
            continue
        rec = {
            "stock_code": stock_code,
            "report_date": report_date,
            "source": "akshare_financial_abstract",
            "updated_at": datetime.now().isoformat(),
        }
        has_value = False
        for target_key in _ROW_KEY_MAP:
            row = metric_rows.get(target_key)
            value = _parse_float(row.get(date_col)) if row else None
            rec[target_key] = value
            if value is not None:
                has_value = True
        if has_value:
            records.append(rec)

    return records


def _upsert_indicator_records(conn, stock_code: str, records: list[dict], synced_at: str, error: Optional[str] = None) -> int:
    inserted = 0
    for rec in records:
        conn.execute("""
            INSERT OR REPLACE INTO fact_financial_indicator_ak
            (stock_code, report_date, roe_ak, roa_ak, gross_margin_ak, net_margin_ak,
             current_ratio_ak, quick_ratio_ak, debt_ratio_ak, asset_turnover_ak,
             inventory_turnover_ak, receivables_turnover_ak, revenue_growth_yoy_ak,
             net_profit_growth_yoy_ak, source, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_code,
            rec.get("report_date"),
            rec.get("roe_ak"),
            rec.get("roa_ak"),
            rec.get("gross_margin_ak"),
            rec.get("net_margin_ak"),
            rec.get("current_ratio_ak"),
            rec.get("quick_ratio_ak"),
            rec.get("debt_ratio_ak"),
            rec.get("asset_turnover_ak"),
            rec.get("inventory_turnover_ak"),
            rec.get("receivables_turnover_ak"),
            rec.get("revenue_growth_yoy_ak"),
            rec.get("net_profit_growth_yoy_ak"),
            rec.get("source"),
            rec.get("updated_at"),
        ))
        inserted += 1

    row = conn.execute(
        """
        SELECT COUNT(*) AS cnt, MAX(report_date) AS latest_report_date
        FROM fact_financial_indicator_ak
        WHERE stock_code = ?
        """,
        (stock_code,),
    ).fetchone()
    history_rows = row["cnt"] if row else 0
    last_report_date = row["latest_report_date"] if row else None
    status = "ok" if history_rows >= INDICATOR_HISTORY_TARGET_ROWS else ("empty" if history_rows == 0 else "partial")
    if error and history_rows == 0:
        status = "failed"
    conn.execute("""
        INSERT INTO financial_indicator_sync_state
        (stock_code, history_rows, last_report_date, last_synced_at, status, error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(stock_code) DO UPDATE SET
            history_rows = excluded.history_rows,
            last_report_date = excluded.last_report_date,
            last_synced_at = excluded.last_synced_at,
            status = excluded.status,
            error = excluded.error,
            updated_at = excluded.updated_at
    """, (
        stock_code,
        history_rows,
        last_report_date,
        synced_at,
        status,
        error,
        synced_at,
    ))
    return inserted


def build_financial_indicator_latest(conn) -> int:
    ensure_tables(conn)
    now = datetime.now().isoformat()
    conn.execute("DELETE FROM dim_financial_indicator_latest")
    conn.execute("""
        INSERT INTO dim_financial_indicator_latest
        (stock_code, latest_report_date, roe_ak, roa_ak, gross_margin_ak, net_margin_ak,
         current_ratio_ak, quick_ratio_ak, debt_ratio_ak, asset_turnover_ak,
         inventory_turnover_ak, receivables_turnover_ak, revenue_growth_yoy_ak,
         net_profit_growth_yoy_ak, history_rows, updated_at)
        SELECT
            f.stock_code,
            f.report_date,
            f.roe_ak,
            f.roa_ak,
            f.gross_margin_ak,
            f.net_margin_ak,
            f.current_ratio_ak,
            f.quick_ratio_ak,
            f.debt_ratio_ak,
            f.asset_turnover_ak,
            f.inventory_turnover_ak,
            f.receivables_turnover_ak,
            f.revenue_growth_yoy_ak,
            f.net_profit_growth_yoy_ak,
            hist.history_rows,
            ?
        FROM fact_financial_indicator_ak f
        JOIN (
            SELECT stock_code, COUNT(*) AS history_rows
            FROM fact_financial_indicator_ak
            GROUP BY stock_code
        ) hist ON hist.stock_code = f.stock_code
        WHERE f.report_date = (
            SELECT MAX(f2.report_date)
            FROM fact_financial_indicator_ak f2
            WHERE f2.stock_code = f.stock_code
        )
    """, (now,))
    conn.commit()
    return conn.execute("SELECT COUNT(*) FROM dim_financial_indicator_latest").fetchone()[0]


async def sync_financial_indicator_data(conn, stock_codes: Optional[list] = None) -> int:
    ensure_tables(conn)
    loop = asyncio.get_running_loop()
    candidates = _select_indicator_candidates(conn, stock_codes=stock_codes, limit=INDICATOR_BATCH_SIZE)
    if not candidates:
        logger.info("[财务指标] 历史覆盖已达到当前批次目标，无需同步")
        return 0

    logger.info(f"[财务指标] 开始同步 {len(candidates)} 只股票的扩展财务指标")
    synced_at = datetime.now().isoformat()
    total = 0
    for code in candidates:
        try:
            df = await loop.run_in_executor(None, _fetch_financial_abstract, code)
            records = _parse_indicator_rows(code, df)
            total += _upsert_indicator_records(conn, code, records, synced_at)
        except Exception as exc:
            _upsert_indicator_records(conn, code, [], synced_at, error=str(exc)[:300])
    conn.commit()
    dim_count = build_financial_indicator_latest(conn)
    logger.info(f"[财务指标] 同步完成: {total} 条记录, 最新快照 {dim_count} 只股票")
    return total

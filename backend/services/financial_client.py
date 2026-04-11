"""
financial_client.py — 财务数据同步与计算

当前财务底座分为两层：
1. mootdx finance() 提供最新一期稳定快照
2. AKShare/Sina 财报接口提供历史报表序列

数据流：
    mootdx + akshare/sina
        -> raw_gpcw_financial
        -> fact_financial_derived
        -> dim_financial_latest

单点计算原则：
所有财务指标和历史同比逻辑只在本模块计算，其他模块只读取结果表。
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime
from typing import Iterable, Optional

logger = logging.getLogger("cm-api")

FIN_HISTORY_TARGET_ROWS = 8
FIN_HISTORY_FETCH_LIMIT = 12
FIN_HISTORY_BATCH_SIZE = 24

RAW_FINANCIAL_COLUMNS = [
    "stock_code",
    "report_date",
    "notice_date",
    "report_type",
    "is_audited",
    "total_assets",
    "total_liabilities",
    "net_assets",
    "current_assets",
    "current_liabilities",
    "revenue",
    "operating_profit",
    "net_profit",
    "operating_cashflow",
    "total_shares",
    "float_shares",
    "holder_count",
    "contract_liabilities",
    "eps",
    "nav_per_share",
    "gross_profit",
    "inventory",
    "undistributed_profit",
    "source_file",
    "ingested_at",
]


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def _ensure_columns(conn, table_name: str, columns: dict[str, str]) -> None:
    existing = _table_columns(conn, table_name)
    for col, ddl in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {ddl}")


# ============================================================
# Schema
# ============================================================

def ensure_tables(conn):
    """创建财务数据相关表，并补齐增量演进字段。"""
    conn.executescript("""
        -- 原始层：最新快照 + 历史报表整合后的关键字段（只追加）
        CREATE TABLE IF NOT EXISTS raw_gpcw_financial (
            stock_code           TEXT NOT NULL,
            report_date          TEXT NOT NULL,
            total_assets         REAL,
            total_liabilities    REAL,
            net_assets           REAL,
            current_assets       REAL,
            current_liabilities  REAL,
            revenue              REAL,
            operating_profit     REAL,
            net_profit           REAL,
            operating_cashflow   REAL,
            total_shares         REAL,
            float_shares         REAL,
            holder_count         INTEGER,
            contract_liabilities REAL,
            eps                  REAL,
            nav_per_share        REAL,
            gross_profit         REAL,
            inventory            REAL,
            undistributed_profit REAL,
            source_file          TEXT,
            ingested_at          TEXT,
            PRIMARY KEY (stock_code, report_date)
        );
        CREATE INDEX IF NOT EXISTS idx_rgf_report ON raw_gpcw_financial(report_date);
        CREATE INDEX IF NOT EXISTS idx_rgf_stock_report ON raw_gpcw_financial(stock_code, report_date);

        -- 事实层：派生财务指标（可重算）
        CREATE TABLE IF NOT EXISTS fact_financial_derived (
            stock_code              TEXT NOT NULL,
            report_date             TEXT NOT NULL,
            report_season           TEXT,
            roe                     REAL,
            debt_ratio              REAL,
            current_ratio           REAL,
            gross_margin            REAL,
            net_margin              REAL,
            revenue_yoy             REAL,
            profit_yoy              REAL,
            ocf_to_profit           REAL,
            contract_to_revenue     REAL,
            holder_count_change_pct REAL,
            float_shares            REAL,
            total_shares            REAL,
            updated_at              TEXT,
            PRIMARY KEY (stock_code, report_date)
        );
        CREATE INDEX IF NOT EXISTS idx_ffd_report ON fact_financial_derived(report_date);

        -- 维度层：每只股票最新财务快照
        CREATE TABLE IF NOT EXISTS dim_financial_latest (
            stock_code              TEXT PRIMARY KEY,
            latest_report_date      TEXT,
            roe                     REAL,
            debt_ratio              REAL,
            current_ratio           REAL,
            gross_margin            REAL,
            revenue_yoy             REAL,
            profit_yoy              REAL,
            ocf_to_profit           REAL,
            contract_to_revenue     REAL,
            holder_count            INTEGER,
            holder_count_change_pct REAL,
            float_shares            REAL,
            total_shares            REAL,
            updated_at              TEXT
        );

        -- 系统层：财务同步状态
        CREATE TABLE IF NOT EXISTS financial_sync_state (
            stock_code        TEXT PRIMARY KEY,
            history_rows      INTEGER DEFAULT 0,
            last_report_date  TEXT,
            last_snapshot_at  TEXT,
            last_history_at   TEXT,
            status            TEXT DEFAULT 'pending',
            error             TEXT,
            updated_at        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_fss_status ON financial_sync_state(status);
    """)

    _ensure_columns(conn, "raw_gpcw_financial", {
        "notice_date": "TEXT",
        "report_type": "TEXT",
        "is_audited": "INTEGER",
    })
    _ensure_columns(conn, "dim_financial_latest", {
        "net_margin": "REAL",
        "history_rows": "INTEGER DEFAULT 0",
    })
    conn.commit()


# ============================================================
# 基础解析
# ============================================================

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
    if not text or text.lower() in {"nan", "none", "null"} or text in {"--", "-", "不适用"}:
        return None

    text = text.replace(",", "").replace("%", "").replace("元", "").replace("股", "")
    text = text.replace("万元", "").replace("亿元", "").replace(" ", "")
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        return float(text)
    except Exception:
        return None


def _parse_int(value):
    parsed = _parse_float(value)
    return int(parsed) if parsed is not None else None


def _normalize_date(value) -> Optional[str]:
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


def _parse_audited(value) -> Optional[int]:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return None
    if text in {"是", "已审计", "审计"}:
        return 1
    if text in {"否", "未审计"}:
        return 0
    return None


def _infer_report_date_from_notice_date(notice_date: Optional[str]) -> Optional[str]:
    notice = _normalize_date(notice_date)
    if not notice:
        return None
    try:
        dt = datetime.strptime(notice, "%Y-%m-%d")
    except ValueError:
        return None

    month = dt.month
    day = dt.day
    quarter_ends = {"03-31", "06-30", "09-30", "12-31"}
    if notice[5:] in quarter_ends:
        return notice
    if month <= 3:
        return f"{dt.year - 1}-12-31"
    if month == 4:
        return f"{dt.year - 1}-12-31" if day <= 20 else f"{dt.year}-03-31"
    if month in {5, 6}:
        return f"{dt.year}-03-31" if month == 5 else f"{dt.year}-06-30"
    if month in {7, 8}:
        return f"{dt.year}-06-30"
    if month in {9, 10}:
        return f"{dt.year}-09-30"
    return f"{dt.year}-09-30"


def _pick_value(row: dict, keys: Iterable[str]):
    for key in keys:
        if key in row:
            value = row.get(key)
            if value is None:
                continue
            text = str(value).strip()
            if text and text not in {"--", "-", "nan", "None", "null"}:
                return value
    return None


def _normalize_stock_code(code: str) -> str:
    return str(code or "").strip()


def _to_sina_symbol(stock_code: str) -> Optional[str]:
    code = _normalize_stock_code(stock_code)
    if len(code) != 6 or not code.isdigit():
        return None
    if code.startswith("6"):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    return None


def _report_season(report_date: str) -> str:
    """从报告日期推断季度 (Q1/Q2/Q3/Q4)"""
    if not report_date:
        return ""
    month = report_date[5:7] if len(report_date) >= 7 else ""
    return {"03": "Q1", "06": "Q2", "09": "Q3", "12": "Q4"}.get(month, "")


def _safe_div(a, b):
    """安全除法，避免除零"""
    if b is None or b == 0:
        return None
    if a is None:
        return None
    return a / b


# ============================================================
# mootdx 最新快照
# ============================================================

def _parse_finance_record(fin_row: dict) -> dict:
    """从 mootdx client.finance() 的单行结果提取关键字段。"""
    return {
        "total_assets": _parse_float(fin_row.get("zongzichan")),
        "total_liabilities": (_parse_float(fin_row.get("liudongfuzhai")) or 0) + (_parse_float(fin_row.get("changqifuzhai")) or 0),
        "net_assets": _parse_float(fin_row.get("jingzichan")),
        "current_assets": _parse_float(fin_row.get("liudongzichan")),
        "current_liabilities": _parse_float(fin_row.get("liudongfuzhai")),
        "revenue": _parse_float(fin_row.get("zhuyingshouru")),
        "operating_profit": _parse_float(fin_row.get("yingyelirun")),
        "net_profit": _parse_float(fin_row.get("jinglirun")),
        "operating_cashflow": _parse_float(fin_row.get("jingyingxianjinliu")),
        "total_shares": _parse_float(fin_row.get("zongguben")),
        "float_shares": _parse_float(fin_row.get("liutongguben")),
        "holder_count": _parse_int(fin_row.get("gudongrenshu")),
        "eps": _parse_float(fin_row.get("meigushouyi")) or _parse_float(fin_row.get("meigujingzichan")),
        "nav_per_share": _parse_float(fin_row.get("meigujingzichan")),
        "inventory": _parse_float(fin_row.get("cunhuo")),
        "undistributed_profit": _parse_float(fin_row.get("weifenpeilirun")),
        "gross_profit": _parse_float(fin_row.get("zhuyinglirun")),
    }


def _upsert_raw_financial(conn, record: dict) -> None:
    placeholders = ",".join("?" for _ in RAW_FINANCIAL_COLUMNS)
    update_cols = [col for col in RAW_FINANCIAL_COLUMNS if col not in {"stock_code", "report_date"}]
    update_clause = ", ".join(
        f"{col} = COALESCE(excluded.{col}, raw_gpcw_financial.{col})"
        for col in update_cols
    )
    conn.execute(
        f"""
        INSERT INTO raw_gpcw_financial ({",".join(RAW_FINANCIAL_COLUMNS)})
        VALUES ({placeholders})
        ON CONFLICT(stock_code, report_date) DO UPDATE SET
            {update_clause}
        """,
        tuple(record.get(col) for col in RAW_FINANCIAL_COLUMNS),
    )


def _update_snapshot_state(conn, stock_codes: Iterable[str], snapshot_at: str) -> None:
    for code in stock_codes:
        row = conn.execute(
            """
            SELECT COUNT(*) AS cnt, MAX(report_date) AS latest_report
            FROM raw_gpcw_financial
            WHERE stock_code = ?
            """,
            (code,),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO financial_sync_state
            (stock_code, history_rows, last_report_date, last_snapshot_at, status, error, updated_at)
            VALUES (?, ?, ?, ?, 'ok', NULL, ?)
            ON CONFLICT(stock_code) DO UPDATE SET
                history_rows = excluded.history_rows,
                last_report_date = excluded.last_report_date,
                last_snapshot_at = excluded.last_snapshot_at,
                status = CASE
                    WHEN financial_sync_state.status = 'failed' THEN financial_sync_state.status
                    ELSE 'ok'
                END,
                error = CASE
                    WHEN financial_sync_state.status = 'failed' THEN financial_sync_state.error
                    ELSE NULL
                END,
                updated_at = excluded.updated_at
            """,
            (
                code,
                row["cnt"] if row else 0,
                row["latest_report"] if row else None,
                snapshot_at,
                snapshot_at,
            ),
        )


def _resolve_snapshot_report_date(conn, stock_code: str, notice_date: Optional[str]) -> Optional[str]:
    notice = _normalize_date(notice_date)
    if notice:
        exact = conn.execute(
            """
            SELECT report_date
            FROM raw_gpcw_financial
            WHERE stock_code = ?
              AND report_type != 'latest_snapshot'
              AND notice_date = ?
            ORDER BY report_date DESC
            LIMIT 1
            """,
            (stock_code, notice),
        ).fetchone()
        if exact and exact["report_date"]:
            return exact["report_date"]

        nearby = conn.execute(
            """
            SELECT report_date
            FROM raw_gpcw_financial
            WHERE stock_code = ?
              AND report_type != 'latest_snapshot'
              AND notice_date IS NOT NULL
              AND notice_date <= ?
            ORDER BY notice_date DESC, report_date DESC
            LIMIT 1
            """,
            (stock_code, notice),
        ).fetchone()
        if nearby and nearby["report_date"]:
            return nearby["report_date"]

    return _infer_report_date_from_notice_date(notice)


def _cleanup_snapshot_stub(conn, stock_code: str, notice_date: Optional[str], report_date: Optional[str]) -> None:
    notice = _normalize_date(notice_date)
    if not notice or not report_date:
        return
    conn.execute(
        """
        DELETE FROM raw_gpcw_financial
        WHERE stock_code = ?
          AND report_type = 'latest_snapshot'
          AND notice_date = ?
          AND report_date != ?
        """,
        (stock_code, notice, report_date),
    )


_FINANCIAL_TDX_SERVERS = (
    ("110.41.147.114", 7709),
    ("124.70.199.56", 7709),
    ("121.36.225.169", 7709),
    ("123.60.70.228", 7709),
    ("116.205.163.254", 7709),
    ("116.205.171.132", 7709),
    ("116.205.183.150", 7709),
)


def _fetch_latest_snapshot_batch(codes):
    try:
        from mootdx.quotes import Quotes
    except ImportError:
        logger.warning("[财务] mootdx 未安装，跳过最新快照同步")
        return {}

    for server in _FINANCIAL_TDX_SERVERS:
        try:
            client = Quotes.factory(
                market="std",
                multithread=False,
                heartbeat=False,
                server=server,
                timeout=5,
            )
            results = {}
            for code in codes:
                try:
                    fin = client.finance(symbol=code)
                    if fin is not None and not fin.empty:
                        results[code] = fin.iloc[0].to_dict()
                except Exception:
                    continue
            try:
                client.close()
            except Exception:
                pass
            if results:
                return results
        except Exception as exc:
            logger.debug(f"[财务] mootdx {server} 连接失败: {exc}")
            continue

    logger.error("[财务] mootdx 所有服务器均连接失败")
    return {}


# ============================================================
# AKShare/Sina 历史财报回填
# ============================================================

def _extract_balance_rows(df, source_name: str) -> list[dict]:
    records = []
    if df is None or df.empty:
        return records

    for row in df.to_dict("records"):
        report_date = _normalize_date(row.get("报告日"))
        if not report_date:
            continue
        report_type = str(row.get("类型") or "").strip()
        if report_type and "合并" not in report_type:
            continue
        records.append({
            "report_date": report_date,
            "notice_date": _normalize_date(row.get("公告日期")),
            "report_type": report_type or None,
            "is_audited": _parse_audited(row.get("是否审计")),
            "total_assets": _parse_float(_pick_value(row, ["资产总计"])),
            "total_liabilities": _parse_float(_pick_value(row, ["负债合计"])),
            "net_assets": _parse_float(_pick_value(row, [
                "归属于母公司股东权益合计",
                "归属于母公司股东权益",
                "股东权益合计(净资产)",
                "所有者权益(或股东权益)合计",
            ])),
            "current_assets": _parse_float(_pick_value(row, ["流动资产合计"])),
            "current_liabilities": _parse_float(_pick_value(row, ["流动负债合计"])),
            "total_shares": _parse_float(_pick_value(row, ["实收资本(或股本)", "股本", "实收资本"])),
            "contract_liabilities": _parse_float(_pick_value(row, ["合同负债"])),
            "inventory": _parse_float(_pick_value(row, ["存货"])),
            "undistributed_profit": _parse_float(_pick_value(row, ["未分配利润"])),
            "source_file": source_name,
        })
    return records


def _extract_income_rows(df, source_name: str) -> list[dict]:
    records = []
    if df is None or df.empty:
        return records

    for row in df.to_dict("records"):
        report_date = _normalize_date(row.get("报告日"))
        if not report_date:
            continue
        report_type = str(row.get("类型") or "").strip()
        if report_type and "合并" not in report_type:
            continue

        revenue = _parse_float(_pick_value(row, ["营业总收入", "营业收入"]))
        operating_cost = _parse_float(_pick_value(row, ["营业成本"]))
        records.append({
            "report_date": report_date,
            "notice_date": _normalize_date(row.get("公告日期")),
            "report_type": report_type or None,
            "is_audited": _parse_audited(row.get("是否审计")),
            "revenue": revenue,
            "operating_profit": _parse_float(_pick_value(row, ["营业利润"])),
            "net_profit": _parse_float(_pick_value(row, [
                "归属于母公司所有者的净利润",
                "归属于母公司股东的净利润",
                "归属于母公司净利润",
                "净利润",
            ])),
            "eps": _parse_float(_pick_value(row, ["基本每股收益"])),
            "gross_profit": (revenue - operating_cost) if revenue is not None and operating_cost is not None else None,
            "source_file": source_name,
        })
    return records


def _extract_cashflow_rows(df, source_name: str) -> list[dict]:
    records = []
    if df is None or df.empty:
        return records

    for row in df.to_dict("records"):
        report_date = _normalize_date(row.get("报告日"))
        if not report_date:
            continue
        report_type = str(row.get("类型") or "").strip()
        if report_type and "合并" not in report_type:
            continue
        records.append({
            "report_date": report_date,
            "notice_date": _normalize_date(row.get("公告日期")),
            "report_type": report_type or None,
            "is_audited": _parse_audited(row.get("是否审计")),
            "operating_cashflow": _parse_float(_pick_value(row, ["经营活动产生的现金流量净额"])),
            "source_file": source_name,
        })
    return records


def _merge_history_records(stock_code: str, *parts: list[dict]) -> list[dict]:
    merged: dict[str, dict] = {}
    for rows in parts:
        for row in rows:
            report_date = row.get("report_date")
            if not report_date:
                continue
            target = merged.setdefault(report_date, {
                "stock_code": stock_code,
                "report_date": report_date,
                "notice_date": None,
                "report_type": None,
                "is_audited": None,
                "source_file": None,
                "ingested_at": datetime.now().isoformat(),
            })
            for key, value in row.items():
                if key == "report_date":
                    continue
                if value is None:
                    continue
                target[key] = value

    records = list(merged.values())
    records.sort(key=lambda item: item["report_date"], reverse=True)
    return records[:FIN_HISTORY_FETCH_LIMIT]


def _fetch_sina_history_batch(stock_codes: list[str]) -> tuple[list[dict], dict[str, dict]]:
    try:
        import akshare as ak
    except ImportError:
        logger.warning("[财务] akshare 未安装，跳过历史财务回填")
        return [], {code: {"status": "failed", "error": "akshare 未安装"} for code in stock_codes}

    all_records: list[dict] = []
    states: dict[str, dict] = {}

    for code in stock_codes:
        symbol = _to_sina_symbol(code)
        if not symbol:
            states[code] = {"status": "skipped", "error": "当前财报历史接口暂不支持该市场"}
            continue

        try:
            balance_df = ak.stock_financial_report_sina(stock=symbol, symbol="资产负债表")
            income_df = ak.stock_financial_report_sina(stock=symbol, symbol="利润表")
            cashflow_df = ak.stock_financial_report_sina(stock=symbol, symbol="现金流量表")
            merged = _merge_history_records(
                code,
                _extract_balance_rows(balance_df, "akshare_sina_balance"),
                _extract_income_rows(income_df, "akshare_sina_income"),
                _extract_cashflow_rows(cashflow_df, "akshare_sina_cashflow"),
            )
            if not merged:
                states[code] = {"status": "empty", "error": "未获取到历史财报"}
                continue

            all_records.extend(merged)
            states[code] = {
                "status": "ok",
                "history_rows": len(merged),
                "last_report_date": merged[0]["report_date"],
            }
        except Exception as exc:
            states[code] = {"status": "failed", "error": str(exc)[:300]}

    return all_records, states


def _select_history_candidates(conn, stock_codes: Optional[list] = None, limit: int = FIN_HISTORY_BATCH_SIZE) -> list[str]:
    ensure_tables(conn)

    params: list = [FIN_HISTORY_TARGET_ROWS]
    in_clause = ""
    if stock_codes:
        normalized = [_normalize_stock_code(code) for code in stock_codes if _normalize_stock_code(code)]
        if not normalized:
            return []
        placeholders = ",".join("?" for _ in normalized)
        in_clause = f" AND a.stock_code IN ({placeholders}) "
        params.extend(normalized)

    params.append(limit)
    rows = conn.execute(
        f"""
        WITH fin AS (
            SELECT stock_code, COUNT(*) AS history_rows, MAX(report_date) AS latest_report_date
            FROM raw_gpcw_financial
            GROUP BY stock_code
        )
        SELECT a.stock_code
        FROM dim_active_a_stock a
        LEFT JOIN excluded_stocks e ON e.stock_code = a.stock_code
        LEFT JOIN fin f ON f.stock_code = a.stock_code
        LEFT JOIN financial_sync_state s ON s.stock_code = a.stock_code
        LEFT JOIN mart_current_relationship m ON m.stock_code = a.stock_code
        LEFT JOIN mart_stock_trend t ON t.stock_code = a.stock_code
        WHERE e.stock_code IS NULL
          AND (
                COALESCE(f.history_rows, 0) < ?
             OR COALESCE(s.status, '') IN ('failed', 'empty', 'partial')
          )
          {in_clause}
        GROUP BY a.stock_code
        ORDER BY
            CASE
                WHEN m.stock_code IS NOT NULL THEN 0
                WHEN t.stock_code IS NOT NULL THEN 1
                ELSE 2
            END,
            COALESCE(f.history_rows, 0) ASC,
            CASE WHEN s.last_history_at IS NULL THEN 0 ELSE 1 END,
            COALESCE(s.last_history_at, ''),
            a.stock_code
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [row["stock_code"] for row in rows]


def _apply_history_backfill(conn, stock_codes: list[str], records: list[dict], states: dict[str, dict], synced_at: str) -> int:
    inserted = 0
    touched = set()
    for record in records:
        row = {col: record.get(col) for col in RAW_FINANCIAL_COLUMNS}
        row["ingested_at"] = synced_at
        _upsert_raw_financial(conn, row)
        touched.add(record["stock_code"])
        inserted += 1

    for code in stock_codes:
        count_row = conn.execute(
            """
            SELECT COUNT(*) AS cnt, MAX(report_date) AS latest_report
            FROM raw_gpcw_financial
            WHERE stock_code = ?
            """,
            (code,),
        ).fetchone()
        state = states.get(code, {})
        history_rows = count_row["cnt"] if count_row else 0
        last_report_date = count_row["latest_report"] if count_row else None
        status = state.get("status") or ("ok" if history_rows >= FIN_HISTORY_TARGET_ROWS else "partial")
        if status == "ok" and history_rows < FIN_HISTORY_TARGET_ROWS:
            status = "partial"
        conn.execute(
            """
            INSERT INTO financial_sync_state
            (stock_code, history_rows, last_report_date, last_history_at, status, error, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(stock_code) DO UPDATE SET
                history_rows = excluded.history_rows,
                last_report_date = excluded.last_report_date,
                last_history_at = excluded.last_history_at,
                status = excluded.status,
                error = excluded.error,
                updated_at = excluded.updated_at
            """,
            (
                code,
                history_rows,
                last_report_date,
                synced_at,
                status,
                state.get("error"),
                synced_at,
            ),
        )
    return inserted


# ============================================================
# 公共同步入口
# ============================================================

async def sync_financial_data(conn, stock_codes: Optional[list] = None) -> int:
    """同步最新快照，并增量回填历史财务序列。"""
    ensure_tables(conn)

    if not stock_codes:
        rows = conn.execute(
            "SELECT DISTINCT a.stock_code "
            "FROM dim_active_a_stock a "
            "LEFT JOIN excluded_stocks e ON e.stock_code = a.stock_code "
            "WHERE e.stock_code IS NULL"
        ).fetchall()
        stock_codes = [r["stock_code"] for r in rows]
    else:
        stock_codes = [_normalize_stock_code(code) for code in stock_codes if _normalize_stock_code(code)]

    if not stock_codes:
        logger.warning("[财务] dim_active_a_stock 为空，请先跑「数据获取 → 同步十大股东」拉取主数据")
        return 0

    loop = asyncio.get_running_loop()
    now = datetime.now().isoformat()
    history_candidates = _select_history_candidates(conn, stock_codes=stock_codes, limit=FIN_HISTORY_BATCH_SIZE)
    history_upserts = 0
    if history_candidates:
        logger.info(
            f"[财务] 开始回填历史财报: 候选 {len(history_candidates)} 只，目标每只最多 {FIN_HISTORY_FETCH_LIMIT} 期"
        )
        records, states = await loop.run_in_executor(None, _fetch_sina_history_batch, history_candidates)
        history_upserts = _apply_history_backfill(conn, history_candidates, records, states, now)
        conn.commit()
        success_count = sum(1 for state in states.values() if state.get("status") == "ok")
        partial_count = sum(1 for state in states.values() if state.get("status") == "partial")
        failed_count = len(history_candidates) - success_count - partial_count
        logger.info(
            f"[财务] 历史回填完成: {history_upserts} 条记录, 成功 {success_count}, 未满目标 {partial_count}, 失败/空结果 {failed_count}"
        )
    else:
        logger.info("[财务] 历史财报覆盖已达当前批次目标，无需回填")

    logger.info(f"[财务] 开始同步 {len(stock_codes)} 只股票的最新财务快照")
    batch_size = 50
    all_results = {}
    for i in range(0, len(stock_codes), batch_size):
        batch = stock_codes[i:i + batch_size]
        batch_results = await loop.run_in_executor(None, _fetch_latest_snapshot_batch, batch)
        all_results.update(batch_results)
        if (i // batch_size) % 10 == 0 and i > 0:
            logger.info(f"[财务] 最新快照已获取 {len(all_results)}/{len(stock_codes)}")

    if not all_results:
        logger.warning("[财务] 未获取到任何最新财务快照")
    latest_upserts = 0

    for code, raw in all_results.items():
        parsed = _parse_finance_record(raw)
        notice_date = _normalize_date(raw.get("updated_date"))
        report_date = _resolve_snapshot_report_date(conn, code, notice_date) or notice_date
        record = {
            "stock_code": code,
            "report_date": report_date,
            "notice_date": notice_date,
            "report_type": "latest_snapshot",
            "is_audited": None,
            "total_assets": parsed.get("total_assets"),
            "total_liabilities": parsed.get("total_liabilities"),
            "net_assets": parsed.get("net_assets"),
            "current_assets": parsed.get("current_assets"),
            "current_liabilities": parsed.get("current_liabilities"),
            "revenue": parsed.get("revenue"),
            "operating_profit": parsed.get("operating_profit"),
            "net_profit": parsed.get("net_profit"),
            "operating_cashflow": parsed.get("operating_cashflow"),
            "total_shares": parsed.get("total_shares"),
            "float_shares": parsed.get("float_shares"),
            "holder_count": parsed.get("holder_count"),
            "contract_liabilities": None,
            "eps": parsed.get("eps"),
            "nav_per_share": parsed.get("nav_per_share"),
            "gross_profit": parsed.get("gross_profit"),
            "inventory": parsed.get("inventory"),
            "undistributed_profit": parsed.get("undistributed_profit"),
            "source_file": "mootdx_finance",
            "ingested_at": now,
        }
        _upsert_raw_financial(conn, record)
        _cleanup_snapshot_stub(conn, code, notice_date, report_date)
        latest_upserts += 1

    if all_results:
        _update_snapshot_state(conn, all_results.keys(), now)
        conn.commit()
    logger.info(f"[财务] 最新快照同步完成: {latest_upserts} 条")

    capital_total = 0
    try:
        from services.capital_client import sync_capital_behavior_data
        capital_total = await sync_capital_behavior_data(conn, stock_codes=stock_codes)
    except Exception as exc:
        logger.warning(f"[财务] 资本行为增强同步失败，跳过本轮: {exc}")

    indicator_total = 0
    try:
        from services.financial_indicator_client import sync_financial_indicator_data
        indicator_total = await sync_financial_indicator_data(conn, stock_codes=stock_codes)
    except Exception as exc:
        logger.warning(f"[财务] 扩展财务指标同步失败，跳过本轮: {exc}")

    quality_feature_total = 0
    try:
        from services.quality_feature_engine import build_quality_features
        quality_feature_total = build_quality_features(conn)
    except Exception as exc:
        logger.warning(f"[财务] 质量特征构建失败，跳过本轮: {exc}")

    archetype_total = 0
    try:
        from services.stock_archetype_engine import build_stock_archetypes
        archetype_total = build_stock_archetypes(conn)
    except Exception as exc:
        logger.warning(f"[财务] 股票类型构建失败，跳过本轮: {exc}")

    total = latest_upserts + history_upserts
    logger.info(
        f"[财务] 同步结束: 最新 {latest_upserts} 条, 历史 {history_upserts} 条, "
        f"资本行为 {capital_total} 条, 扩展指标 {indicator_total} 条, "
        f"质量特征 {quality_feature_total} 只, 股票类型 {archetype_total} 只"
    )
    return total + capital_total + indicator_total


# ============================================================
# 计算派生指标
# ============================================================

def calc_financial_derived(conn) -> int:
    """从 raw_gpcw_financial 计算派生指标，写入 fact + dim 表。"""
    ensure_tables(conn)

    rows = conn.execute("""
        SELECT * FROM raw_gpcw_financial ORDER BY stock_code, report_date
    """).fetchall()

    if not rows:
        logger.info("[财务] 无原始数据，跳过派生计算")
        return 0

    now = datetime.now().isoformat()
    count = 0

    by_stock = defaultdict(list)
    for row in rows:
        by_stock[row["stock_code"]].append(dict(row))

    conn.execute("DELETE FROM fact_financial_derived")

    for code, records in by_stock.items():
        records.sort(key=lambda item: item["report_date"])

        for i, rec in enumerate(records):
            rd = rec["report_date"]
            season = _report_season(rd)

            roe = _safe_div(rec.get("net_profit"), rec.get("net_assets"))
            debt_ratio = _safe_div(rec.get("total_liabilities"), rec.get("total_assets"))
            current_ratio = _safe_div(rec.get("current_assets"), rec.get("current_liabilities"))
            gross_margin = _safe_div(rec.get("gross_profit"), rec.get("revenue"))
            net_margin = _safe_div(rec.get("net_profit"), rec.get("revenue"))
            ocf_to_profit = _safe_div(rec.get("operating_cashflow"), rec.get("net_profit"))
            contract_to_revenue = _safe_div(rec.get("contract_liabilities"), rec.get("revenue"))

            revenue_yoy = None
            profit_yoy = None
            holder_count_change = None

            target_year = int(rd[:4]) - 1 if rd and len(rd) >= 4 else None
            target_date = f"{target_year}{rd[4:]}" if target_year else None
            if target_date:
                prev_same_q = next((prev for prev in records[:i] if prev["report_date"] == target_date), None)
                if prev_same_q:
                    revenue_yoy = _safe_div(
                        (rec.get("revenue") or 0) - (prev_same_q.get("revenue") or 0),
                        abs(prev_same_q.get("revenue") or 0) or None,
                    )
                    profit_yoy = _safe_div(
                        (rec.get("net_profit") or 0) - (prev_same_q.get("net_profit") or 0),
                        abs(prev_same_q.get("net_profit") or 0) or None,
                    )

            if i > 0:
                prev_rec = records[i - 1]
                if rec.get("holder_count") and prev_rec.get("holder_count"):
                    holder_count_change = _safe_div(
                        rec["holder_count"] - prev_rec["holder_count"],
                        prev_rec["holder_count"],
                    )

            conn.execute("""
                INSERT OR REPLACE INTO fact_financial_derived
                (stock_code, report_date, report_season, roe, debt_ratio, current_ratio,
                 gross_margin, net_margin, revenue_yoy, profit_yoy, ocf_to_profit,
                 contract_to_revenue, holder_count_change_pct, float_shares, total_shares, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                code,
                rd,
                season,
                roe,
                debt_ratio,
                current_ratio,
                gross_margin,
                net_margin,
                revenue_yoy,
                profit_yoy,
                ocf_to_profit,
                contract_to_revenue,
                holder_count_change,
                rec.get("float_shares"),
                rec.get("total_shares"),
                now,
            ))
            count += 1

    conn.execute("DELETE FROM dim_financial_latest")
    conn.execute("""
        INSERT INTO dim_financial_latest
        (stock_code, latest_report_date, roe, debt_ratio, current_ratio, gross_margin,
         net_margin, revenue_yoy, profit_yoy, ocf_to_profit, contract_to_revenue,
         holder_count, holder_count_change_pct, float_shares, total_shares, history_rows, updated_at)
        SELECT
            f.stock_code,
            f.report_date,
            f.roe,
            f.debt_ratio,
            f.current_ratio,
            f.gross_margin,
            f.net_margin,
            f.revenue_yoy,
            f.profit_yoy,
            f.ocf_to_profit,
            f.contract_to_revenue,
            r.holder_count,
            f.holder_count_change_pct,
            f.float_shares,
            f.total_shares,
            hist.history_rows,
            ?
        FROM fact_financial_derived f
        JOIN raw_gpcw_financial r
          ON f.stock_code = r.stock_code AND f.report_date = r.report_date
        JOIN (
            SELECT stock_code, COUNT(*) AS history_rows
            FROM raw_gpcw_financial
            GROUP BY stock_code
        ) hist
          ON hist.stock_code = f.stock_code
        WHERE f.report_date = (
            SELECT MAX(f2.report_date)
            FROM fact_financial_derived f2
            WHERE f2.stock_code = f.stock_code
        )
    """, (now,))

    conn.commit()
    dim_count = conn.execute("SELECT COUNT(*) FROM dim_financial_latest").fetchone()[0]
    logger.info(f"[财务] 派生计算完成: {count} 条事实, {dim_count} 条最新快照")
    return count

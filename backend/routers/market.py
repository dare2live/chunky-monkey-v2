"""
全市场数据路由

负责从东财 API 下载十大流通股东数据到 market_raw_holdings。
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, Query

from services.db import get_conn
from services.utils import safe_float as _safe_float

logger = logging.getLogger("cm-api")
router = APIRouter()

# 东财 API 配置
EASTMONEY_ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
REPORT_NAME = "RPT_F10_EH_FREEHOLDERS"
PAGE_SIZE = 500
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}


def _compact_date(val) -> str:
    if not val:
        return ""
    s = str(val).strip()[:10]
    return s.replace("-", "").replace("/", "")


def _safe_int(val):
    try:
        return int(val)
    except (TypeError, ValueError):
        return None


def _first(row, *keys):
    for k in keys:
        if k in row and row[k] is not None:
            return row[k]
    return None


def _map_api_row(row: dict) -> dict:
    """将东财 API 返回的一行映射为 market_raw_holdings 字段"""
    secucode = str(row.get("SECUCODE", "") or "")
    code = secucode.split(".")[0] if "." in secucode else str(row.get("SECURITY_CODE", ""))

    return {
        "holder_name": str(row.get("HOLDER_NAME", "") or "").strip(),
        "stock_code": code,
        "stock_name": str(row.get("SECURITY_NAME_ABBR", "") or "").strip(),
        "report_date": _compact_date(_first(row, "END_DATE", "REPORT_DATE")),
        "notice_date": _compact_date(_first(row, "UPDATE_DATE", "NOTICE_DATE")),
        "holder_rank": _safe_int(_first(row, "HOLDER_RANK", "HOLDER_RANKN")),
        "hold_amount": _safe_float(_first(row, "FREE_HOLDNUM", "HOLD_NUM")),
        "hold_market_cap": _safe_float(_first(row, "HOLDER_MARKET_CAP", "HOLD_MARKET_CAP")),
        "hold_ratio": _safe_float(_first(row, "HOLD_RATIO", "FREE_RATIO", "FREEHOLDRATIO")),
        "holder_type": str(row.get("HOLDER_NEWTYPE", "") or row.get("HOLDER_TYPE", "") or ""),
        "hold_change": str(row.get("HOLDER_STATEE", "") or row.get("HOLDSTATE", "") or ""),
        "hold_change_num": _safe_float(_first(row, "HOLD_NUM_CHANGE", "HOLD_CHANGE", "HOLD_CHANGE_NUM")),
        "raw_json": json.dumps(row, ensure_ascii=False, default=str),
    }


async def _fetch_page(client: httpx.AsyncClient, filter_str: str, page: int) -> dict:
    params = {
        "sortColumns": "UPDATE_DATE,SECURITY_CODE,HOLDER_RANK",
        "sortTypes": "-1,1,1",
        "pageSize": PAGE_SIZE,
        "pageNumber": page,
        "reportName": REPORT_NAME,
        "columns": "ALL",
        "source": "WEB",
        "client": "WEB",
        "filter": filter_str,
    }
    resp = await client.get(EASTMONEY_ENDPOINT, params=params)
    resp.raise_for_status()
    data = resp.json()
    
    from services.api_schemas import EastMoneyHoldingsResponse
    from pydantic import ValidationError
    
    try:
        valid_response = EastMoneyHoldingsResponse(**data)
        if not valid_response.success:
            raise RuntimeError(f"东财API错误: {valid_response.message or '未知'}")
        
        # Override the payload data with validated and cleanly parsed dicts if exists
        if hasattr(valid_response, "result") and valid_response.result and "data" in valid_response.result:
            if "result" in data and isinstance(data["result"], dict):
                data["result"]["data"] = valid_response.get_data_items()
    except ValidationError as e:
        logger.error(f"东财API防腐层截断 - Schema验证失败: {e}")
        raise ValueError(f"东财数据 Schema 校验失败 (防腐层阻断): {e}")
        
    return data


def _upsert_batch(conn, rows: list) -> int:
    """批量 UPSERT 到 market_raw_holdings，返回插入数"""
    now = datetime.now().isoformat()
    inserted = 0
    for r in rows:
        try:
            rank_val = r["holder_rank"]
            if rank_val is not None:
                conn.execute("""
                    DELETE FROM market_raw_holdings
                    WHERE holder_name = ? AND stock_code = ? AND report_date = ? AND holder_rank IS NULL
                """, (r["holder_name"], r["stock_code"], r["report_date"]))

            conn.execute("""
                INSERT INTO market_raw_holdings
                    (holder_name, stock_code, stock_name, report_date, notice_date,
                     holder_rank, hold_amount, hold_market_cap, hold_ratio,
                     holder_type, hold_change, hold_change_num, raw_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(holder_name, stock_code, report_date, holder_rank) DO UPDATE SET
                    stock_name=excluded.stock_name,
                    notice_date=excluded.notice_date,
                    hold_amount=excluded.hold_amount,
                    hold_market_cap=excluded.hold_market_cap,
                    hold_ratio=excluded.hold_ratio,
                    holder_type=excluded.holder_type,
                    hold_change=excluded.hold_change,
                    hold_change_num=excluded.hold_change_num,
                    raw_json=excluded.raw_json
            """, (
                r["holder_name"], r["stock_code"], r["stock_name"],
                r["report_date"], r["notice_date"],
                rank_val, r["hold_amount"], r["hold_market_cap"], r["hold_ratio"],
                r["holder_type"], r["hold_change"], r["hold_change_num"],
                r["raw_json"], now
            ))
            inserted += 1
        except Exception as e:
            logger.debug(f"[upsert] skip: {e}")
    return inserted


@router.get("/market/status")
async def market_status():
    """全市场数据概况"""
    conn = get_conn()
    try:
        from services.audit import load_quality_audit_snapshot

        audit = load_quality_audit_snapshot(conn)
        if audit and audit.get("layers"):
            raw = audit["layers"].get("raw", {})
            holdings = audit["layers"].get("holdings", {})
            current_rel = audit["layers"].get("current_relationship", {})
            return {
                "total_records": raw.get("count", 0),
                "latest_notice_date": raw.get("latest_notice"),
                "total_stocks": raw.get("stocks", 0),
                "matched_stocks": holdings.get("stocks", 0),
                "current_stocks": current_rel.get("stocks", 0),
                "total_periods": raw.get("total_periods", 0),
                "snapshot_meta": audit.get("snapshot_meta"),
            }

        total = conn.execute("SELECT COUNT(*) FROM market_raw_holdings").fetchone()[0]
        latest = conn.execute("SELECT MAX(notice_date) FROM market_raw_holdings").fetchone()[0]
        stocks = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM market_raw_holdings").fetchone()[0]
        matched_stocks = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM inst_holdings").fetchone()[0]
        current_stocks = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM mart_current_relationship").fetchone()[0]
        periods = conn.execute("SELECT COUNT(DISTINCT report_date) FROM market_raw_holdings").fetchone()[0]
        return {
            "total_records": total,
            "latest_notice_date": latest,
            "total_stocks": stocks,
            "matched_stocks": matched_stocks,
            "current_stocks": current_stocks,
            "total_periods": periods,
        }
    finally:
        conn.close()


@router.get("/market/stock-latest-periods")
async def stock_latest_periods():
    """每只股票的最新3个报告期"""
    conn = get_conn()
    try:
        rows = conn.execute("""
            SELECT stock_code, report_date
            FROM market_raw_holdings
            WHERE stock_code IS NOT NULL AND report_date IS NOT NULL
            GROUP BY stock_code, report_date
            ORDER BY stock_code, report_date DESC
        """).fetchall()

        data = {}
        for row in rows:
            code = row["stock_code"]
            if code not in data:
                data[code] = []
            if len(data[code]) < 3:
                data[code].append(row["report_date"])

        return {"ok": True, "data": data, "total_stocks": len(data)}
    finally:
        conn.close()

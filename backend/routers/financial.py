"""
财务数据 API 路由

提供 gpcw 财务指标的查询接口，数据来源于 financial_client.py 的计算结果。
"""

from fastapi import APIRouter, Query
from services.db import get_conn

router = APIRouter()


@router.get("/latest/{stock_code}")
async def get_latest(stock_code: str):
    """单股最新财务指标"""
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM dim_financial_latest WHERE stock_code = ?",
            (stock_code,)
        ).fetchone()
        if not row:
            return {"ok": False, "message": "无数据"}
        return {"ok": True, "data": dict(row)}
    finally:
        conn.close()


@router.get("/history/{stock_code}")
async def get_history(stock_code: str, limit: int = Query(20, ge=1, le=100)):
    """单股季度历史"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM fact_financial_derived WHERE stock_code = ? "
            "ORDER BY report_date DESC LIMIT ?",
            (stock_code, limit)
        ).fetchall()
        return {"ok": True, "data": [dict(r) for r in rows]}
    finally:
        conn.close()


@router.get("/bulk")
async def get_bulk(limit: int = Query(5000, ge=1, le=10000)):
    """批量最新财务快照（供选股引擎和前端列表用）"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM dim_financial_latest LIMIT ?", (limit,)
        ).fetchall()
        return {"ok": True, "count": len(rows), "data": [dict(r) for r in rows]}
    finally:
        conn.close()

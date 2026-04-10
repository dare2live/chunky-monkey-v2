from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List, Optional
import asyncio
import logging
from datetime import datetime

from services.etf_engine import sync_etf_universe, calc_etf_momentum, calc_etf_overview
from services.etf_mining_engine import build_etf_mining_snapshot, analyze_etf_deep
from services.market_db import get_market_conn

router = APIRouter(tags=["ETF_Quant"])
# 使用 cm-api logger，让 ETF 日志走入 routers/updater.py 的 _UILogHandler
logger = logging.getLogger("cm-api")


# ============================================================
# 后台运行状态 + 进度
# 与 routers/updater.py 的 _is_running / _ui_logs 走同一套基础设施，
# 但 ETF 自有进度状态，避免和主流水线互相覆盖
# ============================================================

_etf_state: Dict[str, Any] = {
    "running": False,
    "stage": "idle",       # idle | fetch_list | write_universe | sync_kline | done | error
    "current": 0,
    "total": 0,
    "message": "",
    "started_at": None,
    "finished_at": None,
    "result": None,
    "error": None,
    "log_seq_start": 0,    # 本次任务开始时的 _ui_log_seq，用于切片日志
}


def _get_log_seq_now() -> int:
    """读取 routers/updater.py 当前的 UI log 序号，用于划定本次 ETF 任务的日志窗口。"""
    try:
        from routers import updater as _u
        return getattr(_u, "_ui_log_seq", 0) or 0
    except Exception:
        return 0


def _get_ui_logs_after(seq: int) -> List[dict]:
    try:
        from routers import updater as _u
        return [r for r in getattr(_u, "_ui_logs", []) if r.get("id", 0) > seq]
    except Exception:
        return []


def _progress_cb(stage: str, current: int, total: int, message: str) -> None:
    _etf_state["stage"] = stage
    _etf_state["current"] = current
    _etf_state["total"] = total
    if message:
        _etf_state["message"] = message


@router.get("/list")
async def get_etf_list() -> Dict[str, Any]:
    """返回 ETF 列表与动量计算结果（基于 market_data.db 中已同步的 K 线）"""
    from services.db import get_conn
    conn = get_conn()
    mkt_conn = get_market_conn()
    try:
        results = calc_etf_momentum(conn, mkt_conn)
        overview = calc_etf_overview(results)
        return {"status": "ok", "data": results, "count": len(results), "overview": overview}
    except Exception as e:
        logger.error(f"[ETF] 获取列表失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()
        mkt_conn.close()


@router.get("/status")
async def etf_status(log_limit: int = Query(60, ge=0, le=400)) -> Dict[str, Any]:
    """ETF 同步任务状态 + 实时日志窗口（前端轮询用）"""
    state = dict(_etf_state)
    if log_limit > 0 and state.get("started_at"):
        logs = _get_ui_logs_after(state["log_seq_start"])
        # ETF 相关行优先；保留最后 N 条
        etf_logs = [r for r in logs if "[ETF]" in (r.get("message") or "")]
        state["logs"] = etf_logs[-log_limit:]
    else:
        state["logs"] = []
    return {"status": "ok", "data": state}


@router.post("/sync")
async def api_sync_etf(
    sync_kline: bool = Query(True, description="是否同时同步 K 线"),
    kline_days: int = Query(120, description="同步最近多少天 K 线"),
    max_etfs: int = Query(None, description="限制 ETF 数量（调试用）"),
) -> Dict[str, Any]:
    """触发 ETF 资产池 + K 线同步（mootdx）

    异步执行：立即返回，前端通过 GET /api/etf/status 轮询进度与日志。
    """
    if _etf_state.get("running"):
        return {
            "status": "ok",
            "message": "ETF 同步正在进行中",
            "data": dict(_etf_state),
        }

    # 重置状态
    _etf_state.update({
        "running": True,
        "stage": "starting",
        "current": 0,
        "total": 0,
        "message": "ETF 同步启动中…",
        "started_at": datetime.now().isoformat(),
        "finished_at": None,
        "result": None,
        "error": None,
        "log_seq_start": _get_log_seq_now(),
    })
    logger.info(f"[ETF] 同步任务启动 sync_kline={sync_kline} days={kline_days}")

    async def _run():
        from services.db import get_conn
        conn = get_conn()
        mkt_conn = get_market_conn()
        try:
            result = await sync_etf_universe(
                conn, mkt_conn,
                sync_kline=sync_kline,
                kline_days=kline_days,
                max_etfs=max_etfs,
                progress_cb=_progress_cb,
            )
            _etf_state.update({
                "stage": "done",
                "result": result,
                "message": (
                    f"完成：ETF {result['etf_count']} / "
                    f"K 线 {result['kline_etf_count']} / 行 {result['kline_rows']}"
                ),
            })
            logger.info(f"[ETF] 同步完成 {result}")
        except Exception as e:
            _etf_state.update({
                "stage": "error",
                "error": str(e),
                "message": f"同步失败：{e}",
            })
            logger.error(f"[ETF] 同步失败: {e}")
        finally:
            _etf_state["running"] = False
            _etf_state["finished_at"] = datetime.now().isoformat()
            try:
                conn.close()
            except Exception:
                pass
            try:
                mkt_conn.close()
            except Exception:
                pass

    asyncio.create_task(_run())
    return {
        "status": "ok",
        "message": "ETF 同步已启动，请在状态面板观察进度",
        "data": dict(_etf_state),
    }


@router.get("/mining")
async def get_etf_mining(
    grid_topn: int = Query(6, ge=1, le=12),
    trend_topn: int = Query(6, ge=1, le=12),
    rotation_topn: int = Query(5, ge=1, le=10),
) -> Dict[str, Any]:
    """ETF 挖掘建议。

    输出三块：
    - 网格候选：确定性回测后的建议步长
    - 趋势持有：当前动作建议
    - 下一轮动行业：聚合现有股票 Qlib 结果得到的行业观察名单
    """
    from services.db import get_conn

    conn = get_conn()
    mkt_conn = get_market_conn()
    try:
        data = build_etf_mining_snapshot(
            conn,
            mkt_conn,
            grid_topn=grid_topn,
            trend_topn=trend_topn,
            rotation_topn=rotation_topn,
        )
        return {"status": "ok", "data": data}
    except Exception as e:
        logger.error(f"[ETF] ETF 挖掘建议生成失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()
        mkt_conn.close()


@router.get("/analysis/{code}")
async def get_etf_analysis(code: str) -> Dict[str, Any]:
    """单只 ETF 深度量化分析。

    返回多步长回测对比、买入持有基准、多周期稳定性检验、量化结论。
    前端用于展示详细分析面板。
    """
    import re
    if not re.match(r"^\d{6}$", code):
        raise HTTPException(status_code=400, detail="ETF 代码格式错误")

    from services.db import get_conn

    conn = get_conn()
    mkt_conn = get_market_conn()
    try:
        result = analyze_etf_deep(conn, mkt_conn, code)
        if result is None:
            raise HTTPException(status_code=404, detail=f"ETF {code} 不存在或数据不足")
        return {"status": "ok", "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[ETF] 深度分析 {code} 失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()
        mkt_conn.close()


@router.get("/qlib-summary")
async def get_etf_qlib_summary() -> Dict[str, Any]:
    """ETF 视角的 Qlib 概览：按行业聚合 Qlib 预测，映射到对应 ETF。"""
    from services.db import get_conn

    conn = get_conn()
    try:
        # 最新模型
        model_row = conn.execute(
            "SELECT model_id, stock_count, train_start, test_end, ic_mean, created_at "
            "FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not model_row:
            return {"status": "ok", "data": {"model": None, "sectors": []}}
        model = dict(model_row)
        model_id = model["model_id"]

        # 按行业聚合 Qlib 预测
        sector_rows = conn.execute(
            """
            SELECT ctx.sw_level1 AS sector_name,
                   AVG(p.qlib_percentile) AS avg_percentile,
                   AVG(p.qlib_score) AS avg_score,
                   SUM(CASE WHEN p.qlib_percentile >= 80 THEN 1 ELSE 0 END) AS high_count,
                   SUM(CASE WHEN p.qlib_percentile <= 20 THEN 1 ELSE 0 END) AS low_count,
                   COUNT(*) AS stock_count,
                   msm.rotation_bucket,
                   msm.rotation_score
            FROM qlib_predictions p
            INNER JOIN dim_stock_industry_context_latest ctx ON ctx.stock_code = p.stock_code
            LEFT JOIN mart_sector_momentum msm ON msm.sector_name = ctx.sw_level1
            WHERE p.model_id = ?
              AND ctx.sw_level1 IS NOT NULL AND ctx.sw_level1 != ''
            GROUP BY ctx.sw_level1
            HAVING COUNT(*) >= 3
            ORDER BY AVG(p.qlib_percentile) DESC
            """,
            (model_id,),
        ).fetchall()

        sectors = []
        for row in sector_rows:
            s = dict(row)
            # 找到该行业对应的 ETF
            etf_rows = conn.execute(
                "SELECT code, name FROM dim_asset_universe "
                "WHERE asset_type='etf' AND category=? "
                "ORDER BY code LIMIT 5",
                (s["sector_name"],),
            ).fetchall()
            s["etfs"] = [dict(e) for e in etf_rows]
            sectors.append(s)

        return {"status": "ok", "data": {"model": model, "sectors": sectors}}
    except Exception as e:
        logger.error(f"[ETF] Qlib 概览失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

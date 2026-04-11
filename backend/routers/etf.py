from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List, Optional
import asyncio
import logging
from datetime import datetime

from services.etf_engine import sync_etf_universe
from services.etf_mining_engine import analyze_etf_deep
from services.etf_db import get_etf_conn
from services.etf_snapshot_manager import get_latest_etf_snapshot_bundle, persist_latest_etf_snapshot

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
    "stage": "idle",       # idle | fetch_list | write_universe | sync_kline | build_snapshot | done | error
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
async def get_etf_list(force_refresh: bool = Query(False, description="是否强制重算最新 ETF 快照")) -> Dict[str, Any]:
    """返回 ETF 列表与最新缓存快照。"""
    conn = get_etf_conn()
    try:
        bundle = get_latest_etf_snapshot_bundle(conn, conn, force_refresh=force_refresh)
        return {
            "status": "ok",
            "data": bundle["rows"],
            "count": len(bundle["rows"]),
            "overview": bundle["overview"],
            "snapshot": {
                "snapshot_id": bundle.get("snapshot_id"),
                "computed_at": bundle.get("computed_at"),
                "etf_count": bundle.get("etf_count"),
                "is_stale": bundle.get("is_stale", False),
            },
            "source_status": bundle.get("source_status") or {},
        }
    except Exception as e:
        logger.error(f"[ETF] 获取列表失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


@router.get("/workbench")
async def get_etf_workbench(force_refresh: bool = Query(False, description="是否强制重算最新 ETF 快照")) -> Dict[str, Any]:
    from routers.updater import check_connectivity, get_cached_connectivity

    conn = get_etf_conn()
    try:
        connectivity = await check_connectivity(force=True) if force_refresh else get_cached_connectivity()
        bundle = get_latest_etf_snapshot_bundle(
            conn,
            conn,
            force_refresh=force_refresh,
            connectivity=connectivity,
        )
        mining = bundle.get("mining_snapshot") or {}
        factor_snapshot = bundle.get("factor_snapshot") or {}
        return {
            "status": "ok",
            "data": {
                "snapshot": {
                    "snapshot_id": bundle.get("snapshot_id"),
                    "computed_at": bundle.get("computed_at"),
                    "etf_count": bundle.get("etf_count"),
                    "is_stale": bundle.get("is_stale", False),
                },
                "source_status": bundle.get("source_status") or {},
                "overview": bundle.get("overview") or {},
                "mining": {
                    "grid_candidates": (mining.get("grid_candidates") or [])[:5],
                    "trend_candidates": (mining.get("trend_candidates") or [])[:5],
                    "next_rotation_watchlist": (mining.get("next_rotation_watchlist") or [])[:5],
                    "factor_snapshot_id": mining.get("factor_snapshot_id"),
                },
                "factor_snapshot": {
                    "model": factor_snapshot.get("model") or {},
                    "leaders": (factor_snapshot.get("leaders") or [])[:6],
                    "categories": (factor_snapshot.get("categories") or [])[:6],
                },
                "sync_state": {
                    "running": _etf_state.get("running"),
                    "stage": _etf_state.get("stage"),
                    "message": _etf_state.get("message"),
                    "started_at": _etf_state.get("started_at"),
                    "finished_at": _etf_state.get("finished_at"),
                    "result": _etf_state.get("result"),
                    "error": _etf_state.get("error"),
                },
            },
        }
    except Exception as e:
        logger.error(f"[ETF] ETF 工作台失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


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
    kline_start_date: str = Query("20230101", description="同步 K 线起始日期，格式 YYYYMMDD"),
    kline_days: int = Query(120, description="旧参数：仅当未提供起始日期时回退使用"),
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
    logger.info(
        f"[ETF] 同步任务启动 sync_kline={sync_kline} start={kline_start_date or '-'} days={kline_days}"
    )

    async def _run():
        conn = get_etf_conn()
        try:
            result = await sync_etf_universe(
                conn, conn,
                sync_kline=sync_kline,
                kline_days=kline_days,
                kline_start_date=kline_start_date,
                max_etfs=max_etfs,
                progress_cb=_progress_cb,
            )
            _progress_cb("build_snapshot", result.get("etf_count") or 0, result.get("etf_count") or 0, "重建 ETF 最新快照")
            snapshot = persist_latest_etf_snapshot(conn, conn)
            result["snapshot_id"] = snapshot.get("snapshot_id")
            result["snapshot_computed_at"] = snapshot.get("computed_at")
            _etf_state.update({
                "stage": "done",
                "result": result,
                "message": (
                    f"完成：ETF {result['etf_count']} / "
                    f"K 线 {result['kline_etf_count']} / 行 {result['kline_rows']} / 快照 {result['snapshot_id']}"
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
    force_refresh: bool = Query(False, description="是否强制重算最新 ETF 快照"),
) -> Dict[str, Any]:
    """ETF 挖掘建议。

    输出三块：
    - 网格交易：回测验证后仍具超额的标的与建议步长
    - 买入持有：趋势和因子同时占优的标的
    - 下一轮动行业：基于 ETF 原生因子聚合的类别观察名单
    """
    conn = get_etf_conn()
    try:
        bundle = get_latest_etf_snapshot_bundle(conn, conn, force_refresh=force_refresh)
        data = dict(bundle.get("mining_snapshot") or {})
        data["grid_candidates"] = (data.get("grid_candidates") or [])[:grid_topn]
        data["trend_candidates"] = (data.get("trend_candidates") or [])[:trend_topn]
        data["next_rotation_watchlist"] = (data.get("next_rotation_watchlist") or [])[:rotation_topn]
        return {
            "status": "ok",
            "data": data,
            "snapshot": {
                "snapshot_id": bundle.get("snapshot_id"),
                "computed_at": bundle.get("computed_at"),
                "is_stale": bundle.get("is_stale", False),
            },
        }
    except Exception as e:
        logger.error(f"[ETF] ETF 挖掘建议生成失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


@router.get("/analysis/{code}")
async def get_etf_analysis(code: str) -> Dict[str, Any]:
    """单只 ETF 深度量化分析。

    返回多步长回测对比、买入持有基准、多周期稳定性检验、量化结论。
    前端用于展示详细分析面板。
    """
    import re
    if not re.match(r"^\d{6}$", code):
        raise HTTPException(status_code=400, detail="ETF 代码格式错误")

    conn = get_etf_conn()
    try:
        result = analyze_etf_deep(conn, conn, code)
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


@router.get("/qlib-summary")
async def get_etf_qlib_summary(force_refresh: bool = Query(False, description="是否强制重算最新 ETF 快照")) -> Dict[str, Any]:
    """ETF 原生因子快照。

    保留原路由名仅为兼容前端，但返回内容不再依赖股票 Qlib 聚合。
    """
    conn = get_etf_conn()
    try:
        bundle = get_latest_etf_snapshot_bundle(conn, conn, force_refresh=force_refresh)
        return {
            "status": "ok",
            "data": bundle.get("factor_snapshot") or {},
            "snapshot": {
                "snapshot_id": bundle.get("snapshot_id"),
                "computed_at": bundle.get("computed_at"),
                "is_stale": bundle.get("is_stale", False),
            },
        }
    except Exception as e:
        logger.error(f"[ETF] ETF 因子快照失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()


@router.get("/qlib-consensus")
async def get_etf_qlib_consensus(
    topk: int = Query(50, ge=10, le=200, description="返回的 ETF Qlib 结果条数"),
    force_refresh: bool = Query(False, description="是否强制重建 ETF-only Qlib 特征、训练与预测"),
) -> Dict[str, Any]:
    """返回 ETF-only Qlib 模型摘要、预测结果与管线状态。"""
    from services.etf_qlib_engine import get_latest_etf_qlib_signal_snapshot

    conn = get_etf_conn()
    try:
        return {
            "status": "ok",
            "data": get_latest_etf_qlib_signal_snapshot(conn, topk=topk, force_refresh=force_refresh),
        }
    except Exception as e:
        logger.error(f"[ETF] ETF Qlib 共识失败: {e}")
        return {"status": "error", "message": str(e)}
    finally:
        conn.close()

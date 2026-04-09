"""
Qlib AI 评分路由

独立于主项目的 /api/inst 路由，前缀 /api/qlib。
pyqlib 未安装时查询 API 仍可用，仅训练功能不可用。
"""

import asyncio
import logging

from fastapi import APIRouter, Query
from pydantic import BaseModel
from typing import Optional

from services.db import get_conn
from services.market_db import get_market_conn

logger = logging.getLogger("cm-api")
router = APIRouter()

_is_training = False
_train_error = None


class QlibTrainParams(BaseModel):
    train_start: str = "2023-01-01"
    train_end: str = "2025-03-31"
    valid_end: str = "2025-09-30"
    test_end: str = "2026-01-31"
    num_boost_round: int = 500
    early_stopping_rounds: int = 50
    num_leaves: int = 64
    learning_rate: float = 0.05
    subsample: float = 0.8
    colsample_bytree: float = 0.8
    use_alpha158: bool = True
    use_financial: bool = True
    use_institution: bool = True


@router.get("/status")
async def qlib_status():
    """模型状态 + 依赖可用性"""
    from services.qlib_full_engine import is_available, get_model_status

    available, error = is_available()
    conn = get_conn()
    try:
        model = get_model_status(conn)
        return {
            "ok": True,
            "available": available,
            "dependency_error": error,
            "model": model,
            "training": _is_training,
        }
    finally:
        conn.close()


@router.get("/factors")
async def qlib_factors(model_id: str = Query(None)):
    """因子重要性"""
    from services.qlib_full_engine import get_factor_importance

    conn = get_conn()
    try:
        data = get_factor_importance(conn, model_id=model_id)
        return {"ok": True, "data": data, "total": len(data)}
    finally:
        conn.close()


@router.post("/train")
async def qlib_train(body: Optional[QlibTrainParams] = None):
    """触发完整 Qlib 模型训练（Alpha158 + 自定义因子，异步）"""
    global _is_training, _train_error
    from services.qlib_full_engine import is_available

    available, error = is_available()
    if not available:
        return {"ok": False, "message": f"pyqlib 不可用: {error}"}

    if _is_training:
        return {"ok": False, "message": "模型正在训练中"}

    _is_training = True
    _train_error = None
    params = (body or QlibTrainParams()).model_dump()

    async def _run():
        global _is_training, _train_error
        try:
            from services.qlib_full_engine import train_full_model

            def _worker():
                smart_conn = get_conn(timeout=300)
                try:
                    result = train_full_model(smart_conn, params=params)
                    try:
                        from services.stock_forecast_engine import build_stock_forecast_features
                        from services.scoring import calculate_stock_scores
                        from services.setup_tracker import refresh_setup_tracking

                        forecast_count = build_stock_forecast_features(smart_conn)
                        score_count = calculate_stock_scores(smart_conn)
                        tracking = refresh_setup_tracking(smart_conn)
                        result["forecast_features"] = forecast_count
                        result["stock_scores"] = score_count
                        result["setup_snapshots"] = tracking.get("snapshots")
                    except Exception as exc:
                        logger.warning(f"[Qlib] 训练后回流预测/评分失败: {exc}")
                    return result
                finally:
                    smart_conn.close()

            result = await asyncio.to_thread(_worker)
            logger.info(f"[Qlib] 训练完成: {result}")
        except Exception as e:
            _train_error = str(e)
            logger.error(f"[Qlib] 训练失败: {e}")
        finally:
            _is_training = False

    asyncio.create_task(_run())
    return {"ok": True, "message": "Qlib 训练已启动，完成后会同步刷新股票研究列表"}


# ============================================================
# Qlib 数据管理
# ============================================================

@router.get("/data-status")
async def qlib_data_status():
    """Qlib 二进制数据状态"""
    from services.qlib_data_handler import get_qlib_data_status
    return {"ok": True, "data": get_qlib_data_status()}


@router.post("/dump-data")
async def qlib_dump_data():
    """触发 K 线 → Qlib 二进制转换"""
    from services.qlib_data_handler import dump_bin_from_db

    def _worker():
        mkt_conn = get_market_conn()
        try:
            return dump_bin_from_db(mkt_conn)
        finally:
            mkt_conn.close()

    result = await asyncio.to_thread(_worker)
    return {"ok": True, "data": result}

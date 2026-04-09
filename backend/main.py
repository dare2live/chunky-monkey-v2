"""
Chunky Monkey v2 — 机构事件研究系统

FastAPI 入口
"""

import logging
import sys
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, Response

# 确保 backend 目录在 path 中
sys.path.insert(0, str(Path(__file__).resolve().parent))

from services.db import init_db, get_conn
from services.market_db import init_market_db

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cm-api")

# 初始化数据库（仅建表，不做迁移）
init_db()
init_market_db()

# 清理重启后遗留的“运行中”步骤状态，避免前端误以为仍在执行
try:
    _conn = get_conn()
    try:
        _conn.execute("""
            UPDATE step_status
            SET status = 'idle',
                started_at = NULL,
                finished_at = NULL,
                error = NULL,
                records = 0
            WHERE status = 'running'
        """)
        _conn.commit()
    finally:
        _conn.close()
except Exception:
    pass

# FastAPI app
app = FastAPI(title="Chunky Monkey v2", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
from routers.market import router as market_router
from routers.institution import router as institution_router
from routers.updater import router as updater_router

app.include_router(market_router, prefix="/api/inst", tags=["market"])
app.include_router(institution_router, prefix="/api/inst", tags=["institution"])
app.include_router(updater_router, prefix="/api/inst", tags=["updater"])

from routers.qlib import router as qlib_router
from routers.etf import router as etf_router

# 模块化路由注册
def register_modules(app):
    try:
        conn = get_conn()
        from services.db import get_enabled_modules
        modules = get_enabled_modules(conn)
        conn.close()
    except Exception:
        modules = {"qlib": False, "etf": True, "akquant": False}

    if modules.get("qlib"):
        app.include_router(qlib_router, prefix="/api/qlib", tags=["qlib"])
    if modules.get("etf"):
        app.include_router(etf_router, prefix="/api/etf", tags=["etf"])
    
    return modules

app_modules = register_modules(app)

from routers.financial import router as financial_router
app.include_router(financial_router, prefix="/api/financial", tags=["financial"])

from routers.screening import router as screening_router
app.include_router(screening_router, prefix="/api/screening", tags=["screening"])

# 设置选项相关的API (比如开启/关闭功能模块)
@app.post("/api/settings/modules")
async def toggle_modules(settings: dict):
    try:
        conn = get_conn()
        for k, v in settings.items():
            if k in ["qlib", "etf", "akquant"]:
                val = "1" if v else "0"
                conn.execute(
                    "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                    (f"module_{k}_enabled", val)
                )
        conn.commit()
        conn.close()
        return {"status": "ok", "message": "配置已保存，请重启后端服务生效"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

# 健康检查
@app.get("/health")
async def health():
    try:
        conn = get_conn()
        from services.db import get_enabled_modules
        current_modules = get_enabled_modules(conn)
        conn.close()
    except Exception:
        current_modules = app_modules
        
    enabled = [k for k, v in current_modules.items() if v]
    return {
        "status": "ok",
        "enabled_modules": enabled,
        "available_modules": ["qlib", "etf", "akquant"],
        "module_deps": {"qlib": "需 pyqlib>=0.9.7", "akquant": "远期规划"}
    }


@app.get("/favicon.ico")
async def favicon():
    return Response(status_code=204)


# 静态文件
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ASSETS_DIR = PROJECT_ROOT / "assets"
INDEX_HTML = PROJECT_ROOT / "index.html"

if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=str(ASSETS_DIR)), name="assets")


@app.get("/")
async def index():
    if INDEX_HTML.exists():
        return FileResponse(str(INDEX_HTML))
    return {"message": "Chunky Monkey v2 API", "docs": "/docs"}

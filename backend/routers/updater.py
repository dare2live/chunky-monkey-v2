"""
数据更新管线

当前主 DAG（Phase 5 清理后）：
  1. sync_raw            — 下载十大股东
  2. match_inst          — 匹配跟踪机构
  3. sync_market_data    — 同步行情数据
  4. gen_events          — 生成事件
  5. calc_returns        — 计算收益
  6. sync_industry       — 申万行业
  7. build_current_rel   — 构建当前关系
  8. build_profiles      — 机构画像
  9. build_industry_stat — 行业统计
 10. build_trends        — 生成股票列表
 11. calc_sector_momentum — 板块动量分析
 12. build_stage_features — 阶段特征构建
 13. build_forecast_features — 预测特征构建
 14. calc_inst_scores    — 机构评分
 15. calc_stock_scores   — 股票评分
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Optional

import httpx
from fastapi import APIRouter
from fastapi.responses import FileResponse, Response

from services.db import get_conn
from services.gap_queue import (
    load_tracked_stock_names,
    mark_current_missing_as,
    mark_gap_failed,
    mark_gap_resolved,
    mark_gap_retrying,
    reconcile_gap_queue_snapshot,
    summarize_gap_queue,
)
from services.industry import industry_join_clause, summarize_industry_coverage

logger = logging.getLogger("cm-api")
router = APIRouter()

_UI_LOG_LIMIT = 400
_ui_logs = []
_ui_log_seq = 0


class _UILogHandler(logging.Handler):
    """把 cm-api 日志同步到前端状态接口，供工作台展示。"""

    def emit(self, record):
        global _ui_log_seq
        try:
            message = record.getMessage()
            if not message:
                return
            _ui_log_seq += 1
            _ui_logs.append({
                "id": _ui_log_seq,
                "ts": datetime.now().isoformat(),
                "level": record.levelname.lower(),
                "message": message,
            })
            if len(_ui_logs) > _UI_LOG_LIMIT:
                del _ui_logs[:-_UI_LOG_LIMIT]
        except Exception:
            pass


if not getattr(logger, "_cm_ui_handler_attached", False):
    _ui_handler = _UILogHandler(level=logging.INFO)
    logger.addHandler(_ui_handler)
    logger._cm_ui_handler_attached = True


def _reset_ui_logs():
    global _ui_logs, _ui_log_seq
    _ui_logs = []
    _ui_log_seq = 0

# ============================================================
# 步骤定义
# ============================================================

STEPS = [
    {"id": "sync_raw",              "name": "下载十大股东",     "group": "data", "order": 1},
    {"id": "match_inst",            "name": "匹配跟踪机构",    "group": "data", "order": 2},
    {"id": "sync_market_data",      "name": "同步行情数据",    "group": "data", "order": 3},
    {"id": "sync_financial",        "name": "同步财务数据",    "group": "data", "order": 4},
    {"id": "gen_events",            "name": "生成事件",        "group": "calc", "order": 5},
    {"id": "calc_returns",          "name": "计算收益",        "group": "calc", "order": 6},
    {"id": "sync_industry",         "name": "申万行业",        "group": "data", "order": 7},
    {"id": "calc_financial_derived","name": "计算财务指标",    "group": "calc", "order": 8},
    {"id": "build_current_rel",     "name": "构建当前关系",    "group": "mart", "order": 9},
    {"id": "build_profiles",        "name": "机构画像",        "group": "mart", "order": 10},
    {"id": "build_industry_stat",   "name": "行业统计",        "group": "mart", "order": 11},
    {"id": "build_trends",          "name": "生成股票列表",    "group": "mart", "order": 12},
    {"id": "calc_screening",        "name": "TDX选股筛选",     "group": "mart", "order": 13},
    {"id": "calc_sector_momentum",  "name": "板块动量分析",    "group": "mart", "order": 14},
    {"id": "build_stage_features",  "name": "阶段特征构建",    "group": "mart", "order": 15},
    {"id": "build_forecast_features","name": "预测特征构建",   "group": "mart", "order": 16},
    {"id": "calc_inst_scores",      "name": "机构评分",        "group": "mart", "order": 17},
    {"id": "calc_stock_scores",     "name": "股票评分",        "group": "mart", "order": 18},
]

# 硬依赖：failed → 跳过本步骤
HARD_DEPS = {
    "sync_raw": [],
    "match_inst": ["sync_raw"],
    "sync_market_data": ["match_inst"],
    "sync_financial": [],
    "gen_events": ["match_inst"],
    "calc_returns": ["gen_events"],
    "sync_industry": ["match_inst"],
    "calc_financial_derived": ["sync_financial"],
    "build_current_rel": ["gen_events"],
    "build_profiles": ["build_current_rel"],
    "build_industry_stat": ["build_current_rel"],
    "build_trends": ["build_current_rel"],
    "calc_screening": ["sync_market_data"],
    "calc_sector_momentum": ["sync_market_data", "sync_industry"],
    "build_stage_features": ["build_trends", "calc_sector_momentum"],
    "build_forecast_features": ["build_stage_features"],
    "calc_inst_scores": ["build_profiles", "build_industry_stat"],
    "calc_stock_scores": ["calc_inst_scores", "build_stage_features", "build_forecast_features"],
}

# 软依赖：failed/skipped → 继续执行但标注 data_completeness='partial'
SOFT_DEPS = {
    "calc_returns": ["sync_market_data"],
    "build_current_rel": ["calc_returns", "sync_industry"],
    "build_profiles": ["calc_returns"],
    "build_industry_stat": ["calc_returns", "sync_industry"],
    "build_trends": ["calc_returns", "sync_industry"],
    "calc_screening": ["calc_financial_derived"],
    "calc_sector_momentum": ["build_trends"],
    "build_stage_features": ["calc_financial_derived"],
    "build_forecast_features": [],
    "calc_inst_scores": ["calc_returns"],
    "calc_stock_scores": ["calc_returns", "calc_screening"],
}

_is_running = False
_stop_requested = False
_run_context = None
_last_run_context = None


class _RunStopped(Exception):
    """用户主动停止当前更新链路。"""


def _raise_if_stop():
    if _stop_requested:
        raise _RunStopped("用户已停止")


def _set_run_context(mode: str, step_id: Optional[str] = None, step_name: Optional[str] = None, step_ids=None):
    global _run_context
    _run_context = {
        "mode": mode,
        "step_id": step_id,
        "step_name": step_name,
        "step_ids": list(step_ids) if step_ids else None,
        "started_at": datetime.now().isoformat(),
    }


def _set_last_noop_context(mode: str, message: str):
    global _last_run_context
    now = datetime.now().isoformat()
    _last_run_context = {
        "mode": mode,
        "step_id": None,
        "step_name": None,
        "step_ids": [],
        "started_at": now,
        "finished_at": now,
        "noop": True,
        "message": message,
    }


def _finish_run_context(extra: Optional[dict] = None):
    global _run_context, _last_run_context
    if _run_context:
        ctx = dict(_run_context)
        ctx["finished_at"] = datetime.now().isoformat()
        if extra:
            ctx.update(extra)
        _last_run_context = ctx
    _run_context = None
    # 跑完任何更新后立即让 audit 缓存失效，下一次 /update/audit 走最新数据
    try:
        from services.audit import invalidate_audit_cache
        invalidate_audit_cache()
    except Exception:
        pass


def _prime_step_status_rows(conn, active_step_ids, *, inactive_mode: str = "idle",
                            skip_reasons: Optional[dict] = None):
    """在后台任务真正启动前，先把本轮 step_status 写成 pending/idle/skipped。"""
    valid_ids = {s["id"] for s in STEPS}
    conn.execute(
        "DELETE FROM step_status WHERE step_id NOT IN ({})".format(
            ",".join("?" * len(valid_ids))
        ), list(valid_ids)
    )
    selected = set(active_step_ids or [])
    skip_reasons = skip_reasons or {}
    for s in STEPS:
        sid = s["id"]
        if sid in selected:
            conn.execute("""
                INSERT OR REPLACE INTO step_status
                (step_id, group_name, step_name, step_order, status, error, records, started_at, finished_at)
                VALUES (?, ?, ?, ?, 'pending', NULL, NULL, NULL, NULL)
            """, (sid, s["group"], s["name"], s["order"]))
        else:
            status = "skipped" if inactive_mode == "skipped" else "idle"
            error = skip_reasons.get(sid, "数据已是最新，无需更新") if status == "skipped" else None
            conn.execute("""
                INSERT OR REPLACE INTO step_status
                (step_id, group_name, step_name, step_order, status, error, records, started_at, finished_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, NULL, NULL)
            """, (sid, s["group"], s["name"], s["order"], status, error))
    conn.commit()


def _scope_rows(rows, context: Optional[dict]):
    if not rows:
        return []
    if context:
        step_ids = context.get("step_ids") or []
        if step_ids:
            selected = set(step_ids)
            scoped = [r for r in rows if r.get("step_id") in selected]
            if scoped:
                return scoped
        step_id = context.get("step_id")
        if step_id:
            scoped = [r for r in rows if r.get("step_id") == step_id]
            if scoped:
                return scoped
    return [
        r for r in rows
        if (r.get("status") and r.get("status") != "idle")
        or r.get("started_at") or r.get("finished_at") or r.get("records")
    ]


def _summarize_rows(rows):
    summary = {
        "total": len(rows),
        "done": 0,
        "completed": 0,
        "failed": 0,
        "skipped": 0,
        "stopped": 0,
        "running": 0,
        "pending": 0,
        "latest_at": "",
    }
    latest_ms = 0
    for row in rows:
        status = row.get("status")
        if status in {"completed", "failed", "skipped", "stopped"}:
            summary["done"] += 1
        if status == "completed":
            summary["completed"] += 1
        elif status == "failed":
            summary["failed"] += 1
        elif status == "skipped":
            summary["skipped"] += 1
        elif status == "stopped":
            summary["stopped"] += 1
        elif status == "running":
            summary["running"] += 1
        elif status == "pending":
            summary["pending"] += 1
        ts = row.get("finished_at") or row.get("started_at") or ""
        parsed = _parse_sync_time(ts)
        if parsed:
            ms = parsed.timestamp()
            if ms >= latest_ms:
                latest_ms = ms
                summary["latest_at"] = ts
    summary["pct"] = round(summary["done"] / summary["total"] * 100) if summary["total"] else 0
    return summary


def _is_blocking_upstream_state(conn, step_id: str) -> bool:
    row = conn.execute("SELECT status, error FROM step_status WHERE step_id = ?", (step_id,)).fetchone()
    if not row:
        return False
    status = row["status"] if isinstance(row, dict) or hasattr(row, "__getitem__") else row[0]
    error = (row["error"] if isinstance(row, dict) or hasattr(row, "__getitem__") else row[1]) or ""
    if status in {"failed", "stopped"}:
        return True
    if status != "skipped":
        return False
    benign_tokens = ("无需更新", "已是最新", "无新增", "已完整")
    return not any(token in error for token in benign_tokens)


def _mode_label(mode: Optional[str]) -> str:
    return {
        "smart": "智能更新",
        "single": "单步更新",
        "all": "全量更新",
    }.get(mode or "", "更新")


def _build_status_summary(rows, running: bool, stop_requested: bool,
                          run_context: Optional[dict], last_run_context: Optional[dict]):
    def _active_rows(items):
        return [row for row in items if row.get("status") == "running"]

    def _activity_meta(items):
        active = _active_rows(items)
        active_names = [
            row.get("step_name") or row.get("step_id")
            for row in active
            if (row.get("step_name") or row.get("step_id"))
        ]
        latest_at = ""
        latest_ms = 0
        for row in items:
            for key in ("finished_at", "started_at"):
                parsed = _parse_sync_time(row.get(key) or "")
                if not parsed:
                    continue
                ms = parsed.timestamp()
                if ms >= latest_ms:
                    latest_ms = ms
                    latest_at = row.get(key) or ""
        return {
            "active_step_ids": [row.get("step_id") for row in active if row.get("step_id")],
            "active_step_names": active_names,
            "latest_status_at": latest_at,
        }

    if running and run_context:
        scoped = _scope_rows(rows, run_context)
        stat = _summarize_rows(scoped)
        activity = _activity_meta(scoped)
        mode = run_context.get("mode")
        label = _mode_label(mode)
        if mode == "single":
            scope_name = run_context.get("step_name") or label
            if stat["total"] > 1:
                message = f"{scope_name}续跑链路 · {stat['done']}/{stat['total']} · {stat['pct']}%"
            else:
                message = f"{scope_name} · {stat['done']}/{stat['total']} · {stat['pct']}%"
        else:
            message = f"{label} · {stat['done']}/{stat['total']} · {stat['pct']}%"
        if stop_requested:
            message = "停止中 · " + message
        if activity["active_step_names"]:
            message += " · 当前：" + " / ".join(activity["active_step_names"][:2])
        return {
            "kind": "running",
            "mode": mode,
            "show_progress": True,
            "pct": stat["pct"],
            "message": message,
            "counts": stat,
            **activity,
        }

    if last_run_context and last_run_context.get("noop"):
        return {
            "kind": "noop",
            "mode": last_run_context.get("mode"),
            "show_progress": False,
            "pct": 0,
            "message": last_run_context.get("message") or "数据已是最新，无需更新",
            "counts": {
                "total": 0,
                "done": 0,
                "completed": 0,
                "failed": 0,
                "skipped": 0,
                "stopped": 0,
                "latest_at": last_run_context.get("finished_at") or "",
                "pct": 0,
            },
        }

    context = last_run_context
    scoped = _scope_rows(rows, context)
    if scoped:
        stat = _summarize_rows(scoped)
        activity = _activity_meta(scoped)
        mode = (context or {}).get("mode")
        label = _mode_label(mode)
        if mode == "single":
            title = (context or {}).get("step_name") or label
            if stat["total"] > 1:
                message = (
                    f"上次续跑 {title} · {stat['completed']}成功"
                    f" · {stat['failed']}失败 · {stat['skipped']}跳过"
                )
            else:
                message = (
                    f"上次单步 {title} · {stat['completed']}成功"
                    f" · {stat['failed']}失败 · {stat['skipped']}跳过"
                )
        else:
            message = (
                f"上次{label} {stat['completed']}成功"
                f" · {stat['failed']}失败 · {stat['skipped']}跳过"
            )
        if stat["stopped"]:
            message += f" · {stat['stopped']}停止"
        return {
            "kind": "last",
            "mode": mode,
            "show_progress": True,
            "pct": stat["pct"],
            "message": message,
            "counts": stat,
            **activity,
        }

    return {
        "kind": "idle",
        "mode": None,
        "show_progress": False,
        "pct": 0,
        "message": "暂无更新记录",
        "counts": {
            "total": 0,
            "done": 0,
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "stopped": 0,
            "running": 0,
            "pending": 0,
            "latest_at": "",
            "pct": 0,
        },
        "active_step_ids": [],
        "active_step_names": [],
        "latest_status_at": "",
    }


def _tracked_stock_names(conn) -> dict[str, Optional[str]]:
    return load_tracked_stock_names(conn)


def _mark_steps_status(conn, step_ids, status: str, error: str, *,
                       started_at: Optional[str] = None,
                       finished_at: Optional[str] = None):
    if not step_ids:
        return
    now = datetime.now().isoformat()
    started = started_at if started_at is not None else now
    finished = finished_at if finished_at is not None else now
    for sid in step_ids:
        conn.execute(
            "UPDATE step_status SET status=?, error=?, "
            "started_at=COALESCE(started_at, ?), finished_at=? "
            "WHERE step_id=?",
            (status, error, started, finished, sid),
        )
    conn.commit()


def _parse_sync_time(value: str):
    if not value:
        return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    return None


def _is_recent_successful_sync(state: dict, cooldown_hours: int = 24) -> bool:
    if not state:
        return False
    if state.get("last_error"):
        return False
    synced_at = _parse_sync_time(
        state.get("last_success_at") or state.get("last_attempt_at") or ""
    )
    if not synced_at:
        return False
    return datetime.now() - synced_at < timedelta(hours=cooldown_hours)


def _monthly_stale_cutoff() -> str:
    """月线只要求覆盖到“上一个完整月份”。

    月线源通常不会在月初就稳定提供当月 K，因此不能拿“本月第一天”当过期阈值，
    否则每次手动同步都会把上月已完整的股票重复判成 stale。
    """
    first_day_this_month = datetime.now().replace(day=1)
    first_day_prev_month = (first_day_this_month - timedelta(days=1)).replace(day=1)
    return first_day_prev_month.strftime("%Y-%m-%d")


def _collect_downstream_steps(start_step_id):
    """返回包含自身在内、受该步骤影响的下游步骤（按 DAG 顺序）"""
    valid_ids = {s["id"] for s in STEPS}
    reverse = {sid: set() for sid in valid_ids}
    for child, deps in HARD_DEPS.items():
        for dep in deps:
            if dep in valid_ids and child in valid_ids:
                reverse[dep].add(child)
    for child, deps in SOFT_DEPS.items():
        for dep in deps:
            if dep in valid_ids and child in valid_ids:
                reverse[dep].add(child)

    seen = {start_step_id}
    queue = [start_step_id]
    while queue:
        current = queue.pop(0)
        for nxt in reverse.get(current, set()):
            if nxt not in seen:
                seen.add(nxt)
                queue.append(nxt)

    return [s["id"] for s in STEPS if s["id"] in seen]

# ============================================================
# 连通性检测
# ============================================================

_CONNECTIVITY_TARGETS = {
    "holdings_source": "https://datacenter-web.eastmoney.com",
}

_CONNECTIVITY_LABELS = {
    "holdings_source": "股东源",
    "kline_source": "K线源",
    "industry_source": "行业源",
}


async def check_connectivity() -> dict:
    """测试数据源连通性，返回 {source: bool, ...} + message

    K线源用 requests 库测试（与 akshare 保持一致），
    股东源用 httpx 测试（与 sync_raw 保持一致）。
    """
    results = {}

    # 股东源：用 httpx（sync_raw 用 httpx）
    try:
        async with httpx.AsyncClient(timeout=5.0, trust_env=False) as client:
            resp = await client.get(_CONNECTIVITY_TARGETS["holdings_source"])
            results["holdings_source"] = resp.status_code < 500
    except Exception:
        results["holdings_source"] = False

    from services.akshare_client import test_kline_availability, test_industry_availability

    try:
        kline_ok, kline_source = await asyncio.wait_for(test_kline_availability(), timeout=20)
        results["kline_source"] = kline_ok
        if kline_source:
            results["kline_source_detail"] = kline_source
    except Exception:
        results["kline_source"] = False

    try:
        industry_ok, industry_source = await asyncio.wait_for(test_industry_availability(), timeout=30)
        results["industry_source"] = industry_ok
        if industry_source:
            results["industry_source_detail"] = industry_source
    except Exception:
        results["industry_source"] = False

    # 构建提示消息
    unreachable = [_CONNECTIVITY_LABELS[k] for k, v in results.items() if not v]
    if not unreachable:
        results["message"] = "所有数据源正常"
    else:
        results["message"] = f"{'、'.join(unreachable)}不可用，建议切换至手机热点"

    return results


def _should_stop():
    return _stop_requested


# ============================================================
# 步骤执行函数
# ============================================================

async def _download_with_filter(conn, client, filter_str, label="") -> int:
    """通用分页下载，返回插入条数"""
    from routers.market import _fetch_page, _map_api_row, _upsert_batch

    first = None
    for attempt in range(3):
        try:
            first = await _fetch_page(client, filter_str, 1)
            break
        except Exception as e:
            logger.warning(f"[下载{label}] 首页失败 ({attempt+1}/3): {e}")
            if attempt < 2:
                await asyncio.sleep(2)
    if not first:
        logger.warning(f"[下载{label}] 首页请求失败，跳过")
        return 0

    result = first.get("result", {})
    total_pages = int(result.get("pages", 1)) or 1
    total_count = int(result.get("count", 0))
    logger.info(f"[下载{label}] 共 {total_count} 条, {total_pages} 页")

    total_inserted = 0
    page_data = result.get("data", [])
    if page_data:
        mapped = [_map_api_row(r) for r in page_data]
        total_inserted += _upsert_batch(conn, mapped)
        conn.commit()

    for page in range(2, total_pages + 1):
        if _should_stop():
            logger.info(f"[下载{label}] 用户停止 ({page-1}/{total_pages})")
            raise _RunStopped("用户已停止")
        try:
            await asyncio.sleep(1.5)
            data = await _fetch_page(client, filter_str, page)
            pd = data.get("result", {}).get("data", [])
            if pd:
                mapped = [_map_api_row(r) for r in pd]
                total_inserted += _upsert_batch(conn, mapped)
                conn.commit()
            if page % 50 == 0:
                logger.info(f"[下载{label}] {page}/{total_pages} ({total_inserted})")
        except Exception as e:
            logger.warning(f"[下载{label}] 第{page}页失败: {e}")
            await asyncio.sleep(3)

    return total_inserted


async def _step_sync_raw(conn) -> int:
    """从东财下载十大流通股东（含缺口自动补齐）"""
    from routers.market import _BROWSER_HEADERS

    count = conn.execute("SELECT COUNT(*) FROM market_raw_holdings").fetchone()[0]
    logger.info(f"[下载] 现有 {count} 条")

    total_inserted = 0

    async with httpx.AsyncClient(timeout=30.0, trust_env=False, headers=_BROWSER_HEADERS) as client:
        # --- 主下载（全量或增量） ---
        if count == 0:
            filter_str = "(END_DATE>='2023-01-01')"
            logger.info("[下载] 全量下载 (2023-01-01 起)...")
        else:
            row = conn.execute(
                "SELECT MAX(notice_date) FROM market_raw_holdings WHERE notice_date IS NOT NULL AND notice_date != ''"
            ).fetchone()
            since = row[0] if row and row[0] else "20230101"
            if len(since) == 8 and "-" not in since:
                since = f"{since[:4]}-{since[4:6]}-{since[6:8]}"
            filter_str = f"(UPDATE_DATE>='{since}')"
            logger.info(f"[下载] 增量 (>= {since})...")

        total_inserted += await _download_with_filter(conn, client, filter_str)

        _raise_if_stop()

        # --- 缺口检测与补齐 ---
        # 检查所有历史季度是否完整（最新季度除外，可能还在披露中）
        gap_quarters = []
        all_quarters = []
        for y in range(2023, datetime.now().year + 1):
            for q in ["0331", "0630", "0930", "1231"]:
                all_quarters.append(f"{y}{q}")
        # 排除未来和最近一个季度（可能还在披露中）
        latest_full = conn.execute(
            "SELECT report_date FROM market_raw_holdings GROUP BY report_date HAVING COUNT(*) > 50000 ORDER BY report_date DESC LIMIT 1"
        ).fetchone()
        cutoff = latest_full[0] if latest_full else "20260101"
        for qdate in all_quarters:
            if qdate > cutoff:
                continue
            cnt = conn.execute(
                "SELECT COUNT(*) FROM market_raw_holdings WHERE report_date = ?", (qdate,)
            ).fetchone()[0]
            # 正常季度应有 50000+ 条记录（十大股东 × 全市场约 5000+ 股票）
            if cnt < 50000:
                gap_quarters.append((qdate, cnt))

        if gap_quarters:
            logger.info(f"[下载] 检测到 {len(gap_quarters)} 个缺口季度: {[(q,c) for q,c in gap_quarters]}")
            for qdate, existing in gap_quarters:
                _raise_if_stop()
                fmt_date = f"{qdate[:4]}-{qdate[4:6]}-{qdate[6:8]}"
                gap_filter = f"(END_DATE='{fmt_date}')"
                logger.info(f"[下载] 补齐 {qdate} (现有 {existing} 条)...")
                inserted = await _download_with_filter(conn, client, gap_filter, label=f"-补齐{qdate}")
                total_inserted += inserted
                new_cnt = conn.execute(
                    "SELECT COUNT(*) FROM market_raw_holdings WHERE report_date = ?", (qdate,)
                ).fetchone()[0]
                logger.info(f"[下载] {qdate} 补齐后: {new_cnt} 条 (+{inserted})")

    final = conn.execute("SELECT COUNT(*) FROM market_raw_holdings").fetchone()[0]
    logger.info(f"[下载] 完成: +{total_inserted}, 总{final}")
    return final


def _build_exclusion_set(conn) -> set:
    """构建排除股票代码集合（主数据过滤 + 类别规则 + 手工股票拉黑）"""
    from services.security_master import get_active_a_stock_codes

    excluded = set()
    invalid_master_codes = set()
    manual_rows = conn.execute(
        "SELECT DISTINCT stock_code FROM excluded_stocks WHERE stock_code IS NOT NULL"
    ).fetchall()
    manual_codes = {r["stock_code"] for r in manual_rows if r["stock_code"]}
    excluded.update(manual_codes)

    active_codes = None
    try:
        active_codes = get_active_a_stock_codes(conn)
    except Exception as e:
        logger.warning(f"[排除] 当前A股主数据不可用，回退分类规则: {e}")

    # 加载启用的排除类别
    categories = conn.execute(
        "SELECT category FROM exclusion_categories WHERE enabled = 1"
    ).fetchall()
    enabled_cats = {r["category"] for r in categories}

    # 从 market_raw_holdings 获取所有唯一的 (stock_code, stock_name)
    all_stocks = conn.execute(
        "SELECT DISTINCT stock_code, stock_name FROM market_raw_holdings WHERE stock_code IS NOT NULL"
    ).fetchall()

    for row in all_stocks:
        code = row["stock_code"]
        name = row["stock_name"] or ""

        if not code or len(code) != 6 or not code.isdigit():
            invalid_master_codes.add(code)
            excluded.add(code)
            continue

        # 基础有效性：必须出现在当前A股主数据里
        if active_codes is not None and code not in active_codes:
            invalid_master_codes.add(code)
            excluded.add(code)
            continue

        # ST/*ST：按股票名称判断
        if "ST" in enabled_cats and ("ST" in name.upper()):
            excluded.add(code)
            continue

        # 北交所：8/9开头的6位代码
        if "BSE" in enabled_cats and code and len(code) == 6 and code[0] in ("8", "9"):
            excluded.add(code)
            continue

        # 新三板：4开头（包含老三板400开头）
        if code and len(code) == 6 and code[0] == "4":
            if "OTC" in enabled_cats and code.startswith("400"):
                excluded.add(code)
                continue
            if "NEEQ" in enabled_cats:
                excluded.add(code)
                continue

        # B股：200/900开头
        if "B_SHARE" in enabled_cats and code and len(code) == 6:
            if code.startswith("200") or code.startswith("900"):
                excluded.add(code)
                continue

        # 退市股：名称含"退"字
        if "DELISTED" in enabled_cats and "退" in name:
            excluded.add(code)
            continue

    if invalid_master_codes:
        preview = ",".join(sorted(invalid_master_codes)[:10])
        suffix = "..." if len(invalid_master_codes) > 10 else ""
        logger.info(
            f"[排除] 当前A股主数据过滤 {len(invalid_master_codes)} 只无效代码: {preview}{suffix}"
        )

    logger.info(
        f"[排除] 主数据过滤 + 分类规则 + 手工拉黑，共 {len(excluded)} 只股票被排除"
        f"（手工 {len(manual_codes)} 只）"
    )
    return excluded


async def _step_match_inst(conn) -> int:
    """匹配跟踪机构持仓"""
    institutions = conn.execute(
        "SELECT id, name, aliases FROM inst_institutions WHERE enabled = 1 AND blacklisted = 0 AND merged_into IS NULL"
    ).fetchall()

    if not institutions:
        logger.warning("[匹配] 无跟踪机构")
        return 0

    logger.info(f"[匹配] 加载 {len(institutions)} 个机构")

    # 构建排除集合
    excluded_codes = _build_exclusion_set(conn)

    # 清空旧匹配结果并重建（事务保护）
    now = datetime.now().isoformat()
    total = 0

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM inst_holdings")

        for inst in institutions:
            _raise_if_stop()
            inst_id = inst["id"]
            inst_name = inst["name"]

            # 构建匹配名单（主名 + 别名）
            names = [inst_name]
            try:
                aliases = json.loads(inst["aliases"] or "[]")
                names.extend([a for a in aliases if a])
            except Exception:
                pass

            # 在 market_raw_holdings 中匹配
            for name in names:
                _raise_if_stop()
                rows = conn.execute("""
                    SELECT holder_name, stock_code, stock_name, report_date, notice_date,
                           holder_rank, hold_amount, hold_market_cap, hold_ratio,
                           hold_change, hold_change_num, holder_type
                    FROM market_raw_holdings
                    WHERE holder_name = ?
                """, (name,)).fetchall()

                for r in rows:
                    # 排除过滤
                    if r["stock_code"] in excluded_codes:
                        continue
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO inst_holdings
                            (institution_id, holder_name, holder_type, stock_code, stock_name,
                             report_date, notice_date, holder_rank, hold_amount, hold_market_cap,
                             hold_ratio, hold_change, hold_change_num, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            inst_id, r["holder_name"], r["holder_type"],
                            r["stock_code"], r["stock_name"],
                            r["report_date"], r["notice_date"],
                            r["holder_rank"], r["hold_amount"], r["hold_market_cap"],
                            r["hold_ratio"], r["hold_change"], r["hold_change_num"],
                            now
                        ))
                        total += 1
                    except Exception:
                        pass

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info(f"[匹配] 完成: {total} 条持仓记录")
    return total



# [Phase 5 已删除] _step_kline_monthly 和 _step_kline_daily 已被 _step_sync_market_data 替代


async def _step_gen_events(conn) -> int:
    """生成机构事件"""
    from services.event_engine import generate_events, generate_exit_events
    count = generate_events(conn)
    count += generate_exit_events(conn)
    return count


async def _run_blocking_db_task(task_fn, timeout: int = 120):
    """把纯本地重算移到线程里，避免阻塞状态接口轮询。"""
    def _worker():
        worker_conn = get_conn(timeout=timeout)
        try:
            return task_fn(worker_conn)
        finally:
            worker_conn.close()
    return await asyncio.to_thread(_worker)


async def _step_calc_returns(conn) -> int:
    """计算事件收益"""
    from services.return_engine import calculate_returns
    return await _run_blocking_db_task(calculate_returns)


async def _step_build_profiles(conn) -> int:
    """计算机构画像 mart_institution_profile"""
    from services.holdings import refresh_stock_latest_cache

    def _followability_hint(safe_cnt, safe_wr30, eff30, high_cnt, high_wr30):
        """根据可跟统计给出简短提示。"""
        safe_cnt = safe_cnt or 0
        high_cnt = high_cnt or 0
        if safe_cnt < 5:
            return "样本偏少"
        if eff30 is not None and eff30 >= 80 and (safe_wr30 or 0) >= 60:
            return "可跟性强"
        if high_cnt >= 5 and safe_wr30 is not None and high_wr30 is not None and high_wr30 + 10 < safe_wr30:
            return "不宜追高"
        if eff30 is not None and eff30 >= 50 and (safe_wr30 or 0) >= 50:
            return "可跟性中等"
        return "信号损耗较大"

    refresh_stock_latest_cache(conn)
    now = datetime.now().isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM mart_institution_profile")

        institutions = conn.execute(
            "SELECT id, name, display_name, type FROM inst_institutions WHERE enabled = 1 AND blacklisted = 0 AND merged_into IS NULL"
        ).fetchall()

        # 刷新缓存表
        from services.holdings import refresh_stock_latest_cache
        refresh_stock_latest_cache(conn)
        # 一次性预计算所有机构的持仓摘要
        _inst_summaries = {}
        for r in conn.execute("""
            SELECT h.institution_id,
                   COUNT(*) as stock_count,
                   SUM(h.hold_market_cap) as total_cap,
                   MAX(h.notice_date) as latest_notice
            FROM inst_holdings h
            INNER JOIN (
                SELECT stock_code, max_rd
                FROM _cache_stock_latest_rd
            ) lat ON h.stock_code = lat.stock_code AND h.report_date = lat.max_rd
            GROUP BY h.institution_id
        """).fetchall():
            _inst_summaries[r["institution_id"]] = dict(r)

        count = 0
        for inst in institutions:
            _raise_if_stop()
            inst_id = inst["id"]

            # 基础统计
            stats = conn.execute("""
                SELECT COUNT(*) as total_events, COUNT(DISTINCT stock_code) as total_stocks,
                       COUNT(DISTINCT report_date) as total_periods
                FROM fact_institution_event WHERE institution_id = ?
            """, (inst_id,)).fetchone()

            # 收益统计（从增强后的 fact_institution_event 直接读取）
            returns = conn.execute("""
                SELECT AVG(e.gain_10d), AVG(e.gain_30d), AVG(e.gain_60d), AVG(e.gain_120d),
                       AVG(e.max_drawdown_30d), AVG(e.max_drawdown_60d)
                FROM fact_institution_event e
                WHERE e.institution_id = ? AND e.gain_30d IS NOT NULL
            """, (inst_id,)).fetchone()

            # 胜率（从增强后的 fact_institution_event 直接读取）
            win30 = conn.execute("""
                SELECT COUNT(CASE WHEN e.gain_30d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1)
                FROM fact_institution_event e
                WHERE e.institution_id = ? AND e.gain_30d IS NOT NULL
            """, (inst_id,)).fetchone()

            win60 = conn.execute("""
                SELECT COUNT(CASE WHEN e.gain_60d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1)
                FROM fact_institution_event e
                WHERE e.institution_id = ? AND e.gain_60d IS NOT NULL
            """, (inst_id,)).fetchone()

            win90 = conn.execute("""
                SELECT COUNT(CASE WHEN e.gain_90d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1)
                FROM fact_institution_event e
                WHERE e.institution_id = ? AND e.gain_90d IS NOT NULL
            """, (inst_id,)).fetchone()

            # 总胜率（任意一个周期盈利即算赢）
            total_wr = conn.execute("""
                SELECT COUNT(CASE WHEN COALESCE(e.gain_30d, 0) > 0 OR COALESCE(e.gain_60d, 0) > 0
                                  OR COALESCE(e.gain_90d, 0) > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1)
                FROM fact_institution_event e
                WHERE e.institution_id = ?
            """, (inst_id,)).fetchone()

            # Phase 1: 买入类事件统计（new_entry + increase）
            buy_stats = conn.execute("""
                SELECT COUNT(*) as cnt,
                       AVG(e.gain_30d) as avg30, AVG(e.gain_60d) as avg60, AVG(e.gain_120d) as avg120,
                       AVG(e.max_drawdown_30d) as dd30, AVG(e.max_drawdown_60d) as dd60,
                       COUNT(CASE WHEN e.gain_30d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr30,
                       COUNT(CASE WHEN e.gain_60d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr60,
                       COUNT(CASE WHEN e.gain_120d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr120
                FROM fact_institution_event e
                WHERE e.institution_id = ?
                  AND e.event_type IN ('new_entry', 'increase')
                  AND e.gain_30d IS NOT NULL
            """, (inst_id,)).fetchone()

            follow_stats = conn.execute("""
                SELECT
                    AVG(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_pct IS NOT NULL
                        THEN e.premium_pct END) as avg_premium,
                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_pct <= 5
                        THEN 1 END) as safe_cnt,
                    AVG(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_pct <= 5
                        THEN e.gain_30d END) as safe_avg30,
                    AVG(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_pct <= 5
                        THEN e.max_drawdown_30d END) as safe_dd30,
                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_pct <= 5 AND e.gain_30d > 0
                        THEN 1 END) * 100.0 /
                        MAX(COUNT(CASE
                            WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_pct <= 5
                            THEN 1 END), 1) as safe_wr30,

                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'discount'
                        THEN 1 END) as discount_cnt,
                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'discount' AND e.gain_30d > 0
                        THEN 1 END) * 100.0 /
                        MAX(COUNT(CASE
                            WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'discount'
                            THEN 1 END), 1) as discount_wr30,

                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'near_cost'
                        THEN 1 END) as near_cnt,
                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'near_cost' AND e.gain_30d > 0
                        THEN 1 END) * 100.0 /
                        MAX(COUNT(CASE
                            WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'near_cost'
                            THEN 1 END), 1) as near_wr30,

                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'premium'
                        THEN 1 END) as premium_cnt,
                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'premium' AND e.gain_30d > 0
                        THEN 1 END) * 100.0 /
                        MAX(COUNT(CASE
                            WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'premium'
                            THEN 1 END), 1) as premium_wr30,

                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'high_premium'
                        THEN 1 END) as high_cnt,
                    COUNT(CASE
                        WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'high_premium' AND e.gain_30d > 0
                        THEN 1 END) * 100.0 /
                        MAX(COUNT(CASE
                            WHEN e.event_type IN ('new_entry', 'increase') AND e.premium_bucket = 'high_premium'
                            THEN 1 END), 1) as high_wr30
                FROM fact_institution_event e
                WHERE e.institution_id = ?
                  AND e.event_type IN ('new_entry', 'increase')
                  AND e.gain_30d IS NOT NULL
            """, (inst_id,)).fetchone()

            signal_transfer_eff = None
            buy_avg30 = buy_stats["avg30"] if buy_stats else None
            safe_avg30 = follow_stats["safe_avg30"] if follow_stats else None
            if buy_avg30 is not None and buy_avg30 > 0 and safe_avg30 is not None:
                signal_transfer_eff = round(safe_avg30 / buy_avg30 * 100, 2)

            follow_hint = _followability_hint(
                follow_stats["safe_cnt"] if follow_stats else 0,
                follow_stats["safe_wr30"] if follow_stats else None,
                signal_transfer_eff,
                follow_stats["high_cnt"] if follow_stats else 0,
                follow_stats["high_wr30"] if follow_stats else None,
            )

            # Phase 4: 持仓周期计算
            # historical_median_holding_days: 已闭合持仓周期的中位天数
            # new_entry.notice_date → exit.notice_date = 一个闭合周期
            closed_periods = []
            holding_events = conn.execute("""
                SELECT stock_code, event_type, notice_date
                FROM fact_institution_event
                WHERE institution_id = ? AND notice_date IS NOT NULL AND notice_date != ''
                ORDER BY stock_code, report_date
            """, (inst_id,)).fetchall()
            # 按 stock_code 分组找闭合周期
            _stock_entries = {}
            for he in holding_events:
                sc = he["stock_code"]
                if he["event_type"] == "new_entry":
                    _stock_entries[sc] = he["notice_date"]
                elif he["event_type"] == "exit" and sc in _stock_entries:
                    try:
                        from datetime import datetime as _dt
                        entry_d = _dt.strptime(_stock_entries[sc][:10], "%Y-%m-%d")
                        exit_d = _dt.strptime(he["notice_date"][:10], "%Y-%m-%d")
                        days = (exit_d - entry_d).days
                        if days > 0:
                            closed_periods.append(days)
                    except (ValueError, TypeError):
                        pass
                    _stock_entries.pop(sc, None)

            hist_median_days = None
            if closed_periods:
                closed_periods.sort()
                mid = len(closed_periods) // 2
                hist_median_days = closed_periods[mid] if len(closed_periods) % 2 else (closed_periods[mid-1] + closed_periods[mid]) // 2

            # current_avg_held_days: 当前持仓的平均估算持有天数
            curr_held = conn.execute("""
                SELECT AVG(current_held_days) FROM mart_current_relationship
                WHERE institution_id = ? AND current_held_days IS NOT NULL
            """, (inst_id,)).fetchone()
            curr_avg_held = int(curr_held[0]) if curr_held and curr_held[0] else None

            # 当前持仓（从预计算的摘要中取）
            _s = _inst_summaries.get(inst_id, {})
            current = (_s.get("stock_count", 0), _s.get("total_cap"), _s.get("latest_notice"))

            # 近期事件统计
            recent = conn.execute("""
                SELECT COUNT(CASE WHEN e.event_type = 'new_entry' THEN 1 END),
                       COUNT(CASE WHEN e.event_type = 'increase' THEN 1 END),
                       COUNT(CASE WHEN e.event_type = 'exit' THEN 1 END)
                FROM fact_institution_event e
                INNER JOIN mart_current_relationship m
                    ON e.institution_id = m.institution_id AND e.stock_code = m.stock_code
                    AND e.report_date = m.report_date
                WHERE e.institution_id = ?
            """, (inst_id,)).fetchone()

            conn.execute("""
                INSERT OR REPLACE INTO mart_institution_profile
                (institution_id, institution_name, display_name, inst_type,
                 total_events, total_stocks, total_periods,
                 avg_gain_10d, avg_gain_30d, avg_gain_60d, avg_gain_120d,
                 win_rate_30d, win_rate_60d, win_rate_90d, total_win_rate,
                median_max_drawdown_30d, median_max_drawdown_60d,
                current_stock_count, current_total_cap, latest_notice_date,
                recent_new_entry_count, recent_increase_count, recent_exit_count,
                buy_event_count, buy_avg_gain_30d, buy_avg_gain_60d, buy_avg_gain_120d,
                buy_win_rate_30d, buy_win_rate_60d, buy_win_rate_120d,
                buy_median_max_drawdown_30d, buy_median_max_drawdown_60d,
                avg_premium_pct, safe_follow_event_count, safe_follow_win_rate_30d,
                safe_follow_avg_gain_30d, safe_follow_avg_drawdown_30d,
                premium_discount_event_count, premium_discount_win_rate_30d,
                premium_near_cost_event_count, premium_near_cost_win_rate_30d,
                premium_premium_event_count, premium_premium_win_rate_30d,
                premium_high_event_count, premium_high_win_rate_30d,
                signal_transfer_efficiency_30d, followability_hint,
                historical_median_holding_days, current_avg_held_days,
                updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                inst_id, inst["name"], inst["display_name"], inst["type"],
                stats["total_events"], stats["total_stocks"], stats["total_periods"],
                returns[0], returns[1], returns[2], returns[3],
                win30[0] if win30 else None, win60[0] if win60 else None,
                win90[0] if win90 else None, total_wr[0] if total_wr else None,
                returns[4], returns[5],
                current[0], current[1], current[2],
                recent[0], recent[1], recent[2],
                buy_stats["cnt"] if buy_stats else 0,
                buy_stats["avg30"] if buy_stats else None,
                buy_stats["avg60"] if buy_stats else None,
                buy_stats["avg120"] if buy_stats else None,
                buy_stats["wr30"] if buy_stats else None,
                buy_stats["wr60"] if buy_stats else None,
                buy_stats["wr120"] if buy_stats else None,
                buy_stats["dd30"] if buy_stats else None,
                buy_stats["dd60"] if buy_stats else None,
                follow_stats["avg_premium"] if follow_stats else None,
                follow_stats["safe_cnt"] if follow_stats else 0,
                follow_stats["safe_wr30"] if follow_stats else None,
                follow_stats["safe_avg30"] if follow_stats else None,
                follow_stats["safe_dd30"] if follow_stats else None,
                follow_stats["discount_cnt"] if follow_stats else 0,
                follow_stats["discount_wr30"] if follow_stats else None,
                follow_stats["near_cnt"] if follow_stats else 0,
                follow_stats["near_wr30"] if follow_stats else None,
                follow_stats["premium_cnt"] if follow_stats else 0,
                follow_stats["premium_wr30"] if follow_stats else None,
                follow_stats["high_cnt"] if follow_stats else 0,
                follow_stats["high_wr30"] if follow_stats else None,
                signal_transfer_eff, follow_hint,
                hist_median_days, curr_avg_held,
                now
            ))
            count += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info(f"[画像] 完成: {count} 个机构")
    return count


async def _step_build_trends(conn) -> int:
    """计算股票趋势 mart_stock_trend"""
    from services.holdings import refresh_stock_latest_cache
    refresh_stock_latest_cache(conn)
    now = datetime.now().isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM mart_stock_trend")

        # 股票列表骨架以 mart_current_relationship 为真相源，
        # 历史机构数/资金趋势再回看 inst_holdings 的近3期数据。
        stocks = conn.execute("""
            SELECT DISTINCT stock_code, stock_name
            FROM mart_current_relationship
            WHERE stock_code IS NOT NULL
        """).fetchall()

        # 加载最新的 Qlib rank
        try:
            latest_model = conn.execute(
                "SELECT model_id FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
            qlib_map = {}
            if latest_model:
                preds = conn.execute(
                    "SELECT stock_code, qlib_rank, qlib_score, qlib_percentile "
                    "FROM qlib_predictions WHERE model_id = ?",
                    (latest_model[0],)
                ).fetchall()
                qlib_map = {
                    p[0]: {"qlib_rank": p[1], "qlib_score": p[2], "qlib_percentile": p[3]}
                    for p in preds
                }
        except Exception:
            qlib_map = {}

        count = 0
        for stock in stocks:
            _raise_if_stop()
            code = stock["stock_code"]
            name = stock["stock_name"]

            # 该股票自己最近3个报告期（per-stock，不用全局固定日期）
            stock_periods = conn.execute("""
                SELECT DISTINCT report_date FROM inst_holdings
                WHERE stock_code = ? ORDER BY report_date DESC LIMIT 3
            """, (code,)).fetchall()
            q_dates = [r[0] for r in stock_periods]

            # 机构增减趋势：近3期机构家数 + 合计持仓
            inst_counts = []
            inst_caps = []
            for qd in q_dates:
                r = conn.execute("""
                    SELECT COUNT(DISTINCT institution_id), SUM(hold_market_cap)
                    FROM inst_holdings WHERE stock_code = ? AND report_date = ?
                """, (code, qd)).fetchone()
                inst_counts.append(r[0] or 0)
                inst_caps.append(r[1] or 0)

            # 补齐到3个
            while len(inst_counts) < 3:
                inst_counts.append(0)
                inst_caps.append(0)

            # 趋势计算
            def trend_str(vals):
                if len(vals) < 2:
                    return "—"
                parts = []
                for i in range(len(vals) - 1):
                    if vals[i] > vals[i + 1]:
                        parts.append("↑")
                    elif vals[i] < vals[i + 1]:
                        parts.append("↓")
                    else:
                        parts.append("→")
                return "".join(parts)

            inst_trend = trend_str(inst_counts)
            cap_trend = trend_str(inst_caps)

            # 最新事件
            latest_ev = conn.execute("""
                SELECT event_type, holder_name, change_pct, report_date, notice_date
                FROM fact_institution_event
                WHERE stock_code = ? ORDER BY report_date DESC, notice_date DESC LIMIT 3
            """, (code,)).fetchall()

            latest_events_json = json.dumps(
                [{"inst": e["holder_name"][:20], "type": e["event_type"], "pct": e["change_pct"]} for e in latest_ev],
                ensure_ascii=False
            ) if latest_ev else "[]"
            latest_rd = latest_ev[0]["report_date"] if latest_ev else None
            latest_nd = latest_ev[0]["notice_date"] if latest_ev else None
            
            # AI 评分排名
            qlib_info = qlib_map.get(code) or {}
            qlib_rank = qlib_info.get("qlib_rank")
            qlib_score = qlib_info.get("qlib_score")
            qlib_percentile = qlib_info.get("qlib_percentile")

            # 股价趋势（从 market_data.db 读取）
            from services.market_db import get_market_conn
            _mkt = get_market_conn()
            price_rows = _mkt.execute("""
                SELECT date, close FROM price_kline
                WHERE code = ? AND freq = 'monthly' AND adjust = 'qfq'
                ORDER BY date DESC LIMIT 3
            """, (code,)).fetchall()

            price_1m = None
            price_20d = None
            price_trend = "—"
            if len(price_rows) >= 2 and price_rows[1][1] and price_rows[1][1] > 0:
                price_1m = (price_rows[0][1] - price_rows[1][1]) / price_rows[1][1] * 100

            # 20日涨幅（从 market_data.db 日K线）
            daily_rows = _mkt.execute("""
                SELECT close FROM price_kline
                WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
                ORDER BY date DESC LIMIT 21
            """, (code,)).fetchall()
            _mkt.close()
            if len(daily_rows) >= 21 and daily_rows[-1][0] and daily_rows[-1][0] > 0:
                price_20d = (daily_rows[0][0] - daily_rows[-1][0]) / daily_rows[-1][0] * 100

            if len(price_rows) >= 3:
                ups = sum(1 for i in range(len(price_rows) - 1) if price_rows[i][1] and price_rows[i + 1][1] and price_rows[i][1] > price_rows[i + 1][1])
                if ups >= 2:
                    price_trend = "连涨"
                elif ups == 0:
                    price_trend = "连跌"
                else:
                    price_trend = "震荡"

            conn.execute("""
                INSERT OR REPLACE INTO mart_stock_trend
                (stock_code, stock_name, inst_count_t0, inst_count_t1, inst_count_t2,
                 inst_cap_t0, inst_cap_t1, inst_cap_t2, inst_trend, cap_trend,
                 latest_events, latest_report_date, latest_notice_date,
                 price_1m_pct, price_20d_pct, price_trend, qlib_rank,
                 qlib_score, qlib_percentile, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code, name, inst_counts[0], inst_counts[1], inst_counts[2],
                inst_caps[0], inst_caps[1], inst_caps[2], inst_trend, cap_trend,
                latest_events_json, latest_rd, latest_nd,
                price_1m, price_20d, price_trend, qlib_rank,
                qlib_score, qlib_percentile, now
            ))
            count += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info(f"[趋势] 完成: {count} 只股票")
    return count


async def _step_sync_industry(conn) -> int:
    """获取申万三级行业分类"""
    from services.akshare_client import fetch_sw_industry_all

    # 构建排除集合
    excluded_codes = _build_exclusion_set(conn)
    stock_names = _tracked_stock_names(conn)
    reconcile_gap_queue_snapshot(conn, stock_names=stock_names, datasets=("industry",), commit=True)

    count = 0
    detail = {
        "industry_sync": {
            "status": "running",
            "updated_rows": 0,
            "before_missing": summarize_gap_queue(conn, datasets=("industry",))["datasets"][0]["unresolved"],
            "after_missing": None,
            "gap_summary": summarize_gap_queue(conn, datasets=("industry",), limit_per_dataset=6)["datasets"][0],
        }
    }

    def _push_progress():
        _update_step(
            conn,
            "sync_industry",
            error=json.dumps(detail, ensure_ascii=False),
            records=count,
        )

    _raise_if_stop()
    industry_data = await fetch_sw_industry_all()
    if not industry_data:
        mark_current_missing_as(
            conn,
            "industry",
            status="blocked",
            reason="行业源无返回，当前未执行补齐",
            last_error="industry_source_empty",
            stock_names=stock_names,
            commit=False,
        )
        gap_summary = summarize_gap_queue(conn, datasets=("industry",), limit_per_dataset=6)["datasets"][0]
        detail["industry_sync"] = {
            "status": "blocked",
            "updated_rows": 0,
            "before_missing": detail["industry_sync"]["before_missing"],
            "after_missing": gap_summary["unresolved"],
            "reason": "行业源无返回，当前未执行补齐",
            "gap_summary": gap_summary,
        }
        _push_progress()
        logger.warning("[行业] 未获取到数据")
        return 0

    now = datetime.now().isoformat()
    for item in industry_data:
        _raise_if_stop()
        code = item["stock_code"]
        if code in excluded_codes:
            continue
        conn.execute("""
            INSERT OR REPLACE INTO dim_stock_industry
            (stock_code, sw_level1, sw_level2, sw_level3, sw_code, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (code, item["sw_level1"], item["sw_level2"], item["sw_level3"],
              item["sw_code"], now))
        count += 1

    reconcile_gap_queue_snapshot(conn, stock_names=stock_names, datasets=("industry",), commit=False)
    gap_summary = summarize_gap_queue(conn, datasets=("industry",), limit_per_dataset=6)["datasets"][0]
    detail["industry_sync"] = {
        "status": "partial" if gap_summary["unresolved"] else "success",
        "updated_rows": count,
        "before_missing": detail["industry_sync"]["before_missing"],
        "after_missing": gap_summary["unresolved"],
        "gap_summary": gap_summary,
    }
    conn.commit()
    _push_progress()
    logger.info(f"[行业] 完成: {count} 只股票")
    return count


async def _step_build_industry_stat(conn) -> int:
    """计算机构在各行业的表现统计"""
    now = datetime.now().isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM mart_institution_industry_stat")

        institutions = conn.execute(
            "SELECT id FROM inst_institutions WHERE enabled = 1 AND blacklisted = 0 AND merged_into IS NULL"
        ).fetchall()

        count = 0
        for inst in institutions:
            _raise_if_stop()
            inst_id = inst["id"]

            # 按行业分组统计（一二三级都做）
            for level_col, level_name in [("sw_level1", "level1"), ("sw_level2", "level2"), ("sw_level3", "level3")]:
                _raise_if_stop()
                industry_join = industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER")
                # 从增强后的 fact_institution_event + 统一行业解析口径读取
                rows = conn.execute(f"""
                    SELECT industry_dim.{level_col} as industry,
                           COUNT(*) as cnt,
                           AVG(e.gain_30d) as avg30, AVG(e.gain_60d) as avg60,
                           AVG(e.gain_90d) as avg90, AVG(e.gain_120d) as avg120,
                           COUNT(CASE WHEN e.gain_30d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr30,
                           COUNT(CASE WHEN e.gain_60d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr60,
                           COUNT(CASE WHEN e.gain_90d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr90,
                           COUNT(CASE WHEN e.gain_30d > 0 OR e.gain_60d > 0 THEN 1 END) * 100.0 / MAX(COUNT(*), 1) as wr_total,
                           AVG(e.max_drawdown_30d) as dd30, AVG(e.max_drawdown_60d) as dd60
                    FROM fact_institution_event e
                    {industry_join}
                    WHERE e.institution_id = ? AND e.gain_30d IS NOT NULL
                        AND industry_dim.{level_col} IS NOT NULL AND industry_dim.{level_col} != ''
                    GROUP BY industry_dim.{level_col}
                    HAVING cnt >= 1
                """, (inst_id,)).fetchall()

                for r in rows:
                    conn.execute("""
                        INSERT OR REPLACE INTO mart_institution_industry_stat
                        (institution_id, sw_level, industry_name, sample_events,
                         avg_gain_30d, avg_gain_60d, avg_gain_90d, avg_gain_120d,
                         win_rate_30d, win_rate_60d, win_rate_90d, total_win_rate,
                         max_drawdown_30d, max_drawdown_60d, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (inst_id, level_name, r["industry"], r["cnt"],
                          r["avg30"], r["avg60"], r["avg90"], r["avg120"],
                          r["wr30"], r["wr60"], r["wr90"], r["wr_total"],
                          r["dd30"], r["dd60"], now))
                    count += 1

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    logger.info(f"[行业统计] 完成: {count} 条")
    return count


async def _step_sync_market_data(conn) -> int:
    """同步行情数据：合并原 kline_monthly + kline_daily，写入 market_data.db"""
    import json as _json
    from services.market_db import (
        get_market_conn, upsert_price_rows, update_sync_state,
        get_all_sync_states
    )
    from services.akshare_client import fetch_stock_kline_monthly, fetch_stock_kline_daily

    mkt_conn = get_market_conn()
    sub_status = {}
    stock_names = _tracked_stock_names(conn)
    codes = list(stock_names.keys())
    total_rows = 0

    def _dataset_gap_summary(dataset: str, limit: int = 6) -> dict:
        return summarize_gap_queue(conn, datasets=(dataset,), limit_per_dataset=limit)["datasets"][0]

    def _push_progress():
        _update_step(
            conn,
            "sync_market_data",
            error=_json.dumps(sub_status, ensure_ascii=False),
            records=total_rows,
        )

    if not codes:
        mkt_conn.close()
        return 0

    reconcile_gap_queue_snapshot(
        conn,
        stock_names=stock_names,
        datasets=("monthly_kline", "daily_kline"),
        mkt_conn=mkt_conn,
        commit=True,
    )

    # --- 月 K ---
    monthly_rows_total = 0
    try:
        existing_monthly = {s["code"]: s for s in get_all_sync_states(mkt_conn, "monthly")}
        monthly_price_codes = {
            r["code"]
            for r in mkt_conn.execute(
                "SELECT DISTINCT code FROM price_kline WHERE freq='monthly' AND adjust='qfq'"
            ).fetchall()
        }
        missing_m = [c for c in codes if c not in monthly_price_codes]
        missing_m_set = set(missing_m)
        stale_cutoff = _monthly_stale_cutoff()
        # stale: 月线只要求覆盖到上一个完整月份；否则会在月初反复重拉上月已完整的数据
        stale_m = [c for c in codes if c in existing_monthly
                   and existing_monthly[c]["max_date"]
                   and existing_monthly[c]["max_date"] < stale_cutoff]
        cooldown_m = [
            c for c in stale_m
            if c in existing_monthly and _is_recent_successful_sync(existing_monthly[c])
        ]
        to_fetch_m = [
            c for c in set(missing_m + stale_m)
            if c not in set(cooldown_m)
        ]

        processed_m = 0
        success_m = 0
        failed_m_codes = []
        monthly_gap_before = _dataset_gap_summary("monthly_kline")
        sub_status["monthly_sync"] = {
            "status": "running" if to_fetch_m else "skipped",
            "done_codes": 0,
            "total_codes": len(to_fetch_m),
            "success_codes": 0,
            "rows": 0,
            "failed_count": 0,
            "failed_codes": [],
            "before_missing": monthly_gap_before["unresolved"],
            "after_missing": monthly_gap_before["unresolved"],
            "gap_summary": monthly_gap_before,
        }
        logger.info(f"[行情同步] 月K待同步: {len(to_fetch_m)} 只")
        if cooldown_m:
            logger.info(f"[行情同步] 月K跳过近期已校验: {len(cooldown_m)} 只")
        _push_progress()
        for code in to_fetch_m:
            _raise_if_stop()
            try:
                if code in missing_m_set:
                    mark_gap_retrying(
                        conn,
                        "monthly_kline",
                        code,
                        stock_name=stock_names.get(code),
                        reason="正在尝试补齐月K",
                        commit=False,
                    )
                df, source = await fetch_stock_kline_monthly(code, limit=36, start_date="20230101")
                if df is not None and not df.empty:
                    rows_data = [
                        {"code": code, "date": str(r["date"])[:10], "freq": "monthly",
                         "adjust": "qfq", "open": r["open"], "high": r["high"],
                         "low": r["low"], "close": r["close"],
                         "volume": r.get("volume"), "amount": r.get("amount")}
                        for _, r in df.iterrows()
                    ]
                    write_source = f"akshare_{source}" if source else "akshare_unknown"
                    upsert_price_rows(mkt_conn, rows_data, source=write_source)
                    dates = [r["date"] for r in rows_data]
                    update_sync_state(mkt_conn, code, "monthly", source=write_source,
                                      min_date=min(dates), max_date=max(dates),
                                      row_count=len(rows_data))
                    success_m += 1
                    total_rows += len(rows_data)
                    monthly_rows_total += len(rows_data)
                    if code in missing_m_set:
                        mark_gap_resolved(
                            conn,
                            "monthly_kline",
                            code,
                            stock_name=stock_names.get(code),
                            reason="月K已补齐",
                            commit=False,
                        )
                else:
                    failed_m_codes.append(code)
                    empty_error = f"{source or 'unknown'}_empty"
                    update_sync_state(
                        mkt_conn, code, "monthly", row_count=0,
                        error=empty_error,
                    )
                    if code in missing_m_set:
                        mark_gap_failed(
                            conn,
                            "monthly_kline",
                            code,
                            stock_name=stock_names.get(code),
                            last_error=empty_error,
                            touched_attempt=False,
                            commit=False,
                        )
            except _RunStopped:
                monthly_gap = _dataset_gap_summary("monthly_kline")
                sub_status["monthly_sync"].update({
                    "status": "stopped",
                    "done_codes": processed_m,
                    "success_codes": success_m,
                    "rows": monthly_rows_total,
                    "failed_count": len(failed_m_codes),
                    "failed_codes": failed_m_codes[:20],
                    "current_code": code,
                    "after_missing": monthly_gap["unresolved"],
                    "gap_summary": monthly_gap,
                })
                _push_progress()
                raise
            except Exception as e:
                failed_m_codes.append(code)
                update_sync_state(
                    mkt_conn, code, "monthly", row_count=0,
                    error=str(e)[:200],
                )
                if code in missing_m_set:
                    mark_gap_failed(
                        conn,
                        "monthly_kline",
                        code,
                        stock_name=stock_names.get(code),
                        last_error=str(e)[:200],
                        touched_attempt=False,
                        commit=False,
                    )
                logger.warning(f"[行情同步] 月K {code} 失败: {e}")
            processed_m += 1
            monthly_gap = _dataset_gap_summary("monthly_kline")
            sub_status["monthly_sync"].update({
                "done_codes": processed_m,
                "success_codes": success_m,
                "rows": monthly_rows_total,
                "failed_count": len(failed_m_codes),
                "failed_codes": failed_m_codes[:20],
                "current_code": code,
                "after_missing": monthly_gap["unresolved"],
                "gap_summary": monthly_gap,
            })
            if len(to_fetch_m) <= 20 or processed_m == len(to_fetch_m) or processed_m % 10 == 0:
                logger.info(
                    f"[行情同步] 月K进度: {processed_m}/{len(to_fetch_m)}"
                    f"，失败 {len(failed_m_codes)}"
                )
                _push_progress()

        reconcile_gap_queue_snapshot(
            conn,
            stock_names=stock_names,
            datasets=("monthly_kline",),
            mkt_conn=mkt_conn,
            commit=False,
        )
        monthly_gap = _dataset_gap_summary("monthly_kline")
        sub_status["monthly_sync"] = {
            "status": (
                "skipped" if not to_fetch_m
                else ("success" if not failed_m_codes else "partial")
            ),
            "done_codes": processed_m,
            "success_codes": success_m,
            "total_codes": len(to_fetch_m),
            "rows": monthly_rows_total,
            "failed_count": len(failed_m_codes),
            "failed_codes": failed_m_codes[:20],
            "before_missing": monthly_gap_before["unresolved"],
            "after_missing": monthly_gap["unresolved"],
            "gap_summary": monthly_gap,
        }
        if failed_m_codes:
            logger.warning("[行情同步] 月K未获取到: " + ", ".join(failed_m_codes[:20]))
        _push_progress()
    except Exception as e:
        monthly_gap = _dataset_gap_summary("monthly_kline")
        sub_status["monthly_sync"] = {
            "status": "stopped" if isinstance(e, _RunStopped) else "failed",
            "done_codes": sub_status.get("monthly_sync", {}).get("done_codes", 0),
            "total_codes": sub_status.get("monthly_sync", {}).get("total_codes", 0),
            "rows": monthly_rows_total,
            "success_codes": sub_status.get("monthly_sync", {}).get("success_codes", 0),
            "failed_count": sub_status.get("monthly_sync", {}).get("failed_count", 0),
            "failed_codes": sub_status.get("monthly_sync", {}).get("failed_codes", []),
            "before_missing": sub_status.get("monthly_sync", {}).get("before_missing"),
            "after_missing": monthly_gap["unresolved"],
            "gap_summary": monthly_gap,
            "error": str(e)[:200],
        }
        _push_progress()
        if isinstance(e, _RunStopped):
            raise
        logger.error(f"[行情同步] 月K失败: {e}")

    # --- 日 K ---
    daily_rows_total = 0
    try:
        existing_daily = {s["code"]: s for s in get_all_sync_states(mkt_conn, "daily")}
        daily_price_codes = {
            r["code"]
            for r in mkt_conn.execute(
                "SELECT DISTINCT code FROM price_kline WHERE freq='daily' AND adjust='qfq'"
            ).fetchall()
        }
        missing_d = [c for c in codes if c not in daily_price_codes]
        missing_d_set = set(missing_d)
        # 用交易日历判断：max_date < 最新已收盘交易日 → 需补差额
        latest_trade = conn.execute(
            "SELECT MAX(trade_date) FROM dim_trading_calendar "
            "WHERE is_trading=1 AND trade_date <= ?",
            (datetime.now().strftime("%Y-%m-%d"),)
        ).fetchone()
        latest_trade_date = latest_trade[0] if latest_trade and latest_trade[0] else datetime.now().strftime("%Y-%m-%d")
        # 查询当前停牌列表（从东财停复牌接口）
        suspended_codes = set()
        try:
            import akshare as ak
            tfp_df = await asyncio.to_thread(
                ak.stock_tfp_em, date=latest_trade_date.replace("-", "")
            )
            if tfp_df is not None and not tfp_df.empty:
                suspended_codes = {str(r).strip() for r in tfp_df["代码"].tolist() if r}
                logger.info(f"[行情同步] 停复牌接口: {len(suspended_codes)} 只股票当前停牌")
        except Exception as e:
            logger.warning(f"[行情同步] 停复牌查询失败（不影响同步）: {e}")

        stale_d = []
        suspended_d = []
        for c in codes:
            if c not in existing_daily:
                continue
            state = existing_daily[c]
            if not state.get("max_date") or state["max_date"] >= latest_trade_date:
                continue
            if c in suspended_codes:
                suspended_d.append(c)
                continue
            stale_d.append(c)
        uptodate_d = len(codes) - len(missing_d) - len(stale_d) - len(suspended_d)
        logger.info(f"[行情同步] 最新交易日={latest_trade_date}, 已最新={uptodate_d}只, 需补={len(stale_d)}只, 停牌={len(suspended_d)}只, 缺失={len(missing_d)}只")
        to_fetch_d = list(set(missing_d + stale_d))

        d_count = 0
        processed_d = 0
        failed_codes = []
        sem = asyncio.Semaphore(4)
        daily_gap_before = _dataset_gap_summary("daily_kline")
        sub_status["daily_sync"] = {
            "status": "running" if to_fetch_d else "skipped",
            "done_codes": 0,
            "total_codes": len(to_fetch_d),
            "success_codes": 0,
            "rows": 0,
            "failed_count": 0,
            "failed_codes": [],
            "before_missing": daily_gap_before["unresolved"],
            "after_missing": daily_gap_before["unresolved"],
            "gap_summary": daily_gap_before,
        }
        logger.info(f"[行情同步] 日K待同步: {len(to_fetch_d)} 只")
        _push_progress()

        async def _fetch_one(code):
            nonlocal d_count, daily_rows_total
            async with sem:
                _raise_if_stop()
                try:
                    if code in missing_d_set:
                        mark_gap_retrying(
                            conn,
                            "daily_kline",
                            code,
                            stock_name=stock_names.get(code),
                            reason="正在尝试补齐日K",
                            commit=False,
                        )
                    state = existing_daily.get(code)
                    if state and state.get("max_date"):
                        try:
                            start_dt = datetime.strptime(state["max_date"][:10], "%Y-%m-%d") - timedelta(days=20)
                            start_date = start_dt.strftime("%Y%m%d")
                        except Exception:
                            start_date = "20230101"
                    else:
                        start_date = "20230101"

                    df, source = await fetch_stock_kline_daily(
                        code,
                        days=150,
                        start_date=start_date,
                        end_date=datetime.now().strftime("%Y%m%d"),
                    )
                    if df is not None and not df.empty:
                        rows_data = [
                            {"code": code, "date": str(r["date"])[:10], "freq": "daily",
                             "adjust": "qfq", "open": r["open"], "high": r["high"],
                             "low": r["low"], "close": r["close"],
                             "volume": r.get("volume"), "amount": r.get("amount")}
                            for _, r in df.iterrows()
                        ]
                        write_source = f"akshare_{source}" if source else "akshare_unknown"
                        upsert_price_rows(mkt_conn, rows_data, source=write_source)
                        dates = [r["date"] for r in rows_data]
                        update_sync_state(mkt_conn, code, "daily", source=write_source,
                                          min_date=min(dates), max_date=max(dates),
                                          row_count=len(rows_data))
                        d_count += 1
                        daily_rows_total += len(rows_data)
                        if code in missing_d_set:
                            mark_gap_resolved(
                                conn,
                                "daily_kline",
                                code,
                                stock_name=stock_names.get(code),
                                reason="日K已补齐",
                                commit=False,
                            )
                    else:
                        failed_codes.append(code)
                        empty_error = f"{source or 'unknown'}_empty"
                        update_sync_state(
                            mkt_conn, code, "daily", row_count=0,
                            error=empty_error,
                        )
                        if code in missing_d_set:
                            mark_gap_failed(
                                conn,
                                "daily_kline",
                                code,
                                stock_name=stock_names.get(code),
                                last_error=empty_error,
                                touched_attempt=False,
                                commit=False,
                            )
                except _RunStopped:
                    raise
                except Exception as e:
                    failed_codes.append(code)
                    update_sync_state(
                        mkt_conn, code, "daily", row_count=0,
                        error=str(e)[:200],
                    )
                    if code in missing_d_set:
                        mark_gap_failed(
                            conn,
                            "daily_kline",
                            code,
                            stock_name=stock_names.get(code),
                            last_error=str(e)[:200],
                            touched_attempt=False,
                            commit=False,
                        )
                    logger.warning(f"[行情同步] 日K {code} 失败: {e}")

        # 分批并发
        for i in range(0, len(to_fetch_d), 20):
            _raise_if_stop()
            batch = to_fetch_d[i:i + 20]
            await asyncio.gather(*[_fetch_one(c) for c in batch])
            processed_d = min(i + len(batch), len(to_fetch_d))
            daily_gap = _dataset_gap_summary("daily_kline")
            sub_status["daily_sync"].update({
                "done_codes": processed_d,
                "success_codes": d_count,
                "rows": daily_rows_total,
                "failed_count": len(failed_codes),
                "failed_codes": failed_codes[:20],
                "last_batch_size": len(batch),
                "after_missing": daily_gap["unresolved"],
                "gap_summary": daily_gap,
            })
            logger.info(
                f"[行情同步] 日K进度: {processed_d}/{len(to_fetch_d)}"
                f"，失败 {len(failed_codes)}"
            )
            _push_progress()

        reconcile_gap_queue_snapshot(
            conn,
            stock_names=stock_names,
            datasets=("daily_kline",),
            mkt_conn=mkt_conn,
            commit=False,
        )
        daily_gap = _dataset_gap_summary("daily_kline")
        total_rows += daily_rows_total
        status = "skipped" if not to_fetch_d else ("success" if not failed_codes else "partial")
        sub_status["daily_sync"] = {
            "status": status,
            "done_codes": processed_d,
            "success_codes": d_count,
            "total_codes": len(to_fetch_d),
            "rows": daily_rows_total,
            "failed_count": len(failed_codes),
            "failed_codes": failed_codes[:20],  # 只保留前 20 个
            "before_missing": daily_gap_before["unresolved"],
            "after_missing": daily_gap["unresolved"],
            "gap_summary": daily_gap,
        }
        if failed_codes:
            logger.warning("[行情同步] 日K未获取到: " + ", ".join(failed_codes[:20]))
        _push_progress()
    except Exception as e:
        daily_gap = _dataset_gap_summary("daily_kline")
        sub_status["daily_sync"] = {
            "status": "stopped" if isinstance(e, _RunStopped) else "failed",
            "done_codes": sub_status.get("daily_sync", {}).get("done_codes", 0),
            "total_codes": sub_status.get("daily_sync", {}).get("total_codes", 0),
            "rows": daily_rows_total,
            "success_codes": sub_status.get("daily_sync", {}).get("success_codes", 0),
            "failed_count": len(failed_codes) if "failed_codes" in locals() else 0,
            "failed_codes": failed_codes[:20] if "failed_codes" in locals() else [],
            "before_missing": sub_status.get("daily_sync", {}).get("before_missing"),
            "after_missing": daily_gap["unresolved"],
            "gap_summary": daily_gap,
            "error": str(e)[:200],
        }
        _push_progress()
        if isinstance(e, _RunStopped):
            raise
        logger.error(f"[行情同步] 日K失败: {e}")

    sub_status["sync_state_refresh"] = {"status": "success"}
    mkt_conn.close()

    # 把子阶段详情写入 step_status.error（JSON 格式）
    _push_progress()

    logger.info(f"[行情同步] 完成: {total_rows} 行")
    return total_rows


async def _step_build_current_rel(conn) -> int:
    """构建 mart_current_relationship 物化表"""
    from services.holdings import build_current_relationship
    return build_current_relationship(conn)


async def _step_sync_financial(conn) -> int:
    """同步财务数据（mootdx finance）"""
    from services.financial_client import sync_financial_data
    return await sync_financial_data(conn)


async def _step_calc_financial_derived(conn) -> int:
    """计算财务派生指标"""
    from services.financial_client import calc_financial_derived
    return calc_financial_derived(conn)


async def _step_calc_screening(conn) -> int:
    """TDX 选股筛选"""
    from services.screening_engine import run_all_screens
    from services.market_db import get_market_conn
    mkt_conn = get_market_conn()
    try:
        return run_all_screens(conn, mkt_conn)
    finally:
        mkt_conn.close()


async def _step_calc_sector_momentum(conn) -> int:
    """板块动量分析 + 双重确认信号"""
    from services.sector_momentum import calc_sector_momentum, calc_dual_confirm
    from services.industry_context_engine import build_stock_industry_context
    from services.market_db import get_market_conn
    mkt_conn = get_market_conn()
    try:
        sector_count = calc_sector_momentum(conn, mkt_conn)
        dual_count = calc_dual_confirm(conn)
        context_count = build_stock_industry_context(conn)
        return sector_count + dual_count + context_count
    finally:
        mkt_conn.close()


async def _step_build_stage_features(conn) -> int:
    """阶段特征构建"""
    from services.stock_stage_engine import build_stock_stage_features
    from services.market_db import get_market_conn
    mkt_conn = get_market_conn()
    try:
        return build_stock_stage_features(conn, mkt_conn)
    finally:
        mkt_conn.close()


async def _step_build_forecast_features(conn) -> int:
    """预测特征构建"""
    from services.stock_forecast_engine import build_stock_forecast_features
    return build_stock_forecast_features(conn)


async def _step_calc_inst_scores(conn) -> int:
    """计算机构评分"""
    from services.scoring import calculate_institution_scores
    return calculate_institution_scores(conn)


async def _step_calc_stock_scores(conn) -> int:
    """计算股票评分"""
    from services.scoring import calculate_stock_scores
    from services.setup_tracker import refresh_setup_tracking

    count = calculate_stock_scores(conn)
    tracking = refresh_setup_tracking(conn)
    logger.info(
        f"[Setup跟踪] 快照 {tracking['snapshot_date']} · {tracking['snapshots']} 条候选 · 刷新 {tracking['refreshed']} 条"
    )
    return count


RUNNERS = {
    "sync_raw": _step_sync_raw,
    "match_inst": _step_match_inst,
    "sync_market_data": _step_sync_market_data,
    "sync_financial": _step_sync_financial,
    "gen_events": _step_gen_events,
    "calc_returns": _step_calc_returns,
    "sync_industry": _step_sync_industry,
    "calc_financial_derived": _step_calc_financial_derived,
    "build_current_rel": _step_build_current_rel,
    "build_profiles": _step_build_profiles,
    "build_industry_stat": _step_build_industry_stat,
    "build_trends": _step_build_trends,
    "calc_screening": _step_calc_screening,
    "calc_sector_momentum": _step_calc_sector_momentum,
    "build_stage_features": _step_build_stage_features,
    "build_forecast_features": _step_build_forecast_features,
    "calc_inst_scores": _step_calc_inst_scores,
    "calc_stock_scores": _step_calc_stock_scores,
}


# ============================================================
# API 端点
# ============================================================

def _calibrate_data_completeness(conn, step_id, skipped, failed):
    """
    Phase 2: data_completeness 校准（基于实际数据覆盖率，不只是步骤状态）。

    判定规则（写死）：
    - build_profiles: calc_returns skipped/failed OR 收益覆盖率 < 50% → partial
    - build_industry_stat: calc_returns 或 sync_industry 缺失 OR 行业覆盖率 < 80% → partial
    - build_trends: 收益或行业覆盖任一不足 → partial
    """
    calc_returns_missing = _is_blocking_upstream_state(conn, "calc_returns")
    sync_industry_missing = _is_blocking_upstream_state(conn, "sync_industry")

    # 查实际覆盖率
    returns_partial = calc_returns_missing
    industry_partial = sync_industry_missing
    if not returns_partial:
        try:
            from services.market_db import get_market_conn
            mkt_conn = get_market_conn()
            latest_market_date = mkt_conn.execute(
                "SELECT MAX(date) FROM price_kline WHERE freq='daily' AND adjust='qfq'"
            ).fetchone()[0]
            mkt_conn.close()
            total_events = conn.execute(
                """
                SELECT COUNT(*)
                FROM fact_institution_event
                WHERE notice_date IS NOT NULL AND notice_date != ''
                  AND tradable_date IS NOT NULL AND tradable_date != ''
                  AND (? IS NOT NULL AND tradable_date <= ?)
                """,
                (latest_market_date, latest_market_date),
            ).fetchone()[0]
            events_with_gain = conn.execute(
                """
                SELECT COUNT(*)
                FROM fact_institution_event
                WHERE return_to_now IS NOT NULL
                  AND tradable_date IS NOT NULL AND tradable_date != ''
                  AND (? IS NOT NULL AND tradable_date <= ?)
                """,
                (latest_market_date, latest_market_date),
            ).fetchone()[0]
            if total_events > 0 and events_with_gain / total_events < 0.5:
                returns_partial = True
                logger.info(f"[data_completeness] 收益覆盖率 {events_with_gain}/{total_events} = "
                           f"{events_with_gain/total_events:.0%} < 50% → partial")
        except Exception:
            pass
    if not industry_partial:
        try:
            coverage = summarize_industry_coverage(
                conn,
                "SELECT DISTINCT stock_code FROM inst_holdings WHERE stock_code IS NOT NULL",
            )
            total_holdings = coverage["total_codes"]
            with_industry = coverage["complete_codes"]
            if total_holdings > 0 and with_industry / total_holdings < 0.8:
                industry_partial = True
                logger.info(f"[data_completeness] 行业覆盖率 {with_industry}/{total_holdings} = "
                           f"{with_industry/total_holdings:.0%} < 80% → partial")
        except Exception:
            pass

    table_map = {
        "build_profiles": ("mart_institution_profile", returns_partial),
        "build_industry_stat": ("mart_institution_industry_stat",
                                returns_partial or industry_partial),
        "build_trends": ("mart_stock_trend",
                          returns_partial or industry_partial),
    }

    if step_id in table_map:
        table, is_partial = table_map[step_id]
        completeness = "partial" if is_partial else "complete"
        try:
            conn.execute(f"UPDATE {table} SET data_completeness = ?", (completeness,))
            conn.commit()
            if is_partial:
                logger.info(f"[data_completeness] {table} → partial")
        except Exception:
            pass


def _update_step(conn, step_id, **kwargs):
    sets = []
    vals = []
    for k, v in kwargs.items():
        sets.append(f"{k} = ?")
        vals.append(v)
    if not sets:
        return
    vals.append(step_id)
    conn.execute(f"UPDATE step_status SET {', '.join(sets)} WHERE step_id = ?", vals)
    conn.commit()


@router.post("/update/all")
async def update_all():
    """一键更新全部（当前主 DAG）"""
    global _is_running, _stop_requested
    if _is_running:
        return {"ok": False, "message": "更新正在进行中"}

    _reset_ui_logs()
    _is_running = True
    _stop_requested = False
    _set_run_context("all", step_ids=[s["id"] for s in STEPS])

    async def _run():
        global _is_running, _stop_requested
        conn = get_conn(timeout=120)
        try:
            # Reset any stuck "running" steps from previous crashed runs
            conn.execute("""
                UPDATE step_status SET status = 'failed', error = '上次运行异常中断'
                WHERE status = 'running' AND started_at < datetime('now', '-1 hour')
            """)
            conn.commit()

            # 清除旧 DAG 残留的步骤状态
            valid_ids = {s["id"] for s in STEPS}
            conn.execute(
                "DELETE FROM step_status WHERE step_id NOT IN ({})".format(
                    ",".join("?" * len(valid_ids))
                ), list(valid_ids)
            )

            # 初始化步骤状态
            for s in STEPS:
                conn.execute("""
                    INSERT OR REPLACE INTO step_status (step_id, group_name, step_name, step_order, status)
                    VALUES (?, ?, ?, ?, 'pending')
                """, (s["id"], s["group"], s["name"], s["order"]))
            conn.commit()

            completed = set()
            failed = set()
            skipped = set()
            stopped = set()

            # 预检连通性
            conn_status = await check_connectivity()
            kline_available = conn_status.get("kline_source", False)
            industry_available = conn_status.get("industry_source", False)
            if not kline_available:
                logger.warning(f"[更新] K线源不可用 — {conn_status.get('message', '')}")

            for step in STEPS:
                if _should_stop():
                    logger.info("[更新] 用户停止")
                    remaining = [s["id"] for s in STEPS if s["id"] not in completed and s["id"] not in failed and s["id"] not in skipped]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    break

                sid = step["id"]
                hard = HARD_DEPS.get(sid, [])
                soft = SOFT_DEPS.get(sid, [])

                # 硬依赖 failed → 跳过
                if any(d in failed for d in hard):
                    _update_step(conn, sid, status="skipped", error="硬依赖步骤失败")
                    skipped.add(sid)
                    continue

                # 网络依赖检查
                if sid == "sync_market_data" and not kline_available:
                    stock_names = _tracked_stock_names(conn)
                    mark_current_missing_as(
                        conn,
                        "daily_kline",
                        status="blocked",
                        reason="K线源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=stock_names,
                        commit=False,
                    )
                    mark_current_missing_as(
                        conn,
                        "monthly_kline",
                        status="blocked",
                        reason="K线源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=stock_names,
                        commit=True,
                    )
                    _update_step(conn, sid, status="skipped", error="K线源不可用")
                    skipped.add(sid)
                    continue
                if sid == "sync_industry" and not industry_available:
                    mark_current_missing_as(
                        conn,
                        "industry",
                        status="blocked",
                        reason="行业源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=_tracked_stock_names(conn),
                        commit=True,
                    )
                    _update_step(conn, sid, status="skipped", error="行业源不可用")
                    skipped.add(sid)
                    continue

                # 软依赖检查：标注但不阻断
                soft_missing = [d for d in soft if d in failed or d in skipped]

                _update_step(conn, sid, status="running", started_at=datetime.now().isoformat())
                logger.info(f"[更新] 开始: {step['name']}")

                try:
                    runner = RUNNERS[sid]
                    step_conn = get_conn(timeout=120)
                    try:
                        result = await runner(step_conn)
                    finally:
                        step_conn.close()

                    # calc_returns 可能返回字符串表示 skipped
                    if isinstance(result, str):
                        _update_step(conn, sid, status="skipped",
                                     finished_at=datetime.now().isoformat(), error=result)
                        skipped.add(sid)
                        logger.warning(f"[更新] 跳过: {step['name']}: {result}")
                        continue

                    count = result or 0
                    _update_step(conn, sid, status="completed",
                                 finished_at=datetime.now().isoformat(), records=count)
                    completed.add(sid)

                    # Phase 1: data_completeness 校准
                    _calibrate_data_completeness(conn, sid, skipped, failed)

                    logger.info(f"[更新] 完成: {step['name']} ({count})")
                except _RunStopped as e:
                    _update_step(conn, sid, status="stopped",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    stopped.add(sid)
                    remaining = [
                        s["id"] for s in STEPS
                        if s["id"] not in completed and s["id"] not in failed
                        and s["id"] not in skipped and s["id"] not in stopped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    logger.info(f"[更新] 已停止: {step['name']}")
                    break
                except Exception as e:
                    _update_step(conn, sid, status="failed",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    failed.add(sid)
                    logger.error(f"[更新] 失败: {step['name']}: {e}")

            logger.info(f"[更新] 全部完成: {len(completed)} 成功, {len(failed)} 失败, {len(skipped)} 跳过, {len(stopped)} 停止")
        except Exception as e:
            logger.error(f"[更新] 异常: {e}")
        finally:
            conn.close()
            _is_running = False
            _stop_requested = False
            _finish_run_context()

    asyncio.create_task(_run())
    return {"ok": True, "steps": len(STEPS)}


@router.get("/update/status")
async def update_status():
    """更新状态"""
    def _parse_detail(raw):
        if not raw or not isinstance(raw, str):
            return None
        raw = raw.strip()
        if not raw.startswith("{"):
            return None
        try:
            parsed = json.loads(raw)
        except Exception:
            return None
        return parsed if isinstance(parsed, dict) else None

    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM step_status ORDER BY step_order").fetchall()
        steps = []
        for row in rows:
            item = dict(row)
            detail = _parse_detail(item.get("error"))
            if detail is not None:
                item["detail"] = detail
            steps.append(item)
        summary = _build_status_summary(steps, _is_running, _stop_requested, _run_context, _last_run_context)
        return {
            "running": _is_running,
            "stop_requested": _stop_requested,
            "run_context": dict(_run_context) if _run_context else None,
            "last_run_context": dict(_last_run_context) if _last_run_context else None,
            "summary": summary,
            "steps": steps,
            "logs": list(_ui_logs),
            "server_time": datetime.now().isoformat(),
        }
    finally:
        conn.close()


@router.post("/update/stop")
async def update_stop():
    """停止更新"""
    global _stop_requested
    _stop_requested = True
    logger.info("[更新] 已请求停止")
    return {"ok": True, "message": "已请求停止"}


@router.post("/update/reset-derived")
async def reset_derived():
    """清空可重算派生层，保留原始数据、持仓、行业和K线源数据"""
    conn = get_conn(timeout=120)
    try:
        counts = {
            "events": conn.execute("SELECT COUNT(*) FROM fact_institution_event").fetchone()[0],
            "current_rel": conn.execute("SELECT COUNT(*) FROM mart_current_relationship").fetchone()[0],
            "profiles": conn.execute("SELECT COUNT(*) FROM mart_institution_profile").fetchone()[0],
            "industry_stat": conn.execute("SELECT COUNT(*) FROM mart_institution_industry_stat").fetchone()[0],
            "trends": conn.execute("SELECT COUNT(*) FROM mart_stock_trend").fetchone()[0],
            "steps": conn.execute("SELECT COUNT(*) FROM step_status").fetchone()[0],
        }

        conn.execute("DELETE FROM fact_institution_event")
        conn.execute("DELETE FROM mart_current_relationship")
        conn.execute("DELETE FROM mart_institution_profile")
        conn.execute("DELETE FROM mart_institution_industry_stat")
        conn.execute("DELETE FROM mart_stock_trend")
        conn.execute("DELETE FROM step_status")
        conn.commit()

        total = sum(counts.values())
        return {
            "ok": True,
            "message": f"已清空 {total} 条派生数据，请重新执行智能更新",
            "counts": counts,
        }
    finally:
        conn.close()


@router.get("/update/connectivity")
async def connectivity_check():
    """测试数据源连通性"""
    return await check_connectivity()


@router.get("/update/audit")
async def data_audit(force: bool = False):
    """数据质量审计

    force=true：跳过 8 秒进程级缓存，强制重算（手动刷新场景）
    """
    from services.audit import run_quality_audit
    conn = get_conn()
    try:
        reconcile_gap_queue_snapshot(conn, commit=True)
        payload = run_quality_audit(conn, use_cache=not force)
        return payload
    finally:
        conn.close()


@router.get("/update/smart-plan")
async def smart_plan():
    """智能更新计划（不执行，只返回建议）"""
    from services.audit import build_smart_plan
    conn = get_conn()
    try:
        plan = build_smart_plan(conn)
        return {"ok": True, "plan": plan}
    finally:
        conn.close()


@router.post("/update/smart")
async def smart_update():
    """智能更新（先审计再决定跑什么）"""
    global _is_running, _stop_requested
    if _is_running:
        return {"ok": False, "message": "更新正在进行中"}

    from services.audit import build_smart_plan

    _reset_ui_logs()
    _is_running = True
    _stop_requested = False

    conn_plan = get_conn()
    try:
        plan = build_smart_plan(conn_plan)
    finally:
        conn_plan.close()

    steps_to_run = plan["steps"]
    if not steps_to_run:
        _is_running = False
        _set_last_noop_context("smart", "数据已是最新，无需更新")
        return {
            "ok": True,
            "message": "数据已是最新，无需更新",
            "plan": plan,
            "steps": 0,
            "step_ids": [],
            "noop": True,
        }
    _set_run_context("smart", step_ids=steps_to_run)
    conn_init = get_conn(timeout=120)
    try:
        _prime_step_status_rows(
            conn_init,
            steps_to_run,
            inactive_mode="skipped",
            skip_reasons=plan.get("skip_reasons", {}),
        )
    finally:
        conn_init.close()
    logger.info(f"[智能更新] 已请求: {len(steps_to_run)} 个步骤待执行")

    async def _run():
        global _is_running, _stop_requested
        conn = get_conn(timeout=120)
        try:
            # Reset stuck steps
            conn.execute("""
                UPDATE step_status SET status = 'failed', error = '上次运行异常中断'
                WHERE status = 'running' AND started_at < datetime('now', '-1 hour')
            """)
            conn.commit()

            # 预检连通性
            conn_status = await check_connectivity()
            kline_available = conn_status.get("kline_source", False)
            industry_available = conn_status.get("industry_source", False)

            completed = set()
            failed = set()
            skipped = set()
            stopped = set()

            # 标记智能计划跳过的步骤
            for s in STEPS:
                if s["id"] not in steps_to_run:
                    skipped.add(s["id"])

            for step in STEPS:
                if _should_stop():
                    logger.info("[智能更新] 用户停止")
                    remaining = [
                        s["id"] for s in STEPS
                        if s["id"] in steps_to_run
                        and s["id"] not in completed and s["id"] not in failed
                        and s["id"] not in skipped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    break
                sid = step["id"]
                if sid not in steps_to_run:
                    continue

                hard = HARD_DEPS.get(sid, [])
                soft = SOFT_DEPS.get(sid, [])

                # 硬依赖 failed → 跳过
                if any(d in failed for d in hard):
                    _update_step(conn, sid, status="skipped", error="硬依赖步骤失败")
                    skipped.add(sid)
                    continue

                # 网络依赖检查
                if sid == "sync_market_data" and not kline_available:
                    stock_names = _tracked_stock_names(conn)
                    mark_current_missing_as(
                        conn,
                        "daily_kline",
                        status="blocked",
                        reason="K线源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=stock_names,
                        commit=False,
                    )
                    mark_current_missing_as(
                        conn,
                        "monthly_kline",
                        status="blocked",
                        reason="K线源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=stock_names,
                        commit=True,
                    )
                    _update_step(conn, sid, status="skipped", error="K线源不可用")
                    skipped.add(sid)
                    continue
                if sid == "sync_industry" and not industry_available:
                    mark_current_missing_as(
                        conn,
                        "industry",
                        status="blocked",
                        reason="行业源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=_tracked_stock_names(conn),
                        commit=True,
                    )
                    _update_step(conn, sid, status="skipped", error="行业源不可用")
                    skipped.add(sid)
                    continue

                _update_step(conn, sid, status="running", started_at=datetime.now().isoformat())
                logger.info(f"[智能更新] 开始: {step['name']}")

                try:
                    runner = RUNNERS[sid]
                    step_conn = get_conn(timeout=120)
                    try:
                        result = await runner(step_conn)
                    finally:
                        step_conn.close()

                    if isinstance(result, str):
                        _update_step(conn, sid, status="skipped",
                                     finished_at=datetime.now().isoformat(), error=result)
                        skipped.add(sid)
                        logger.warning(f"[智能更新] 跳过: {step['name']}: {result}")
                        continue

                    count = result or 0
                    _update_step(conn, sid, status="completed",
                                 finished_at=datetime.now().isoformat(), records=count)
                    completed.add(sid)
                    _calibrate_data_completeness(conn, sid, skipped, failed)
                except _RunStopped as e:
                    _update_step(conn, sid, status="stopped",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    stopped.add(sid)
                    remaining = [
                        s["id"] for s in STEPS
                        if s["id"] in steps_to_run
                        and s["id"] not in completed and s["id"] not in failed
                        and s["id"] not in skipped and s["id"] not in stopped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    logger.info(f"[智能更新] 已停止: {step['name']}")
                    break
                except Exception as e:
                    _update_step(conn, sid, status="failed",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    failed.add(sid)
                    logger.error(f"[智能更新] 失败: {step['name']}: {e}")

            result_counts = {
                "completed": len(completed),
                "failed": len(failed),
                "skipped": len(skipped),
                "stopped": len(stopped),
            }
            logger.info(f"[智能更新] 完成: {len(completed)} 成功, {len(failed)} 失败, {len(skipped)} 跳过, {len(stopped)} 停止")
        except Exception as e:
            logger.error(f"[智能更新] 异常: {e}")
        finally:
            conn.close()
            _is_running = False
            _stop_requested = False
            _finish_run_context({"result": locals().get("result_counts")})

    asyncio.create_task(_run())
    return {"ok": True, "steps": len(steps_to_run), "step_ids": steps_to_run, "plan": plan}


@router.post("/update/step/{step_id}")
async def run_single_step(step_id: str):
    """执行单个步骤"""
    global _is_running, _stop_requested
    if step_id not in RUNNERS:
        return {"ok": False, "error": f"未知步骤: {step_id}"}
    if _is_running:
        return {"ok": False, "message": "更新正在进行中"}

    step_meta = next((s for s in STEPS if s["id"] == step_id), None)
    step_name = (step_meta or {}).get("name", step_id)
    step_ids = _collect_downstream_steps(step_id)
    _reset_ui_logs()
    _is_running = True
    _stop_requested = False
    _set_run_context("single", step_id, step_name, step_ids=step_ids)
    conn_init = get_conn(timeout=120)
    try:
        _prime_step_status_rows(conn_init, step_ids, inactive_mode="idle")
    finally:
        conn_init.close()
    logger.info(f"[单步] 已请求: {step_name}")

    async def _run():
        global _is_running, _stop_requested
        conn = get_conn(timeout=120)
        try:
            selected = set(step_ids)
            kline_available = True
            industry_available = True
            if "sync_market_data" in selected:
                conn_status = await check_connectivity()
                kline_available = conn_status.get("kline_source", False)
                if "sync_industry" in selected:
                    industry_available = conn_status.get("industry_source", False)
            elif "sync_industry" in selected:
                conn_status = await check_connectivity()
                industry_available = conn_status.get("industry_source", False)

            completed = set()
            failed = set()
            skipped = set()
            stopped = set()

            for sid in step_ids:
                if _should_stop():
                    logger.info("[单步] 用户停止")
                    remaining = [
                        x for x in step_ids
                        if x not in completed and x not in failed and x not in skipped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    break

                step = next((s for s in STEPS if s["id"] == sid), None)
                step_label = (step or {}).get("name", sid)
                hard = [d for d in HARD_DEPS.get(sid, []) if d in selected]

                if any(d in failed for d in hard):
                    _update_step(conn, sid, status="skipped",
                                 finished_at=datetime.now().isoformat(),
                                 error="上游步骤失败，已跳过")
                    skipped.add(sid)
                    logger.warning(f"[单步] 跳过: {step_label}: 上游步骤失败")
                    continue

                if sid == "sync_market_data" and not kline_available:
                    stock_names = _tracked_stock_names(conn)
                    mark_current_missing_as(
                        conn,
                        "daily_kline",
                        status="blocked",
                        reason="K线源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=stock_names,
                        commit=False,
                    )
                    mark_current_missing_as(
                        conn,
                        "monthly_kline",
                        status="blocked",
                        reason="K线源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=stock_names,
                        commit=True,
                    )
                    _update_step(conn, sid, status="skipped",
                                 started_at=datetime.now().isoformat(),
                                 finished_at=datetime.now().isoformat(),
                                 error="K线源不可用")
                    skipped.add(sid)
                    logger.warning(f"[单步] 跳过: {step_label}: K线源不可用")
                    continue
                if sid == "sync_industry" and not industry_available:
                    mark_current_missing_as(
                        conn,
                        "industry",
                        status="blocked",
                        reason="行业源不可用，当前未执行同步",
                        last_error=conn_status.get("message", ""),
                        stock_names=_tracked_stock_names(conn),
                        commit=True,
                    )
                    _update_step(conn, sid, status="skipped",
                                 started_at=datetime.now().isoformat(),
                                 finished_at=datetime.now().isoformat(),
                                 error="行业源不可用")
                    skipped.add(sid)
                    logger.warning(f"[单步] 跳过: {step_label}: 行业源不可用")
                    continue

                _update_step(conn, sid, status="running",
                             started_at=datetime.now().isoformat(),
                             finished_at=None, error=None)
                if sid == step_id:
                    logger.info(f"[单步] 开始: {step_label}")
                else:
                    logger.info(f"[单步续跑] 开始: {step_label}")

                try:
                    count = await RUNNERS[sid](conn)
                    if isinstance(count, str):
                        _update_step(conn, sid, status="skipped",
                                     finished_at=datetime.now().isoformat(), error=count)
                        skipped.add(sid)
                        logger.warning(f"[单步{'续跑' if sid != step_id else ''}] 跳过: {step_label}: {count}")
                        continue

                    _update_step(conn, sid, status="completed",
                                 finished_at=datetime.now().isoformat(), records=count or 0)
                    completed.add(sid)
                    _calibrate_data_completeness(conn, sid, skipped, failed)
                    logger.info(f"[单步{'续跑' if sid != step_id else ''}] 完成: {step_label}: {count}")
                except _RunStopped as e:
                    _update_step(conn, sid, status="stopped",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    stopped.add(sid)
                    remaining = [
                        x for x in step_ids
                        if x not in completed and x not in failed and x not in skipped and x not in stopped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    logger.info(f"[单步{'续跑' if sid != step_id else ''}] 已停止: {step_label}")
                    break
                except Exception as e:
                    _update_step(conn, sid, status="failed",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    failed.add(sid)
                    logger.error(f"[单步{'续跑' if sid != step_id else ''}] 失败: {step_label}: {e}")

            result_counts = {
                "completed": len(completed),
                "failed": len(failed),
                "skipped": len(skipped),
                "stopped": len(stopped),
            }
            logger.info(f"[单步] 链路完成: {len(completed)} 成功, {len(failed)} 失败, {len(skipped)} 跳过, {len(stopped)} 停止")
        except Exception as e:
            _update_step(conn, step_id, status="failed",
                         finished_at=datetime.now().isoformat(), error=str(e)[:200])
            logger.error(f"[单步] {step_name} 失败: {e}")
        finally:
            conn.close()
            _is_running = False
            _stop_requested = False
            _finish_run_context({"result": locals().get("result_counts")})

    asyncio.create_task(_run())
    return {"ok": True, "step_id": step_id, "name": step_name, "steps": step_ids}


# ============================================================
# 救生艇：独立运行 AKShare 十大股东新进查询
# ============================================================

_lifeboat_running = False
_lifeboat_result = None


@router.post("/lifeboat/run")
async def run_lifeboat():
    """运行救生艇脚本（异步），返回运行状态"""
    global _lifeboat_running, _lifeboat_result
    if _lifeboat_running:
        return {"ok": False, "message": "救生艇正在运行中，请稍候"}

    import subprocess
    script_path = Path(__file__).resolve().parent.parent.parent / "lifeboat" / "fetch_and_report.py"
    if not script_path.exists():
        return {"ok": False, "message": f"救生艇脚本不存在: {script_path}"}

    _lifeboat_running = True
    _lifeboat_result = None

    async def _run():
        global _lifeboat_running, _lifeboat_result
        try:
            proc = await asyncio.create_subprocess_exec(
                "python3", str(script_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=str(script_path.parent),
            )
            stdout, _ = await proc.communicate()
            output = stdout.decode("utf-8", errors="replace") if stdout else ""
            if proc.returncode == 0:
                _lifeboat_result = {"ok": True, "message": "救生艇报告已生成", "output": output[-500:]}
                logger.info("[救生艇] 运行完成")
            else:
                _lifeboat_result = {"ok": False, "message": f"运行失败 (exit {proc.returncode})", "output": output[-500:]}
                logger.error(f"[救生艇] 失败: {output[-200:]}")
        except Exception as e:
            _lifeboat_result = {"ok": False, "message": str(e)}
            logger.error(f"[救生艇] 异常: {e}")
        finally:
            _lifeboat_running = False

    asyncio.create_task(_run())
    return {"ok": True, "message": "救生艇已启动，请稍候约2分钟"}


@router.get("/lifeboat/status")
async def lifeboat_status():
    """查询救生艇运行状态"""
    if _lifeboat_running:
        return {"running": True, "result": None}
    return {"running": False, "result": _lifeboat_result}


@router.get("/lifeboat/report")
async def lifeboat_report():
    """返回救生艇 HTML 报告内容"""
    report_path = Path(__file__).resolve().parent.parent.parent / "lifeboat" / "report.html"
    if not report_path.exists():
        return Response(content="<h3>尚未生成救生艇报告。请先运行。</h3>", media_type="text/html")
    return FileResponse(str(report_path), media_type="text/html")


@router.post("/update/sync")
async def update_sync():
    """只跑数据获取组"""
    return await _run_group_pipeline("sync", "数据获取组", "data")


@router.post("/update/calc")
async def update_calc():
    """只跑事实计算组"""
    return await _run_group_pipeline("calc", "事实计算组", "calc")


@router.post("/update/mart")
async def update_mart():
    """只跑集市构建组"""
    return await _run_group_pipeline("mart", "集市构建组", "mart")


async def _run_group_pipeline(run_mode: str, run_name: str, group_id: str):
    global _is_running, _stop_requested
    if _is_running:
        return {"ok": False, "message": "更新正在进行中"}

    steps_in_group = [s for s in STEPS if s.get("group") == group_id]
    step_ids = [s["id"] for s in steps_in_group]
    if not step_ids:
        return {"ok": False, "error": f"未知的分组: {group_id}"}
        
    _reset_ui_logs()
    _is_running = True
    _stop_requested = False
    _set_run_context(run_mode, step_ids=step_ids)
    
    conn_init = get_conn(timeout=120)
    try:
        _prime_step_status_rows(conn_init, step_ids, inactive_mode="idle")
    finally:
        conn_init.close()
        
    logger.info(f"[{run_name}] 已请求: {len(step_ids)} 个步骤")

    async def _run():
        global _is_running, _stop_requested
        conn = get_conn(timeout=120)
        try:
            selected = set(step_ids)
            
            conn_status = await check_connectivity()
            kline_available = conn_status.get("kline_source", False)
            industry_available = conn_status.get("industry_source", False)

            completed = set()
            failed = set()
            skipped = set()
            stopped = set()

            for step in steps_in_group:
                sid = step["id"]
                if _should_stop():
                    logger.info(f"[{run_name}] 用户停止")
                    remaining = [
                        x for x in step_ids
                        if x not in completed and x not in failed and x not in skipped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    break

                hard = [d for d in HARD_DEPS.get(sid, []) if d in selected]

                if any(d in failed for d in hard):
                    _update_step(conn, sid, status="skipped",
                                 finished_at=datetime.now().isoformat(),
                                 error="上游步骤失败，已跳过")
                    skipped.add(sid)
                    continue

                if sid == "sync_market_data" and not kline_available:
                    _update_step(conn, sid, status="skipped", error="K线源不可用")
                    skipped.add(sid)
                    continue
                if sid == "sync_industry" and not industry_available:
                    _update_step(conn, sid, status="skipped", error="行业源不可用")
                    skipped.add(sid)
                    continue

                _update_step(conn, sid, status="running", started_at=datetime.now().isoformat())
                logger.info(f"[{run_name}] 开始: {step['name']}")

                try:
                    runner = RUNNERS[sid]
                    step_conn = get_conn(timeout=120)
                    try:
                        result = await runner(step_conn)
                    finally:
                        step_conn.close()

                    if isinstance(result, str):
                        _update_step(conn, sid, status="skipped",
                                     finished_at=datetime.now().isoformat(), error=result)
                        skipped.add(sid)
                        logger.warning(f"[{run_name}] 跳过: {step['name']}: {result}")
                        continue

                    count = result or 0
                    _update_step(conn, sid, status="completed",
                                 finished_at=datetime.now().isoformat(), records=count)
                    completed.add(sid)

                    _calibrate_data_completeness(conn, sid, skipped, failed)

                    logger.info(f"[{run_name}] 完成: {step['name']} ({count})")
                except _RunStopped as e:
                    _update_step(conn, sid, status="stopped",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    stopped.add(sid)
                    remaining = [
                        x for x in step_ids
                        if x not in completed and x not in failed
                        and x not in skipped and x not in stopped
                    ]
                    _mark_steps_status(conn, remaining, "stopped", "用户已停止")
                    stopped.update(remaining)
                    break
                except Exception as e:
                    _update_step(conn, sid, status="failed",
                                 finished_at=datetime.now().isoformat(), error=str(e)[:200])
                    failed.add(sid)
                    logger.error(f"[{run_name}] 失败: {step['name']}: {e}")

            result_counts = {
                "completed": len(completed),
                "failed": len(failed),
                "skipped": len(skipped),
                "stopped": len(stopped),
            }
        except Exception as e:
            logger.error(f"[{run_name}] 异常: {e}")
        finally:
            conn.close()
            _is_running = False
            _stop_requested = False
            _finish_run_context({"result": locals().get("result_counts")})

    asyncio.create_task(_run())
    return {"ok": True, "steps": len(step_ids), "step_ids": step_ids}

"""
持仓查询服务 — 统一口径

所有关于"机构持有哪些股票"和"股票被哪些机构持有"的查询，
都必须通过这个模块，确保口径一致：

核心逻辑：每只股票取该股票全市场 MAX(report_date) 作为最新报告期，
然后看该报告期中哪些跟踪机构在十大流通股东名单里。

同时检测"退出"：倒数第二新的报告期中有该机构，但最新报告期中没有。
"""

import logging
from datetime import datetime

logger = logging.getLogger("cm-api")


def _parse_date_like(value):
    """兼容 YYYYMMDD / YYYY-MM-DD 两种日期格式。"""
    if not value:
        return None
    raw = str(value).strip()
    digits = raw.replace("-", "")
    try:
        if len(digits) == 8 and digits.isdigit():
            return datetime.strptime(digits, "%Y%m%d")
        return datetime.strptime(raw[:10], "%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _safe_float(value):
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _chunked(items, size=200):
    for idx in range(0, len(items), size):
        yield items[idx:idx + size]


def refresh_stock_latest_cache(conn):
    """刷新每只股票最新报告期的缓存表"""
    conn.execute("DROP TABLE IF EXISTS _cache_stock_latest_rd")
    conn.execute("""
        CREATE TABLE _cache_stock_latest_rd AS
        SELECT stock_code, MAX(report_date) as max_rd
        FROM market_raw_holdings
        GROUP BY stock_code
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_slr ON _cache_stock_latest_rd(stock_code)")
    conn.execute("DROP TABLE IF EXISTS _cache_holder_search")
    conn.execute("""
        CREATE TABLE _cache_holder_search AS
        WITH latest_holder_rows AS (
            SELECT m.holder_name,
                   m.holder_type,
                   m.stock_code,
                   m.notice_date,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.holder_name
                       ORDER BY COALESCE(m.notice_date, '') DESC, m.stock_code
                   ) AS rn
            FROM market_raw_holdings m
            INNER JOIN _cache_stock_latest_rd latest
                ON m.stock_code = latest.stock_code
               AND m.report_date = latest.max_rd
        )
        SELECT holder_name,
               MAX(CASE WHEN rn = 1 THEN holder_type END) AS holder_type,
               COUNT(DISTINCT stock_code) AS stock_count,
               MAX(notice_date) AS latest_notice
        FROM latest_holder_rows
        GROUP BY holder_name
    """)
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cache_holder_search_name ON _cache_holder_search(holder_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_holder_search_type ON _cache_holder_search(holder_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_cache_holder_search_count ON _cache_holder_search(stock_count DESC)")
    conn.commit()
    cnt = conn.execute("SELECT COUNT(*) FROM _cache_stock_latest_rd").fetchone()[0]
    holder_cnt = conn.execute("SELECT COUNT(*) FROM _cache_holder_search").fetchone()[0]
    logger.info(f"[缓存] 刷新 stock_latest_rd: {cnt} 只股票 · holder_search: {holder_cnt} 个机构")


def _ensure_cache(conn):
    """确保缓存表存在（如果不存在则创建）"""
    exists = conn.execute("""
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type='table' AND name IN ('_cache_stock_latest_rd', '_cache_holder_search')
    """).fetchone()[0]
    if exists < 2:
        refresh_stock_latest_cache(conn)


def get_stock_latest_rd(conn):
    """获取每只股票全市场最新报告期"""
    _ensure_cache(conn)
    rows = conn.execute("""
        SELECT stock_code, max_rd
        FROM _cache_stock_latest_rd
    """).fetchall()
    return {r["stock_code"]: r["max_rd"] for r in rows}


def get_stock_prev_rd(conn):
    """获取每只股票全市场倒数第二个报告期"""
    rows = conn.execute("""
        SELECT stock_code, report_date,
               ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY report_date DESC) as rn
        FROM (SELECT DISTINCT stock_code, report_date FROM market_raw_holdings)
    """).fetchall()
    result = {}
    for r in rows:
        if r["rn"] == 2:
            result[r["stock_code"]] = r["report_date"]
    return result


def get_inst_current_holdings(conn, inst_id):
    """获取某机构的当前持仓 — 从 mart_current_relationship 读取（单一真相源）"""
    rows = conn.execute("""
        SELECT * FROM mart_current_relationship
        WHERE institution_id = ?
        ORDER BY hold_market_cap DESC
    """, (inst_id,)).fetchall()

    result = []
    for h in rows:
        code, rd = h["stock_code"], h["report_date"]
        # 同股其他机构也从 MCR 读取
        others = conn.execute("""
            SELECT institution_id as id, display_name as name, inst_type as type
            FROM mart_current_relationship
            WHERE stock_code = ? AND institution_id != ?
        """, (code, inst_id)).fetchall()

        result.append({
            "stock_code": code, "stock_name": h["stock_name"],
            "report_date": rd, "notice_date": h["notice_date"],
            "hold_amount": h["hold_amount"], "hold_market_cap": h["hold_market_cap"],
            "hold_ratio": h["hold_ratio"],
            "sw_level1": h["sw_level1"], "sw_level2": h["sw_level2"], "sw_level3": h["sw_level3"],
            "event_type": h["event_type"], "change_pct": h["change_pct"],
            "report_season": h["report_season"],
            "inst_ref_cost": h["inst_ref_cost"],
            "inst_cost_method": h["inst_cost_method"],
            "premium_pct": h["premium_pct"],
            "premium_bucket": h["premium_bucket"],
            "follow_gate": h["follow_gate"],
            "follow_gate_reason": h["follow_gate_reason"],
            "gain_10d": h["gain_10d"], "gain_30d": h["gain_30d"],
            "gain_60d": h["gain_60d"], "gain_120d": h["gain_120d"],
            "other_institutions": [{"id": o["id"], "name": o["name"], "type": o["type"]} for o in others],
        })
    return result


def get_inst_exits(conn, inst_id):
    """获取某机构已退出的股票（倒数第二期有、最新期没有）"""
    _ensure_cache(conn)
    # 该机构在倒数第二期持有但最新期不持有的股票
    rows = conn.execute("""
        SELECT h.stock_code, h.stock_name, lat.max_rd as exit_report_date,
               h.report_date as prev_report_date, h.hold_amount as prev_hold_amount,
               h.hold_market_cap as prev_hold_market_cap
        FROM inst_holdings h
        INNER JOIN (
            SELECT stock_code, MAX(report_date) as max_rd
            FROM market_raw_holdings GROUP BY stock_code
        ) lat ON h.stock_code = lat.stock_code
        WHERE h.institution_id = ?
          AND h.report_date = (
              -- 该股票倒数第二个报告期
              SELECT report_date FROM (
                  SELECT DISTINCT report_date
                  FROM market_raw_holdings WHERE stock_code = h.stock_code
                  ORDER BY report_date DESC LIMIT 1 OFFSET 1
              )
          )
          AND NOT EXISTS (
              SELECT 1 FROM inst_holdings h2
              WHERE h2.institution_id = h.institution_id
                AND h2.stock_code = h.stock_code
                AND h2.report_date = lat.max_rd
          )
    """, (inst_id,)).fetchall()
    return [dict(r) for r in rows]


def get_stock_institutions(conn, stock_code):
    """获取某股票最新报告期中的跟踪机构列表 — 从 mart_current_relationship 读取（单一真相源）

    返回 (list[dict], latest_rd)
    """
    rows = conn.execute("""
        SELECT institution_id, display_name as inst_name, inst_type,
               report_date, notice_date,
               holder_rank, hold_amount, hold_market_cap, hold_ratio,
               event_type, change_pct,
               report_season, inst_ref_cost, inst_cost_method,
               premium_pct, premium_bucket, follow_gate, follow_gate_reason,
               price_entry, return_to_now, gain_30d,
               max_drawdown_30d, max_drawdown_60d, path_state,
               notice_age_days, disclosure_lag_days, current_held_days
        FROM mart_current_relationship
        WHERE stock_code = ?
        ORDER BY hold_market_cap DESC
    """, (stock_code,)).fetchall()

    if not rows:
        return [], None

    latest_rd = rows[0]["report_date"]
    result = [dict(r) for r in rows]
    return result, latest_rd


def get_inst_summary(conn, inst_id):
    """获取机构持仓摘要 — 从 mart_current_relationship 读取（单一真相源）"""
    row = conn.execute("""
        SELECT COUNT(*) as cnt, SUM(hold_market_cap) as cap, MAX(notice_date) as notice
        FROM mart_current_relationship
        WHERE institution_id = ?
    """, (inst_id,)).fetchone()
    return {
        "current_stock_count": row["cnt"] or 0,
        "current_total_cap": row["cap"],
        "latest_notice_date": row["notice"],
    }


# ============================================================
# 内部工具函数
# ============================================================

_CHANGE_MAP = {"新进": "new_entry", "加仓": "increase", "增加": "increase",
               "减仓": "decrease", "减少": "decrease", "不变": "unchanged"}


def _get_event_and_return(conn, inst_id, stock_code, report_date, hold_change=None):
    """获取事件和收益（从增强后的 fact_institution_event 读取，不再读 fact_event_return）"""
    ev = conn.execute("""
        SELECT event_type, change_pct, gain_10d, gain_30d, gain_60d, gain_120d
        FROM fact_institution_event
        WHERE institution_id = ? AND stock_code = ? AND report_date = ?
    """, (inst_id, stock_code, report_date)).fetchone()

    event_type = ev["event_type"] if ev else None
    if not event_type and hold_change:
        event_type = _CHANGE_MAP.get((hold_change or "").strip())

    # Phase 5: 直接从增强后的 fact_institution_event 读取（fact_event_return 已退役）
    gains = {
        "gain_10d": ev["gain_10d"] if ev else None,
        "gain_30d": ev["gain_30d"] if ev else None,
        "gain_60d": ev["gain_60d"] if ev else None,
        "gain_120d": ev["gain_120d"] if ev else None,
    }

    return (
        {"event_type": event_type, "change_pct": ev["change_pct"] if ev else None},
        gains,
    )


def _get_other_institutions(conn, stock_code, report_date, exclude_inst_id):
    """获取同一报告期持有该股票的其他跟踪机构"""
    rows = conn.execute("""
        SELECT i.id, COALESCE(NULLIF(i.display_name,''), i.name) as name, i.type
        FROM inst_holdings oh
        JOIN inst_institutions i ON oh.institution_id = i.id
        WHERE oh.stock_code = ? AND oh.report_date = ? AND oh.institution_id != ?
          AND i.enabled = 1 AND i.blacklisted = 0 AND i.merged_into IS NULL
    """, (stock_code, report_date, exclude_inst_id)).fetchall()
    return [{"id": o["id"], "name": o["name"], "type": o["type"]} for o in rows]


# ============================================================
# Phase 0: mart_current_relationship 物化表 + 共享 loaders
# ============================================================

def build_current_relationship(conn) -> int:
    """
    构建 mart_current_relationship 物化表。

    口径定义（per-stock latest）：
    - 对每只 stock_code，先取全市场 MAX(report_date) 作为该股票的"当前报告期"
    - 再在这一期中找哪些 tracked 机构仍在十大流通股东名单里
    - 这才是真正的"当前持仓关系"

    步骤 1：每只股票取全市场最新报告期，再取该期中的 tracked 机构持仓
    步骤 2：左连增强后的 fact_institution_event（带收益/路径字段）
    步骤 3：左连 inst_institutions（display_name, inst_type）
    步骤 4：行业通过 load_industry_map() 批量填充
    步骤 5：计算时间字段
    """
    from services.industry import load_industry_map

    logger.info("[当前关系] 开始构建 mart_current_relationship ...")
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    now_iso = now.isoformat()

    # 步骤 1+2+3：per-stock latest 口径
    # 先取每只股票全市场最新报告期，再在该期中找 tracked 机构
    rows = conn.execute("""
        SELECT
            h.institution_id,
            i.name      AS institution_name,
            COALESCE(NULLIF(i.display_name, ''), i.name) AS display_name,
            i.type      AS inst_type,
            h.stock_code,
            h.stock_name,
            h.report_date,
            h.notice_date,
            h.hold_amount,
            h.hold_market_cap,
            h.hold_ratio,
            h.hold_change,
            e.event_type,
            e.change_pct,
            e.gain_10d, e.gain_30d, e.gain_60d, e.gain_90d, e.gain_120d,
            e.max_drawdown_30d, e.max_drawdown_60d,
            e.report_season,
            e.inst_ref_cost, e.inst_cost_method,
            e.premium_pct, e.premium_bucket,
            e.follow_gate, e.follow_gate_reason,
            e.price_entry,
            e.return_to_now,
            e.path_state
        FROM inst_holdings h
        INNER JOIN (
            -- 每只股票全市场最新报告期（per-stock latest，不是 per-inst-stock latest）
            SELECT stock_code, MAX(report_date) AS max_rd
            FROM market_raw_holdings
            GROUP BY stock_code
        ) latest ON h.stock_code = latest.stock_code
               AND h.report_date = latest.max_rd
        JOIN inst_institutions i ON h.institution_id = i.id
        LEFT JOIN fact_institution_event e
            ON h.institution_id = e.institution_id
           AND h.stock_code = e.stock_code
           AND h.report_date = e.report_date
        WHERE i.enabled = 1 AND i.blacklisted = 0 AND i.merged_into IS NULL
    """).fetchall()

    if not rows:
        logger.warning("[当前关系] 无持仓数据")
        return 0

    # 步骤 4：批量加载行业映射
    industry_map = load_industry_map(conn)

    # 步骤 5：查找每个机构-股票的首次进入日期（entry_notice_date / entry_report_date）
    # 从事件表中取最早的 new_entry 事件
    entry_dates = {}
    entry_rows = conn.execute("""
        SELECT institution_id, stock_code,
               MIN(report_date) AS entry_rd,
               MIN(notice_date) AS entry_nd
        FROM fact_institution_event
        WHERE event_type = 'new_entry'
        GROUP BY institution_id, stock_code
    """).fetchall()
    for er in entry_rows:
        entry_dates[(er["institution_id"], er["stock_code"])] = {
            "entry_report_date": er["entry_rd"],
            "entry_notice_date": er["entry_nd"],
        }

    # 清空并重建
    conn.execute("DELETE FROM mart_current_relationship")

    inserts = []
    for r in rows:
        code = r["stock_code"]
        ind = industry_map.get(code, {})
        key = (r["institution_id"], code)
        entry = entry_dates.get(key, {})

        # 时间字段计算
        notice_date = r["notice_date"]
        report_date = r["report_date"]
        entry_nd = entry.get("entry_notice_date")

        notice_age = None
        if notice_date:
            nd = _parse_date_like(notice_date)
            if nd:
                notice_age = (now - nd).days

        disclosure_lag = None
        if report_date and notice_date:
            rd = _parse_date_like(report_date)
            nd = _parse_date_like(notice_date)
            if rd and nd:
                disclosure_lag = (nd - rd).days

        current_held_days = None
        if entry_nd:
            end = _parse_date_like(entry_nd)
            if end:
                current_held_days = (now - end).days

        # ⚠️ 单一真相源：cost / premium / follow_gate 直接复用 fact_institution_event
        # 之前的「持仓链成本」重算导致 656 对 (inst,stock,rd) follow_gate 不一致，已删除
        inserts.append((
            r["institution_id"], r["institution_name"], r["display_name"],
            r["inst_type"], code, r["stock_name"],
            report_date, notice_date,
            None,  # holder_rank (未在 inst_holdings 中直接有)
            r["hold_amount"], r["hold_market_cap"], r["hold_ratio"],
            r["hold_change"],
            r["event_type"], r["change_pct"],
            r["gain_10d"], r["gain_30d"], r["gain_60d"],
            r["gain_90d"], r["gain_120d"],
            r["max_drawdown_30d"], r["max_drawdown_60d"],
            r["report_season"],
            r["inst_ref_cost"], r["inst_cost_method"],
            r["premium_pct"], r["premium_bucket"],
            r["follow_gate"], r["follow_gate_reason"],
            r["price_entry"], r["return_to_now"], r["path_state"],
            entry.get("entry_report_date"), entry_nd,
            notice_age, disclosure_lag, current_held_days,
            ind.get("sw_level1"), ind.get("sw_level2"), ind.get("sw_level3"),
            1 if (r["return_to_now"] is not None or r["gain_30d"] is not None) else 0,
            1 if ind.get("sw_level1") else 0,
            now_iso,
        ))

    # 批量插入
    for i in range(0, len(inserts), 500):
        batch = inserts[i:i + 500]
        conn.executemany("""
            INSERT OR REPLACE INTO mart_current_relationship (
                institution_id, institution_name, display_name, inst_type,
                stock_code, stock_name, report_date, notice_date, holder_rank,
                hold_amount, hold_market_cap, hold_ratio, hold_change,
                event_type, change_pct,
                gain_10d, gain_30d, gain_60d, gain_90d, gain_120d,
                max_drawdown_30d, max_drawdown_60d,
                report_season, inst_ref_cost, inst_cost_method,
                premium_pct, premium_bucket, follow_gate, follow_gate_reason,
                price_entry, return_to_now, path_state,
                entry_report_date, entry_notice_date,
                notice_age_days, disclosure_lag_days, current_held_days,
                sw_level1, sw_level2, sw_level3,
                has_return_data, has_industry_data, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, batch)

    conn.commit()
    logger.info(f"[当前关系] 构建完成: {len(inserts)} 条")
    return len(inserts)


# ---------------------------------------------------------------------------
# 共享 loaders：所有页面统一读 mart_current_relationship
# ---------------------------------------------------------------------------

def load_current_holder_map(conn) -> dict:
    """返回 {stock_code: [inst dicts]}，用于股票列表/详情"""
    rows = conn.execute(
        "SELECT * FROM mart_current_relationship ORDER BY hold_market_cap DESC"
    ).fetchall()
    result = {}
    for r in rows:
        code = r["stock_code"]
        if code not in result:
            result[code] = []
        result[code].append(dict(r))
    return result


def load_current_stock_map(conn) -> dict:
    """返回 {institution_id: [stock dicts]}，用于机构列表/详情"""
    rows = conn.execute(
        "SELECT * FROM mart_current_relationship ORDER BY hold_market_cap DESC"
    ).fetchall()
    result = {}
    for r in rows:
        iid = r["institution_id"]
        if iid not in result:
            result[iid] = []
        result[iid].append(dict(r))
    return result


def get_stock_current_summary(conn, stock_code) -> dict:
    """从物化表获取某股票的当前摘要"""
    rows = conn.execute(
        "SELECT * FROM mart_current_relationship WHERE stock_code=? "
        "ORDER BY hold_market_cap DESC",
        (stock_code,)
    ).fetchall()
    if not rows:
        return {"tracked_count": 0, "holders": [], "total_cap": 0}
    return {
        "tracked_count": len(rows),
        "holders": [dict(r) for r in rows],
        "total_cap": sum(r["hold_market_cap"] or 0 for r in rows),
        "latest_notice_date": max((r["notice_date"] or "") for r in rows) or None,
    }


def get_inst_current_summary(conn, inst_id) -> dict:
    """从物化表获取某机构的当前摘要"""
    rows = conn.execute(
        "SELECT * FROM mart_current_relationship WHERE institution_id=? "
        "ORDER BY hold_market_cap DESC",
        (inst_id,)
    ).fetchall()
    if not rows:
        return {"current_stock_count": 0, "holdings": [], "current_total_cap": 0}
    return {
        "current_stock_count": len(rows),
        "holdings": [dict(r) for r in rows],
        "current_total_cap": sum(r["hold_market_cap"] or 0 for r in rows),
        "latest_notice_date": max((r["notice_date"] or "") for r in rows) or None,
    }

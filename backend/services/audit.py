"""
数据质量审计和智能更新计划

提供：
- run_quality_audit() — 数据质量报告
- build_smart_plan() — 决定哪些步骤需要跑
"""

import json
import logging
import time
from datetime import datetime
from typing import Optional

from services.db import get_conn
from services.gap_queue import reconcile_gap_queue_snapshot
from services.industry import count_industry_rows, summarize_industry_coverage
from services.market_db import get_market_conn

logger = logging.getLogger("cm-api")


def _scalar(conn, sql: str, params=()):
    row = conn.execute(sql, params).fetchone()
    if not row:
        return 0
    val = row[0]
    return 0 if val is None else val


def _pct(part: int, total: int) -> float:
    return round(part / total * 100, 1) if total else 0


# ============================================================
# 性能优化：审计结果级联缓存（2026-04-08）
# 工作台/页面会重复轮询 audit + plan，原本每次 5s+
# 通过进程级 TTL 缓存把热查询的代价摊平
# ============================================================

_AUDIT_CACHE: dict = {"ts": 0.0, "payload": None}
_PLAN_CACHE: dict = {"ts": 0.0, "payload": None, "force": None}
_TFP_CACHE: dict = {"date": None, "codes": None, "ts": 0.0}
_AUDIT_TTL_SECONDS = 8.0  # 工作台轮询节奏 1-2s，8s 内复用
_TFP_TTL_SECONDS = 1800   # 停牌列表 30 分钟刷新一次
_AUDIT_SNAPSHOT_KEY = "holder_workbench"
_AUDIT_SNAPSHOT_SCHEMA_VERSION = 1


def _json_dumps(payload) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_loads(text):
    if not text:
        return None
    try:
        return json.loads(text)
    except Exception:
        return None


def _attach_snapshot_meta(payload: dict, *, computed_at: Optional[str], source: Optional[str]) -> dict:
    result = dict(payload or {})
    result["snapshot_meta"] = {
        "state_key": _AUDIT_SNAPSHOT_KEY,
        "schema_version": _AUDIT_SNAPSHOT_SCHEMA_VERSION,
        "computed_at": computed_at or "",
        "source": source or "",
    }
    return result


def load_quality_audit_snapshot(conn) -> Optional[dict]:
    row = conn.execute(
        """
        SELECT computed_at, source, audit_json
        FROM mart_audit_snapshot_state
        WHERE state_key = ?
        LIMIT 1
        """,
        (_AUDIT_SNAPSHOT_KEY,),
    ).fetchone()
    if not row:
        return None
    payload = _json_loads(row["audit_json"])
    if not isinstance(payload, dict) or not payload.get("layers"):
        return None
    snapshot = _attach_snapshot_meta(
        payload,
        computed_at=row["computed_at"],
        source=row["source"],
    )
    _AUDIT_CACHE["payload"] = snapshot
    _AUDIT_CACHE["ts"] = time.time()
    return snapshot


def persist_quality_audit_snapshot(conn, audit_payload: dict, *, source: str) -> dict:
    computed_at = datetime.now().isoformat()
    conn.execute(
        """
        INSERT INTO mart_audit_snapshot_state
            (state_key, schema_version, computed_at, source, audit_json)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(state_key) DO UPDATE SET
            schema_version = excluded.schema_version,
            computed_at = excluded.computed_at,
            source = excluded.source,
            audit_json = excluded.audit_json
        """,
        (
            _AUDIT_SNAPSHOT_KEY,
            _AUDIT_SNAPSHOT_SCHEMA_VERSION,
            computed_at,
            source,
            _json_dumps(audit_payload),
        ),
    )
    conn.commit()
    snapshot = _attach_snapshot_meta(audit_payload, computed_at=computed_at, source=source)
    _AUDIT_CACHE["payload"] = snapshot
    _AUDIT_CACHE["ts"] = time.time()
    return snapshot


def refresh_quality_audit_snapshot(conn, *, source: str = "manual") -> dict:
    reconcile_gap_queue_snapshot(conn, commit=True)
    audit_payload = run_quality_audit(conn, use_cache=False)
    return persist_quality_audit_snapshot(conn, audit_payload, source=source)


def get_quality_audit(conn, *, force: bool = False) -> dict:
    if not force:
        snapshot = load_quality_audit_snapshot(conn)
        if snapshot:
            return snapshot
    return refresh_quality_audit_snapshot(conn, source="manual_force" if force else "bootstrap")


def invalidate_audit_cache() -> None:
    """更新步骤跑完后调用，清掉缓存以便下一次重新审计。"""
    _AUDIT_CACHE["ts"] = 0.0
    _AUDIT_CACHE["payload"] = None
    _PLAN_CACHE["ts"] = 0.0
    _PLAN_CACHE["payload"] = None


def _get_suspended_codes(trade_date: str) -> set:
    """带 TTL 缓存的停牌列表查询；akshare 调用一次 ~1s。"""
    if not trade_date:
        return set()
    now = time.time()
    if (
        _TFP_CACHE.get("date") == trade_date
        and _TFP_CACHE.get("codes") is not None
        and now - _TFP_CACHE.get("ts", 0) < _TFP_TTL_SECONDS
    ):
        return _TFP_CACHE["codes"]
    codes: set = set()
    try:
        import akshare as ak
        tfp_df = ak.stock_tfp_em(date=trade_date.replace("-", ""))
        if tfp_df is not None and not tfp_df.empty and "代码" in tfp_df.columns:
            codes = {str(r).strip() for r in tfp_df["代码"].tolist() if r}
    except Exception:
        pass
    _TFP_CACHE.update({"date": trade_date, "codes": codes, "ts": now})
    return codes


def run_quality_audit(conn, use_cache: bool = True) -> dict:
    """运行数据质量审计，返回各层状态报告

    use_cache=True：8 秒内复用上次结果（工作台轮询场景）
    use_cache=False：强制重算（更新流程结尾、用户主动 reload）
    """
    if use_cache:
        cached = _AUDIT_CACHE.get("payload")
        if cached and (time.time() - _AUDIT_CACHE.get("ts", 0)) < _AUDIT_TTL_SECONDS:
            return cached

    latest_market_date = None
    try:
        mkt_conn = get_market_conn()
        row = mkt_conn.execute(
            "SELECT MAX(date) FROM price_kline WHERE freq='daily' AND adjust='qfq'"
        ).fetchone()
        latest_market_date = row[0] if row else None
        mkt_conn.close()
    except Exception:
        latest_market_date = None

    # RAW 层 — 用 MAX(rowid) 代替 COUNT(*) 节省 ~580ms（market_raw_holdings 是 append-only）
    raw_count = _scalar(conn, "SELECT MAX(rowid) FROM market_raw_holdings")
    raw_stocks = _scalar(conn, "SELECT COUNT(DISTINCT stock_code) FROM market_raw_holdings")
    raw_latest = conn.execute("SELECT MAX(notice_date) FROM market_raw_holdings").fetchone()[0]
    raw_total_periods = _scalar(conn, "SELECT COUNT(DISTINCT report_date) FROM market_raw_holdings")
    raw_periods = conn.execute(
        "SELECT DISTINCT report_date FROM market_raw_holdings ORDER BY report_date DESC LIMIT 5"
    ).fetchall()

    # 机构层
    inst_total = _scalar(conn, "SELECT COUNT(*) FROM inst_institutions")
    inst_tracked = _scalar(
        conn,
        "SELECT COUNT(*) FROM inst_institutions WHERE enabled=1 AND blacklisted=0 AND merged_into IS NULL",
    )

    # 持仓层（匹配后的历史持仓全集）
    holdings_count = _scalar(conn, "SELECT COUNT(*) FROM inst_holdings")
    holdings_inst_count = _scalar(conn, "SELECT COUNT(DISTINCT institution_id) FROM inst_holdings")
    holdings_stock_count = _scalar(conn, "SELECT COUNT(DISTINCT stock_code) FROM inst_holdings")
    tracked_without_holdings = max(inst_tracked - holdings_inst_count, 0)

    # 事件层
    events_count = _scalar(conn, "SELECT COUNT(*) FROM fact_institution_event")
    events_stock_count = _scalar(conn, "SELECT COUNT(DISTINCT stock_code) FROM fact_institution_event")
    events_inst_count = _scalar(conn, "SELECT COUNT(DISTINCT institution_id) FROM fact_institution_event")

    # 收益层 — 审计口径按“可计算收益事件”统计，不把 exit 这类无公告锚点事件当缺口
    returns_eligible_total = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM fact_institution_event
        WHERE notice_date IS NOT NULL AND notice_date != ''
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
        """,
        (latest_market_date, latest_market_date),
    )
    returns_eligible_inst_count = _scalar(
        conn,
        """
        SELECT COUNT(DISTINCT institution_id)
        FROM fact_institution_event
        WHERE notice_date IS NOT NULL AND notice_date != ''
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
        """,
        (latest_market_date, latest_market_date),
    )
    returns_not_ready_future = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM fact_institution_event
        WHERE notice_date IS NOT NULL AND notice_date != ''
          AND (
            tradable_date IS NULL OR tradable_date = ''
            OR (? IS NOT NULL AND tradable_date > ?)
            OR (? IS NULL)
          )
        """,
        (latest_market_date, latest_market_date, latest_market_date),
    )
    returns_not_ready_path = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM fact_institution_event
        WHERE notice_date IS NOT NULL AND notice_date != ''
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
          AND price_entry IS NOT NULL AND price_entry > 0
          AND return_to_now IS NULL
        """,
        (latest_market_date, latest_market_date),
    )
    returns_missing_entry_price = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM fact_institution_event
        WHERE notice_date IS NOT NULL AND notice_date != ''
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
          AND (price_entry IS NULL OR price_entry <= 0)
          AND return_to_now IS NULL
        """,
        (latest_market_date, latest_market_date),
    )
    # 先计算停牌事件计数（供后续使用）
    try:
        suspended_waiting_events = _scalar(
            conn,
            "SELECT COUNT(*) FROM fact_institution_event WHERE price_entry_status = 'suspended_waiting'"
        )
    except Exception:
        suspended_waiting_events = 0

    returns_mature_total = max(returns_eligible_total - returns_not_ready_path - suspended_waiting_events, 0)
    returns_ineligible = max(events_count - returns_eligible_total - returns_not_ready_future, 0)
    returns_count = _scalar(
        conn,
        """
        SELECT COUNT(*)
        FROM fact_institution_event
        WHERE return_to_now IS NOT NULL
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
        """,
        (latest_market_date, latest_market_date),
    )
    returns_mature_inst_count = _scalar(
        conn,
        """
        SELECT COUNT(DISTINCT institution_id)
        FROM fact_institution_event
        WHERE notice_date IS NOT NULL AND notice_date != ''
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
          AND NOT (price_entry IS NOT NULL AND price_entry > 0 AND return_to_now IS NULL)
        """,
        (latest_market_date, latest_market_date),
    )
    returns_inst_count = _scalar(
        conn,
        """
        SELECT COUNT(DISTINCT institution_id)
        FROM fact_institution_event
        WHERE return_to_now IS NOT NULL
          AND tradable_date IS NOT NULL AND tradable_date != ''
          AND (? IS NOT NULL AND tradable_date <= ?)
        """,
        (latest_market_date, latest_market_date),
    )
    returns_actionable_missing = max(returns_mature_total - returns_count, 0)
    returns_other_missing = max(returns_actionable_missing - returns_missing_entry_price, 0)
    returns_not_ready = returns_not_ready_future + returns_not_ready_path
    returns_coverage = _pct(returns_count, returns_mature_total)

    # K线层 — 优先从 market_data.db 读取
    # 用 market_sync_state 代替 SELECT DISTINCT code FROM price_kline 节省 ~2.5s
    # （前者每只股票一行 ~6k 行，后者要扫数百万行 K 线）
    try:
        mkt_conn = get_market_conn()
        daily_codes = {
            r[0]
            for r in mkt_conn.execute(
                "SELECT code FROM market_sync_state "
                "WHERE dataset='price_kline' AND freq='daily' AND adjust='qfq' "
                "AND code IS NOT NULL"
            ).fetchall()
        }
        mkt_conn.close()
    except Exception:
        daily_codes = set()  # market_data.db 不可用

    holding_code_rows = conn.execute(
        "SELECT DISTINCT stock_code FROM inst_holdings "
        "WHERE stock_code IS NOT NULL "
        "AND stock_code NOT IN (SELECT stock_code FROM excluded_stocks)"
    ).fetchall()
    holding_codes = {r[0] for r in holding_code_rows}
    holding_stocks = len(holding_codes)
    kline_stocks = len(daily_codes)
    kline_covered = len(holding_codes & daily_codes)
    kline_missing = len(holding_codes - daily_codes)
    kline_coverage = _pct(kline_covered, holding_stocks)

    # K 线完整性：用停复牌接口区分停牌和真正缺失
    kline_stale_count = 0
    kline_suspended_count = 0
    kline_latest_trade = None
    try:
        latest_trade_row = conn.execute(
            "SELECT MAX(trade_date) AS d FROM dim_trading_calendar "
            "WHERE is_trading=1 AND trade_date <= ?",
            (datetime.now().strftime("%Y-%m-%d"),)
        ).fetchone()
        kline_latest_trade = latest_trade_row[0] if latest_trade_row else None
        if kline_latest_trade:
            mkt_conn2 = get_market_conn()
            stale_rows = mkt_conn2.execute(
                "SELECT code, max_date FROM market_sync_state "
                "WHERE dataset='price_kline' AND freq='daily' AND adjust='qfq' "
                "AND max_date < ?",
                (kline_latest_trade,)
            ).fetchall()
            mkt_conn2.close()
            excluded_codes = {r[0] for r in conn.execute("SELECT stock_code FROM excluded_stocks").fetchall()}
            active_codes = {r[0] for r in conn.execute("SELECT stock_code FROM dim_active_a_stock").fetchall()}
            # 查停牌列表（带 30 分钟 TTL 缓存，避免每次审计都打 akshare 网络）
            suspended_codes = _get_suspended_codes(kline_latest_trade)
            for r in stale_rows:
                code = r["code"]
                if code in excluded_codes or code not in active_codes:
                    continue
                if code in suspended_codes:
                    kline_suspended_count += 1
                else:
                    kline_stale_count += 1
    except Exception:
        pass

    # 行业层（股票维度：是否给匹配持仓股补齐三级行业）
    industry_dim_count = count_industry_rows(conn)
    industry_row = summarize_industry_coverage(
        conn,
        "SELECT DISTINCT stock_code FROM inst_holdings WHERE stock_code IS NOT NULL",
    )
    industry_level1 = industry_row["level1_codes"]
    industry_level2 = industry_row["level2_codes"]
    industry_level3 = industry_row["level3_codes"]
    industry_complete_codes = industry_row["complete_codes"]
    industry_missing_r = max(holding_stocks - industry_complete_codes, 0)
    industry_coverage = _pct(industry_complete_codes, holding_stocks)

    # 当前关系层基线（业务口径：每只股票取全市场最新报告期中的 tracked 机构）
    current_expected = conn.execute("""
        WITH latest AS (
            SELECT stock_code, MAX(report_date) AS max_rd
            FROM market_raw_holdings
            GROUP BY stock_code
        ),
        expected AS (
            SELECT h.institution_id, h.stock_code
            FROM inst_holdings h
            JOIN latest l ON h.stock_code = l.stock_code AND h.report_date = l.max_rd
            JOIN inst_institutions i ON i.id = h.institution_id
            WHERE i.enabled = 1 AND i.blacklisted = 0 AND i.merged_into IS NULL
        )
        SELECT
            COUNT(*) AS rows_cnt,
            COUNT(DISTINCT institution_id) AS inst_cnt,
            COUNT(DISTINCT stock_code) AS stock_cnt
        FROM expected
    """).fetchone()
    expected_current_rows = current_expected["rows_cnt"] or 0
    expected_current_inst = current_expected["inst_cnt"] or 0
    expected_current_stocks = current_expected["stock_cnt"] or 0

    # 当前关系层
    try:
        current_rel_count = _scalar(conn, "SELECT COUNT(*) FROM mart_current_relationship")
        current_rel_inst_count = _scalar(
            conn, "SELECT COUNT(DISTINCT institution_id) FROM mart_current_relationship"
        )
        current_rel_stock_count = _scalar(
            conn, "SELECT COUNT(DISTINCT stock_code) FROM mart_current_relationship"
        )
        current_rel_industry_stocks = _scalar(
            conn,
            "SELECT COUNT(DISTINCT stock_code) FROM mart_current_relationship WHERE has_industry_data = 1",
        )
    except Exception:
        current_rel_count = 0
        current_rel_inst_count = 0
        current_rel_stock_count = 0
        current_rel_industry_stocks = 0

    tracked_without_current = max(inst_tracked - expected_current_inst, 0)
    matched_without_current = max(holdings_inst_count - expected_current_inst, 0)

    # Mart 层
    profile_count = _scalar(conn, "SELECT COUNT(*) FROM mart_institution_profile")
    profile_scored = _scalar(
        conn, "SELECT COUNT(*) FROM mart_institution_profile WHERE quality_score IS NOT NULL"
    )

    industry_stat_row = conn.execute("""
        SELECT
            COUNT(*) AS row_cnt,
            COUNT(DISTINCT institution_id) AS inst_cnt,
            COUNT(DISTINCT CASE WHEN sw_level = 'level1' THEN institution_id END) AS level1_inst,
            COUNT(DISTINCT CASE WHEN sw_level = 'level2' THEN institution_id END) AS level2_inst,
            COUNT(DISTINCT CASE WHEN sw_level = 'level3' THEN institution_id END) AS level3_inst
        FROM mart_institution_industry_stat
    """).fetchone()
    industry_stat_count = industry_stat_row["row_cnt"] or 0
    industry_stat_inst_count = industry_stat_row["inst_cnt"] or 0
    industry_stat_level1_inst = industry_stat_row["level1_inst"] or 0
    industry_stat_level2_inst = industry_stat_row["level2_inst"] or 0
    industry_stat_level3_inst = industry_stat_row["level3_inst"] or 0
    industry_stat_complete_inst = _scalar(conn, """
        SELECT COUNT(*) FROM (
            SELECT institution_id
            FROM mart_institution_industry_stat
            GROUP BY institution_id
            HAVING SUM(CASE WHEN sw_level = 'level1' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN sw_level = 'level2' THEN 1 ELSE 0 END) > 0
               AND SUM(CASE WHEN sw_level = 'level3' THEN 1 ELSE 0 END) > 0
        )
    """)
    industry_stat_expected_inst = _scalar(
        conn,
        """
        SELECT COUNT(DISTINCT institution_id)
        FROM fact_institution_event
        WHERE gain_30d IS NOT NULL
          AND institution_id IN (
              SELECT id
              FROM inst_institutions
              WHERE enabled = 1 AND blacklisted = 0 AND merged_into IS NULL
          )
        """,
    )
    industry_stat_inactive_alias_inst = max(returns_inst_count - industry_stat_expected_inst, 0)

    trend_count = _scalar(conn, "SELECT COUNT(*) FROM mart_stock_trend")
    trend_scored = _scalar(
        conn, "SELECT COUNT(*) FROM mart_stock_trend WHERE composite_priority_score IS NOT NULL"
    )
    trend_without_current_rel = _scalar(conn, """
        SELECT COUNT(*) FROM mart_stock_trend t
        WHERE NOT EXISTS (
            SELECT 1 FROM mart_current_relationship m WHERE m.stock_code = t.stock_code
        )
    """)

    # 财务层
    try:
        from services.financial_client import FIN_HISTORY_TARGET_ROWS
    except Exception:
        FIN_HISTORY_TARGET_ROWS = 8
    try:
        fin_raw_count = _scalar(conn, "SELECT COUNT(*) FROM raw_gpcw_financial")
    except Exception:
        fin_raw_count = 0
    try:
        fin_derived_count = _scalar(conn, "SELECT COUNT(*) FROM fact_financial_derived")
        fin_latest_count = _scalar(conn, "SELECT COUNT(*) FROM dim_financial_latest")
        fin_history_ready = _scalar(conn, """
            SELECT COUNT(*) FROM (
                SELECT stock_code
                FROM raw_gpcw_financial
                GROUP BY stock_code
                HAVING COUNT(*) >= ?
            )
        """, (FIN_HISTORY_TARGET_ROWS,))
        fin_research_scope = _scalar(conn, "SELECT COUNT(*) FROM mart_stock_trend")
        if fin_research_scope > 0:
            fin_research_history_ready = _scalar(conn, """
                SELECT COUNT(*) FROM (
                    SELECT t.stock_code
                    FROM mart_stock_trend t
                    LEFT JOIN raw_gpcw_financial r ON r.stock_code = t.stock_code
                    GROUP BY t.stock_code
                    HAVING COUNT(r.report_date) >= ?
                )
            """, (FIN_HISTORY_TARGET_ROWS,))
        else:
            fin_research_history_ready = 0
    except Exception:
        fin_derived_count = 0
        fin_latest_count = 0
        fin_history_ready = 0
        fin_research_scope = 0
        fin_research_history_ready = 0
    fin_history_gap = max(fin_latest_count - fin_history_ready, 0)
    fin_research_history_gap = max(fin_research_scope - fin_research_history_ready, 0)
    try:
        capital_latest_count = _scalar(conn, "SELECT COUNT(*) FROM dim_capital_behavior_latest")
        capital_detail_synced_count = _scalar(conn, """
            SELECT COUNT(*) FROM capital_detail_sync_state
            WHERE status IN ('ok', 'partial', 'empty')
        """)
        if fin_research_scope > 0:
            capital_detail_research_ready = _scalar(conn, """
                SELECT COUNT(*) FROM (
                    SELECT t.stock_code
                    FROM mart_stock_trend t
                    LEFT JOIN capital_detail_sync_state s ON s.stock_code = t.stock_code
                    WHERE COALESCE(s.status, '') IN ('ok', 'partial', 'empty')
                )
            """)
        else:
            capital_detail_research_ready = 0
    except Exception:
        capital_latest_count = 0
        capital_detail_synced_count = 0
        capital_detail_research_ready = 0
    capital_detail_research_gap = max(fin_research_scope - capital_detail_research_ready, 0)
    try:
        indicator_latest_count = _scalar(conn, "SELECT COUNT(*) FROM dim_financial_indicator_latest")
        indicator_history_ready = _scalar(conn, """
            SELECT COUNT(*) FROM (
                SELECT stock_code
                FROM fact_financial_indicator_ak
                GROUP BY stock_code
                HAVING COUNT(*) >= ?
            )
        """, (FIN_HISTORY_TARGET_ROWS,))
        if fin_research_scope > 0:
            indicator_research_ready = _scalar(conn, """
                SELECT COUNT(*) FROM (
                    SELECT t.stock_code
                    FROM mart_stock_trend t
                    LEFT JOIN fact_financial_indicator_ak f ON f.stock_code = t.stock_code
                    GROUP BY t.stock_code
                    HAVING COUNT(f.report_date) >= ?
                )
            """, (FIN_HISTORY_TARGET_ROWS,))
        else:
            indicator_research_ready = 0
    except Exception:
        indicator_latest_count = 0
        indicator_history_ready = 0
        indicator_research_ready = 0
    indicator_research_gap = max(fin_research_scope - indicator_research_ready, 0)
    try:
        quality_feature_latest_count = _scalar(conn, "SELECT COUNT(*) FROM dim_stock_quality_latest")
    except Exception:
        quality_feature_latest_count = 0
    try:
        archetype_latest_count = _scalar(conn, "SELECT COUNT(*) FROM dim_stock_archetype_latest")
    except Exception:
        archetype_latest_count = 0

    # 选股层
    try:
        screen_count = _scalar(conn, "SELECT COUNT(*) FROM mart_stock_screening")
        screen_hits = _scalar(conn, "SELECT COUNT(*) FROM mart_stock_screening WHERE hit_count > 0")
        screen_date_row = conn.execute(
            "SELECT screen_date FROM mart_stock_screening ORDER BY screen_date DESC LIMIT 1"
        ).fetchone()
        screen_date = screen_date_row[0] if screen_date_row else None
    except Exception:
        screen_count = 0
        screen_hits = 0
        screen_date = None

    # 板块动量层
    try:
        sector_count = _scalar(conn, "SELECT COUNT(*) FROM mart_sector_momentum")
        dual_confirm_count = _scalar(conn, "SELECT COUNT(*) FROM mart_dual_confirm WHERE dual_confirm = 1")
        industry_context_count = _scalar(conn, "SELECT COUNT(*) FROM dim_stock_industry_context_latest")
        stage_feature_count = _scalar(conn, "SELECT COUNT(*) FROM dim_stock_stage_latest")
        forecast_feature_count = _scalar(conn, "SELECT COUNT(*) FROM dim_stock_forecast_latest")
    except Exception:
        sector_count = 0
        dual_confirm_count = 0
        industry_context_count = 0
        stage_feature_count = 0
        forecast_feature_count = 0

    # 收益层：停牌事件技术（已在上方提早计算完毕）

    # 健康分
    score = 100
    if raw_count == 0:
        score -= 30
    if holdings_count == 0:
        score -= 20
    if kline_missing > 100:
        score -= 15
    elif kline_missing > 0:
        score -= 5
    if returns_coverage < 50:
        score -= 15
    elif returns_coverage < 80:
        score -= 5
    if industry_missing_r > 100:
        score -= 10
    elif industry_missing_r > 0:
        score -= 3
    if fin_research_scope > 0 and fin_research_history_gap > max(20, fin_research_scope * 0.15):
        score -= 6
    elif fin_research_history_gap > 0:
        score -= 2
    if profile_count == 0:
        score -= 5

    payload = {
        "score": max(0, score),
        "baselines": {
            "tracked_institutions": inst_tracked,
            "raw_stock_universe": raw_stocks,
            "matched_holdings": {
                "rows": holdings_count,
                "institutions": holdings_inst_count,
                "stocks": holdings_stock_count,
            },
            "current_snapshot": {
                "rows": expected_current_rows,
                "institutions": expected_current_inst,
                "stocks": expected_current_stocks,
            },
            "return_ready": {
                "institutions": returns_mature_inst_count,
                "events": returns_mature_total,
                "eligible_events": returns_eligible_total,
            },
        },
        "layers": {
            "raw": {
                "count": raw_count,
                "stocks": raw_stocks,
                "latest_notice": raw_latest or "",
                "total_periods": raw_total_periods,
                "periods": [r[0] for r in raw_periods],
            },
            "institutions": {"total": inst_total, "tracked": inst_tracked},
            "holdings": {
                "count": holdings_count,
                "stocks": holding_stocks,
                "institutions": holdings_inst_count,
                "tracked_institutions": inst_tracked,
                "missing_institutions": tracked_without_holdings,
                "coverage_institutions": _pct(holdings_inst_count, inst_tracked),
            },
            "events": {
                "count": events_count,
                "stocks": events_stock_count,
                "institutions": events_inst_count,
                "expected_stocks": holding_stocks,
                "missing_stocks": max(holding_stocks - events_stock_count, 0),
                "expected_institutions": holdings_inst_count,
                "missing_institutions": max(holdings_inst_count - events_inst_count, 0),
            },
            "returns": {
                "count": returns_count,
                "total": returns_mature_total,
                "eligible_total": returns_eligible_total,
                "all_events": events_count,
                "ineligible_events": returns_ineligible,
                "not_ready_events": returns_not_ready,
                "not_ready_future_events": returns_not_ready_future,
                "not_ready_path_events": returns_not_ready_path,
                "missing_entry_price_events": returns_missing_entry_price,
                "actionable_missing_events": returns_actionable_missing,
                "other_missing_events": returns_other_missing,
                "suspended_waiting_events": suspended_waiting_events,
                "coverage": returns_coverage,
                "institutions": returns_inst_count,
                "expected_institutions": returns_mature_inst_count,
                "missing_institutions": max(returns_mature_inst_count - returns_inst_count, 0),
            },
            "kline": {
                "stocks": kline_stocks,
                "covered_stocks": kline_covered,
                "expected_stocks": holding_stocks,
                "tracked": holding_stocks,
                "missing": kline_missing,
                "coverage": kline_coverage,
                "stale_stocks": kline_stale_count,
                "suspended_stocks": kline_suspended_count,
                "delisted_stocks": kline_stale_count - kline_suspended_count,
                "latest_trade_date": kline_latest_trade,
            },
            "industry": {
                "count": industry_dim_count,
                "covered_stocks": industry_complete_codes,
                "expected_stocks": holding_stocks,
                "tracked": holding_stocks,
                "missing": industry_missing_r,
                "coverage": industry_coverage,
                "level1_stocks": industry_level1,
                "level2_stocks": industry_level2,
                "level3_stocks": industry_level3,
                "complete_three_level_stocks": industry_complete_codes,
            },
            "profiles": {
                "count": profile_count,
                "scored": profile_scored,
                "expected_institutions": inst_tracked,
                "current_institutions": expected_current_inst,
                "tracked_without_current": tracked_without_current,
            },
            "industry_stat": {
                "count": industry_stat_count,
                "institutions": industry_stat_inst_count,
                "expected_institutions": industry_stat_expected_inst,
                "missing_institutions": max(industry_stat_expected_inst - industry_stat_inst_count, 0),
                "level1_institutions": industry_stat_level1_inst,
                "level2_institutions": industry_stat_level2_inst,
                "level3_institutions": industry_stat_level3_inst,
                "complete_three_level_institutions": industry_stat_complete_inst,
                "tracked_without_holdings": tracked_without_holdings,
                "matched_without_returns": max(events_inst_count - returns_inst_count, 0),
                "inactive_or_merged_institutions": industry_stat_inactive_alias_inst,
            },
            "trends": {
                "count": trend_count,
                "scored": trend_scored,
                "expected_stocks": expected_current_stocks,
                "missing_stocks": max(expected_current_stocks - trend_count, 0),
                "extra_stocks": max(trend_count - expected_current_stocks, 0),
                "without_current_relationship": trend_without_current_rel,
            },
            "financial": {
                "raw_count": fin_raw_count,
                "derived_count": fin_derived_count,
                "latest_count": fin_latest_count,
                "history_target_rows": FIN_HISTORY_TARGET_ROWS,
                "history_ready_stocks": fin_history_ready,
                "history_gap_stocks": fin_history_gap,
                "research_scope": fin_research_scope,
                "research_history_ready": fin_research_history_ready,
                "research_history_gap": fin_research_history_gap,
                "capital_latest_count": capital_latest_count,
                "capital_detail_synced_count": capital_detail_synced_count,
                "capital_detail_research_ready": capital_detail_research_ready,
                "capital_detail_research_gap": capital_detail_research_gap,
                "indicator_latest_count": indicator_latest_count,
                "indicator_history_ready": indicator_history_ready,
                "indicator_research_ready": indicator_research_ready,
                "indicator_research_gap": indicator_research_gap,
                "quality_feature_latest_count": quality_feature_latest_count,
                "archetype_latest_count": archetype_latest_count,
                "expected_stocks": holding_stocks,
            },
            "screening": {
                "count": screen_count,
                "hits": screen_hits,
                "screen_date": screen_date,
                "expected_stocks": holding_stocks,
            },
            "sector_momentum": {
                "count": sector_count,
                "dual_confirm_count": dual_confirm_count,
                "industry_context_count": industry_context_count,
                "stage_feature_count": stage_feature_count,
                "forecast_feature_count": forecast_feature_count,
            },
            "current_relationship": {
                "count": current_rel_count,
                "institutions": current_rel_inst_count,
                "stocks": current_rel_stock_count,
                "expected_count": expected_current_rows,
                "expected_institutions": expected_current_inst,
                "expected_stocks": expected_current_stocks,
                "row_gap": current_rel_count - expected_current_rows,
                "institution_gap": current_rel_inst_count - expected_current_inst,
                "stock_gap": current_rel_stock_count - expected_current_stocks,
                "industry_stocks": current_rel_industry_stocks,
                "industry_missing_stocks": max(current_rel_stock_count - current_rel_industry_stocks, 0),
                "tracked_without_current": tracked_without_current,
                "matched_without_current": matched_without_current,
            },
        }
    }
    _AUDIT_CACHE["payload"] = payload
    _AUDIT_CACHE["ts"] = time.time()
    return payload


def build_smart_plan(conn, force_all=False, *, audit: Optional[dict] = None, use_cache: bool = True) -> dict:
    """根据数据状态智能生成更新计划

    返回：
    - steps: 需要执行的步骤 ID 列表
    - reason: 执行原因列表
    - skip_reasons: {step_id: "具体跳过原因"} — 用于前端展示
    """
    # 所有可能的步骤 ID
    ALL_STEPS = [
        "sync_raw", "match_inst", "sync_market_data",
        "gen_events", "calc_returns", "sync_industry",
        "sync_financial", "calc_financial_derived",
        "build_current_rel", "build_profiles", "build_industry_stat", "build_trends",
        "calc_screening", "calc_sector_momentum", "build_stage_features", "build_forecast_features",
        "calc_inst_scores", "calc_stock_scores",
    ]

    plan = {"steps": [], "reason": [], "skip_reasons": {}}

    if force_all:
        plan["steps"] = list(ALL_STEPS)
        plan["reason"].append("强制全量更新")
        return plan

    # 进程级缓存：8 秒内复用上一次 plan，避免工作台 1s 轮询打爆 audit
    if use_cache:
        cached_plan = _PLAN_CACHE.get("payload")
        if (
            cached_plan
            and _PLAN_CACHE.get("force") == force_all
            and (time.time() - _PLAN_CACHE.get("ts", 0)) < _AUDIT_TTL_SECONDS
        ):
            return cached_plan

    # 运行质量审计（这里允许复用 audit 缓存——run_quality_audit 自带 TTL）
    if audit is None:
        audit = load_quality_audit_snapshot(conn) or run_quality_audit(conn, use_cache=use_cache)

    # 1. 原始数据是否过期（> 1 天无新数据）
    raw_latest = audit["layers"]["raw"].get("latest_notice", "")
    if raw_latest:
        try:
            latest_dt = datetime.strptime(raw_latest[:8], "%Y%m%d")
            if (datetime.now() - latest_dt).days > 1:
                plan["steps"].append("sync_raw")
                plan["reason"].append(f"原始数据最后更新: {raw_latest}，已过期")
            else:
                plan["skip_reasons"]["sync_raw"] = f"原始数据已是最新（{raw_latest}）"
        except (ValueError, TypeError):
            plan["steps"].append("sync_raw")
            plan["reason"].append("无法解析原始数据日期")
    else:
        plan["steps"].append("sync_raw")
        plan["reason"].append("无原始数据")

    # 2. 持仓是否需要重新匹配
    if audit["layers"]["holdings"]["count"] == 0 and audit["layers"]["institutions"]["tracked"] > 0:
        plan["steps"].append("match_inst")
        plan["reason"].append("无匹配持仓")
    elif "sync_raw" in plan["steps"]:
        plan["steps"].append("match_inst")
        plan["reason"].append("原始数据更新后需重新匹配")
    else:
        plan["skip_reasons"]["match_inst"] = "无新增原始数据需要匹配"

    # 3. 行情是否缺失或过期
    # 真正的判断：有多少股票的 max_date 落后于最新交易日
    missing_kline = audit["layers"]["kline"].get("missing", 0)
    # 用审计已算好的 stale 数（已排除退市/ST）
    stale_stock_count = audit["layers"]["kline"].get("stale_stocks", 0)
    if missing_kline > 0:
        plan["steps"].append("sync_market_data")
        plan["reason"].append(f"{missing_kline} 只股票缺K线")
    elif stale_stock_count > 0:
        plan["steps"].append("sync_market_data")
        plan["reason"].append(f"{stale_stock_count} 只股票日K未覆盖最新交易日")
    elif "match_inst" in plan["steps"]:
        plan["steps"].append("sync_market_data")
        plan["reason"].append("持仓变更后补齐K线")
    else:
        plan["skip_reasons"]["sync_market_data"] = "K线已完整且已覆盖最新交易日"

    # 4. 事件是否需要重算
    if audit["layers"]["events"]["count"] == 0 and audit["layers"]["holdings"]["count"] > 0:
        plan["steps"].append("gen_events")
        plan["reason"].append("无事件数据")
    elif "match_inst" in plan["steps"]:
        plan["steps"].append("gen_events")
        plan["reason"].append("持仓变更后需重新生成事件")
    else:
        plan["skip_reasons"]["gen_events"] = "无新增持仓数据需要生成事件"

    # 5. 收益是否需要重算
    returns_total = audit["layers"]["returns"].get("total", 0)
    if audit["layers"]["returns"]["count"] == 0 and returns_total > 0:
        plan["steps"].append("calc_returns")
        plan["reason"].append("无收益数据")
    elif "gen_events" in plan["steps"] or "sync_market_data" in plan["steps"]:
        plan["steps"].append("calc_returns")
        plan["reason"].append("事件或K线变更后需重算收益")
    elif audit["layers"]["events"]["count"] > 0 and returns_total == 0:
        plan["skip_reasons"]["calc_returns"] = "当前事件无可计算收益的公告锚点"
    else:
        plan["skip_reasons"]["calc_returns"] = "事件和K线未变更"

    # 6. 行业是否缺失
    missing_industry = audit["layers"]["industry"].get("missing", 0)
    if missing_industry > 0:
        plan["steps"].append("sync_industry")
        plan["reason"].append(f"{missing_industry} 只股票缺行业分类")
    elif audit["layers"]["industry"]["count"] == 0:
        plan["steps"].append("sync_industry")
        plan["reason"].append("无行业数据")
    else:
        plan["skip_reasons"]["sync_industry"] = "行业数据已完整"

    # 6b. 财务数据是否需要同步
    try:
        from services.financial_client import FIN_HISTORY_TARGET_ROWS
    except Exception:
        FIN_HISTORY_TARGET_ROWS = 8
    try:
        fin_count = conn.execute("SELECT COUNT(*) FROM dim_financial_latest").fetchone()[0]
        quality_feature_count = conn.execute("SELECT COUNT(*) FROM dim_stock_quality_latest").fetchone()[0]
        fin_history_gap = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT t.stock_code
                FROM mart_stock_trend t
                LEFT JOIN raw_gpcw_financial r ON r.stock_code = t.stock_code
                GROUP BY t.stock_code
                HAVING COUNT(r.report_date) < ?
            )
        """, (FIN_HISTORY_TARGET_ROWS,)).fetchone()[0]
        indicator_history_gap = conn.execute("""
            SELECT COUNT(*) FROM (
                SELECT t.stock_code
                FROM mart_stock_trend t
                LEFT JOIN fact_financial_indicator_ak f ON f.stock_code = t.stock_code
                GROUP BY t.stock_code
                HAVING COUNT(f.report_date) < ?
            )
        """, (FIN_HISTORY_TARGET_ROWS,)).fetchone()[0]
    except Exception:
        fin_count = 0
        quality_feature_count = 0
        fin_history_gap = 0
        indicator_history_gap = 0
    if fin_count == 0:
        plan["steps"].append("sync_financial")
        plan["reason"].append("无财务数据")
    elif quality_feature_count == 0:
        plan["steps"].append("sync_financial")
        plan["reason"].append("无质量特征中间层")
    elif fin_history_gap > 0:
        plan["steps"].append("sync_financial")
        plan["reason"].append(f"{fin_history_gap} 只研究股票财务历史不足 {FIN_HISTORY_TARGET_ROWS} 期")
    elif indicator_history_gap > 0:
        plan["steps"].append("sync_financial")
        plan["reason"].append(f"{indicator_history_gap} 只研究股票扩展财务指标不足 {FIN_HISTORY_TARGET_ROWS} 期")
    elif "sync_market_data" in plan["steps"]:
        plan["steps"].append("sync_financial")
        plan["reason"].append("行情更新后同步财务数据")
    else:
        plan["skip_reasons"]["sync_financial"] = "财务数据已存在且上游未变更"

    # 6c. 财务派生的计算
    if "sync_financial" in plan["steps"]:
        plan["steps"].append("calc_financial_derived")
        plan["reason"].append("财务数据更新后重算派生指标")
    else:
        plan["skip_reasons"]["calc_financial_derived"] = "财务数据未变更"

    # 7. 当前关系层
    if any(s in plan["steps"] for s in ["gen_events", "calc_returns", "sync_industry", "match_inst"]):
        plan["steps"].append("build_current_rel")
        plan["reason"].append("上游变更后重建当前关系")
    elif audit["layers"]["current_relationship"]["count"] == 0:
        plan["steps"].append("build_current_rel")
        plan["reason"].append("当前关系表为空，需构建")
    else:
        plan["skip_reasons"]["build_current_rel"] = "上游未变更，当前关系已是最新"

    # 8. Mart 层
    if any(s in plan["steps"] for s in ["build_current_rel", "calc_returns", "match_inst"]):
        plan["steps"].append("build_profiles")
        plan["reason"].append("上游变更后重算机构画像")
    else:
        plan["skip_reasons"]["build_profiles"] = "上游未变更，无需重算"

    if any(s in plan["steps"] for s in ["build_current_rel", "calc_returns", "sync_industry"]):
        plan["steps"].append("build_industry_stat")
        plan["reason"].append("上游变更后重算行业统计")
    else:
        plan["skip_reasons"]["build_industry_stat"] = "上游未变更，无需重算"

    if any(s in plan["steps"] for s in ["build_current_rel", "gen_events"]):
        plan["steps"].append("build_trends")
        plan["reason"].append("上游变更后重算股票趋势")
    else:
        plan["skip_reasons"]["build_trends"] = "上游未变更，无需重算"

    # 9. 阶段特征层
    if any(s in plan["steps"] for s in ["build_trends", "calc_sector_momentum", "calc_financial_derived"]):
        plan["steps"].append("build_stage_features")
        plan["reason"].append("趋势、行业或财务变更后重算阶段特征")
    else:
        try:
            stage_row = conn.execute("SELECT COUNT(*) FROM dim_stock_stage_latest").fetchone()
            if not stage_row or stage_row[0] == 0:
                plan["steps"].append("build_stage_features")
                plan["reason"].append("无阶段特征中间层")
            else:
                plan["skip_reasons"]["build_stage_features"] = "上游未变更，阶段特征已是最新"
        except Exception:
            plan["skip_reasons"]["build_stage_features"] = "阶段特征表不存在"

    # 10. 预测特征层
    try:
        trained_model_row = conn.execute(
            "SELECT model_id FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        forecast_model_row = conn.execute(
            "SELECT model_id FROM dim_stock_forecast_latest LIMIT 1"
        ).fetchone()
        trained_model_id = trained_model_row[0] if trained_model_row else None
        forecast_model_id = forecast_model_row[0] if forecast_model_row else None
    except Exception:
        trained_model_id = None
        forecast_model_id = None
    if any(s in plan["steps"] for s in ["build_stage_features"]):
        plan["steps"].append("build_forecast_features")
        plan["reason"].append("阶段特征变更后重算预测特征")
    elif trained_model_id and trained_model_id != forecast_model_id:
        plan["steps"].append("build_forecast_features")
        plan["reason"].append("Qlib 最新模型尚未回流预测特征层")
    elif trained_model_id and not forecast_model_id:
        plan["steps"].append("build_forecast_features")
        plan["reason"].append("无预测特征中间层")
    elif not trained_model_id:
        plan["skip_reasons"]["build_forecast_features"] = "无已训练 Qlib 模型"
    else:
        plan["skip_reasons"]["build_forecast_features"] = "预测特征已是最新"

    # 11. 机构评分
    if any(s in plan["steps"] for s in ["build_profiles", "build_industry_stat"]):
        plan["steps"].append("calc_inst_scores")
        plan["reason"].append("画像或行业统计变更后重算机构评分")
    else:
        plan["skip_reasons"]["calc_inst_scores"] = "上游未变更，无需重算"

    # 12. 股票评分
    if any(s in plan["steps"] for s in ["calc_inst_scores", "build_stage_features", "build_forecast_features"]):
        plan["steps"].append("calc_stock_scores")
        plan["reason"].append("机构评分、阶段特征或预测特征变更后重算股票评分")
    else:
        plan["skip_reasons"]["calc_stock_scores"] = "上游未变更，无需重算"

    # 13. 选股筛选
    if any(s in plan["steps"] for s in ["sync_market_data", "calc_financial_derived"]):
        plan["steps"].append("calc_screening")
        plan["reason"].append("行情或财务变更后重新筛选")
    else:
        try:
            screen_row = conn.execute(
                "SELECT screen_date FROM mart_stock_screening LIMIT 1"
            ).fetchone()
            if not screen_row:
                plan["steps"].append("calc_screening")
                plan["reason"].append("无选股结果")
            else:
                plan["skip_reasons"]["calc_screening"] = "行情和财务未变更"
        except Exception:
            plan["skip_reasons"]["calc_screening"] = "选股表不存在"

    # 14. 板块动量
    if any(s in plan["steps"] for s in ["sync_market_data", "sync_industry"]):
        plan["steps"].append("calc_sector_momentum")
        plan["reason"].append("行情或行业变更后重算板块动量")
    else:
        try:
            sector_row = conn.execute(
                "SELECT COUNT(*) FROM mart_sector_momentum"
            ).fetchone()
            context_row = conn.execute(
                "SELECT COUNT(*) FROM dim_stock_industry_context_latest"
            ).fetchone()
            if not sector_row or sector_row[0] == 0:
                plan["steps"].append("calc_sector_momentum")
                plan["reason"].append("无板块动量数据")
            elif not context_row or context_row[0] == 0:
                plan["steps"].append("calc_sector_momentum")
                plan["reason"].append("无行业上下文中间层")
            else:
                plan["skip_reasons"]["calc_sector_momentum"] = "行情和行业未变更"
        except Exception:
            plan["skip_reasons"]["calc_sector_momentum"] = "动量表不存在"

    # 去重保持顺序
    seen = set()
    unique_steps = []
    for s in plan["steps"]:
        if s not in seen:
            seen.add(s)
            unique_steps.append(s)
    plan["steps"] = unique_steps

    plan["audit"] = audit
    if use_cache:
        _PLAN_CACHE["payload"] = plan
        _PLAN_CACHE["ts"] = time.time()
        _PLAN_CACHE["force"] = force_all
    return plan

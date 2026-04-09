"""
sector_momentum.py — 板块动量模块

计算申万行业板块的技术状态（MACD/均线/趋势），
与机构事件叠加产生"双重确认"信号。

核心逻辑：
  机构行为(new_entry/increase) + 板块技术面启动(MACD金叉/底部反转)
  = 双重确认信号（含金量高于单维度信号）

数据来源：
  - 板块指数 K 线：AKShare 申万行业指数日线
  - 机构事件：fact_institution_event
  - 行业映射：dim_stock_industry (sw_level1/sw_level2)

计算结果存入 mart_sector_momentum 表，被 scoring.py / screening_engine.py 读取。
单点计算、多处复用。
"""

import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from services.ta_lib import ma, ema, macd, cross, hhv, llv, barslast

logger = logging.getLogger("cm-api")


# ============================================================
# Schema
# ============================================================

def ensure_tables(conn):
    """创建板块动量表"""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS mart_sector_momentum (
            sector_name     TEXT PRIMARY KEY,
            sector_code     TEXT,
            sector_level    TEXT DEFAULT 'L1',
            calc_date       TEXT,
            close           REAL,
            ma20            REAL,
            ma60            REAL,
            macd_dif        REAL,
            macd_dea        REAL,
            macd_hist       REAL,
            macd_cross      INTEGER DEFAULT 0,
            macd_cross_days INTEGER,
            trend_state     TEXT,
            price_vs_ma20   REAL,
            price_vs_ma60   REAL,
            pullback_from_high REAL,
            rally_from_low  REAL,
            return_1m       REAL,
            return_3m       REAL,
            return_6m       REAL,
            return_12m      REAL,
            excess_1m       REAL,
            excess_3m       REAL,
            excess_6m       REAL,
            excess_12m      REAL,
            rotation_score  REAL,
            rotation_rank   INTEGER,
            rotation_rank_1m INTEGER,
            rotation_rank_3m INTEGER,
            rotation_bucket TEXT,
            rotation_blacklisted INTEGER DEFAULT 0,
            momentum_score  REAL,
            detail_json     TEXT,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_msm_score ON mart_sector_momentum(momentum_score);

        CREATE TABLE IF NOT EXISTS mart_dual_confirm (
            stock_code      TEXT NOT NULL,
            institution_id  TEXT NOT NULL,
            event_type      TEXT,
            report_date     TEXT,
            sector_name     TEXT,
            sector_momentum_score REAL,
            sector_trend_state TEXT,
            sector_macd_cross INTEGER,
            dual_confirm    INTEGER DEFAULT 0,
            confirm_detail  TEXT,
            updated_at      TEXT,
            PRIMARY KEY (stock_code, institution_id, report_date)
        );
        CREATE INDEX IF NOT EXISTS idx_mdc_dual ON mart_dual_confirm(dual_confirm);
        CREATE INDEX IF NOT EXISTS idx_mdc_stock ON mart_dual_confirm(stock_code);
    """)
    for col in [
        "return_1m REAL", "return_3m REAL", "return_6m REAL", "return_12m REAL",
        "excess_1m REAL", "excess_3m REAL", "excess_6m REAL", "excess_12m REAL",
        "rotation_score REAL", "rotation_rank INTEGER", "rotation_rank_1m INTEGER",
        "rotation_rank_3m INTEGER", "rotation_bucket TEXT", "rotation_blacklisted INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(f"ALTER TABLE mart_sector_momentum ADD COLUMN {col}")
        except Exception:
            pass
    conn.commit()


# ============================================================
# 板块技术状态计算
# ============================================================

def _calc_sector_state(df: pd.DataFrame) -> dict:
    """计算单个板块的技术状态。
    输入：按日期排序的 OHLCV DataFrame。
    """
    if len(df) < 60:
        return {"trend_state": "unknown", "momentum_score": 0}

    c = df["close"]
    h = df["high"]
    l_ = df["low"]

    # 均线
    ma20 = ma(c, 20)
    ma60 = ma(c, 60)

    # MACD
    dif, dea, hist = macd(c)

    # MACD 金叉
    macd_golden = cross(dif, dea)
    macd_cross_bl = barslast(macd_golden)

    # 趋势状态判定
    last = len(df) - 1
    last_close = float(c.iloc[last])
    last_ma20 = float(ma20.iloc[last]) if not pd.isna(ma20.iloc[last]) else last_close
    last_ma60 = float(ma60.iloc[last]) if not pd.isna(ma60.iloc[last]) else last_close
    last_dif = float(dif.iloc[last]) if not pd.isna(dif.iloc[last]) else 0
    last_dea = float(dea.iloc[last]) if not pd.isna(dea.iloc[last]) else 0
    last_hist = float(hist.iloc[last]) if not pd.isna(hist.iloc[last]) else 0
    last_macd_cross = bool(macd_golden.iloc[last]) if last < len(macd_golden) else False
    cross_days = int(macd_cross_bl.iloc[last]) if not pd.isna(macd_cross_bl.iloc[last]) else -1

    # 高低点
    _hhv60 = hhv(h, 60)
    _llv60 = llv(l_, 60)
    high_60 = float(_hhv60.iloc[last]) if not pd.isna(_hhv60.iloc[last]) else last_close
    low_60 = float(_llv60.iloc[last]) if not pd.isna(_llv60.iloc[last]) else last_close

    pullback = (high_60 - last_close) / high_60 if high_60 > 0 else 0
    rally = (last_close - low_60) / low_60 if low_60 > 0 else 0

    price_vs_ma20 = (last_close / last_ma20 - 1) if last_ma20 > 0 else 0
    price_vs_ma60 = (last_close / last_ma60 - 1) if last_ma60 > 0 else 0

    # 趋势判定
    if last_close > last_ma20 > last_ma60 and last_dif > last_dea:
        trend = "bullish"
    elif last_close < last_ma20 < last_ma60 and last_dif < last_dea:
        trend = "bearish"
    elif last_dif > last_dea and last_close > last_ma60:
        trend = "recovering"
    elif last_dif < last_dea and last_close < last_ma60:
        trend = "weakening"
    else:
        trend = "neutral"

    # 动量评分 (0-100)
    # MACD 方向（30分）+ 均线位置（30分）+ 趋势力度（20分）+ 金叉新鲜度（20分）
    score = 0

    # MACD 方向
    if last_dif > 0 and last_dif > last_dea:
        score += 30
    elif last_dif > last_dea:
        score += 20
    elif last_dif > 0:
        score += 10

    # 均线位置
    if last_close > last_ma20 > last_ma60:
        score += 30
    elif last_close > last_ma60:
        score += 20
    elif last_close > last_ma20:
        score += 10

    # 趋势力度（DIF 与 DEA 的距离）
    dif_spread = abs(last_dif - last_dea) / last_close * 100 if last_close > 0 else 0
    if dif_spread > 2:
        score += 20
    elif dif_spread > 1:
        score += 15
    elif dif_spread > 0.5:
        score += 10

    # 金叉新鲜度
    if 0 <= cross_days <= 3:
        score += 20
    elif 0 <= cross_days <= 10:
        score += 10

    return {
        "close": last_close,
        "ma20": last_ma20,
        "ma60": last_ma60,
        "macd_dif": round(last_dif, 4),
        "macd_dea": round(last_dea, 4),
        "macd_hist": round(last_hist, 4),
        "macd_cross": 1 if last_macd_cross else 0,
        "macd_cross_days": cross_days,
        "trend_state": trend,
        "price_vs_ma20": round(price_vs_ma20, 4),
        "price_vs_ma60": round(price_vs_ma60, 4),
        "pullback_from_high": round(pullback, 4),
        "rally_from_low": round(rally, 4),
        "momentum_score": min(score, 100),
    }


def _calc_window_return(series: pd.Series, window: int) -> float:
    if series is None or len(series) <= window:
        return 0.0
    last = float(series.iloc[-1])
    prev = float(series.iloc[-window - 1])
    if prev == 0:
        return 0.0
    return round((last / prev - 1) * 100, 2)


def _rank_sector_rotation(rows: list[dict]) -> list[dict]:
    if not rows:
        return []

    total = len(rows)
    edge_n = 3 if total >= 9 else 2 if total >= 5 else 1

    short_sorted = sorted(
        rows,
        key=lambda item: (
            -(item.get("excess_1m") or 0.0),
            -(item.get("momentum_score") or 0.0),
            item.get("sector_name") or "",
        ),
    )
    long_sorted = sorted(
        rows,
        key=lambda item: (
            -(item.get("excess_3m") or 0.0),
            -(item.get("momentum_score") or 0.0),
            item.get("sector_name") or "",
        ),
    )
    short_rank = {item["sector_name"]: idx + 1 for idx, item in enumerate(short_sorted)}
    long_rank = {item["sector_name"]: idx + 1 for idx, item in enumerate(long_sorted)}

    combined = []
    for item in rows:
        ex1 = float(item.get("excess_1m") or 0.0)
        ex3 = float(item.get("excess_3m") or 0.0)
        momentum = float(item.get("momentum_score") or 0.0)
        combined_score = ex1 * 0.55 + ex3 * 0.45 + (momentum - 50.0) * 0.06
        combined.append({
            **item,
            "_rotation_combined": combined_score,
        })

    combined_sorted = sorted(
        combined,
        key=lambda item: (
            -(item.get("_rotation_combined") or 0.0),
            short_rank.get(item["sector_name"], total),
            long_rank.get(item["sector_name"], total),
            item.get("sector_name") or "",
        ),
    )

    ranked = []
    for idx, item in enumerate(combined_sorted, start=1):
        if total <= 1:
            rotation_score = 50.0
        else:
            rotation_score = round(100.0 - ((idx - 1) / (total - 1)) * 100.0, 1)
        bucket = "neutral"
        if idx <= edge_n:
            bucket = "leader"
        elif idx > total - edge_n:
            bucket = "blacklist"
        ranked.append({
            **item,
            "rotation_rank": idx,
            "rotation_rank_1m": short_rank.get(item["sector_name"]),
            "rotation_rank_3m": long_rank.get(item["sector_name"]),
            "rotation_score": rotation_score,
            "rotation_bucket": bucket,
            "rotation_blacklisted": 1 if bucket == "blacklist" else 0,
        })
    return ranked


# ============================================================
# 主计算入口
# ============================================================

def calc_sector_momentum(smart_conn, mkt_conn) -> int:
    """计算所有板块的动量状态，写入 mart_sector_momentum。

    板块指数数据来源：用板块内成分股 K 线合成等权指数。
    如果有真正的板块指数 K 线（指数类型），优先使用。
    """
    ensure_tables(smart_conn)

    # 获取申万一级行业列表
    industries = smart_conn.execute(
        "SELECT DISTINCT sw_level1 FROM dim_stock_industry WHERE sw_level1 IS NOT NULL AND sw_level1 != ''"
    ).fetchall()

    if not industries:
        logger.info("[板块动量] 无行业分类数据")
        return 0

    # 获取行业-股票映射
    industry_stocks = {}
    for row in smart_conn.execute(
        "SELECT stock_code, sw_level1 FROM dim_stock_industry WHERE sw_level1 IS NOT NULL"
    ).fetchall():
        industry_stocks.setdefault(row["sw_level1"], []).append(row["stock_code"])

    # 全市场等权基线：作为行业强弱的相对参照
    benchmark_close = None
    all_codes = sorted({code for codes in industry_stocks.values() for code in codes})
    if all_codes:
        try:
            placeholders = ",".join("?" for _ in all_codes)
            benchmark_rows = mkt_conn.execute(
                f"SELECT code, date, close FROM price_kline "
                f"WHERE code IN ({placeholders}) AND freq='daily' AND adjust='qfq' "
                f"AND date >= date('now', '-420 day') ORDER BY date",
                all_codes
            ).fetchall()
            if benchmark_rows:
                bdf = pd.DataFrame([dict(r) for r in benchmark_rows])
                bpivot = bdf.pivot(index="date", columns="code", values="close")
                if len(bpivot) >= 60:
                    daily_ret = bpivot.pct_change(fill_method=None)
                    bench_ret = daily_ret.mean(axis=1).fillna(0)
                    benchmark_close = (1 + bench_ret).cumprod() * 1000
        except Exception as e:
            logger.warning(f"[板块动量] 市场等权基线计算失败: {e}")
            benchmark_close = None

    now = datetime.now().isoformat()
    calc_date = datetime.now().strftime("%Y-%m-%d")
    count = 0
    total_sectors = len(industries)
    sector_rotation_rows = []

    for sec_idx, ind_row in enumerate(industries):
        sector = ind_row["sw_level1"]
        codes = industry_stocks.get(sector, [])
        if len(codes) < 5:
            continue

        try:
            # 加载成分股 K 线，合成等权指数
            placeholders = ",".join("?" for _ in codes)
            kline_rows = mkt_conn.execute(
                f"SELECT code, date, close, high, low FROM price_kline "
                f"WHERE code IN ({placeholders}) AND freq='daily' AND adjust='qfq' "
                f"ORDER BY date",
                codes
            ).fetchall()

            if not kline_rows:
                continue

            kdf = pd.DataFrame([dict(r) for r in kline_rows])
            # 等权指数：用日收益率均值反推指数，避免高价股主导
            pivot = kdf.pivot(index="date", columns="code", values="close")
            if len(pivot) < 60:
                continue

            # 日收益率 → 等权平均收益率 → 指数值（基准1000点）
            daily_ret = pivot.pct_change(fill_method=None)
            idx_ret = daily_ret.mean(axis=1).fillna(0)
            idx_close = (1 + idx_ret).cumprod() * 1000

            pivot_h = kdf.pivot(index="date", columns="code", values="high")
            pivot_l = kdf.pivot(index="date", columns="code", values="low")
            idx_high_ret = pivot_h.pct_change(fill_method=None).mean(axis=1).fillna(0)
            idx_low_ret = pivot_l.pct_change(fill_method=None).mean(axis=1).fillna(0)
            idx_high = (1 + idx_high_ret).cumprod() * 1000
            idx_low = (1 + idx_low_ret).cumprod() * 1000

            # 对齐索引
            common_idx = idx_close.index.intersection(idx_high.index).intersection(idx_low.index)
            sector_close_series = idx_close.loc[common_idx].sort_index()
            sector_high_series = idx_high.loc[common_idx].sort_index()
            sector_low_series = idx_low.loc[common_idx].sort_index()
            sector_df = pd.DataFrame({
                "close": sector_close_series,
                "high": sector_high_series,
                "low": sector_low_series,
            }).reset_index(drop=True)

            if len(sector_df) < 60:
                continue

            state = _calc_sector_state(sector_df)
            return_1m = _calc_window_return(sector_df["close"], 20)
            return_3m = _calc_window_return(sector_df["close"], 60)
            return_6m = _calc_window_return(sector_df["close"], 120)
            return_12m = _calc_window_return(sector_df["close"], 240)
            excess_1m = excess_3m = excess_6m = excess_12m = 0.0
            if benchmark_close is not None:
                aligned_bench = benchmark_close.reindex(sector_close_series.index).dropna()
                if len(aligned_bench) >= 60:
                    bench_1m = _calc_window_return(aligned_bench, 20)
                    bench_3m = _calc_window_return(aligned_bench, 60)
                    bench_6m = _calc_window_return(aligned_bench, 120)
                    bench_12m = _calc_window_return(aligned_bench, 240)
                    excess_1m = round(return_1m - bench_1m, 2)
                    excess_3m = round(return_3m - bench_3m, 2)
                    excess_6m = round(return_6m - bench_6m, 2)
                    excess_12m = round(return_12m - bench_12m, 2)

            state.update({
                "return_1m": return_1m,
                "return_3m": return_3m,
                "return_6m": return_6m,
                "return_12m": return_12m,
                "excess_1m": excess_1m,
                "excess_3m": excess_3m,
                "excess_6m": excess_6m,
                "excess_12m": excess_12m,
            })
            sector_rotation_rows.append({
                "sector_name": sector,
                "excess_1m": excess_1m,
                "excess_3m": excess_3m,
                "momentum_score": state.get("momentum_score"),
            })

            smart_conn.execute("""
                INSERT OR REPLACE INTO mart_sector_momentum
                (sector_name, sector_level, calc_date, close, ma20, ma60,
                 macd_dif, macd_dea, macd_hist, macd_cross, macd_cross_days,
                 trend_state, price_vs_ma20, price_vs_ma60,
                 pullback_from_high, rally_from_low,
                 return_1m, return_3m, return_6m, return_12m,
                 excess_1m, excess_3m, excess_6m, excess_12m,
                 rotation_score, rotation_rank, rotation_rank_1m, rotation_rank_3m,
                 rotation_bucket, rotation_blacklisted, momentum_score, detail_json, updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                sector, "L1", calc_date, state["close"], state["ma20"], state["ma60"],
                state["macd_dif"], state["macd_dea"], state["macd_hist"],
                state["macd_cross"], state["macd_cross_days"],
                state["trend_state"], state["price_vs_ma20"], state["price_vs_ma60"],
                state["pullback_from_high"], state["rally_from_low"],
                state["return_1m"], state["return_3m"], state["return_6m"], state["return_12m"],
                state["excess_1m"], state["excess_3m"], state["excess_6m"], state["excess_12m"],
                None, None, None, None, None, 0, state["momentum_score"],
                json.dumps(state, ensure_ascii=False), now,
            ))
            count += 1
        except Exception as e:
            logger.warning(f"[板块动量] {sector} 计算失败: {e}")
            continue

    for item in _rank_sector_rotation(sector_rotation_rows):
        smart_conn.execute("""
            UPDATE mart_sector_momentum
            SET rotation_score = ?,
                rotation_rank = ?,
                rotation_rank_1m = ?,
                rotation_rank_3m = ?,
                rotation_bucket = ?,
                rotation_blacklisted = ?
            WHERE sector_name = ? AND calc_date = ?
        """, (
            item.get("rotation_score"),
            item.get("rotation_rank"),
            item.get("rotation_rank_1m"),
            item.get("rotation_rank_3m"),
            item.get("rotation_bucket"),
            item.get("rotation_blacklisted"),
            item.get("sector_name"),
            calc_date,
        ))

    smart_conn.commit()
    logger.info(f"[板块动量] 完成: {count}/{total_sectors} 个行业板块")
    return count


# ============================================================
# 双重确认信号
# ============================================================

def calc_dual_confirm(smart_conn) -> int:
    """为最近的机构事件叠加板块动量，产生双重确认信号。

    逻辑：
    - 机构 new_entry/increase 事件
    - 所属板块 momentum_score ≥ 60 或 trend_state in (bullish, recovering) 且 macd_cross_days ≤ 10
    = dual_confirm = 1（双重确认）
    """
    ensure_tables(smart_conn)

    # 获取板块动量
    sector_rows = smart_conn.execute(
        "SELECT sector_name, momentum_score, trend_state, macd_cross FROM mart_sector_momentum"
    ).fetchall()
    sector_map = {r["sector_name"]: dict(r) for r in sector_rows}

    if not sector_map:
        logger.info("[双重确认] 无板块动量数据")
        return 0

    # 获取最近的机构 new_entry/increase 事件
    events = smart_conn.execute("""
        SELECT e.stock_code, e.institution_id, e.event_type, e.report_date,
               si.sw_level1 as sector_name
        FROM fact_institution_event e
        LEFT JOIN dim_stock_industry si ON e.stock_code = si.stock_code
        WHERE e.event_type IN ('new_entry', 'increase')
          AND e.report_date >= date('now', '-6 months')
          AND si.sw_level1 IS NOT NULL
        ORDER BY e.report_date DESC
    """).fetchall()

    if not events:
        logger.info("[双重确认] 无符合条件的事件")
        return 0

    now = datetime.now().isoformat()
    count = 0

    for evt in events:
        sector = evt["sector_name"]
        sm = sector_map.get(sector)
        if not sm:
            continue

        score = sm.get("momentum_score", 0) or 0
        trend = sm.get("trend_state", "")
        mc = sm.get("macd_cross", 0)

        # 双重确认条件
        dual = 0
        reasons = []
        if score >= 60:
            dual = 1
            reasons.append(f"板块动量评分{score}")
        if trend in ("bullish", "recovering") and mc:
            dual = 1
            reasons.append(f"板块{trend}+MACD金叉")

        smart_conn.execute("""
            INSERT OR REPLACE INTO mart_dual_confirm
            (stock_code, institution_id, event_type, report_date,
             sector_name, sector_momentum_score, sector_trend_state,
             sector_macd_cross, dual_confirm, confirm_detail, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, (
            evt["stock_code"], evt["institution_id"], evt["event_type"],
            evt["report_date"], sector, score, trend, mc, dual,
            json.dumps({"reasons": reasons}, ensure_ascii=False) if reasons else None,
            now,
        ))
        count += 1

    smart_conn.commit()
    dual_count = smart_conn.execute(
        "SELECT COUNT(*) FROM mart_dual_confirm WHERE dual_confirm = 1"
    ).fetchone()[0]
    logger.info(f"[双重确认] 完成: {count} 条事件, {dual_count} 条双重确认")
    return count

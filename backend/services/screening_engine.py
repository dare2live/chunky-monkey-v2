"""
screening_engine.py — TDX 选股引擎

实现通达信选股公式 1/3/5，所有技术指标计算委托 ta_lib.py。
结果存入 mart_stock_screening 单张表，前端和 scoring.py 只读此表。

公式 1: MA5 长期低于 MA90 后突破 MA145，流通市值 ≥ 30 亿
公式 3: 多级信号迭代买卖点 + 均线多头排列 + 回撤
公式 5: 连跌后首日上涨 + MACD 金叉（DIFF ≥ 0）
"""

import json
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from services.ta_lib import (
    ma, ema, hhv, llv, cross, barslast, barslastcount, count,
    ref, rolling_sum, islastbar, macd, float_market_cap,
)

logger = logging.getLogger("cm-api")


# ============================================================
# Schema
# ============================================================

def ensure_tables(conn):
    """创建选股结果表"""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS mart_stock_screening (
            stock_code      TEXT PRIMARY KEY,
            stock_name      TEXT,
            screen_date     TEXT,
            f1_hit          INTEGER DEFAULT 0,
            f1_detail       TEXT,
            f3_hit          INTEGER DEFAULT 0,
            f3_detail       TEXT,
            f5_hit          INTEGER DEFAULT 0,
            f5_detail       TEXT,
            hit_count       INTEGER DEFAULT 0,
            float_market_cap REAL,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_mss_hit ON mart_stock_screening(hit_count);
        CREATE INDEX IF NOT EXISTS idx_mss_date ON mart_stock_screening(screen_date);
    """)
    conn.commit()


# ============================================================
# 公式 1: MA5 长期低于 MA90 → 突破 MA145
# ============================================================

def _formula_1(df: pd.DataFrame, flt_mcap: float) -> dict:
    """
    MA5 长期(≥45天)低于 MA90，然后突破 MA145，
    且近 10 日中 MA5 上升 ≥ 7 天，流通市值 ≥ 30 亿。
    """
    if len(df) < 150:
        return {"hit": False, "reason": "数据不足"}

    c = df["close"]
    ma5 = ma(c, 5)
    ma90 = ma(c, 90)
    ma145 = ma(c, 145)
    top = pd.concat([ma90, ma145], axis=1).max(axis=1)

    # TJ1: MA5 低于 MA90 连续 ≥ 45 天
    ls = barslast(ma5 >= ma90)
    tj1 = (ls >= 45) & (ma5 < ma90)

    # TJ2: 近 10 日中 MA5 上升 ≥ 7 天
    ma5_up = ma5 > ref(ma5, 1)
    tj2 = count(ma5_up, 10) >= 7

    # TJ3: 近 11 日内突破 MA145 且当前在 MA145 之上
    cross_145 = cross(c, ma145)
    b145 = barslast(cross_145)
    tj3 = (count(cross_145, 11) >= 1) & (b145 <= 10) & (c > ma145)

    # TJ4: 自突破以来 CLOSE 始终 > MA145
    # 向量化：检查突破后没有任何一天 c < ma145——即 barslastcount(c < ma145) == 0
    # 并且必须在突破后（b145 有效）
    below_since_cross = barslastcount(c < ma145)
    tj4 = (b145.notna()) & (b145 >= 0) & (below_since_cross == 0)

    # TJ5: 近 45 日内 CLOSE > MA145 的天数 = b145+1
    close_above_145 = (c > ma145).astype(int)
    cnt_above = count(close_above_145, 45)
    tj5 = cnt_above == (b145 + 1)

    # TJ6: CLOSE ≤ TOP*1.06 且 CLOSE ≤ MA145*1.10
    tj6 = (c <= top * 1.06) & (c <= ma145 * 1.10)

    # TJ7: 近 45 日内 MA90/MA145 无交叉
    cross_90_145 = cross(ma90, ma145)
    cross_145_90 = cross(ma145, ma90)
    any_cross = cross_90_145 | cross_145_90
    tj7 = count(any_cross.astype(int), 45) == 0

    # 流通市值 ≥ 30 亿
    float_mcap_ok = flt_mcap >= 3_000_000_000 if flt_mcap else False

    # 综合信号：取最后一根 K 线
    signal = tj1 & tj2 & tj3 & tj4 & tj5 & tj6 & tj7
    last = signal.iloc[-1] if len(signal) > 0 else False
    hit = bool(last) and float_mcap_ok

    detail = {}
    if len(df) > 0:
        idx = len(df) - 1
        ma5_rising_cnt = count(ma5_up, 10)
        detail = {
            "ma5_below_ma90_days": int(ls.iloc[idx]) if not pd.isna(ls.iloc[idx]) else 0,
            "ma5_rising_days_in_10": int(ma5_rising_cnt.iloc[idx]) if not pd.isna(ma5_rising_cnt.iloc[idx]) else 0,
            "days_since_cross_145": int(b145.iloc[idx]) if not pd.isna(b145.iloc[idx]) else -1,
            "float_market_cap": flt_mcap,
        }

    return {"hit": hit, "detail": detail}


# ============================================================
# 公式 3: 多级信号迭代 + 均线多头 + 回撤
# ============================================================

def _formula_3(df: pd.DataFrame) -> dict:
    """
    多级买卖信号迭代系统。核心逻辑：
    1. 构建 X_3 (多均线平均) 和 X_4 (加权价格)
    2. 通过 10 轮迭代产生最终买卖信号 (GSB/GSS)
    3. 要求：当前处于卖出信号后 ≤3 天（INSELL）
    4. 90 日内快速反弹比率 ≥ 40%
    5. 均线多头排列 + 回撤条件
    """
    if len(df) < 100:
        return {"hit": False, "reason": "数据不足"}

    c = df["close"]
    o = df["open"]
    h = df["high"]
    l_ = df["low"]

    # X_1 ~ X_3: 多均线系统
    x1 = (ma(c, 3) + ma(c, 7) + ma(c, 13) + ma(c, 27)) / 4
    x2 = ema(c, 5)
    x3 = x1.where(x1.notna(), x2)

    # X_4: 加权价格
    x4 = (h + l_ + 2 * o + 6 * c) / 10

    # 卖出形态 / 买入形态
    x5 = (
        (c < o)
        | ((c < ref(h, 1)) & (c > o))
        | ((c >= o) & (h - c >= c - o) & (c / ref(c, 1) < 1.02))
        | ((c == o) & (h - c >= c - l_) & (c / ref(c, 1) < 1.05))
    )
    x6 = (
        ((c > o) & (c / ref(c, 1) > 0.94))
        | ((c > ref(l_, 1)) & (c < o))
        | ((c <= o) & (c - l_ >= o - c) & (c / ref(c, 1) > 0.98))
        | ((c == o) & (c - l_ >= h - c) & (c / ref(c, 1) > 0.95))
    )

    # 10 轮迭代信号
    prev = x4.copy()
    for _ in range(10):
        cross_down = cross(prev, x3) & x5
        cross_up = cross(x3, prev) & x6
        nxt = prev.copy()
        nxt[cross_down] = x3[cross_down] * 0.98
        nxt[cross_up] = x3[cross_up] * 1.02
        prev = nxt

    x36 = prev

    # 买卖信号
    gsb = cross(x36, x3)  # 买入信号
    gss = cross(x3, x36)  # 卖出信号

    # INSELL: 处于卖出后 ≤ 3 天
    ls_sell = barslast(gss)
    lb_buy = barslast(gsb)
    insell = (ls_sell < lb_buy) & (ls_sell <= 3)

    # 90 日内快速反弹
    gsb_after_gss = gsb & (barslast(gss) > 0)
    fast_b_cond = gsb_after_gss & (barslast(gss) <= 3)
    fastb = count(fast_b_cond.astype(int), 90)
    totb = count(gsb_after_gss.astype(int), 90)
    rate = pd.Series(0.0, index=df.index)
    mask = totb > 0
    rate[mask] = fastb[mask] * 100 / totb[mask]
    histok = (totb >= 1) & (fastb >= 1) & (rate >= 40)

    # 卖出状态分析
    sell_state = barslast(gss) < barslast(gsb)
    sell_state_int = sell_state.astype(int)
    max_run = hhv(barslastcount(sell_state), 45)
    sell_pct = rolling_sum(sell_state_int, 45) * 100 / 45
    green_ok = (max_run <= 8) & (sell_pct <= 60)

    # 卖出质量
    bl_gss = barslast(gss)
    max_len_data = bl_gss.where(gsb & bl_gss.notna(), 0)
    maxlen = hhv(max_len_data, 90)
    longb_cond = gsb & (bl_gss > 10)
    longb = count(longb_cond.astype(int), 90)
    sell_qual = (maxlen <= 20) & (longb <= 2)

    # 均线多头排列
    m20 = ma(c, 20)
    m60 = ma(c, 60)
    m90 = ma(c, 90)
    up = (
        (m20 > m60) & (m60 > m90)
        & (m60 > ref(m60, 20)) & (m90 > ref(m90, 20))
        & (c > m90) & (c > m60 * 0.97)
    )

    # 回撤条件
    ret = c / hhv(h, 20)
    pull = (ret <= 0.995) & (ret >= 0.78)

    # 综合
    signal = insell & histok & sell_qual & green_ok & up & pull
    last_hit = bool(signal.iloc[-1]) if len(signal) > 0 else False

    detail = {}
    if len(df) > 0:
        idx = len(df) - 1
        detail = {
            "insell": bool(insell.iloc[idx]),
            "fast_bounce_rate": float(rate.iloc[idx]) if not pd.isna(rate.iloc[idx]) else 0,
            "ma_bullish": bool(up.iloc[idx]),
            "pullback_ratio": float(ret.iloc[idx]) if not pd.isna(ret.iloc[idx]) else 0,
        }

    return {"hit": last_hit, "detail": detail}


# ============================================================
# 公式 5: 连跌后首日上涨 + MACD 金叉
# ============================================================

def _formula_5(df: pd.DataFrame) -> dict:
    """
    GS十三-上涨1 + MACD 金叉 + DIFF >= 0
    条件：最后一根 K 线，首日上涨（之前连续下跌），MACD 金叉且 DIFF ≥ 0。
    """
    if len(df) < 30:
        return {"hit": False, "reason": "数据不足"}

    c = df["close"]

    # A1: 连续上涨天数 = 1（即今日刚从下跌转为上涨）
    a1 = c > ref(c, 4)
    nt = barslastcount(a1)

    # MACD
    dif, dea, _ = macd(c)

    # 金叉
    macd_cross = cross(dif, dea)

    # 综合：最后一根 + 首日上涨 + MACD 金叉 + DIFF >= 0
    is_last = islastbar(c)
    signal = is_last & a1 & (nt == 1) & macd_cross & (dif >= 0)

    last_hit = bool(signal.iloc[-1]) if len(signal) > 0 else False

    detail = {}
    if len(df) > 0:
        idx = len(df) - 1
        detail = {
            "up_days": int(nt.iloc[idx]) if not pd.isna(nt.iloc[idx]) else 0,
            "dif": float(dif.iloc[idx]) if not pd.isna(dif.iloc[idx]) else 0,
            "dea": float(dea.iloc[idx]) if not pd.isna(dea.iloc[idx]) else 0,
            "macd_cross": bool(macd_cross.iloc[idx]),
        }

    return {"hit": last_hit, "detail": detail}


# ============================================================
# 主入口：批量运行全部选股公式
# ============================================================

def run_all_screens(smart_conn, mkt_conn) -> int:
    """遍历所有股票，执行全部选股公式，写入 mart_stock_screening。
    技术指标只从 ta_lib 算一次，传入各公式。
    """
    ensure_tables(smart_conn)

    # 1. 获取活跃股票列表
    # 真相源：dim_active_a_stock（security_master 维护的当前可交易 A 股清单）
    # 旧 dim_stock 表已退役（曾全表为空导致 calc_screening 静默跳过）
    stock_rows = smart_conn.execute(
        "SELECT a.stock_code, a.stock_name "
        "FROM dim_active_a_stock a "
        "LEFT JOIN excluded_stocks e ON e.stock_code = a.stock_code "
        "WHERE e.stock_code IS NULL"
    ).fetchall()
    stock_map = {r["stock_code"]: r["stock_name"] for r in stock_rows}

    if not stock_map:
        logger.warning("[选股] dim_active_a_stock 为空，请先跑「数据获取 → 同步十大股东」让 security_master 拉取主数据")
        return 0

    # 2. 批量加载财务数据（流通股本）
    fin_map = {}
    try:
        fin_rows = smart_conn.execute(
            "SELECT stock_code, float_shares FROM dim_financial_latest"
        ).fetchall()
        fin_map = {r["stock_code"]: r["float_shares"] for r in fin_rows}
    except Exception as e:
        logger.warning(f"[选股] 加载财务数据失败（公式 1 流通市值将缺失）: {e}")

    # 3. 批量加载 K 线数据
    logger.info(f"[选股] 开始筛选 {len(stock_map)} 只股票")
    codes = list(stock_map.keys())
    placeholders = ",".join("?" for _ in codes)
    kline_rows = mkt_conn.execute(
        f"SELECT code, date, open, high, low, close, volume, amount "
        f"FROM price_kline "
        f"WHERE code IN ({placeholders}) AND freq='daily' AND adjust='qfq' "
        f"ORDER BY code, date",
        codes
    ).fetchall()

    if not kline_rows:
        logger.warning("[选股] 无 K 线数据")
        return 0

    kline_df = pd.DataFrame([dict(r) for r in kline_rows])
    now = datetime.now().isoformat()
    screen_date = datetime.now().strftime("%Y-%m-%d")
    results = []
    stock_groups = kline_df.groupby("code")
    total_stocks = len(stock_groups)

    # 4. 逐股运行公式
    for stock_idx, (code, group) in enumerate(stock_groups):
        # C-3: 进度日志
        if stock_idx > 0 and stock_idx % 500 == 0:
            logger.info(f"[选股] 进度: {stock_idx}/{total_stocks} ({stock_idx*100//total_stocks}%)")
        df = group.sort_values("date").reset_index(drop=True)

        if len(df) < 30:
            continue

        # 流通市值（NaN 防御：last_close 为 NaN 时归零，避免静默跳过 F1）
        float_shares = fin_map.get(code, 0) or 0
        last_close = df["close"].iloc[-1] if len(df) > 0 else 0
        if not np.isfinite(last_close):
            last_close = 0
        flt_mcap = float_shares * last_close if float_shares else 0

        # 运行三个公式
        r1 = _formula_1(df, flt_mcap)
        r3 = _formula_3(df)
        r5 = _formula_5(df)

        hit_count = sum([r1["hit"], r3["hit"], r5["hit"]])

        results.append((
            code, stock_map.get(code, ""), screen_date,
            1 if r1["hit"] else 0, json.dumps(r1.get("detail", {}), ensure_ascii=False),
            1 if r3["hit"] else 0, json.dumps(r3.get("detail", {}), ensure_ascii=False),
            1 if r5["hit"] else 0, json.dumps(r5.get("detail", {}), ensure_ascii=False),
            hit_count, flt_mcap, now,
        ))

    # 5. 写入结果
    smart_conn.execute("DELETE FROM mart_stock_screening")
    smart_conn.executemany("""
        INSERT INTO mart_stock_screening
        (stock_code, stock_name, screen_date, f1_hit, f1_detail, f3_hit, f3_detail,
         f5_hit, f5_detail, hit_count, float_market_cap, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
    """, results)
    smart_conn.commit()

    hit_total = sum(1 for r in results if r[9] > 0)  # hit_count > 0
    f1_count = sum(1 for r in results if r[3] == 1)
    f3_count = sum(1 for r in results if r[5] == 1)
    f5_count = sum(1 for r in results if r[7] == 1)

    logger.info(
        f"[选股] 完成: {len(results)} 只股票, "
        f"F1={f1_count} F3={f3_count} F5={f5_count}, 总命中={hit_total}"
    )
    return len(results)

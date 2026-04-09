"""
ta_lib.py — 共享技术指标计算库

纯计算模块，零数据库依赖。输入 pandas Series/DataFrame，输出 Series/DataFrame。
Qlib 因子引擎和 TDX 选股引擎共用此库，确保"单点计算、多处复用"。

通达信函数对照：
  MA → ma()          EMA → ema()        SMA → sma()
  HHV → hhv()        LLV → llv()        REF → ref()
  BARSLAST → barslast()                  BARSLASTCOUNT → barslastcount()
  CROSS → cross()    COUNT → count()     SUM → rolling_sum()
  STD → std()        MACD → macd()       BARSCOUNT → barscount()
  ISLASTBAR → islastbar()
"""

import numpy as np
import pandas as pd


# ============================================================
# 通达信基础函数
# ============================================================

def ma(series: pd.Series, n: int) -> pd.Series:
    """简单移动平均 MA(S, N)"""
    return series.rolling(n, min_periods=n).mean()


def ema(series: pd.Series, n: int) -> pd.Series:
    """指数移动平均 EMA(S, N)，adjust=False 匹配通达信原生算法"""
    return series.ewm(span=n, min_periods=n, adjust=False).mean()


def sma(series: pd.Series, n: int, m: int = 1) -> pd.Series:
    """通达信 SMA(S, N, M) = (M*S + (N-M)*REF(SMA,1)) / N"""
    src = series.values.astype(float)
    out = np.full(len(src), np.nan)
    if len(src) >= n:
        out[n - 1] = np.nanmean(src[:n])
        for i in range(n, len(src)):
            prev = out[i - 1]
            if np.isnan(prev):
                out[i] = np.nan
            else:
                out[i] = (m * src[i] + (n - m) * prev) / n
    return pd.Series(out, index=series.index)


def ref(series: pd.Series, n: int) -> pd.Series:
    """REF(S, N) — N 日前的值"""
    return series.shift(n)


def hhv(series: pd.Series, n: int) -> pd.Series:
    """HHV(S, N) — N 日内最高值"""
    return series.rolling(n, min_periods=n).max()


def llv(series: pd.Series, n: int) -> pd.Series:
    """LLV(S, N) — N 日内最低值"""
    return series.rolling(n, min_periods=n).min()


def barslast(condition: pd.Series) -> pd.Series:
    """BARSLAST(C) — 距条件最后一次成立的K线数"""
    arr = condition.values.astype(bool)
    out = np.full(len(arr), np.nan)
    counter = np.nan
    for i in range(len(arr)):
        if arr[i]:
            counter = 0.0
        elif not np.isnan(counter):
            counter += 1.0
        out[i] = counter
    return pd.Series(out, index=condition.index)


def barslastcount(condition: pd.Series) -> pd.Series:
    """BARSLASTCOUNT(C) — 从当前向前连续满足条件的天数"""
    arr = condition.values.astype(bool)
    out = np.zeros(len(arr), dtype=np.int64)
    for i in range(len(arr)):
        if arr[i]:
            out[i] = (out[i - 1] + 1) if i > 0 else 1
        # else: out[i] 已经是 0
    return pd.Series(out, index=condition.index)


def barscount(series: pd.Series) -> pd.Series:
    """BARSCOUNT(S) — 从第一个有效值到当前的K线数"""
    first_valid = series.first_valid_index()
    if first_valid is None:
        return pd.Series(0, index=series.index, dtype=int)
    loc = series.index.get_loc(first_valid)
    out = np.zeros(len(series), dtype=np.int64)
    out[loc:] = np.arange(len(series) - loc)
    return pd.Series(out, index=series.index)


def cross(a: pd.Series, b) -> pd.Series:
    """CROSS(A, B) — A 从下方穿越 B（上穿）"""
    if isinstance(b, (int, float)):
        b = pd.Series(b, index=a.index)
    return (a > b) & (a.shift(1) <= b.shift(1))


def count(condition: pd.Series, n: int) -> pd.Series:
    """COUNT(C, N) — N 日内条件成立的天数"""
    return condition.astype(int).rolling(n, min_periods=n).sum()


def rolling_sum(series: pd.Series, n: int) -> pd.Series:
    """SUM(S, N) — N 日累计"""
    return series.rolling(n, min_periods=n).sum()


def std(series: pd.Series, n: int) -> pd.Series:
    """STD(S, N) — N 日标准差"""
    return series.rolling(n, min_periods=n).std()


def islastbar(series: pd.Series) -> pd.Series:
    """ISLASTBAR — 最后一根K线"""
    result = pd.Series(False, index=series.index, dtype=bool)
    if len(result) > 0:
        result.iloc[-1] = True
    return result


# ============================================================
# 通达信财务函数
# ============================================================

def float_market_cap(close: pd.Series, float_shares: float) -> pd.Series:
    """FINANCE(40) — 流通市值 = 收盘价 × 流通股本"""
    return close * float_shares


# ============================================================
# MACD
# ============================================================

def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    MACD 计算，返回 (DIF, DEA, HIST)
    DIF = EMA(C, fast) - EMA(C, slow)
    DEA = EMA(DIF, signal)
    HIST = (DIF - DEA) * 2
    """
    dif = ema(close, fast) - ema(close, slow)
    dea = ema(dif, signal)
    hist = (dif - dea) * 2
    return dif, dea, hist


# ============================================================
# Qlib Alpha 因子（原 qlib_engine._compute_factors_for_df 迁移）
# ============================================================

def compute_alpha_factors(df: pd.DataFrame) -> pd.DataFrame:
    """对单只股票的 OHLCV DataFrame 计算 ~35 个技术因子。
    输入必须按 date 排序，包含 open/high/low/close/volume 列。
    返回与输入同 index 的 DataFrame。
    """
    c = df["close"]
    o = df["open"]
    h = df["high"]
    l_ = df["low"]
    v = df["volume"]

    factors = pd.DataFrame(index=df.index)

    # --- 动量 ---
    for n in [5, 10, 20, 60]:
        factors[f"ROC_{n}"] = c.pct_change(n)
    factors["MOM_5_20"] = c.pct_change(5) - c.pct_change(20)

    # --- 均线 ---
    for n in [5, 10, 20, 60]:
        _ma = ma(c, n)
        factors[f"MA_RATIO_{n}"] = c / _ma - 1

    # --- 波动率 ---
    for n in [5, 20]:
        factors[f"STD_{n}"] = c.pct_change().rolling(n, min_periods=n).std()
    factors["ATR_14"] = pd.concat([
        h - l_,
        (h - c.shift(1)).abs(),
        (l_ - c.shift(1)).abs(),
    ], axis=1).max(axis=1).rolling(14, min_periods=14).mean() / c

    # --- 量价 ---
    for n in [5, 20]:
        factors[f"VROC_{n}"] = v.pct_change(n)
    v_ma20 = v.rolling(20, min_periods=20).mean()
    factors["VOL_RATIO_20"] = v / v_ma20.replace(0, np.nan)

    # --- RSI ---
    delta = c.diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=14).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=14).mean()
    rs = gain / loss.replace(0, np.nan)
    factors["RSI_14"] = 100 - 100 / (1 + rs)

    # --- MACD（复用 macd() 函数确保公式一致性）---
    macd_line, signal_line, _ = macd(c)
    factors["MACD"] = macd_line / c
    factors["MACD_SIGNAL"] = signal_line / c
    factors["MACD_HIST"] = (macd_line - signal_line) / c

    # --- 布林带 ---
    bb_ma = ma(c, 20)
    bb_std = c.rolling(20, min_periods=20).std()
    factors["BOLL_UPPER"] = (c - (bb_ma + 2 * bb_std)) / c
    factors["BOLL_LOWER"] = (c - (bb_ma - 2 * bb_std)) / c
    factors["BOLL_WIDTH"] = 4 * bb_std / bb_ma.replace(0, np.nan)

    # --- K 线形态 ---
    hl = (h - l_).replace(0, np.nan)
    factors["BODY_RATIO"] = (c - o) / hl
    factors["UPPER_SHADOW"] = (h - pd.concat([c, o], axis=1).max(axis=1)) / hl
    factors["LOWER_SHADOW"] = (pd.concat([c, o], axis=1).min(axis=1) - l_) / hl
    factors["HL_RANGE"] = (h - l_) / c

    # --- 换手相关 ---
    amt = df.get("amount", v * c)
    factors["AMOUNT_MA5_RATIO"] = amt.rolling(5).mean() / amt.rolling(20).mean().replace(0, np.nan)

    # --- 数据质量防御 ---
    # 替换无穷值为 NaN（防止 LightGBM 训练崩溃）
    factors = factors.replace([np.inf, -np.inf], np.nan)

    return factors

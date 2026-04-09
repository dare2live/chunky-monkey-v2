"""
stock_archetype_engine.py — 股票类型中间事实层

v1: 高质量稳健型 / 成长兑现型 / 周期/事件驱动型
v2: 新增彼得林奇六类型（快速增长 / 缓慢增长 / 稳定增长 / 周期 / 困境反转 / 隐蔽资产）
    + 各类卖出时机信号（sell_signal_score，三子分可见）
"""

import logging
import os
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from statistics import median, pstdev
from typing import Optional

from services.utils import safe_float as _safe_float, clamp as _clamp

logger = logging.getLogger("cm-api")

# ─── 周期型行业集合（申万一级）────────────────────────────────────────
CYCLICAL_INDUSTRIES = {
    "采掘", "有色金属", "钢铁", "化工",
    "建材", "建筑装饰", "汽车", "交通运输", "房地产",
}


# ─── 通用辅助函数 ─────────────────────────────────────────────────────

def _parse_report_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return None


def _median(values: list) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    return float(median(clean)) if clean else None


def _std(values: list) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if len(clean) < 2:
        return 0.0 if clean else None
    return float(pstdev(clean))


def _positive_count(values: list) -> int:
    return sum(1 for v in values if v is not None and v > 0)


def _down_streak_2(values_desc: list) -> int:
    clean = [v for v in values_desc if v is not None]
    if len(clean) < 3:
        return 0
    return 1 if clean[0] < clean[1] < clean[2] else 0


def _sign_switch_count(values_desc: list) -> int:
    signs = []
    for value in values_desc:
        if value is None or value == 0:
            continue
        signs.append(1 if value > 0 else -1)
    if len(signs) < 2:
        return 0
    count = 0
    for prev, curr in zip(signs[:-1], signs[1:]):
        if prev != curr:
            count += 1
    return count


def _find_same_quarter_prev(rows_by_date: dict, report_date: str, years_back: int = 1) -> Optional[dict]:
    if not report_date or len(report_date) < 10:
        return None
    try:
        target = f"{int(report_date[:4]) - years_back}{report_date[4:]}"
    except Exception:
        return None
    return rows_by_date.get(target)


def _safe_div(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b in (None, 0):
        return None
    return a / b


def _trend_slope(values: list) -> Optional[float]:
    """最小二乘线性斜率，不依赖 numpy。输入为时间顺序（最旧在前）。"""
    clean = [(i, float(v)) for i, v in enumerate(values) if v is not None]
    if len(clean) < 2:
        return None
    n = len(clean)
    x_vals = [c[0] for c in clean]
    y_vals = [c[1] for c in clean]
    x_mean = sum(x_vals) / n
    y_mean = sum(y_vals) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
    den = sum((x - x_mean) ** 2 for x in x_vals)
    if den == 0:
        return 0.0
    return num / den


def _percentile_rank(value: float, series: list) -> float:
    """value 在 series 中的百分位（0-1），越高=越高位。"""
    clean = [v for v in series if v is not None]
    if not clean:
        return 0.5
    below = sum(1 for v in clean if v < value)
    return below / len(clean)


# ─── 表结构管理 ────────────────────────────────────────────────────────

def _safe_add_column(conn, table: str, col: str, col_type: str):
    """安全添加列（若已存在则忽略）。"""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")
    except Exception:
        pass


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_stock_archetype (
            snapshot_date               TEXT NOT NULL,
            stock_code                  TEXT NOT NULL,
            latest_report_date          TEXT,
            sw_level1                   TEXT,
            sw_level2                   TEXT,
            financial_history_rows      INTEGER DEFAULT 0,
            yoy_history_rows            INTEGER DEFAULT 0,
            high_quality_hits           INTEGER DEFAULT 0,
            growth_hits                 INTEGER DEFAULT 0,
            cycle_flags                 INTEGER DEFAULT 0,
            net_profit_positive_8q      INTEGER DEFAULT 0,
            operating_cashflow_positive_8q INTEGER DEFAULT 0,
            revenue_yoy_positive_4q     INTEGER DEFAULT 0,
            profit_yoy_positive_4q      INTEGER DEFAULT 0,
            eps_yoy_positive_4q         INTEGER DEFAULT 0,
            revenue_yoy_median_4q       REAL,
            profit_yoy_median_4q        REAL,
            revenue_yoy_std_4q          REAL,
            profit_yoy_std_4q           REAL,
            latest_revenue_yoy          REAL,
            latest_profit_yoy           REAL,
            revenue_yoy_down_streak_2q  INTEGER DEFAULT 0,
            profit_yoy_down_streak_2q   INTEGER DEFAULT 0,
            net_profit_sign_switch_8q   INTEGER DEFAULT 0,
            inventory_revenue_vol_4q    REAL,
            total_shares_growth_3y      REAL,
            debt_rank                   REAL,
            stock_archetype             TEXT,
            archetype_confidence        REAL,
            archetype_reason            TEXT,
            updated_at                  TEXT,
            PRIMARY KEY (snapshot_date, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_fsa_stock ON fact_stock_archetype(stock_code);

        CREATE TABLE IF NOT EXISTS dim_stock_archetype_latest (
            stock_code                  TEXT PRIMARY KEY,
            snapshot_date               TEXT,
            latest_report_date          TEXT,
            sw_level1                   TEXT,
            sw_level2                   TEXT,
            financial_history_rows      INTEGER DEFAULT 0,
            yoy_history_rows            INTEGER DEFAULT 0,
            high_quality_hits           INTEGER DEFAULT 0,
            growth_hits                 INTEGER DEFAULT 0,
            cycle_flags                 INTEGER DEFAULT 0,
            net_profit_positive_8q      INTEGER DEFAULT 0,
            operating_cashflow_positive_8q INTEGER DEFAULT 0,
            revenue_yoy_positive_4q     INTEGER DEFAULT 0,
            profit_yoy_positive_4q      INTEGER DEFAULT 0,
            eps_yoy_positive_4q         INTEGER DEFAULT 0,
            revenue_yoy_median_4q       REAL,
            profit_yoy_median_4q        REAL,
            revenue_yoy_std_4q          REAL,
            profit_yoy_std_4q           REAL,
            latest_revenue_yoy          REAL,
            latest_profit_yoy           REAL,
            revenue_yoy_down_streak_2q  INTEGER DEFAULT 0,
            profit_yoy_down_streak_2q   INTEGER DEFAULT 0,
            net_profit_sign_switch_8q   INTEGER DEFAULT 0,
            inventory_revenue_vol_4q    REAL,
            total_shares_growth_3y      REAL,
            debt_rank                   REAL,
            stock_archetype             TEXT,
            archetype_confidence        REAL,
            archetype_reason            TEXT,
            updated_at                  TEXT
        );
    """)
    # v2 扩展列：安全添加，兼容旧表
    for table in ("fact_stock_archetype", "dim_stock_archetype_latest"):
        _safe_add_column(conn, table, "ocf_quality_4q_avg",       "REAL")
        _safe_add_column(conn, table, "operating_margin_latest",   "REAL")
        _safe_add_column(conn, table, "operating_margin_pct_8q",   "REAL")
        _safe_add_column(conn, table, "gross_margin_std_4q",       "REAL")
        _safe_add_column(conn, table, "gross_margin_decline_2q",   "REAL")
        _safe_add_column(conn, table, "capital_alloc_ratio",       "REAL")
        _safe_add_column(conn, table, "pe_ttm",                    "REAL")
        _safe_add_column(conn, table, "pb_mrq",                    "REAL")
        _safe_add_column(conn, table, "peg_ratio",                 "REAL")
        _safe_add_column(conn, table, "profit_yoy_trend_slope",    "REAL")
        _safe_add_column(conn, table, "price_recovery_trough",     "REAL")
        _safe_add_column(conn, table, "lynch_type",                "TEXT")
        _safe_add_column(conn, table, "lynch_confidence",          "REAL")
        _safe_add_column(conn, table, "lynch_reason",              "TEXT")
        _safe_add_column(conn, table, "sell_signal_score",         "REAL")
        _safe_add_column(conn, table, "sell_s1_score",             "REAL")
        _safe_add_column(conn, table, "sell_s1_label",             "TEXT")
        _safe_add_column(conn, table, "sell_s2_score",             "REAL")
        _safe_add_column(conn, table, "sell_s2_label",             "TEXT")
        _safe_add_column(conn, table, "sell_s3_score",             "REAL")
        _safe_add_column(conn, table, "sell_s3_label",             "TEXT")
        _safe_add_column(conn, table, "sell_signal_reason",        "TEXT")
    conn.commit()


# ─── Lynch 六类分类 ───────────────────────────────────────────────────

def classify_lynch_type(data: dict) -> tuple:
    """
    返回 (lynch_type, confidence 0-100, reason)
    优先级：困境反转 > 隐蔽资产 > 周期 > 快速增长 > 稳定增长 > 缓慢增长
    """
    np_pos_8q   = data.get("net_profit_positive_8q") or 0
    had_loss    = (8 - np_pos_8q) >= 1
    consec_pos  = data.get("consecutive_profit_q") or 0
    latest_yoy  = data.get("latest_profit_yoy") or 0
    pb          = data.get("pb_mrq")
    ocf_avg     = data.get("ocf_quality_4q_avg") or 0
    sw1         = data.get("sw_level1") or ""
    rev_std     = data.get("revenue_yoy_std_4q") or 0
    rev_avg     = data.get("revenue_yoy_median_4q") or 0
    profit_avg  = data.get("profit_yoy_median_4q") or 0
    cap_ratio   = data.get("capital_alloc_ratio") or 0

    # 1. 困境反转型
    if had_loss and consec_pos >= 2 and latest_yoy > 0:
        conf = int(_clamp(40 + consec_pos * 15, 40, 90))
        return ("困境反转型", conf, f"近 8 季有亏损，现已连续 {consec_pos} 季盈利并同比改善")

    # 2. 隐蔽资产型
    if pb is not None and pb < 0.8 and ocf_avg > 0.3 and not had_loss:
        conf = int(_clamp((0.8 - pb) / 0.8 * 100, 40, 90))
        return ("隐蔽资产型", conf, f"市净率 {pb:.2f} 低于 0.8 且现金流健康")

    # 3. 周期型（行业标签优先；但若营收极稳定则降级为增长类）
    is_cyc_industry = sw1 in CYCLICAL_INDUSTRIES
    is_volatile     = rev_std > 0.25
    very_stable     = rev_std < 0.10          # 营收极稳定则不算周期
    if (is_cyc_industry or is_volatile) and not very_stable:
        conf = 70 if is_cyc_industry else 55
        reason = f"申万一级{sw1}属周期行业" if is_cyc_industry else f"营收增速标准差 {rev_std*100:.0f}% 波动显著"
        return ("周期型", conf, reason)

    # 4. 基于增速分类
    if rev_avg >= 0.20 and profit_avg >= 0.15:
        conf = int(_clamp((rev_avg + profit_avg) / 2 * 200, 50, 90))
        return ("快速增长型", conf, f"营收中位增速 {rev_avg*100:.0f}%，利润 {profit_avg*100:.0f}%")
    elif rev_avg >= 0.05:
        return ("稳定增长型", 70, f"营收增速 {rev_avg*100:.0f}%，介于成熟与成长之间")
    else:
        if cap_ratio >= 2.0:
            reason = f"低增速 + 分红/融资比 {cap_ratio:.1f}，股东友好型成熟企业"
            conf = int(_clamp(60 + cap_ratio * 3, 60, 85))
        else:
            reason = f"营收增速 {rev_avg*100:.0f}%，成熟阶段"
            conf = 60
        return ("缓慢增长型", conf, reason)


# ─── 各类卖出时机评分 ─────────────────────────────────────────────────

def _sell_fast_grower(d: dict) -> tuple:
    """快速增长型：增速滑落 + 估值泡沫 + 利润质量"""
    rev_avg    = d.get("revenue_yoy_median_4q") or 0
    rev_latest = d.get("latest_revenue_yoy") or 0
    peg        = d.get("peg_ratio")
    ocf_avg    = d.get("ocf_quality_4q_avg") or 1.0

    # S1 增速滑落（max 40）
    decel = _clamp(rev_avg - rev_latest, 0, 1)
    if   decel > 0.15: s1 = 40
    elif decel > 0.10: s1 = 28
    elif decel > 0.05: s1 = 15
    else:              s1 = 0
    l1 = f"增速滑落 {decel*100:.0f}pp" if decel > 0.01 else "增速平稳"

    # S2 估值泡沫 PEG（max 35）
    if peg is None:
        s2, l2 = 5, "PE/PEG 数据不足"
    elif peg >= 3.0: s2, l2 = 35, f"PEG={peg:.1f} 估值严重透支增长"
    elif peg >= 2.0: s2, l2 = 25, f"PEG={peg:.1f} 估值偏高"
    elif peg >= 1.5: s2, l2 = 15, f"PEG={peg:.1f} 估值合理偏贵"
    elif peg >= 1.0: s2, l2 =  5, f"PEG={peg:.1f} 估值基本合理"
    else:            s2, l2 =  0, f"PEG={peg:.1f} 便宜"

    # S3 利润质量（max 25）
    if   ocf_avg < 0.30: s3, l3 = 25, f"OCF/利润={ocf_avg:.2f} 盈利质量差"
    elif ocf_avg < 0.60: s3, l3 = 15, f"OCF/利润={ocf_avg:.2f} 盈利质量一般"
    elif ocf_avg < 0.80: s3, l3 =  5, f"OCF/利润={ocf_avg:.2f} 盈利质量尚可"
    else:                s3, l3 =  0, f"OCF/利润={ocf_avg:.2f} 盈利质量好"

    total = round(s1 + s2 + s3, 1)
    reasons = [x for x in [l1 if s1 >= 15 else "", l2 if s2 >= 15 else "", l3 if s3 >= 15 else ""] if x]
    reason = "；".join(reasons) if reasons else "暂无明显卖出信号"
    return (total, s1, l1, s2, l2, s3, l3, reason)


def _sell_slow_grower(d: dict) -> tuple:
    """缓慢增长型：股东回报停止 + 基本面衰退 + 负债恶化"""
    cap_ratio  = d.get("capital_alloc_ratio") or 0
    rev_avg    = d.get("revenue_yoy_median_4q") or 0
    profit_avg = d.get("profit_yoy_median_4q") or 0
    debt_rank  = d.get("debt_rank") or 50  # 高 = 负债低（健康）

    # S1 股东回报（max 40）—— 分红/融资比越低越危险
    if   cap_ratio < 0.3:  s1, l1 = 40, f"分红/融资比仅 {cap_ratio:.1f}，股东利益受损"
    elif cap_ratio < 0.5:  s1, l1 = 28, f"分红/融资比 {cap_ratio:.1f}，回报不足"
    elif cap_ratio < 1.0:  s1, l1 = 15, f"分红/融资比 {cap_ratio:.1f}，略低于理想"
    elif cap_ratio < 2.0:  s1, l1 =  5, f"分红/融资比 {cap_ratio:.1f}，尚可"
    else:                  s1, l1 =  0, f"分红/融资比 {cap_ratio:.1f}，股东友好"

    # S2 基本面衰退（max 35）
    if   rev_avg < -0.05:   s2, l2 = 35, f"营收持续负增长 {rev_avg*100:.0f}%"
    elif rev_avg < 0:       s2, l2 = 20, f"营收微负 {rev_avg*100:.0f}%"
    elif profit_avg < -0.10: s2, l2 = 25, f"利润负增长 {profit_avg*100:.0f}%"
    elif profit_avg < 0:    s2, l2 = 12, f"利润微负 {profit_avg*100:.0f}%"
    else:                   s2, l2 =  0, "基本面平稳"

    # S3 负债恶化（max 25）—— debt_rank 越低 = 负债率越高
    if   debt_rank < 20:   s3, l3 = 25, f"负债排名偏低 ({debt_rank:.0f}/100)，财务风险高"
    elif debt_rank < 35:   s3, l3 = 15, f"负债排名 {debt_rank:.0f}/100，中等偏高"
    elif debt_rank < 50:   s3, l3 =  8, f"负债排名 {debt_rank:.0f}/100，一般"
    else:                  s3, l3 =  0, f"负债排名 {debt_rank:.0f}/100，健康"

    total = round(s1 + s2 + s3, 1)
    reasons = [x for x in [l1 if s1 >= 15 else "", l2 if s2 >= 15 else "", l3 if s3 >= 15 else ""] if x]
    reason = "；".join(reasons) if reasons else "分红持续且基本面稳定，暂无卖出信号"
    return (total, s1, l1, s2, l2, s3, l3, reason)


def _sell_stalwart(d: dict) -> tuple:
    """稳定增长型：过度估值 + 毛利率滑落 + 收入利润背离"""
    pe          = d.get("pe_ttm")
    gm_decline  = d.get("gross_margin_decline_2q") or 0
    rev_latest  = d.get("latest_revenue_yoy") or 0
    profit_lat  = d.get("latest_profit_yoy") or 0

    # S1 过度估值（max 35）
    if pe is None:
        s1, l1 = 5, "PE 数据不足"
    elif pe > 30:  s1, l1 = 35, f"PE={pe:.0f}x 估值偏高"
    elif pe > 25:  s1, l1 = 25, f"PE={pe:.0f}x 较贵"
    elif pe > 20:  s1, l1 = 15, f"PE={pe:.0f}x 合理上沿"
    else:          s1, l1 =  0, f"PE={pe:.0f}x 合理"

    # S2 毛利率滑落（max 35）—— gm_decline > 0 表示近期下滑
    if   gm_decline > 0.05:  s2, l2 = 35, f"毛利率近期下滑 {gm_decline*100:.1f}pp，竞争力侵蚀"
    elif gm_decline > 0.03:  s2, l2 = 25, f"毛利率下滑 {gm_decline*100:.1f}pp"
    elif gm_decline > 0.01:  s2, l2 = 10, f"毛利率微降 {gm_decline*100:.1f}pp"
    else:                    s2, l2 =  0, "毛利率稳定"

    # S3 收入利润背离（max 30）
    if rev_latest > 0.05 and profit_lat < -0.05:
        s3, l3 = 30, f"营收涨 {rev_latest*100:.0f}% 但利润降 {profit_lat*100:.0f}%，利润率压缩"
    elif rev_latest > 0.02 and profit_lat < 0:
        s3, l3 = 18, "营收正增但利润微负，背离值得关注"
    else:
        s3, l3 = 0, "收入利润同向"

    total = round(s1 + s2 + s3, 1)
    reasons = [x for x in [l1 if s1 >= 15 else "", l2 if s2 >= 15 else "", l3 if s3 >= 15 else ""] if x]
    reason = "；".join(reasons) if reasons else "估值合理且竞争力稳定"
    return (total, s1, l1, s2, l2, s3, l3, reason)


def _sell_cyclical(d: dict) -> tuple:
    """
    周期型：利润率高位 + 增速减速 + 低PE陷阱
    ⚠️ 反直觉设计：利润最好时才是卖出时机
    """
    op_pct      = d.get("operating_margin_pct_8q") or 0.5  # 0-1，越高=历史高位
    rev_avg     = d.get("revenue_yoy_median_4q") or 0
    rev_latest  = d.get("latest_revenue_yoy") or 0
    pe          = d.get("pe_ttm")

    # S1 利润率高位（max 40）
    if   op_pct >= 0.90:  s1, l1 = 40, f"营业利润率处于近 8 季 {op_pct*100:.0f}% 分位（历史高位）⚠️"
    elif op_pct >= 0.75:  s1, l1 = 25, f"利润率 {op_pct*100:.0f}% 分位，景气偏高"
    elif op_pct >= 0.60:  s1, l1 = 10, f"利润率 {op_pct*100:.0f}% 分位，景气中上"
    else:                 s1, l1 =  0, f"利润率 {op_pct*100:.0f}% 分位，景气低位（周期底部）"

    # S2 增速开始减速（max 35）
    rev_decel = rev_avg - rev_latest  # 正值 = 增速在减慢
    if   rev_decel > 0.10 and rev_latest > 0:  s2, l2 = 35, f"增速仍正但大幅减速 {rev_decel*100:.0f}pp，周期拐点信号"
    elif rev_decel > 0.05:                      s2, l2 = 20, f"增速减速 {rev_decel*100:.0f}pp"
    elif rev_latest < 0:                        s2, l2 = 30, f"营收已转负增长 {rev_latest*100:.0f}%"
    elif rev_decel > 0:                         s2, l2 = 10, "增速轻微减速"
    else:                                       s2, l2 =  0, "增速维持或上升"

    # S3 低PE陷阱（max 25）⚠️ 高利润率时低PE = 危险
    if pe is None:
        s3, l3 = 0, "PE 数据不足"
    elif pe < 10 and op_pct > 0.70:
        s3, l3 = 25, f"⚠️ PE={pe:.0f}x 低但利润率在历史高位——周期顶部特征，低PE非安全边际"
    elif pe < 12 and op_pct > 0.65:
        s3, l3 = 15, f"PE={pe:.0f}x + 利润率偏高，注意周期风险"
    else:
        s3, l3 = 0, f"PE={pe:.0f}x 与景气位置无明显顶部信号" if pe else "无特殊信号"

    total = round(s1 + s2 + s3, 1)
    reasons = [x for x in [l1 if s1 >= 10 else "", l2 if s2 >= 10 else "", l3 if s3 >= 15 else ""] if x]
    reason = "；".join(reasons) if reasons else "景气处于低位，持有逻辑完整"
    return (total, s1, l1, s2, l2, s3, l3, reason)


def _sell_turnaround(d: dict) -> tuple:
    """困境反转型：复苏充分定价 + 利润动能衰减 + 估值正常化"""
    recovery    = d.get("price_recovery_trough") or 0
    slope       = d.get("profit_yoy_trend_slope") or 0  # 正 = 仍在改善，负 = 动能衰减
    pe          = d.get("pe_ttm")
    pb          = d.get("pb_mrq")

    # S1 复苏定价（max 40）
    if   recovery > 1.50:  s1, l1 = 40, f"从底部反弹 {recovery*100:.0f}%，复苏已充分定价"
    elif recovery > 1.00:  s1, l1 = 30, f"反弹 {recovery*100:.0f}%，接近充分定价"
    elif recovery > 0.50:  s1, l1 = 15, f"反弹 {recovery*100:.0f}%，仍有空间"
    else:                  s1, l1 =  0, f"反弹 {recovery*100:.0f}%，复苏早期"

    # S2 利润动能（max 35）—— slope < 0 = 改善速度在放缓
    if   slope < -0.10:  s2, l2 = 35, "利润改善斜率显著转负，反转动能衰减"
    elif slope < -0.05:  s2, l2 = 20, "利润改善速度放缓"
    elif slope < 0:      s2, l2 = 10, "利润改善轻微放缓"
    else:                s2, l2 =  0, "利润改善仍在加速"

    # S3 估值正常化（max 25）
    if pe is not None and pe > 20 and pb is not None and pb > 1.5:
        s3, l3 = 25, f"PE={pe:.0f}x、PB={pb:.2f}，估值已正常化"
    elif pe is not None and pe > 15:
        s3, l3 = 15, f"PE={pe:.0f}x，估值接近正常"
    else:
        s3, l3 = 0, "估值仍偏低，反转未完全定价"

    total = round(s1 + s2 + s3, 1)
    reasons = [x for x in [l1 if s1 >= 15 else "", l2 if s2 >= 15 else "", l3 if s3 >= 15 else ""] if x]
    reason = "；".join(reasons) if reasons else "复苏逻辑仍成立，持有"
    return (total, s1, l1, s2, l2, s3, l3, reason)


def _sell_asset_play(d: dict) -> tuple:
    """隐蔽资产型：折价消失 + 现金流质量下降 + 估值合理化"""
    pb      = d.get("pb_mrq")
    ocf_avg = d.get("ocf_quality_4q_avg") or 0
    pe      = d.get("pe_ttm")

    # S1 折价消失（max 50）
    if pb is None:
        s1, l1 = 0, "PB 数据不足"
    elif pb > 1.5:   s1, l1 = 50, f"PB={pb:.2f} 折价已消失，超过净资产"
    elif pb > 1.2:   s1, l1 = 35, f"PB={pb:.2f} 折价基本消失"
    elif pb > 1.0:   s1, l1 = 20, f"PB={pb:.2f} 接近净资产"
    elif pb > 0.9:   s1, l1 = 10, f"PB={pb:.2f} 仍有小幅折价"
    else:            s1, l1 =  0, f"PB={pb:.2f} 仍有明显折价"

    # S2 现金流质量（max 30）
    if   ocf_avg < 0:    s2, l2 = 30, f"OCF 为负，现金消耗，支撑资产估值能力下降"
    elif ocf_avg < 0.3:  s2, l2 = 15, f"OCF/利润={ocf_avg:.2f} 偏低"
    else:                s2, l2 =  0, f"OCF/利润={ocf_avg:.2f} 健康"

    # S3 估值合理化（max 20）
    if pe is not None and pe > 20 and pb is not None and pb > 1.0:
        s3, l3 = 20, f"PE={pe:.0f}x + PB={pb:.2f}，隐藏资产已充分定价"
    elif pe is not None and pe > 15:
        s3, l3 = 10, f"PE={pe:.0f}x，估值接近合理"
    else:
        s3, l3 = 0, "估值仍具折价保护"

    total = round(s1 + s2 + s3, 1)
    reasons = [x for x in [l1 if s1 >= 20 else "", l2 if s2 >= 15 else "", l3 if s3 >= 10 else ""] if x]
    reason = "；".join(reasons) if reasons else "折价保护仍在，持有理由完整"
    return (total, s1, l1, s2, l2, s3, l3, reason)


def compute_sell_signal(d: dict, lynch_type: str) -> tuple:
    """统一入口，返回 (total, s1, l1, s2, l2, s3, l3, reason)"""
    dispatch = {
        "快速增长型": _sell_fast_grower,
        "缓慢增长型": _sell_slow_grower,
        "稳定增长型": _sell_stalwart,
        "周期型":     _sell_cyclical,
        "困境反转型": _sell_turnaround,
        "隐蔽资产型": _sell_asset_play,
    }
    fn = dispatch.get(lynch_type)
    if fn:
        return fn(d)
    return (0.0, 0.0, "—", 0.0, "—", 0.0, "—", "类型未定义")


# ─── 辅助：加载市价数据 ─────────────────────────────────────────────────

def _load_price_data() -> dict:
    """返回 {stock_code: latest_close}，从 market_data.db 读取。"""
    try:
        db_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", "market_data.db"
        )
        if not os.path.exists(db_path):
            return {}
        mc = sqlite3.connect(db_path)
        mc.row_factory = sqlite3.Row
        rows = mc.execute("""
            SELECT k.code, k.close
            FROM price_kline k
            INNER JOIN (
                SELECT code, MAX(date) AS max_date
                FROM price_kline
                WHERE freq = 'daily' AND adjust = 'qfq'
                GROUP BY code
            ) latest ON k.code = latest.code AND k.date = latest.max_date
            WHERE k.freq = 'daily' AND k.adjust = 'qfq'
        """).fetchall()
        mc.close()
        return {row["code"]: float(row["close"]) for row in rows}
    except Exception as e:
        logger.warning(f"[股票类型] 无法加载市价: {e}")
        return {}


def _load_capital_data(conn) -> dict:
    """返回 {stock_code: {dividend_cash_sum_5y, allotment_raised_funds_5y}}"""
    try:
        rows = conn.execute("""
            SELECT stock_code, dividend_cash_sum_5y, allotment_raised_funds_5y
            FROM dim_capital_behavior_latest
        """).fetchall()
        return {row["stock_code"]: dict(row) for row in rows}
    except Exception:
        return {}


def _load_gross_margin_data(conn) -> dict:
    """返回 {stock_code: [gross_margin_ak by report_date desc, max 4 rows]}"""
    result = defaultdict(list)
    try:
        rows = conn.execute("""
            SELECT stock_code, report_date, gross_margin_ak
            FROM fact_financial_indicator_ak
            WHERE gross_margin_ak IS NOT NULL
            ORDER BY stock_code, report_date DESC
        """).fetchall()
        for row in rows:
            lst = result[row["stock_code"]]
            if len(lst) < 4:
                lst.append(_safe_float(row["gross_margin_ak"]))
    except Exception:
        pass
    return result


# ─── 主构建函数 ────────────────────────────────────────────────────────

def build_stock_archetypes(conn, snapshot_date: Optional[str] = None) -> int:
    ensure_tables(conn)
    snapshot_date = snapshot_date or date.today().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()

    # ── 加载辅助数据 ──
    price_data   = _load_price_data()
    capital_data = _load_capital_data(conn)
    gm_data      = _load_gross_margin_data(conn)

    # ── 加载财务历史（增加 net_assets, operating_profit） ──
    raw_rows = conn.execute("""
        SELECT r.stock_code, r.report_date, r.revenue, r.net_profit,
               r.operating_cashflow, r.operating_profit, r.inventory,
               r.total_shares, r.net_assets, r.holder_count, r.eps,
               i.sw_level1, i.sw_level2
        FROM raw_gpcw_financial r
        LEFT JOIN dim_stock_industry i ON i.stock_code = r.stock_code
        ORDER BY r.stock_code, r.report_date DESC
    """).fetchall()

    if not raw_rows:
        logger.info("[股票类型] 无财务历史数据，跳过构建")
        return 0

    derived_rows = conn.execute("""
        SELECT stock_code, report_date, revenue_yoy, profit_yoy, ocf_to_profit
        FROM fact_financial_derived
        ORDER BY stock_code, report_date DESC
    """).fetchall()

    quality_rows = []
    try:
        quality_rows = conn.execute("""
            SELECT stock_code, debt_rank, quality_score_v1, quality_profit_raw,
                   quality_cash_raw, quality_growth_raw, quality_balance_raw
            FROM dim_stock_quality_latest
        """).fetchall()
    except Exception:
        pass

    raw_by_stock = defaultdict(list)
    for row in raw_rows:
        raw_by_stock[row["stock_code"]].append(dict(row))

    derived_by_stock = defaultdict(list)
    for row in derived_rows:
        derived_by_stock[row["stock_code"]].append(dict(row))

    quality_by_stock = {row["stock_code"]: dict(row) for row in quality_rows}

    conn.execute("DELETE FROM fact_stock_archetype WHERE snapshot_date = ?", (snapshot_date,))
    inserted = 0

    for stock_code, rows in raw_by_stock.items():
        rows = sorted(rows, key=lambda item: item["report_date"], reverse=True)
        rows_by_date = {row["report_date"]: row for row in rows}
        drows = sorted(derived_by_stock.get(stock_code, []), key=lambda item: item["report_date"], reverse=True)
        sw_level1 = rows[0].get("sw_level1")
        sw_level2 = rows[0].get("sw_level2")
        latest_report_date = rows[0].get("report_date")

        latest_8  = rows[:8]
        yoy_rows  = [r for r in drows if r.get("revenue_yoy") is not None or r.get("profit_yoy") is not None][:4]
        revenue_yoy_series = [_safe_float(r.get("revenue_yoy")) for r in yoy_rows if r.get("revenue_yoy") is not None]
        profit_yoy_series  = [_safe_float(r.get("profit_yoy"))  for r in yoy_rows if r.get("profit_yoy")  is not None]
        ocf_series         = [_safe_float(r.get("ocf_to_profit")) for r in drows[:4] if r.get("ocf_to_profit") is not None]

        latest_revenue_yoy = revenue_yoy_series[0] if revenue_yoy_series else None
        latest_profit_yoy  = profit_yoy_series[0]  if profit_yoy_series  else None

        # ── 原有指标 ──
        net_profit_positive_8q          = _positive_count([_safe_float(r.get("net_profit"))          for r in latest_8])
        operating_cashflow_positive_8q  = _positive_count([_safe_float(r.get("operating_cashflow"))  for r in latest_8])
        revenue_yoy_positive_4q         = _positive_count(revenue_yoy_series[:4])
        profit_yoy_positive_4q          = _positive_count(profit_yoy_series[:4])

        eps_yoy_series = []
        for row in rows[:4]:
            prev_same_q = _find_same_quarter_prev(rows_by_date, row["report_date"], years_back=1)
            eps      = _safe_float(row.get("eps"))
            prev_eps = _safe_float((prev_same_q or {}).get("eps"))
            yoy = _safe_div((eps or 0) - (prev_eps or 0), abs(prev_eps) if prev_eps not in (None, 0) else None)
            if yoy is not None:
                eps_yoy_series.append(yoy)
        eps_yoy_positive_4q = _positive_count(eps_yoy_series[:4])

        revenue_yoy_median_4q   = _median(revenue_yoy_series[:4])
        profit_yoy_median_4q    = _median(profit_yoy_series[:4])
        revenue_yoy_std_4q      = _std(revenue_yoy_series[:4])
        profit_yoy_std_4q       = _std(profit_yoy_series[:4])
        revenue_yoy_down_streak_2q = _down_streak_2(revenue_yoy_series[:3])
        profit_yoy_down_streak_2q  = _down_streak_2(profit_yoy_series[:3])
        net_profit_sign_switch_8q  = _sign_switch_count([_safe_float(r.get("net_profit")) for r in latest_8])

        inventory_revenue_vol_4q = None
        if sw_level1 not in {"银行", "非银金融"}:
            inv_rev_series = []
            for row in rows[:4]:
                ratio = _safe_div(_safe_float(row.get("inventory")), _safe_float(row.get("revenue")))
                if ratio is not None:
                    inv_rev_series.append(ratio)
            inventory_revenue_vol_4q = _std(inv_rev_series[:4])

        total_shares_growth_3y = None
        latest_shares  = _safe_float(rows[0].get("total_shares")) if rows else None
        share_ref_row  = _find_same_quarter_prev(rows_by_date, latest_report_date, years_back=3)
        if not share_ref_row and len(rows) >= 12:
            share_ref_row = rows[min(11, len(rows) - 1)]
        share_ref = _safe_float((share_ref_row or {}).get("total_shares"))
        total_shares_growth_3y = _safe_div(
            (latest_shares or 0) - (share_ref or 0),
            abs(share_ref) if share_ref not in (None, 0) else None
        )

        quality_row       = quality_by_stock.get(stock_code) or {}
        debt_rank         = _safe_float(quality_row.get("debt_rank"))
        quality_score_v1  = _safe_float(quality_row.get("quality_score_v1"))
        quality_profit_raw= _safe_float(quality_row.get("quality_profit_raw"))
        quality_cash_raw  = _safe_float(quality_row.get("quality_cash_raw"))
        quality_growth_raw= _safe_float(quality_row.get("quality_growth_raw"))
        quality_balance_raw=_safe_float(quality_row.get("quality_balance_raw"))

        high_quality_hits = sum([
            net_profit_positive_8q >= 7,
            operating_cashflow_positive_8q >= 6,
            revenue_yoy_positive_4q >= 3,
            profit_yoy_positive_4q >= 3,
            (debt_rank or 0) >= 40,
            total_shares_growth_3y is not None and total_shares_growth_3y <= 0.15,
        ])
        growth_hits = sum([
            revenue_yoy_median_4q is not None and revenue_yoy_median_4q >= 0.20,
            profit_yoy_median_4q  is not None and profit_yoy_median_4q  >= 0.20,
            eps_yoy_positive_4q >= 3,
            net_profit_positive_8q >= 6,
            operating_cashflow_positive_8q >= 5,
        ])
        cycle_flags = sum([
            net_profit_sign_switch_8q >= 2,
            profit_yoy_std_4q is not None and profit_yoy_std_4q > 0.40,
            revenue_yoy_std_4q is not None and revenue_yoy_std_4q > 0.30,
            inventory_revenue_vol_4q is not None and inventory_revenue_vol_4q > 0.12,
            total_shares_growth_3y is not None and total_shares_growth_3y > 0.30,
        ])

        enough_history = len(latest_8) >= 8 and max(len(revenue_yoy_series), len(profit_yoy_series), len(eps_yoy_series)) >= 3

        # ── 原有三类分类 ──
        if growth_hits >= 4 and (
            growth_hits > high_quality_hits or (
                growth_hits == high_quality_hits
                and (revenue_yoy_median_4q or 0) >= 0.20
                and (profit_yoy_median_4q  or 0) >= 0.20
            )
        ):
            stock_archetype, archetype_reason, confidence_base = "成长兑现型", "收入和利润增速中位数较高，成长兑现特征更明显", growth_hits / 5.0
        elif high_quality_hits >= 4:
            stock_archetype, archetype_reason, confidence_base = "高质量稳健型", "净利、现金流和股本稳定性较好，稳健质量特征更明确", high_quality_hits / 6.0
        elif enough_history and high_quality_hits >= 3 and cycle_flags == 0 and (quality_score_v1 or 0) >= 70:
            stock_archetype, archetype_reason, confidence_base = "高质量稳健型", "历史命中接近稳健阈值，且质量分较高，暂归入高质量稳健型", 0.68
        elif enough_history and growth_hits >= 3 and ((revenue_yoy_median_4q or 0) >= 0.15 or (profit_yoy_median_4q or 0) >= 0.15):
            stock_archetype, archetype_reason, confidence_base = "成长兑现型", "成长条件接近正式阈值，收入或利润增速仍具兑现特征", 0.7
        elif not enough_history and (quality_score_v1 or 0) >= 72 and (quality_cash_raw or 0) >= 14 and (quality_profit_raw or 0) >= 18:
            stock_archetype, archetype_reason, confidence_base = "高质量稳健型", "历史期数未完全补齐，按当前质量、现金和利润特征暂定", 0.58
        elif not enough_history and (quality_score_v1 or 0) >= 58 and (quality_growth_raw or 0) >= 4:
            stock_archetype, archetype_reason, confidence_base = "成长兑现型", "历史期数未完全补齐，按当前增长与质量特征暂定", 0.55
        else:
            if cycle_flags >= 2:
                reason_txt = "盈利波动或库存/股本弹性较大，更接近周期或事件驱动"
            elif not enough_history:
                reason_txt = "历史期数未完全补齐，暂按周期/事件驱动型保守处理"
            else:
                reason_txt = "成长和稳健条件未充分满足，暂归入周期/事件驱动"
            stock_archetype, archetype_reason, confidence_base = "周期/事件驱动型", reason_txt, max(cycle_flags / 5.0, 0.45 if enough_history else 0.38)

        history_factor = min(len(latest_8) / 8.0, 1.0)
        yoy_factor     = min(max(len(revenue_yoy_series), len(profit_yoy_series), len(eps_yoy_series)) / 4.0, 1.0)
        archetype_confidence = round(min(100.0, confidence_base * (0.65 + 0.2 * history_factor + 0.15 * yoy_factor) * 100), 2)

        # ══ v2 新增计算 ══════════════════════════════════════════════════

        # OCF 质量 4Q 均值
        ocf_quality_4q_avg = _median(ocf_series[:4])

        # 运营利润率及历史分位
        op_margins = [_safe_div(_safe_float(r.get("operating_profit")), _safe_float(r.get("revenue"))) for r in latest_8]
        op_margins = [v for v in op_margins if v is not None]
        operating_margin_latest  = op_margins[0] if op_margins else None
        operating_margin_pct_8q  = _percentile_rank(op_margins[0], op_margins) if len(op_margins) >= 2 else None

        # 毛利率稳定性（来自 fact_financial_indicator_ak）
        gm_series = gm_data.get(stock_code, [])
        gross_margin_std_4q   = _std(gm_series[:4])
        gross_margin_decline_2q = None
        if len(gm_series) >= 4:
            old_avg = _median(gm_series[2:4])
            new_avg = _median(gm_series[0:2])
            if old_avg is not None and new_avg is not None:
                gross_margin_decline_2q = old_avg - new_avg  # 正 = 近期下滑

        # 股东回报质量（分红/融资比）
        cap = capital_data.get(stock_code) or {}
        div_sum  = _safe_float(cap.get("dividend_cash_sum_5y"))  or 0
        allt_sum = _safe_float(cap.get("allotment_raised_funds_5y")) or 0
        capital_alloc_ratio = round(div_sum / max(allt_sum, 0.01), 2) if div_sum > 0 else 0.0

        # PE TTM / PB MRQ
        latest_price = price_data.get(stock_code)
        pe_ttm, pb_mrq, peg_ratio = None, None, None
        if latest_price and latest_price > 0:
            net_profit_ttm = sum(
                _safe_float(r.get("net_profit")) or 0 for r in rows[:4]
            )
            total_shares   = _safe_float(rows[0].get("total_shares")) if rows else None
            net_assets     = _safe_float(rows[0].get("net_assets"))   if rows else None
            if total_shares and total_shares > 0 and net_profit_ttm > 0:
                eps_ttm = net_profit_ttm / total_shares
                pe_ttm  = round(latest_price / eps_ttm, 1)
            if total_shares and total_shares > 0 and net_assets and net_assets > 0:
                bvps   = net_assets / total_shares
                pb_mrq = round(latest_price / bvps, 2)
            if pe_ttm and profit_yoy_median_4q and profit_yoy_median_4q > 0:
                peg_ratio = round(pe_ttm / (profit_yoy_median_4q * 100), 2)

        # 利润增速趋势斜率（最旧→最新方向）
        profit_yoy_asc = list(reversed(profit_yoy_series))
        profit_yoy_trend_slope = _trend_slope(profit_yoy_asc)

        # 从底部反弹幅度（困境反转型专用）
        price_recovery_trough = None
        if latest_price:
            price_close_8q = [latest_price]  # 近似：用当前价格作为最新数据
            # 从 price_kline 中获取8季对应的低点需要精确日期映射，
            # 此处用财务历史期间的代理：仅当有亏损时才计算
            if net_profit_sign_switch_8q >= 1 and latest_price > 0:
                # 简单代理：若无历史价格数据，跳过
                price_recovery_trough = None  # 待 Stage engine 补充

        # 连续盈利季数（困境反转专用）
        consecutive_profit_q = 0
        for r in rows:
            if (_safe_float(r.get("net_profit")) or 0) > 0:
                consecutive_profit_q += 1
            else:
                break

        # ── Lynch 六类分类 ──
        lynch_data = {
            "net_profit_positive_8q":   net_profit_positive_8q,
            "consecutive_profit_q":     consecutive_profit_q,
            "latest_profit_yoy":        latest_profit_yoy,
            "pb_mrq":                   pb_mrq,
            "ocf_quality_4q_avg":       ocf_quality_4q_avg,
            "sw_level1":                sw_level1,
            "revenue_yoy_std_4q":       revenue_yoy_std_4q,
            "revenue_yoy_median_4q":    revenue_yoy_median_4q,
            "profit_yoy_median_4q":     profit_yoy_median_4q,
            "capital_alloc_ratio":      capital_alloc_ratio,
            "pe_ttm":                   pe_ttm,
            "peg_ratio":                peg_ratio,
            "latest_revenue_yoy":       latest_revenue_yoy,
            "gross_margin_decline_2q":  gross_margin_decline_2q,
            "operating_margin_pct_8q":  operating_margin_pct_8q,
            "profit_yoy_trend_slope":   profit_yoy_trend_slope,
            "price_recovery_trough":    price_recovery_trough,
            "debt_rank":                debt_rank,
        }
        lynch_type, lynch_confidence, lynch_reason = classify_lynch_type(lynch_data)

        # ── 卖出信号评分 ──
        (sell_total, s1, l1, s2, l2, s3, l3, sell_reason) = compute_sell_signal(lynch_data, lynch_type)

        # ── 写入数据库 ──
        conn.execute("""
            INSERT OR REPLACE INTO fact_stock_archetype (
                snapshot_date, stock_code, latest_report_date, sw_level1, sw_level2,
                financial_history_rows, yoy_history_rows, high_quality_hits, growth_hits,
                cycle_flags, net_profit_positive_8q, operating_cashflow_positive_8q,
                revenue_yoy_positive_4q, profit_yoy_positive_4q, eps_yoy_positive_4q,
                revenue_yoy_median_4q, profit_yoy_median_4q, revenue_yoy_std_4q,
                profit_yoy_std_4q, latest_revenue_yoy, latest_profit_yoy,
                revenue_yoy_down_streak_2q, profit_yoy_down_streak_2q,
                net_profit_sign_switch_8q, inventory_revenue_vol_4q,
                total_shares_growth_3y, debt_rank, stock_archetype,
                archetype_confidence, archetype_reason, updated_at,
                ocf_quality_4q_avg, operating_margin_latest, operating_margin_pct_8q,
                gross_margin_std_4q, gross_margin_decline_2q, capital_alloc_ratio,
                pe_ttm, pb_mrq, peg_ratio, profit_yoy_trend_slope, price_recovery_trough,
                lynch_type, lynch_confidence, lynch_reason,
                sell_signal_score, sell_s1_score, sell_s1_label,
                sell_s2_score, sell_s2_label, sell_s3_score, sell_s3_label,
                sell_signal_reason
            ) VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
        """, (
            snapshot_date, stock_code, latest_report_date, sw_level1, sw_level2,
            len(rows),
            max(len(revenue_yoy_series), len(profit_yoy_series), len(eps_yoy_series)),
            high_quality_hits, growth_hits, cycle_flags,
            net_profit_positive_8q, operating_cashflow_positive_8q,
            revenue_yoy_positive_4q, profit_yoy_positive_4q, eps_yoy_positive_4q,
            revenue_yoy_median_4q, profit_yoy_median_4q,
            revenue_yoy_std_4q, profit_yoy_std_4q,
            latest_revenue_yoy, latest_profit_yoy,
            revenue_yoy_down_streak_2q, profit_yoy_down_streak_2q,
            net_profit_sign_switch_8q, inventory_revenue_vol_4q,
            total_shares_growth_3y, debt_rank,
            stock_archetype, archetype_confidence, archetype_reason, now,
            # v2
            ocf_quality_4q_avg, operating_margin_latest, operating_margin_pct_8q,
            gross_margin_std_4q, gross_margin_decline_2q, capital_alloc_ratio,
            pe_ttm, pb_mrq, peg_ratio, profit_yoy_trend_slope, price_recovery_trough,
            lynch_type, lynch_confidence, lynch_reason,
            sell_total, s1, l1, s2, l2, s3, l3, sell_reason,
        ))
        inserted += 1

    # ── 刷新维度表 ──
    conn.execute("DELETE FROM dim_stock_archetype_latest")
    conn.execute("""
        INSERT INTO dim_stock_archetype_latest
        SELECT stock_code, snapshot_date, latest_report_date, sw_level1, sw_level2,
               financial_history_rows, yoy_history_rows, high_quality_hits, growth_hits,
               cycle_flags, net_profit_positive_8q, operating_cashflow_positive_8q,
               revenue_yoy_positive_4q, profit_yoy_positive_4q, eps_yoy_positive_4q,
               revenue_yoy_median_4q, profit_yoy_median_4q, revenue_yoy_std_4q,
               profit_yoy_std_4q, latest_revenue_yoy, latest_profit_yoy,
               revenue_yoy_down_streak_2q, profit_yoy_down_streak_2q,
               net_profit_sign_switch_8q, inventory_revenue_vol_4q,
               total_shares_growth_3y, debt_rank, stock_archetype,
               archetype_confidence, archetype_reason, updated_at,
               ocf_quality_4q_avg, operating_margin_latest, operating_margin_pct_8q,
               gross_margin_std_4q, gross_margin_decline_2q, capital_alloc_ratio,
               pe_ttm, pb_mrq, peg_ratio, profit_yoy_trend_slope, price_recovery_trough,
               lynch_type, lynch_confidence, lynch_reason,
               sell_signal_score, sell_s1_score, sell_s1_label,
               sell_s2_score, sell_s2_label, sell_s3_score, sell_s3_label,
               sell_signal_reason
        FROM fact_stock_archetype
        WHERE snapshot_date = ?
    """, (snapshot_date,))
    conn.commit()
    logger.info(f"[股票类型] 构建完成: {inserted} 只股票, 快照 {snapshot_date}")
    return inserted

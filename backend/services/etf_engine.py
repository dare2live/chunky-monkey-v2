import logging
import asyncio
from datetime import datetime, timedelta
import math
from typing import List, Dict, Optional, Callable

from services.utils import safe_float as _safe_float, clamp as _clamp
from services.constants import (
    ETF_NON_INDUSTRY_CATS, ETF_INDUSTRY_MAP, ETF_CATEGORY_SORT_ORDER,
    ETF_FALLBACK_INDUSTRY, ETF_CROSS_BORDER_KW, ETF_COMMODITY_KW,
    ETF_BOND_KW, ETF_MONEY_KW, ETF_BROAD_KW,
)

# 用 cm-api logger，让 ETF 日志走入 routers/updater.py 的 _UILogHandler，
# 进而出现在工作台/ETF 页的实时日志面板
logger = logging.getLogger("cm-api")

# ETF 引擎：独立模块，不依赖 scoring.py
# 数据源：mootdx（通达信）→ akshare_client.fetch_etf_list / fetch_etf_kline
# 写入：dim_asset_universe（smartmoney.db） + price_kline（market_data.db）

# 进度回调签名：fn(stage: str, current: int, total: int, message: str)
ProgressCb = Optional[Callable[[str, int, int, str], None]]


def _mean(values):
    clean = [_safe_float(v) for v in values if _safe_float(v) is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _infer_etf_category(code: str, name: str) -> str:
    """根据代码和名称精确分类 ETF 类别，行业类进一步细分到具体行业"""
    n = (name or "").lower()
    if any(k in name for k in ETF_CROSS_BORDER_KW) or any(k in n for k in ETF_CROSS_BORDER_KW):
        return "跨境"
    if any(k in name for k in ETF_COMMODITY_KW):
        return "商品"
    if any(k in name for k in ETF_BOND_KW):
        return "债券"
    if any(k in name for k in ETF_MONEY_KW) or any(k in n for k in ETF_MONEY_KW):
        return "货币"
    if any(k in name for k in ETF_BROAD_KW):
        return "宽基"
    for keywords, industry_name in ETF_INDUSTRY_MAP:
        if any(k in name for k in keywords):
            return industry_name
    return ETF_FALLBACK_INDUSTRY


def _calc_amplitude_pct(highs: list, lows: list, window: int) -> Optional[float]:
    if len(highs) < window or len(lows) < window:
        return None
    high_values = [_safe_float(v) for v in highs[:window]]
    low_values = [_safe_float(v) for v in lows[:window]]
    high_values = [v for v in high_values if v is not None]
    low_values = [v for v in low_values if v is not None]
    if not high_values or not low_values:
        return None
    low = min(low_values)
    high = max(high_values)
    if low in (None, 0):
        return None
    return round((high - low) / low * 100, 2)


def _calc_amount_ratio(amounts: list, short: int = 5, long: int = 20) -> Optional[float]:
    if len(amounts) < long:
        return None
    short_avg = _mean(amounts[:short])
    long_avg = _mean(amounts[:long])
    if short_avg is None or long_avg in (None, 0):
        return None
    return round(short_avg / long_avg, 2)


def _classify_etf_setup(
    latest_close: Optional[float],
    ma10: Optional[float],
    ma20: Optional[float],
    ma50: Optional[float],
    amp_5d: Optional[float],
    amp_20d: Optional[float],
    amount_ratio_5_20: Optional[float],
) -> tuple[str, Optional[float]]:
    if latest_close in (None, 0) or ma20 in (None, 0):
        return "待补结构", None

    ma10_ok = ma10 is not None and latest_close >= ma10
    ma20_ok = ma20 is not None and latest_close >= ma20
    ma50_ok = ma50 is not None and latest_close >= ma50
    aligned = (
        ma10 is not None and ma20 is not None and ma50 is not None
        and ma10 >= ma20 >= ma50
    )
    contraction_ratio = None
    if amp_5d is not None and amp_20d not in (None, 0):
        contraction_ratio = amp_5d / amp_20d

    if aligned and ma10_ok and ma20_ok and contraction_ratio is not None and contraction_ratio <= 0.65:
        return "收敛待发", contraction_ratio
    if aligned and ma20_ok and contraction_ratio is not None and contraction_ratio <= 0.95:
        return "趋势跟随", contraction_ratio
    if ma20_ok and ma50_ok and amp_20d is not None and amp_20d <= 6:
        return "低波防守", contraction_ratio
    if (not ma20_ok) or (contraction_ratio is not None and contraction_ratio >= 1.15) or (amount_ratio_5_20 or 0) >= 1.8:
        return "结构松散", contraction_ratio
    return "震荡观察", contraction_ratio


def _classify_etf_strategy(row: Dict) -> tuple[str, str, Optional[float], float]:
    cat = row.get("category") or ""
    # 行业类包含具体行业名（医疗健康、半导体等）和"行业·其他"
    is_industry_like = cat not in ETF_NON_INDUSTRY_CATS
    trend = row.get("trend_status") or ""
    setup_state = row.get("setup_state") or ""
    rotation_bucket = row.get("rotation_bucket") or ""
    momentum_20d = _safe_float(row.get("momentum_20d")) or 0.0
    rel_12w = _safe_float(row.get("relative_strength_12w")) or 0.0
    volatility_20d = _safe_float(row.get("volatility_20d"))
    amplitude_20d = _safe_float(row.get("amplitude_20d"))
    drawdown_60d = _safe_float(row.get("max_drawdown_60d"))

    grid_score = 0.0
    if cat == "宽基" or is_industry_like:
        grid_score += 22
    if volatility_20d is not None and 12 <= volatility_20d <= 32:
        grid_score += 24
    elif volatility_20d is not None and 8 <= volatility_20d < 12:
        grid_score += 12
    if amplitude_20d is not None and 5 <= amplitude_20d <= 18:
        grid_score += 22
    elif amplitude_20d is not None and 18 < amplitude_20d <= 26:
        grid_score += 12
    if abs(momentum_20d) <= 12:
        grid_score += 16
    if trend == "震荡":
        grid_score += 8
    if setup_state == "结构松散":
        grid_score -= 16
    if rotation_bucket == "leader" and momentum_20d >= 12:
        grid_score -= 12
    if rotation_bucket == "blacklist" or trend == "空头":
        grid_score -= 26
    if drawdown_60d is not None and -18 <= drawdown_60d <= -4:
        grid_score += 10
    grid_score = round(_clamp(grid_score, 0, 100), 1)

    suggested_step = None
    if amplitude_20d is not None:
        suggested_step = round(_clamp(amplitude_20d / 6.0, 0.8, 4.5), 1)
    elif volatility_20d is not None:
        suggested_step = round(_clamp(volatility_20d / 8.0, 0.8, 4.5), 1)
    else:
        # 兜底：数据不足时给行业 ETF 典型步长，避免前端空白
        suggested_step = 1.5

    if cat in ("债券", "货币") and (volatility_20d is None or volatility_20d <= 12):
        return "防守停泊", "低波动资产，更适合防守配置和资金停泊。", None, grid_score
    if rotation_bucket == "blacklist" or (trend == "空头" and rel_12w < 0):
        return "暂不参与", "相对宽基偏弱或日线结构破坏，先回避等待轮动改善。", suggested_step, grid_score
    if (cat == "宽基" or is_industry_like) and trend == "多头" and rel_12w > 0 and setup_state in ("收敛待发", "趋势跟随"):
        return "趋势持有", "相对宽基走强且日线结构健康，更适合买入持有或趋势跟随。", suggested_step, grid_score
    if grid_score >= 60:
        return "网格候选", "波动和振幅适中，趋势不过热，更适合区间网格。", suggested_step, grid_score
    return "观察池", "当前轮动或结构都未完全到位，继续观察更合适。", suggested_step, grid_score


def _rotation_eligible(row: Dict) -> bool:
    cat = row.get("category") or ""
    # 行业类 ETF 才参与轮动（具体行业名或"行业·其他"都算行业类）
    non_industry = ("跨境", "商品", "债券", "货币", "宽基")
    if cat in non_industry:
        return False
    code = str(row.get("code") or "")
    valid_prefixes = ("159", "510", "511", "512", "513", "515", "516", "517", "518", "560", "561", "562", "563", "588")
    if not code.startswith(valid_prefixes):
        return False
    momentum_20d = _safe_float(row.get("momentum_20d"))
    momentum_60d = _safe_float(row.get("momentum_60d"))
    volatility_20d = _safe_float(row.get("volatility_20d"))
    amplitude_20d = _safe_float(row.get("amplitude_20d"))
    if momentum_20d is None or momentum_60d is None:
        return False
    if abs(momentum_20d) > 60 or abs(momentum_60d) > 90:
        return False
    if volatility_20d is not None and volatility_20d > 120:
        return False
    if amplitude_20d is not None and amplitude_20d > 80:
        return False
    return True


async def sync_etf_universe(conn, mkt_conn, sync_kline: bool = True,
                              kline_days: int = 120, max_etfs: int = None,
                              progress_cb: ProgressCb = None) -> Dict[str, int]:
    """
    从 mootdx 拉取 ETF 列表写入 dim_asset_universe，并可选地同步最近 N 天 K 线。

    progress_cb(stage, current, total, message) 在关键节点回调，让前端能展示进度条。

    返回 {"etf_count": N, "kline_etf_count": M, "kline_rows": K}
    """
    from services.akshare_client import fetch_etf_list, fetch_etf_kline
    from services.market_db import upsert_price_rows, update_sync_state

    def _progress(stage: str, current: int, total: int, message: str) -> None:
        if progress_cb:
            try:
                progress_cb(stage, current, total, message)
            except Exception:
                pass

    now = datetime.now().isoformat()
    logger.info("[ETF] 开始拉取 ETF 列表 ...")
    _progress("fetch_list", 0, 0, "拉取 ETF 列表")
    etf_list = await fetch_etf_list()
    if not etf_list:
        logger.warning("[ETF] mootdx 未返回 ETF 列表，跳过")
        _progress("done", 0, 0, "未获取到 ETF 列表")
        return {"etf_count": 0, "kline_etf_count": 0, "kline_rows": 0}

    if max_etfs:
        etf_list = etf_list[:max_etfs]
    total_etfs = len(etf_list)
    logger.info(f"[ETF] 共 {total_etfs} 只 ETF 待入库")
    _progress("fetch_list", total_etfs, total_etfs, f"获取到 {total_etfs} 只 ETF")

    # 1) 写入 dim_asset_universe
    etf_count = 0
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM dim_asset_universe WHERE asset_type = 'etf'")
        for e in etf_list:
            code = e.get("code")
            name = (e.get("name") or "").replace("\x00", "").strip()
            market = e.get("market") or ""
            if not code:
                continue
            category = _infer_etf_category(code, name)
            conn.execute("""
                INSERT INTO dim_asset_universe
                  (code, asset_type, name, market, category, is_active, updated_at)
                VALUES (?, 'etf', ?, ?, ?, 1, ?)
            """, (code, name, market, category, now))
            etf_count += 1
        conn.commit()
        logger.info(f"[ETF] 写入 dim_asset_universe: {etf_count} 只 ETF")
        _progress("write_universe", etf_count, total_etfs, f"已写入 {etf_count} 只 ETF")
    except Exception as e:
        conn.rollback()
        logger.error(f"[ETF] 写入资产池失败: {e}")
        _progress("error", 0, total_etfs, f"写入资产池失败：{e}")
        return {"etf_count": 0, "kline_etf_count": 0, "kline_rows": 0}

    # 2) 同步 K 线（可选）
    kline_etf_count = 0
    kline_rows = 0
    if sync_kline:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=kline_days * 2)  # 多拉点防节假日
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")
        batch_id = f"etf_sync_{end_dt.strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"[ETF] 开始同步 K 线 ({start_date}~{end_date}) ...")
        _progress("sync_kline", 0, total_etfs, f"开始同步 K 线 ({start_date}~{end_date})")
        log_every = max(10, total_etfs // 20)  # 大约 20 次进度日志
        for idx, e in enumerate(etf_list):
            code = e.get("code")
            if not code:
                continue
            try:
                df, src = await fetch_etf_kline(code, start_date, end_date)
                if df is None or df.empty:
                    continue
                rows = []
                for _, r in df.iterrows():
                    rows.append({
                        "code": code,
                        "date": r["date"],
                        "freq": "daily",
                        "adjust": "qfq",
                        "open": float(r["open"]) if r.get("open") is not None else None,
                        "high": float(r["high"]) if r.get("high") is not None else None,
                        "low": float(r["low"]) if r.get("low") is not None else None,
                        "close": float(r["close"]) if r.get("close") is not None else None,
                        "volume": float(r["volume"]) if r.get("volume") is not None else None,
                        "amount": float(r["amount"]) if r.get("amount") is not None else None,
                    })
                if rows:
                    upsert_price_rows(mkt_conn, rows, source=src or "mootdx", batch_id=batch_id)
                    kline_etf_count += 1
                    kline_rows += len(rows)
                    try:
                        update_sync_state(
                            mkt_conn, code, "daily",
                            source=src or "mootdx",
                            min_date=rows[0]["date"],
                            max_date=rows[-1]["date"],
                            row_count=len(rows),
                        )
                    except Exception:
                        pass
            except Exception as ex:
                logger.debug(f"[ETF] {code} K 线失败: {ex}")
                continue
            # 节流：每 50 只让出一次事件循环
            if idx % 50 == 49:
                await asyncio.sleep(0)
            # 周期性进度
            if (idx + 1) % log_every == 0 or (idx + 1) == total_etfs:
                logger.info(
                    f"[ETF] K 线进度 {idx + 1}/{total_etfs}（成功 {kline_etf_count}, 行数 {kline_rows}）"
                )
                _progress("sync_kline", idx + 1, total_etfs,
                          f"K 线进度 {idx + 1}/{total_etfs}（成功 {kline_etf_count}）")

        logger.info(f"[ETF] K 线同步完成: {kline_etf_count}/{etf_count} 只, 共 {kline_rows} 行")
        _progress("sync_kline", total_etfs, total_etfs,
                  f"K 线同步完成 {kline_etf_count}/{etf_count} 只 / {kline_rows} 行")

    _progress("done", total_etfs, total_etfs,
              f"完成：ETF {etf_count} / K 线 {kline_etf_count} / 行数 {kline_rows}")
    return {
        "etf_count": etf_count,
        "kline_etf_count": kline_etf_count,
        "kline_rows": kline_rows,
    }


def calc_etf_momentum(conn, mkt_conn) -> List[Dict]:
    """
    基于 market_data.db 的 K 线计算 ETF 动量指标。
    简单口径：20 日收益率 → 转 0~100 分；趋势状态由动量分档。
    """
    etfs = conn.execute(
        "SELECT code, name, category FROM dim_asset_universe "
        "WHERE asset_type = 'etf' AND is_active = 1"
    ).fetchall()
    results = []

    for row in etfs:
        code = row["code"]
        name = row["name"]
        cat = _infer_etf_category(code, name)
        prices_60 = mkt_conn.execute("""
            SELECT date, high, low, close, amount FROM price_kline
            WHERE code = ? AND freq = 'daily' ORDER BY date DESC LIMIT 60
        """, (code,)).fetchall()

        momentum = 0.0
        momentum_60d = 0.0
        volatility_20d = None
        drawdown_60d = None
        ma10 = None
        ma20 = None
        ma50 = None
        amplitude_5d = None
        amplitude_20d = None
        amount_ratio_5_20 = None
        setup_state = "待补结构"
        contraction_ratio = None
        trend = "震荡"
        if len(prices_60) >= 20:
            closes_20 = [_safe_float(item["close"]) for item in prices_60[:20]]
            closes_20 = [item for item in closes_20 if item is not None]
            p0 = closes_20[0] if closes_20 else None
            pn = closes_20[-1] if closes_20 else None
            if pn and pn > 0:
                momentum = (p0 - pn) / pn * 100
                if momentum > 5:
                    trend = "多头"
                elif momentum < -5:
                    trend = "空头"
            if len(closes_20) >= 2:
                rets = []
                for i in range(len(closes_20) - 1):
                    prev_close = closes_20[i + 1]
                    cur_close = closes_20[i]
                    if prev_close and prev_close > 0:
                        rets.append((cur_close - prev_close) / prev_close)
                if rets:
                    mean_ret = sum(rets) / len(rets)
                    variance = sum((r - mean_ret) ** 2 for r in rets) / len(rets)
                    volatility_20d = round(math.sqrt(variance) * math.sqrt(252) * 100, 2)
        if len(prices_60) >= 20:
            closes_60 = [_safe_float(item["close"]) for item in prices_60]
            closes_60 = [item for item in closes_60 if item is not None]
            p0_60 = closes_60[0] if closes_60 else None
            pn_60 = closes_60[-1] if closes_60 else None
            if pn_60 and pn_60 > 0:
                momentum_60d = (p0_60 - pn_60) / pn_60 * 100
            if closes_60:
                peak = max(closes_60)
                latest = closes_60[0]
                if peak and peak > 0 and latest is not None:
                    drawdown_60d = round((latest - peak) / peak * 100, 2)
                ma10 = _mean(closes_60[:10]) if len(closes_60) >= 10 else None
                ma20 = _mean(closes_60[:20]) if len(closes_60) >= 20 else None
                ma50 = _mean(closes_60[:50]) if len(closes_60) >= 50 else None

            highs_60 = [_safe_float(item["high"]) for item in prices_60]
            lows_60 = [_safe_float(item["low"]) for item in prices_60]
            amounts_60 = [_safe_float(item["amount"]) for item in prices_60]
            amplitude_5d = _calc_amplitude_pct(highs_60, lows_60, 5)
            amplitude_20d = _calc_amplitude_pct(highs_60, lows_60, 20)
            amount_ratio_5_20 = _calc_amount_ratio(amounts_60, 5, 20)
            setup_state, contraction_ratio = _classify_etf_setup(
                closes_60[0] if closes_60 else None,
                ma10,
                ma20,
                ma50,
                amplitude_5d,
                amplitude_20d,
                amount_ratio_5_20,
            )

        results.append({
            "code": code,
            "name": name,
            "category": cat,
            "momentum_20d": round(momentum, 2),
            "momentum_60d": round(momentum_60d, 2),
            "volatility_20d": volatility_20d,
            "max_drawdown_60d": drawdown_60d,
            "ma10": round(ma10, 2) if ma10 is not None else None,
            "ma20": round(ma20, 2) if ma20 is not None else None,
            "ma50": round(ma50, 2) if ma50 is not None else None,
            "amplitude_5d": amplitude_5d,
            "amplitude_20d": amplitude_20d,
            "amount_ratio_5_20": amount_ratio_5_20,
            "setup_state": setup_state,
            "contraction_ratio": round(contraction_ratio, 2) if contraction_ratio is not None else None,
            "trend_status": trend,
            "score": round(min(max(momentum * 5 + 50, 0), 100), 1),
        })

    broad_rows = [
        item for item in results
        if item.get("category") == "宽基"
        and _safe_float(item.get("momentum_20d")) is not None
        and _safe_float(item.get("momentum_60d")) is not None
    ]
    benchmark_4w = _mean([item.get("momentum_20d") for item in broad_rows]) or 0.0
    benchmark_12w = _mean([item.get("momentum_60d") for item in broad_rows]) or 0.0
    industry_rows = []
    for item in results:
        rel_4w = None
        rel_12w = None
        if _safe_float(item.get("momentum_20d")) is not None:
            rel_4w = round(float(item["momentum_20d"]) - benchmark_4w, 2)
        if _safe_float(item.get("momentum_60d")) is not None:
            rel_12w = round(float(item["momentum_60d"]) - benchmark_12w, 2)
        item["relative_strength_4w"] = rel_4w
        item["relative_strength_12w"] = rel_12w
        item["rotation_score"] = None
        item["rotation_rank"] = None
        item["rotation_bucket"] = None
        item["benchmark_family"] = "宽基ETF平均基准"
        if _rotation_eligible(item) and rel_4w is not None and rel_12w is not None:
            industry_rows.append(item)

    industry_rows_sorted = sorted(
        industry_rows,
        key=lambda item: (
            -((float(item.get("relative_strength_4w") or 0.0) * 0.55)
              + (float(item.get("relative_strength_12w") or 0.0) * 0.45)
              + ((float(item.get("score") or 50.0) - 50.0) * 0.08)),
            -(item.get("momentum_20d") or 0),
            item.get("name") or "",
        ),
    )
    edge_n = 3 if len(industry_rows_sorted) >= 9 else 2 if len(industry_rows_sorted) >= 5 else 1 if industry_rows_sorted else 0
    total_industry = len(industry_rows_sorted)
    for idx, item in enumerate(industry_rows_sorted, start=1):
        if total_industry <= 1:
            rotation_score = 50.0
        else:
            rotation_score = round(100.0 - ((idx - 1) / (total_industry - 1)) * 100.0, 1)
        bucket = "neutral"
        if edge_n and idx <= edge_n:
            bucket = "leader"
        elif edge_n and idx > total_industry - edge_n:
            bucket = "blacklist"
        item["rotation_score"] = rotation_score
        item["rotation_rank"] = idx
        item["rotation_bucket"] = bucket

    for item in results:
        strategy_type, strategy_reason, grid_step_pct, grid_score = _classify_etf_strategy(item)
        item["strategy_type"] = strategy_type
        item["strategy_reason"] = strategy_reason
        item["grid_step_pct"] = grid_step_pct
        item["grid_score"] = grid_score

    results.sort(
        key=lambda x: (
            ETF_CATEGORY_SORT_ORDER.get(x.get("category") or "", 1),
            0 if x.get("rotation_bucket") == "leader" else 1 if x.get("rotation_bucket") == "neutral" else 2,
            -(x.get("rotation_score") or 0),
            -(x.get("score") or 0),
            -(x.get("momentum_20d") or 0),
        )
    )
    return results


def calc_etf_overview(rows: List[Dict]) -> Dict:
    rows = rows or []
    if not rows:
        return {
            "market_state": "待同步",
            "temperature_label": "未知",
            "temperature_score": None,
            "regime_label": "暂无判断",
            "regime_reason": "ETF 数据尚未同步，无法判断整体环境。",
            "action_hint": "请先同步 ETF 数据。",
            "macro_scenario": "低频流动性情景未激活",
            "macro_note": "需要先有可用的宽基 ETF 温度数据，才能叠加低频宏观解释。",
            "broad_count": 0,
            "positive_20d_ratio": None,
            "avg_momentum_20d": None,
            "avg_momentum_60d": None,
            "avg_volatility_20d": None,
            "avg_drawdown_60d": None,
        }

    broad_rows = [row for row in rows if row.get("category") == "宽基"]
    sample = broad_rows if broad_rows else rows

    def _avg(key: str):
        values = [_safe_float(item.get(key)) for item in sample]
        values = [item for item in values if item is not None]
        return round(sum(values) / len(values), 2) if values else None

    broad_count = len(sample)
    positive_20d = sum(1 for item in sample if (_safe_float(item.get("momentum_20d")) or 0) > 0)
    trend_up = sum(1 for item in sample if item.get("trend_status") == "多头")
    breadth_ratio = round(positive_20d * 100.0 / broad_count, 2) if broad_count else None
    avg_mom20 = _avg("momentum_20d")
    avg_mom60 = _avg("momentum_60d")
    avg_vol20 = _avg("volatility_20d")
    avg_dd60 = _avg("max_drawdown_60d")
    benchmark_4w = _avg("momentum_20d") or 0.0
    benchmark_12w = _avg("momentum_60d") or 0.0

    temp_score = 50.0
    if avg_mom20 is not None:
        temp_score += _clamp(avg_mom20 * 2.5, -22, 22)
    if avg_mom60 is not None:
        temp_score += _clamp(avg_mom60 * 0.8, -15, 15)
    if breadth_ratio is not None:
        temp_score += _clamp((breadth_ratio - 50.0) * 0.35, -12, 12)
    if avg_dd60 is not None:
        temp_score += _clamp(avg_dd60 * 0.4, -10, 4)
    if avg_vol20 is not None and avg_vol20 > 28:
        temp_score -= min((avg_vol20 - 28) * 0.7, 12)
    temp_score = round(_clamp(temp_score, 0, 100), 1)

    if temp_score <= 32:
        temperature_label = "过冷"
        market_state = "panic"
        regime_label = "恐慌待托底"
        regime_reason = "宽基 ETF 广度与动量显著转弱，市场已进入过冷区，更像在等待新的托底确认。"
        action_hint = "不急着全面抄底，优先等宽基止跌和趋势重新确认。"
    elif temp_score <= 48:
        temperature_label = "降温"
        market_state = "cooling"
        regime_label = "降温观察期"
        regime_reason = "市场温度正在回落，说明风险偏好降温更明显，此时不适合追宽基 Beta。"
        action_hint = "以观察为主，等更冷或重新转强后再提高仓位。"
    elif temp_score >= 72 and (breadth_ratio or 0) >= 65:
        temperature_label = "偏热"
        market_state = "heated"
        regime_label = "兑现降温期"
        regime_reason = "宽基 ETF 已处于明显升温区，若叠加低频流动性降温假设，更像兑现阶段而不是新一轮启动。"
        action_hint = "控制追高，更多看兑现与轮动，不把宽基当无条件加仓对象。"
    else:
        temperature_label = "修复"
        market_state = "recovering"
        regime_label = "趋势恢复期"
        regime_reason = "宽基动量和广度正在修复，说明环境由降温向恢复切换，但还没到无差别 risk-on。"
        action_hint = "允许逐步恢复跟踪和试探仓位，再由排序层决定先配谁。"

    macro_note = (
        "低频宏观层把‘国家队阶段性兑现、等待下一次托底窗口’视作情景假设，不当作自动交易事实。"
        "它的作用是约束追高和解释过冷区的潜在缓冲，而不是替代市场温度和排序信号。"
    )

    industry_rows = [item for item in rows if item.get("category") not in ETF_NON_INDUSTRY_CATS]
    leader_rows = [item for item in industry_rows if item.get("rotation_bucket") == "leader"][:3]
    laggard_rows = [item for item in sorted(industry_rows, key=lambda item: item.get("rotation_rank") or 999) if item.get("rotation_bucket") == "blacklist"][:3]
    strategy_counts = {
        "grid": sum(1 for item in rows if item.get("strategy_type") == "网格候选"),
        "trend": sum(1 for item in rows if item.get("strategy_type") == "趋势持有"),
        "defensive": sum(1 for item in rows if item.get("strategy_type") == "防守停泊"),
        "avoid": sum(1 for item in rows if item.get("strategy_type") == "暂不参与"),
    }

    def _pack_watchlist(items: list[Dict]) -> list[Dict]:
        packed = []
        for item in items:
            packed.append({
                "code": item.get("code"),
                "name": item.get("name"),
                "rotation_score": item.get("rotation_score"),
                "relative_strength_4w": item.get("relative_strength_4w"),
                "relative_strength_12w": item.get("relative_strength_12w"),
                "setup_state": item.get("setup_state"),
                "strategy_type": item.get("strategy_type"),
                "grid_step_pct": item.get("grid_step_pct"),
            })
        return packed

    return {
        "market_state": market_state,
        "temperature_label": temperature_label,
        "temperature_score": temp_score,
        "regime_label": regime_label,
        "regime_reason": regime_reason,
        "action_hint": action_hint,
        "macro_scenario": "低频流动性降温假设",
        "macro_note": macro_note,
        "broad_count": broad_count,
        "positive_20d_ratio": breadth_ratio,
        "trend_up_count": trend_up,
        "avg_momentum_20d": avg_mom20,
        "avg_momentum_60d": avg_mom60,
        "avg_volatility_20d": avg_vol20,
        "avg_drawdown_60d": avg_dd60,
        "rotation_rule": "行业 ETF 先看相对宽基 4/12 周强弱，Top3 重点关注，Bottom3 直接回避。",
        "rotation_benchmark_4w": round(benchmark_4w, 2),
        "rotation_benchmark_12w": round(benchmark_12w, 2),
        "rotation_leaders": _pack_watchlist(leader_rows),
        "rotation_laggards": _pack_watchlist(laggard_rows),
        "strategy_counts": strategy_counts,
    }

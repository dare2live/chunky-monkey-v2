"""
AKShare 数据获取客户端

函数：月K线、日K线、交易日历、申万行业分类。

说明：
- K 线优先走东财；失败后自动回退新浪 / 腾讯
- 缺失股票拉全历史，已存在股票走增量续拉
- 行业检测用真实 AKShare 接口，不再只测无关 URL
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

logger = logging.getLogger("cm-api")

# 禁用代理，避免 akshare (requests) 走系统代理导致连接失败
os.environ.pop("http_proxy", None)
os.environ.pop("https_proxy", None)
os.environ.pop("HTTP_PROXY", None)
os.environ.pop("HTTPS_PROXY", None)
os.environ.pop("all_proxy", None)
os.environ.pop("ALL_PROXY", None)
os.environ["NO_PROXY"] = "*"


class NetworkError(Exception):
    pass


def _looks_like_empty_payload_error(err: Exception) -> bool:
    text = str(err or "")
    markers = [
        "Length mismatch",
        "Expected axis has 0 elements",
        "new values have 6 elements",
        "Columns must be same length as key",
        "No tables found",
    ]
    return any(marker in text for marker in markers)


def _market_symbol(code: str) -> str:
    text = str(code or "").strip()
    return f"sh{text}" if text.startswith("6") else f"sz{text}"


async def _safe_akshare_call(func, *args, timeout=30, retries=2, **kwargs):
    """带重试和超时的 akshare 调用"""
    last_err = None
    for attempt in range(retries + 1):
        try:
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, lambda: func(*args, **kwargs)),
                timeout=timeout
            )
            if result is None or (isinstance(result, pd.DataFrame) and result.empty):
                return None
            return result
        except asyncio.TimeoutError:
            last_err = TimeoutError(f"超时 ({timeout}s)")
            logger.debug(f"[akshare] {func.__name__} 超时 ({attempt+1}/{retries+1})")
        except Exception as e:
            last_err = e
            err_str = str(e)
            if "网络" in err_str or "connect" in err_str.lower() or "timeout" in err_str.lower():
                last_err = NetworkError(err_str)
            logger.debug(f"[akshare] {func.__name__} 失败 ({attempt+1}): {e}")

        if attempt < retries:
            await asyncio.sleep(2 * (attempt + 1))

    if last_err:
        raise last_err
    return None


def _normalize_price_frame(df, source: str):
    """统一 K 线 DataFrame 列名为 date/open/high/low/close/volume/amount"""
    if df is None or df.empty:
        return None
    frame = df.copy()

    if source == "eastmoney":
        frame = frame.rename(columns={
            "日期": "date",
            "开盘": "open",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume",
            "成交额": "amount",
        })
    elif source == "sina":
        pass
    elif source == "tx":
        # 腾讯接口通常只有 amount，无 volume
        if "volume" not in frame.columns:
            frame["volume"] = None
    required = ["date", "open", "high", "low", "close"]
    if not all(col in frame.columns for col in required):
        return None
    for col in ["volume", "amount"]:
        if col not in frame.columns:
            frame[col] = None
    frame = frame[["date", "open", "high", "low", "close", "volume", "amount"]].copy()
    frame["date"] = frame["date"].astype(str).str[:10]
    return frame


def _aggregate_monthly_from_daily(df: pd.DataFrame):
    """从日 K 聚合月 K"""
    if df is None or df.empty:
        return None
    frame = df.copy()
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.sort_values("date")
    frame["month"] = frame["date"].dt.to_period("M")

    rows = []
    for _, group in frame.groupby("month", sort=True):
        group = group.sort_values("date")
        rows.append({
            "date": group.iloc[0]["date"].strftime("%Y-%m-01"),
            "open": group.iloc[0]["open"],
            "high": group["high"].max(),
            "low": group["low"].min(),
            "close": group.iloc[-1]["close"],
            "volume": group["volume"].sum(min_count=1),
            "amount": group["amount"].sum(min_count=1),
        })
    return pd.DataFrame(rows)


def _build_path_map_from_cninfo_tree(tree_df: pd.DataFrame):
    """把 cninfo 申万分类树转换成 {code: [l1,l2,l3]}"""
    if tree_df is None or tree_df.empty:
        return {}
    rows = {}
    for _, row in tree_df.iterrows():
        code = str(row.get("类目编码") or "").strip()
        if not code:
            continue
        rows[code] = {
            "name": str(row.get("类目名称") or "").strip(),
            "parent": str(row.get("父类编码") or "").strip(),
            "level": int(row.get("分级") or 0),
        }
    path_map = {}
    for code in rows:
        current = code
        path = []
        seen = set()
        while current and current in rows and current not in seen:
            seen.add(current)
            item = rows[current]
            if item["level"] > 0 and item["name"]:
                path.append(item["name"])
            current = item["parent"]
        if path:
            path_map[code] = list(reversed(path))
            if code.startswith("S"):
                path_map[code[1:]] = path_map[code]
    return path_map


async def _fetch_daily_mootdx(code: str, start_date: str, end_date: str):
    """用 mootdx 从通达信服务器获取日K线（首选数据源）"""
    try:
        from mootdx.quotes import Quotes
        # 计算需要拉取的天数（粗略估算，多拉一些没关系，后面会按日期过滤）
        from datetime import datetime, timedelta
        try:
            start_dt = datetime.strptime(start_date[:8], "%Y%m%d")
            end_dt = datetime.strptime(end_date[:8], "%Y%m%d")
            days_needed = max((end_dt - start_dt).days + 30, 150)
        except Exception:
            days_needed = 800

        def _fetch():
            client = Quotes.factory(market='std', multithread=False, heartbeat=False,
                                    server='119.147.212.81:7709')
            try:
                df = client.bars(symbol=code, frequency=9, offset=min(days_needed, 800))
                return df
            finally:
                try:
                    client.close()
                except Exception:
                    pass

        df = await asyncio.get_event_loop().run_in_executor(None, _fetch)
        if df is None or df.empty:
            return None, None
        # 统一列名（mootdx 既有 vol 又有 volume，rename 会产生重复列；先去重）
        df = df.rename(columns={"vol": "volume"})
        df = df.loc[:, ~df.columns.duplicated()]
        # 日期过滤
        start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        df["date"] = df.index.strftime("%Y-%m-%d") if hasattr(df.index, 'strftime') else df["datetime"].astype(str).str[:10]
        df = df[(df["date"] >= start_fmt) & (df["date"] <= end_fmt)]
        if df.empty:
            return None, None
        # 确保列齐全
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col not in df.columns:
                df[col] = None
        return df[["date", "open", "high", "low", "close", "volume", "amount"]], "mootdx"
    except ImportError:
        return None, None
    except Exception as e:
        logger.debug(f"[mootdx] {code} 失败: {e}")
        return None, None


async def _fetch_daily_with_fallback(code: str, start_date: str, end_date: str):
    # 优先级1: mootdx（通达信服务器，Mac原生）
    df_m, src_m = await _fetch_daily_mootdx(code, start_date, end_date)
    if df_m is not None and not df_m.empty:
        return df_m, src_m

    # 优先级2-4: AKShare 降级链
    import akshare as ak

    attempts = [
        (
            "eastmoney",
            ak.stock_zh_a_hist,
            {
                "symbol": code,
                "period": "daily",
                "start_date": start_date,
                "end_date": end_date,
                "adjust": "qfq",
            },
        ),
        (
            "sina",
            ak.stock_zh_a_daily,
            {
                "symbol": _market_symbol(code),
                "start_date": start_date,
                "end_date": end_date,
                "adjust": "qfq",
            },
        ),
        (
            "tx",
            ak.stock_zh_a_hist_tx,
            {
                "symbol": _market_symbol(code),
                "start_date": start_date,
                "end_date": end_date,
                "adjust": "qfq",
            },
        ),
    ]

    last_err = None
    empty_sources = []
    for source, func, kwargs in attempts:
        try:
            df = await _safe_akshare_call(func, timeout=30, retries=1, **kwargs)
            norm = _normalize_price_frame(df, source)
            if norm is not None and not norm.empty:
                from services.api_schemas import KLineDailyRow
                from pydantic import TypeAdapter, ValidationError
                try:
                    records = norm.to_dict('records')
                    TypeAdapter(list[KLineDailyRow]).validate_python(records)
                    return norm, source
                except ValidationError as e:
                    logger.error(f"[日K fallback] {source} 防腐层截断 - Schema校验失败: {e}")
                    # Force fallback by treating it as an empty or erroneous payload
                    last_err = ValueError(f"{source}: schema validation failed")
                    empty_sources.append(source)
                    continue
            empty_sources.append(source)
            last_err = ValueError(f"{source}: empty")
        except Exception as e:
            if _looks_like_empty_payload_error(e):
                empty_sources.append(source)
                last_err = ValueError(f"{source}: empty")
            else:
                last_err = e
            logger.debug(f"[日K fallback] {code} {source} 失败: {e}")

    if empty_sources and len(empty_sources) == len(attempts):
        raise ValueError("all_sources_empty(eastmoney/sina/tx)")
    if last_err:
        raise last_err
    return None, ""


async def fetch_stock_kline_monthly(code: str, limit: int = 36,
                                    start_date: str = "20230101",
                                    end_date: Optional[str] = None):
    """获取月K线。东财失败时回退到日K聚合月K。"""
    import akshare as ak

    end_date = end_date or datetime.now().strftime("%Y%m%d")

    try:
        df = await _safe_akshare_call(
            ak.stock_zh_a_hist,
            symbol=code,
            period="monthly",
            start_date=start_date,
            end_date=end_date,
            adjust="qfq",
            timeout=30,
        )
        norm = _normalize_price_frame(df, "eastmoney")
        if norm is not None and not norm.empty:
            from services.api_schemas import KLineDailyRow
            from pydantic import TypeAdapter, ValidationError
            try:
                tail_df = norm.tail(limit)
                TypeAdapter(list[KLineDailyRow]).validate_python(tail_df.to_dict('records'))
                return tail_df, "eastmoney"
            except ValidationError as e:
                logger.error(f"[月K] eastmoney 防腐层截断 - Schema校验失败: {e}")
                # Fall back implicitly
    except Exception as e:
        logger.debug(f"[月K] {code} eastmoney 失败: {e}")

    try:
        daily_df, source = await _fetch_daily_with_fallback(code, start_date, end_date)
        monthly = _aggregate_monthly_from_daily(daily_df)
        if monthly is not None and not monthly.empty:
            return monthly.tail(limit), f"{source}_derived_monthly"
    except Exception as e:
        logger.warning(f"[月K] {code}: {e}")
    return None, ""


async def fetch_stock_kline_daily(code: str, days: int = 150,
                                  start_date: Optional[str] = None,
                                  end_date: Optional[str] = None):
    """获取日K线。缺失股票拉全历史，失败时自动回退新浪 / 腾讯。"""
    end_date = end_date or datetime.now().strftime("%Y%m%d")
    start = start_date or (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")

    try:
        df, source = await _fetch_daily_with_fallback(code, start, end_date)
        if df is not None and not df.empty:
            return df, source
    except Exception as e:
        logger.warning(f"[日K] {code}: {e}")
    return None, ""


async def test_kline_availability(sample_code: str = "000001") -> tuple[bool, str]:
    """测试 K 线源可用性；任一源可返回有效数据即算可用。"""
    try:
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=15)).strftime("%Y%m%d")
        _, source = await _fetch_daily_with_fallback(sample_code, start_date, end_date)
        return bool(source), source
    except Exception:
        return False, ""


async def test_industry_availability() -> tuple[bool, str]:
    """测试申万行业源可用性；直接测真实 akshare 接口。"""
    import akshare as ak

    attempts = [
        ("sw_hist", lambda: ak.stock_industry_clf_hist_sw().head(1)),
        ("sw_l1", lambda: ak.sw_index_first_info().head(1)),
    ]
    for source, func in attempts:
        try:
            df = await _safe_akshare_call(func, timeout=20, retries=0)
            if df is not None and not df.empty:
                return True, source
        except Exception:
            continue
    return False, ""


async def fetch_sw_industry_all():
    """获取申万三级行业分类（全市场股票-行业映射）

    策略：
    1. 获取一二三级行业列表，建立层级映射（三级→二级→一级）
    2. 遍历三级行业获取成分股
    3. 用申万历史归属表补齐“当前成分股不包含”的老票/边缘票
    4. 通过层级映射填充一二级行业名
    5. stock_code 去掉 .SH/.SZ 后缀，与 inst_holdings 格式一致

    返回 list[dict]，每个 dict: {stock_code, sw_level1, sw_level2, sw_level3, sw_code}
    """
    import akshare as ak

    results = []

    # 1. 建立行业层级映射
    logger.info("[行业] 获取申万行业层级...")
    try:
        df1 = await _safe_akshare_call(ak.sw_index_first_info, timeout=30)
        df2 = await _safe_akshare_call(ak.sw_index_second_info, timeout=30)
        df3 = await _safe_akshare_call(ak.sw_index_third_info, timeout=30)
        sw_tree_df = await _safe_akshare_call(
            ak.stock_industry_category_cninfo,
            symbol="申银万国行业分类标准",
            timeout=30,
        )
        if sw_tree_df is not None and not sw_tree_df.empty:
            from services.api_schemas import SWIndustryTreeRow
            from pydantic import TypeAdapter, ValidationError
            try:
                TypeAdapter(list[SWIndustryTreeRow]).validate_python(sw_tree_df.to_dict('records'))
            except ValidationError as e:
                logger.error(f"[行业] 防腐层截断 - cninfo 行业树Schema验证失败: {e}")
                sw_tree_df = None
    except Exception as e:
        logger.error(f"[行业] 获取行业层级失败: {e}")
        return results

    if df3 is None or df3.empty:
        logger.warning("[行业] 三级行业列表为空")
        return results

    # 一级：{行业名: 行业名} (自身)
    level1_names = set(df1["行业名称"].tolist()) if df1 is not None else set()

    # 二级→一级映射：{二级名: 一级名}
    l2_to_l1 = {}
    if df2 is not None:
        for _, r in df2.iterrows():
            l2_to_l1[str(r.get("行业名称", "")).strip()] = str(r.get("上级行业", "")).strip()

    # 三级→二级映射：{三级名: 二级名}
    l3_to_l2 = {}
    code_to_l3 = {}
    for _, r in df3.iterrows():
        l3_name = str(r.get("行业名称", "")).strip()
        l3_to_l2[l3_name] = str(r.get("上级行业", "")).strip()
        code_to_l3[str(r.get("行业代码", "")).strip()] = l3_name

    codes = df3["行业代码"].tolist()
    names = df3["行业名称"].tolist()
    logger.info(f"[行业] 共 {len(codes)} 个三级行业，层级映射已建立 (一级{len(level1_names)}，二级{len(l2_to_l1)}，三级{len(l3_to_l2)})")
    path_map = _build_path_map_from_cninfo_tree(sw_tree_df)

    # 2. 逐个三级行业获取当前成分股
    seen = set()
    for i, (sw_code, l3_name) in enumerate(zip(codes, names)):
        try:
            df = await _safe_akshare_call(
                ak.sw_index_third_cons, symbol=str(sw_code),
                timeout=20, retries=1
            )
            if df is None or df.empty:
                continue

            from services.api_schemas import SWIndustryRow
            from pydantic import TypeAdapter, ValidationError
            try:
                TypeAdapter(list[SWIndustryRow]).validate_python(df.to_dict('records'))
            except ValidationError as e:
                logger.error(f"[行业] {sw_code} 防腐层截断 - Schema验证失败: {e}")
                continue

            # 通过层级映射查找一二级
            l3 = str(l3_name).strip()
            l2 = l3_to_l2.get(l3, "")
            l1 = l2_to_l1.get(l2, "")

            for _, row in df.iterrows():
                raw_code = str(row.get("股票代码", "")).strip()
                if not raw_code:
                    continue
                # 去掉 .SH/.SZ 后缀，与 inst_holdings 的 stock_code 格式一致
                stock_code = raw_code.split(".")[0] if "." in raw_code else raw_code
                if stock_code in seen:
                    continue
                seen.add(stock_code)
                results.append({
                    "stock_code": stock_code,
                    "sw_level1": l1,
                    "sw_level2": l2,
                    "sw_level3": l3,
                    "sw_code": str(sw_code),
                })

            if (i + 1) % 20 == 0:
                logger.info(f"[行业] 进度: {i+1}/{len(codes)}, 已获取 {len(results)} 只股票")
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.debug(f"[行业] {sw_code} 失败: {e}")
            await asyncio.sleep(1)

    current_count = len(results)

    # 3. 用申万历史归属补齐非当前成分股
    try:
        hist_df = await _safe_akshare_call(ak.stock_industry_clf_hist_sw, timeout=45, retries=1)
        if hist_df is not None and not hist_df.empty:
            hist_df = hist_df.copy()
            hist_df["symbol6"] = hist_df["symbol"].astype(str).str.zfill(6)
            hist_df["start_date"] = hist_df["start_date"].astype(str)
            hist_df["update_time"] = hist_df["update_time"].astype(str)
            hist_df = hist_df.sort_values(["symbol6", "start_date", "update_time"])
            latest_hist = hist_df.groupby("symbol6", as_index=False).tail(1)

            hist_added = 0
            for _, row in latest_hist.iterrows():
                stock_code = str(row.get("symbol6") or "").strip()
                if not stock_code or stock_code in seen:
                    continue
                industry_code = str(row.get("industry_code") or "").strip()
                path = path_map.get(industry_code) or path_map.get(f"S{industry_code}") or []
                if len(path) < 3:
                    continue
                l1, l2, l3 = path[0], path[1], path[2]
                seen.add(stock_code)
                results.append({
                    "stock_code": stock_code,
                    "sw_level1": l1,
                    "sw_level2": l2,
                    "sw_level3": l3,
                    "sw_code": industry_code,
                })
                hist_added += 1
            logger.info(f"[行业] 历史归属补齐: +{hist_added} 只股票")
    except Exception as e:
        logger.debug(f"[行业] 历史归属补齐失败: {e}")

    logger.info(f"[行业] 完成: {len(results)} 只股票的行业分类（当前成分股 {current_count}）")
    return results


# ============================================================
# ETF / 指数 K 线
# ============================================================

async def fetch_etf_list() -> list[dict]:
    """获取 ETF 列表（通过 mootdx）"""
    def _fetch():
        try:
            from mootdx.quotes import Quotes
            client = Quotes.factory(market='std', multithread=False, heartbeat=False,
                                    server='119.147.212.81:7709')
            results = []
            try:
                for market in [0, 1]:  # 0=深市, 1=沪市
                    stocks = client.stocks(market=market)
                    if stocks is not None and not stocks.empty:
                        for _, row in stocks.iterrows():
                            code = str(row.get("code", "")).strip()
                            name = str(row.get("name", "")).strip()
                            # ETF 规则: 深市 15xxxx, 沪市 51xxxx/56xxxx/58xxxx/588xxx
                            if market == 0 and code.startswith("15"):
                                results.append({"code": code, "name": name, "market": "sz", "asset_type": "etf"})
                            elif market == 1 and (code.startswith("51") or code.startswith("56") or code.startswith("58")):
                                results.append({"code": code, "name": name, "market": "sh", "asset_type": "etf"})
            finally:
                try:
                    client.close()
                except Exception:
                    pass
            return results
        except ImportError:
            return []
        except Exception as e:
            logger.error(f"[ETF] 获取列表失败: {e}")
            return []

    return await asyncio.get_event_loop().run_in_executor(None, _fetch)


async def fetch_etf_kline(code: str, start_date: str, end_date: str):
    """获取 ETF K 线（复用 mootdx bars）"""
    return await _fetch_daily_mootdx(code, start_date, end_date)


async def fetch_index_kline(code: str, start_date: str, end_date: str):
    """获取指数 K 线（通过 mootdx index_bars）"""
    try:
        from mootdx.quotes import Quotes
        from datetime import datetime

        try:
            start_dt = datetime.strptime(start_date[:8], "%Y%m%d")
            end_dt = datetime.strptime(end_date[:8], "%Y%m%d")
            days_needed = max((end_dt - start_dt).days + 30, 150)
        except Exception:
            days_needed = 800

        # 判断市场: 上证指数 sh, 深证指数 sz
        if code.startswith("0") or code.startswith("3"):
            mkt = 1  # 沪市指数
        else:
            mkt = 0  # 深市指数

        def _fetch():
            client = Quotes.factory(market='std', multithread=False, heartbeat=False,
                                    server='119.147.212.81:7709')
            try:
                df = client.index_bars(frequency=9, market=mkt, symbol=code,
                                       offset=min(days_needed, 800))
                return df
            finally:
                try:
                    client.close()
                except Exception:
                    pass

        df = await asyncio.get_event_loop().run_in_executor(None, _fetch)
        if df is None or df.empty:
            return None, None

        df = df.rename(columns={"vol": "volume"})
        start_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"
        df["date"] = df.index.strftime("%Y-%m-%d") if hasattr(df.index, 'strftime') else df["datetime"].astype(str).str[:10]
        df = df[(df["date"] >= start_fmt) & (df["date"] <= end_fmt)]
        if df.empty:
            return None, None
        for col in ["open", "high", "low", "close", "volume", "amount"]:
            if col not in df.columns:
                df[col] = None
        return df[["date", "open", "high", "low", "close", "volume", "amount"]], "mootdx_index"
    except ImportError:
        return None, None
    except Exception as e:
        logger.debug(f"[指数] {code} 失败: {e}")
        return None, None


async def fetch_trading_calendar():
    """获取交易日历"""
    import akshare as ak
    import datetime

    df = await _safe_akshare_call(ak.tool_trade_date_hist_sina, timeout=15)
    if df is None:
        return []

    cutoff = datetime.date(2023, 1, 1)
    df = df[df['trade_date'] >= cutoff]
    return [d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d)[:10]
            for d in df['trade_date']]

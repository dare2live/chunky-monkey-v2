"""
qlib_data_handler.py — Qlib 二进制数据转换

将 market_data.db 中的 K 线数据转换为 Qlib 标准 dump_bin 格式，
使 pyqlib 0.9.7 的 Alpha158/LGBModel/backtest 全套功能可用。
"""

import logging
import struct
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger("cm-api")

_QLIB_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "qlib_data"

# Qlib dump_bin 格式：每个 feature 存为一个 .bin 文件
# 文件内容：date_offset(uint32) + values(float32[])
# 日历文件：calendars/day.txt，每行一个交易日

OHLCV_FIELDS = ["open", "high", "low", "close", "volume"]
FACTOR_FIELD = "adjust_factor"  # 复权因子（如有）


def _ensure_dirs(data_dir: Path):
    """创建 Qlib 目录结构"""
    (data_dir / "calendars").mkdir(parents=True, exist_ok=True)
    (data_dir / "instruments").mkdir(parents=True, exist_ok=True)
    (data_dir / "features").mkdir(parents=True, exist_ok=True)


def _write_bin(path: Path, start_index: int, values: np.ndarray):
    """写入 Qlib 二进制格式：header(start_index as uint32) + float32 数组"""
    with open(path, "wb") as f:
        f.write(struct.pack("<I", start_index))
        f.write(values.astype(np.float32).tobytes())


def dump_bin_from_db(mkt_conn, data_dir: str = None) -> dict:
    """将 market_data.db K 线转换为 Qlib 二进制格式。

    返回 {"stocks": 数量, "trading_days": 数量, "data_dir": 路径}
    """
    data_path = Path(data_dir) if data_dir else _QLIB_DATA_DIR
    _ensure_dirs(data_path)

    logger.info(f"[Qlib-Data] 开始转换, 输出目录: {data_path}")

    # 1. 获取所有日 K 线数据
    rows = mkt_conn.execute(
        "SELECT code, date, open, high, low, close, volume, amount "
        "FROM price_kline WHERE freq='daily' AND adjust='qfq' "
        "ORDER BY date"
    ).fetchall()

    if not rows:
        logger.warning("[Qlib-Data] 无 K 线数据")
        return {"stocks": 0, "trading_days": 0, "data_dir": str(data_path)}

    df = pd.DataFrame([dict(r) for r in rows])
    df["date"] = pd.to_datetime(df["date"])

    # 2. 构建全局交易日历
    all_dates = sorted(df["date"].unique())
    date_to_idx = {d: i for i, d in enumerate(all_dates)}

    # 写入日历文件
    cal_path = data_path / "calendars" / "day.txt"
    with open(cal_path, "w") as f:
        for d in all_dates:
            f.write(pd.Timestamp(d).strftime("%Y-%m-%d") + "\n")

    # 3. 逐股票写入 bin 文件
    stock_count = 0
    instrument_lines = []

    for code, group in df.groupby("code"):
        group = group.sort_values("date").drop_duplicates("date").reset_index(drop=True)
        if len(group) < 10:
            continue

        # Qlib 标准 instrument 名称：SH600519 / SZ000001
        prefix = "SH" if str(code).startswith("6") else "SZ"
        qlib_name = f"{prefix}{code}"

        feat_dir = data_path / "features" / qlib_name
        feat_dir.mkdir(parents=True, exist_ok=True)

        # 按全局日历 reindex —— 停牌日自动填充 NaN，避免特征偏移
        start_date = group["date"].iloc[0]
        end_date = group["date"].iloc[-1]
        full_dates = [d for d in all_dates if d >= start_date and d <= end_date]
        group_indexed = group.set_index("date").reindex(full_dates)

        start_idx = date_to_idx.get(start_date, 0)

        # 写入各字段（停牌日为 NaN，Qlib Alpha158 正确处理 NaN）
        for field in OHLCV_FIELDS:
            values = group_indexed[field].values.copy()
            values = np.where(np.isfinite(values), values, np.nan).astype(np.float32)
            _write_bin(feat_dir / f"{field}.day.bin", start_idx, values)

        # 写入 amount（如果有）
        if "amount" in group_indexed.columns:
            amt = group_indexed["amount"].values.copy()
            amt = np.where(np.isfinite(amt), amt, np.nan).astype(np.float32)
            _write_bin(feat_dir / "amount.day.bin", start_idx, amt)

        # 记录 instrument 有效时间范围
        min_dt = pd.Timestamp(start_date).strftime("%Y-%m-%d")
        max_dt = pd.Timestamp(end_date).strftime("%Y-%m-%d")
        instrument_lines.append(f"{qlib_name}\t{min_dt}\t{max_dt}")
        stock_count += 1

    # 写入 instruments 文件
    inst_path = data_path / "instruments" / "all.txt"
    with open(inst_path, "w") as f:
        f.write("\n".join(instrument_lines) + "\n")

    result = {
        "stocks": stock_count,
        "trading_days": len(all_dates),
        "data_dir": str(data_path),
        "date_range": f"{pd.Timestamp(all_dates[0]).strftime('%Y-%m-%d')} ~ {pd.Timestamp(all_dates[-1]).strftime('%Y-%m-%d')}",
    }
    logger.info(f"[Qlib-Data] 转换完成: {result}")
    return result


def get_qlib_data_status(data_dir: str = None) -> dict:
    """检查 Qlib 数据状态"""
    data_path = Path(data_dir) if data_dir else _QLIB_DATA_DIR

    cal_path = data_path / "calendars" / "day.txt"
    inst_path = data_path / "instruments" / "all.txt"

    if not cal_path.exists() or not inst_path.exists():
        return {"available": False, "data_dir": str(data_path)}

    with open(cal_path) as f:
        dates = [line.strip() for line in f if line.strip()]

    with open(inst_path) as f:
        instruments = [line.strip() for line in f if line.strip()]

    return {
        "available": True,
        "data_dir": str(data_path),
        "trading_days": len(dates),
        "stocks": len(instruments),
        "date_range": f"{dates[0]} ~ {dates[-1]}" if dates else "",
    }

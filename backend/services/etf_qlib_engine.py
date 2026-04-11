"""ETF-only Qlib 管线。

职责：
- ETF 与股票预测链严格隔离，只消费 etf.db 自身数据。
- 构建 ETF-only 特征层、标签层、Qlib 训练与预测结果。
- 产出 ETF-only 参数寻优和回测结果，供 ETF 工作台、机会发现和深度分析复用。
"""

from __future__ import annotations

import json
import logging
import math
import pickle
import struct
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from services.etf_grid_engine import (
    _build_grid_step_candidates,
    _buy_hold_stats,
    _optimize_grid,
    _run_grid_backtest,
    _score_grid_backtest,
    is_supported_exchange_etf_code,
)
from services.utils import safe_float as _safe_float, clamp as _clamp


logger = logging.getLogger("cm-api")

_QLIB_AVAILABLE = True
_QLIB_ERROR = None
try:
    import qlib
    from qlib.config import REG_CN
except ImportError as exc:
    _QLIB_AVAILABLE = False
    _QLIB_ERROR = str(exc)


_BASE_DIR = Path(__file__).resolve().parent.parent.parent
_ETF_QLIB_DATA_DIR = _BASE_DIR / "data" / "qlib_etf_data"
_ETF_QLIB_MODEL_DIR = _BASE_DIR / "data" / "qlib_etf_models"
_ETF_QLIB_RUNS_DIR = _BASE_DIR / "data" / "qlib_etf_runs"

_FEATURE_COLUMNS = (
    "close",
    "amount",
    "momentum_5d",
    "momentum_20d",
    "momentum_60d",
    "volatility_20d",
    "drawdown_60d",
    "amplitude_5d",
    "amplitude_20d",
    "amount_ratio_5_20",
    "ma_gap_10",
    "ma_gap_20",
    "ma_gap_50",
    "range_position_20",
    "range_position_60",
    "trend_score",
    "mean_reversion_score",
)

_REQUIRED_TABLES = [
    ("etf_qlib_feature_store", "ETF 特征层"),
    ("etf_qlib_label_store", "ETF 标签层"),
    ("etf_qlib_model_state", "ETF 模型状态"),
    ("etf_qlib_predictions", "ETF 预测输出"),
    ("etf_qlib_backtest_result", "ETF 回测结果"),
    ("etf_qlib_param_search", "ETF 参数寻优"),
]

_DEFAULT_PARAMS = {
    "sample_step": 20,
    "lookback_days": 60,
    "future_window": 60,
    "max_history_days": 720,
    "num_boost_round": 220,
    "early_stopping_rounds": 40,
    "num_leaves": 48,
    "learning_rate": 0.05,
    "subsample": 0.85,
    "colsample_bytree": 0.85,
}


def is_available() -> tuple[bool, Optional[str]]:
    return _QLIB_AVAILABLE, _QLIB_ERROR


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_round(value, digits: int = 4):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


def _status_label(status: str) -> str:
    return {
        "ready": "已就绪",
        "partial": "部分完成",
        "pending": "待建设",
        "trained": "已训练",
        "training": "训练中",
        "error": "错误",
    }.get(status, status)


def _existing_table_names(conn) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row["name"]) for row in rows if row and row["name"]}


def _table_count(conn, table_name: str) -> int:
    try:
        row = conn.execute(f"SELECT COUNT(*) AS total FROM {table_name}").fetchone()
        return int(row["total"] or 0) if row else 0
    except Exception:
        return 0


def _ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _mean(values: list[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in values if v is not None]
    if not clean:
        return None
    return sum(clean) / len(clean)


def _pct_change(latest: Optional[float], past: Optional[float]) -> Optional[float]:
    if latest is None or past in (None, 0):
        return None
    return round((latest - past) / past * 100.0, 4)


def _volatility(closes: list[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in closes if v is not None]
    if len(clean) < 2:
        return None
    returns = []
    for prev, cur in zip(clean[:-1], clean[1:]):
        if prev and prev > 0:
            returns.append((cur - prev) / prev)
    if not returns:
        return None
    mean_ret = sum(returns) / len(returns)
    variance = sum((item - mean_ret) ** 2 for item in returns) / len(returns)
    return round(math.sqrt(variance) * math.sqrt(252) * 100.0, 4)


def _drawdown(closes: list[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in closes if v is not None]
    if not clean:
        return None
    latest = clean[-1]
    peak = max(clean)
    if peak <= 0:
        return None
    return round((latest - peak) / peak * 100.0, 4)


def _amplitude(highs: list[Optional[float]], lows: list[Optional[float]]) -> Optional[float]:
    hi = [float(v) for v in highs if v is not None]
    lo = [float(v) for v in lows if v is not None]
    if not hi or not lo:
        return None
    base = min(lo)
    if base <= 0:
        return None
    return round((max(hi) - base) / base * 100.0, 4)


def _amount_ratio(amounts: list[Optional[float]], short: int = 5, long: int = 20) -> Optional[float]:
    if len(amounts) < long:
        return None
    short_avg = _mean(amounts[-short:])
    long_avg = _mean(amounts[-long:])
    if short_avg is None or long_avg in (None, 0):
        return None
    return round(short_avg / long_avg, 4)


def _range_position(closes: list[Optional[float]]) -> Optional[float]:
    clean = [float(v) for v in closes if v is not None]
    if not clean:
        return None
    latest = clean[-1]
    low = min(clean)
    high = max(clean)
    if high <= low:
        return 50.0
    return round((latest - low) / (high - low) * 100.0, 4)


def _qlib_instrument(code: str, market: Optional[str] = None) -> str:
    market_text = (market or "").upper()
    if market_text in {"SH", "SZ"}:
        return f"{market_text}{code}"
    return f"{'SH' if str(code).startswith(('5', '6')) else 'SZ'}{code}"


def _write_bin(path: Path, start_index: int, values: np.ndarray) -> None:
    with open(path, "wb") as handle:
        handle.write(struct.pack("<I", start_index))
        handle.write(values.astype(np.float32).tobytes())


def _classify_setup(
    latest_close: Optional[float],
    ma10: Optional[float],
    ma20: Optional[float],
    ma50: Optional[float],
    amplitude_5d: Optional[float],
    amplitude_20d: Optional[float],
    amount_ratio_5_20: Optional[float],
) -> tuple[str, str]:
    if latest_close in (None, 0) or ma20 in (None, 0):
        return "待补结构", "震荡"
    aligned = (
        ma10 is not None and ma20 is not None and ma50 is not None
        and ma10 >= ma20 >= ma50
    )
    trend_status = "震荡"
    if aligned and latest_close >= ma20:
        trend_status = "多头"
    elif latest_close < ma20 and ma20 is not None and ma50 is not None and ma20 < ma50:
        trend_status = "空头"

    contraction_ratio = None
    if amplitude_5d is not None and amplitude_20d not in (None, 0):
        contraction_ratio = amplitude_5d / amplitude_20d

    if aligned and latest_close >= ma20 and contraction_ratio is not None and contraction_ratio <= 0.65:
        return "收敛待发", trend_status
    if aligned and latest_close >= ma20 and contraction_ratio is not None and contraction_ratio <= 0.95:
        return "趋势跟随", trend_status
    if latest_close >= ma20 and latest_close >= ma50 and amplitude_20d is not None and amplitude_20d <= 6:
        return "低波防守", trend_status
    if latest_close < ma20 or (contraction_ratio is not None and contraction_ratio >= 1.15) or (amount_ratio_5_20 or 0) >= 1.8:
        return "结构松散", trend_status
    return "震荡观察", trend_status


def dump_etf_bin_from_db(conn, data_dir: Optional[str] = None) -> dict:
    data_path = Path(data_dir) if data_dir else _ETF_QLIB_DATA_DIR
    _ensure_dir(data_path / "calendars")
    _ensure_dir(data_path / "instruments")
    _ensure_dir(data_path / "features")

    rows = conn.execute(
        """
        SELECT p.code, p.date, p.open, p.high, p.low, p.close, p.volume, p.amount,
               u.market
        FROM etf_price_kline p
        JOIN etf_asset_universe u ON u.code = p.code
        WHERE p.freq = 'daily'
          AND p.adjust = 'qfq'
          AND u.is_active = 1
        ORDER BY p.date ASC
        """
    ).fetchall()
    if not rows:
        return {
            "available": False,
            "data_dir": str(data_path),
            "instruments": 0,
            "trading_days": 0,
        }

    df = pd.DataFrame([dict(row) for row in rows])
    df["date"] = pd.to_datetime(df["date"])
    all_dates = sorted(df["date"].unique())
    date_to_index = {day: idx for idx, day in enumerate(all_dates)}

    cal_path = data_path / "calendars" / "day.txt"
    with open(cal_path, "w", encoding="utf-8") as handle:
        for day in all_dates:
            handle.write(pd.Timestamp(day).strftime("%Y-%m-%d") + "\n")

    instruments = []
    for (code, market), group in df.groupby(["code", "market"]):
        if not is_supported_exchange_etf_code(str(code)):
            continue
        group = group.sort_values("date").drop_duplicates("date").reset_index(drop=True)
        if len(group) < 40:
            continue
        instrument = _qlib_instrument(str(code), market)
        feat_dir = data_path / "features" / instrument
        _ensure_dir(feat_dir)
        start_date = group["date"].iloc[0]
        end_date = group["date"].iloc[-1]
        full_dates = [day for day in all_dates if start_date <= day <= end_date]
        indexed = group.set_index("date").reindex(full_dates)
        start_index = date_to_index.get(start_date, 0)
        for field in ("open", "high", "low", "close", "volume", "amount"):
            values = indexed[field].values.copy()
            values = np.where(np.isfinite(values), values, np.nan).astype(np.float32)
            _write_bin(feat_dir / f"{field}.day.bin", start_index, values)
        instruments.append(
            f"{instrument}\t{pd.Timestamp(start_date).strftime('%Y-%m-%d')}\t{pd.Timestamp(end_date).strftime('%Y-%m-%d')}"
        )

    inst_path = data_path / "instruments" / "all.txt"
    with open(inst_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(instruments) + ("\n" if instruments else ""))

    return {
        "available": True,
        "data_dir": str(data_path),
        "instruments": len(instruments),
        "trading_days": len(all_dates),
        "date_range": f"{pd.Timestamp(all_dates[0]).strftime('%Y-%m-%d')} ~ {pd.Timestamp(all_dates[-1]).strftime('%Y-%m-%d')}",
    }


def _build_feature_row(asset: dict, rows: list[dict], current_idx: int, *, sample_tag: str, is_latest: bool) -> Optional[dict]:
    if current_idx < 59:
        return None
    window_5 = rows[max(0, current_idx - 4): current_idx + 1]
    window_10 = rows[max(0, current_idx - 9): current_idx + 1]
    window_20 = rows[max(0, current_idx - 19): current_idx + 1]
    window_50 = rows[max(0, current_idx - 49): current_idx + 1]
    window_60 = rows[max(0, current_idx - 59): current_idx + 1]

    closes_5 = [_safe_float(item.get("close")) for item in window_5]
    closes_10 = [_safe_float(item.get("close")) for item in window_10]
    closes_20 = [_safe_float(item.get("close")) for item in window_20]
    closes_50 = [_safe_float(item.get("close")) for item in window_50]
    closes_60 = [_safe_float(item.get("close")) for item in window_60]
    latest_close = closes_60[-1]
    ma10 = _mean(closes_10)
    ma20 = _mean(closes_20)
    ma50 = _mean(closes_50)
    amplitude_5d = _amplitude([_safe_float(item.get("high")) for item in window_5], [_safe_float(item.get("low")) for item in window_5])
    amplitude_20d = _amplitude([_safe_float(item.get("high")) for item in window_20], [_safe_float(item.get("low")) for item in window_20])
    amount_ratio_5_20 = _amount_ratio([_safe_float(item.get("amount")) for item in window_20], 5, 20)
    setup_state, trend_status = _classify_setup(latest_close, ma10, ma20, ma50, amplitude_5d, amplitude_20d, amount_ratio_5_20)
    momentum_5d = _pct_change(latest_close, closes_60[-6] if len(closes_60) >= 6 else None)
    momentum_20d = _pct_change(latest_close, closes_60[-21] if len(closes_60) >= 21 else None)
    momentum_60d = _pct_change(latest_close, closes_60[0] if len(closes_60) >= 60 else None)
    volatility_20d = _volatility(closes_20)
    drawdown_60d = _drawdown(closes_60)
    ma_gap_10 = _pct_change(latest_close, ma10)
    ma_gap_20 = _pct_change(latest_close, ma20)
    ma_gap_50 = _pct_change(latest_close, ma50)
    range_position_20 = _range_position(closes_20)
    range_position_60 = _range_position(closes_60)
    trend_score = _safe_round(
        _clamp(
            50.0
            + (momentum_20d or 0.0) * 0.9
            + (momentum_60d or 0.0) * 0.45
            + (ma_gap_20 or 0.0) * 0.6
            + (range_position_20 or 50.0) * 0.15,
            0.0,
            100.0,
        ),
        4,
    )
    mean_reversion_score = _safe_round(
        _clamp(
            58.0
            + (amplitude_20d or 0.0) * 0.35
            + (volatility_20d or 0.0) * 0.25
            - abs(momentum_20d or 0.0) * 0.8
            - abs(ma_gap_20 or 0.0) * 0.4,
            0.0,
            100.0,
        ),
        4,
    )

    if latest_close is None:
        return None

    return {
        "snapshot_date": str(rows[current_idx]["date"]),
        "code": asset.get("code"),
        "name": asset.get("name"),
        "category": asset.get("category"),
        "market": asset.get("market"),
        "qlib_instrument": _qlib_instrument(asset.get("code") or "", asset.get("market")),
        "is_latest": 1 if is_latest else 0,
        "sample_tag": sample_tag,
        "close": latest_close,
        "amount": _safe_float(window_5[-1].get("amount")) if window_5 else None,
        "momentum_5d": momentum_5d,
        "momentum_20d": momentum_20d,
        "momentum_60d": momentum_60d,
        "volatility_20d": volatility_20d,
        "drawdown_60d": drawdown_60d,
        "amplitude_5d": amplitude_5d,
        "amplitude_20d": amplitude_20d,
        "amount_ratio_5_20": amount_ratio_5_20,
        "ma_gap_10": ma_gap_10,
        "ma_gap_20": ma_gap_20,
        "ma_gap_50": ma_gap_50,
        "range_position_20": range_position_20,
        "range_position_60": range_position_60,
        "trend_score": trend_score,
        "mean_reversion_score": mean_reversion_score,
        "setup_state": setup_state,
        "trend_status": trend_status,
        "created_at": _now_iso(),
    }


def build_etf_feature_label_store(
    conn,
    *,
    force_refresh: bool = False,
    sample_step: int = 20,
    lookback_days: int = 60,
    future_window: int = 60,
    max_history_days: int = 720,
) -> dict:
    existing_features = _table_count(conn, "etf_qlib_feature_store")
    existing_labels = _table_count(conn, "etf_qlib_label_store")
    if existing_features and existing_labels and not force_refresh:
        return {
            "feature_rows": existing_features,
            "label_rows": existing_labels,
            "reused": True,
        }

    if force_refresh:
        conn.execute("DELETE FROM etf_qlib_feature_store")
        conn.execute("DELETE FROM etf_qlib_label_store")

    assets = conn.execute(
        """
        SELECT code, name, category, market
        FROM etf_asset_universe
        WHERE is_active = 1
        ORDER BY code
        """
    ).fetchall()

    feature_rows: list[tuple] = []
    label_rows: list[tuple] = []
    now = _now_iso()
    inserted_codes = 0

    for asset_row in assets:
        asset = dict(asset_row)
        code = asset.get("code") or ""
        if not is_supported_exchange_etf_code(code):
            continue
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM etf_price_kline
            WHERE code = ?
              AND freq = 'daily'
              AND adjust = 'qfq'
            ORDER BY date ASC
            """,
            (code,),
        ).fetchall()
        if len(rows) < lookback_days + future_window + 5:
            continue
        rows = [dict(row) for row in rows]
        if len(rows) > max_history_days + future_window:
            rows = rows[-(max_history_days + future_window):]

        inserted_codes += 1
        latest_idx = len(rows) - 1
        sample_indexes = list(range(lookback_days - 1, len(rows) - future_window, sample_step))
        for idx in sample_indexes:
            feature = _build_feature_row(asset, rows, idx, sample_tag="train", is_latest=False)
            if not feature:
                continue
            future_rows = rows[idx + 1: idx + 1 + future_window]
            buy_hold = _buy_hold_stats(future_rows)
            best = _optimize_grid(future_rows, row=feature)
            buy_hold_ret = _safe_float((buy_hold or {}).get("return_pct"))
            grid_ret = _safe_float((best or {}).get("return_pct"))
            grid_excess = _safe_float((best or {}).get("backtest_excess_pct"))
            if grid_excess is None and buy_hold_ret is not None and grid_ret is not None:
                grid_excess = round(grid_ret - buy_hold_ret, 4)
            strategy_label = "网格交易" if best and grid_excess is not None and grid_excess >= 0 else "买入持有"
            feature_rows.append(
                (
                    feature["snapshot_date"], feature["code"], feature["name"], feature["category"], feature["market"],
                    feature["qlib_instrument"], feature["is_latest"], feature["sample_tag"], feature["close"], feature["amount"],
                    feature["momentum_5d"], feature["momentum_20d"], feature["momentum_60d"], feature["volatility_20d"], feature["drawdown_60d"],
                    feature["amplitude_5d"], feature["amplitude_20d"], feature["amount_ratio_5_20"], feature["ma_gap_10"], feature["ma_gap_20"],
                    feature["ma_gap_50"], feature["range_position_20"], feature["range_position_60"], feature["trend_score"], feature["mean_reversion_score"],
                    feature["setup_state"], feature["trend_status"], now,
                )
            )
            label_rows.append(
                (
                    feature["snapshot_date"], feature["code"], future_window, buy_hold_ret, grid_ret, grid_excess,
                    _safe_float((best or {}).get("step_pct")), int((best or {}).get("trade_count") or 0),
                    strategy_label, 1 if strategy_label == "网格交易" else 0, now,
                )
            )

        latest_feature = _build_feature_row(asset, rows, latest_idx, sample_tag="latest", is_latest=True)
        if latest_feature:
            feature_rows.append(
                (
                    latest_feature["snapshot_date"], latest_feature["code"], latest_feature["name"], latest_feature["category"], latest_feature["market"],
                    latest_feature["qlib_instrument"], latest_feature["is_latest"], latest_feature["sample_tag"], latest_feature["close"], latest_feature["amount"],
                    latest_feature["momentum_5d"], latest_feature["momentum_20d"], latest_feature["momentum_60d"], latest_feature["volatility_20d"], latest_feature["drawdown_60d"],
                    latest_feature["amplitude_5d"], latest_feature["amplitude_20d"], latest_feature["amount_ratio_5_20"], latest_feature["ma_gap_10"], latest_feature["ma_gap_20"],
                    latest_feature["ma_gap_50"], latest_feature["range_position_20"], latest_feature["range_position_60"], latest_feature["trend_score"], latest_feature["mean_reversion_score"],
                    latest_feature["setup_state"], latest_feature["trend_status"], now,
                )
            )

    conn.execute("DELETE FROM etf_qlib_feature_store")
    conn.execute("DELETE FROM etf_qlib_label_store")
    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_qlib_feature_store (
            snapshot_date, code, name, category, market, qlib_instrument, is_latest, sample_tag,
            close, amount, momentum_5d, momentum_20d, momentum_60d, volatility_20d, drawdown_60d,
            amplitude_5d, amplitude_20d, amount_ratio_5_20, ma_gap_10, ma_gap_20, ma_gap_50,
            range_position_20, range_position_60, trend_score, mean_reversion_score, setup_state,
            trend_status, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        feature_rows,
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_qlib_label_store (
            snapshot_date, code, future_window, buy_hold_return_pct, grid_return_pct, grid_excess_pct,
            best_step_pct, grid_trade_count, strategy_label, strategy_flag, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        label_rows,
    )
    conn.commit()
    return {
        "feature_rows": len(feature_rows),
        "label_rows": len(label_rows),
        "asset_count": inserted_codes,
        "reused": False,
    }


def _load_training_join_rows(conn, *, future_window: int = 60) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT f.snapshot_date, f.code, f.name, f.category, f.qlib_instrument,
               {', '.join(f'f.{col}' for col in _FEATURE_COLUMNS)},
               l.buy_hold_return_pct, l.grid_return_pct, l.grid_excess_pct,
               l.best_step_pct, l.strategy_label, l.strategy_flag
        FROM etf_qlib_feature_store f
        JOIN etf_qlib_label_store l
          ON l.snapshot_date = f.snapshot_date
         AND l.code = f.code
         AND l.future_window = ?
        WHERE f.sample_tag = 'train'
        ORDER BY f.snapshot_date ASC, f.code ASC
        """,
        (future_window,),
    ).fetchall()
    return [dict(row) for row in rows]


def _rows_to_static_frame(rows: list[dict], label_column: str) -> pd.DataFrame:
    columns = pd.MultiIndex.from_tuples(
        [("feature", col) for col in _FEATURE_COLUMNS] + [("label", "label")]
    )
    data = []
    index = []
    for row in rows:
        label_value = _safe_float(row.get(label_column))
        if label_value is None:
            continue
        index.append((pd.Timestamp(row["snapshot_date"]), row["qlib_instrument"]))
        feature_values = [_safe_float(row.get(col)) for col in _FEATURE_COLUMNS]
        data.append(feature_values + [label_value])
    frame = pd.DataFrame(
        data,
        index=pd.MultiIndex.from_tuples(index, names=["datetime", "instrument"]),
        columns=columns,
    )
    return frame.sort_index()


def _load_latest_feature_rows(conn) -> list[dict]:
    rows = conn.execute(
        f"""
        SELECT snapshot_date, code, name, category, qlib_instrument,
               {', '.join(_FEATURE_COLUMNS)}
        FROM etf_qlib_feature_store
        WHERE is_latest = 1
        ORDER BY snapshot_date ASC, code ASC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def _latest_rows_to_static_frame(rows: list[dict]) -> pd.DataFrame:
    columns = pd.MultiIndex.from_tuples(
        [("feature", col) for col in _FEATURE_COLUMNS] + [("label", "label")]
    )
    data = []
    index = []
    for row in rows:
        index.append((pd.Timestamp(row["snapshot_date"]), row["qlib_instrument"]))
        data.append([_safe_float(row.get(col)) for col in _FEATURE_COLUMNS] + [0.0])
    frame = pd.DataFrame(
        data,
        index=pd.MultiIndex.from_tuples(index, names=["datetime", "instrument"]),
        columns=columns,
    )
    return frame.sort_index()


def _split_segments(frame: pd.DataFrame) -> dict[str, tuple[str, str]]:
    dates = sorted(pd.Timestamp(item).strftime("%Y-%m-%d") for item in frame.index.get_level_values("datetime").unique())
    if len(dates) < 12:
        raise RuntimeError("ETF Qlib 训练样本不足，无法切分 train/valid/test")
    train_idx = max(int(len(dates) * 0.7), 1)
    valid_idx = max(int(len(dates) * 0.85), train_idx + 1)
    valid_idx = min(valid_idx, len(dates) - 2)
    train_end = dates[train_idx - 1]
    valid_end = dates[valid_idx - 1]
    return {
        "train": (dates[0], train_end),
        "valid": (dates[train_idx], valid_end),
        "test": (dates[valid_idx], dates[-1]),
    }


def _build_dataset(static_frame: pd.DataFrame, segments: dict[str, tuple[str, str]]):
    from qlib.data.dataset.loader import StaticDataLoader
    from qlib.data.dataset.handler import DataHandlerLP
    from qlib.data.dataset import DatasetH

    loader = StaticDataLoader(config=static_frame)
    handler = DataHandlerLP(data_loader=loader)
    return DatasetH(handler=handler, segments=segments)


def _safe_corr(pred: pd.Series, actual: pd.Series) -> Optional[float]:
    if pred is None or actual is None:
        return None
    aligned = pd.concat([pred, actual], axis=1).dropna()
    if len(aligned) < 3:
        return None
    try:
        return float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1], method="spearman"))
    except Exception:
        return None


def _safe_mae(pred: pd.Series, actual: pd.Series) -> Optional[float]:
    aligned = pd.concat([pred, actual], axis=1).dropna()
    if aligned.empty:
        return None
    return float(np.mean(np.abs(aligned.iloc[:, 0] - aligned.iloc[:, 1])))


def _rank_and_percentile(values: dict[str, Optional[float]]) -> dict[str, dict[str, Optional[float]]]:
    clean = [(code, float(value)) for code, value in values.items() if value is not None]
    clean.sort(key=lambda item: (-item[1], item[0]))
    total = len(clean)
    ranked: dict[str, dict[str, Optional[float]]] = {code: {"rank": None, "percentile": None} for code in values}
    for idx, (code, _) in enumerate(clean, start=1):
        percentile = 100.0 if total <= 1 else round(100.0 - ((idx - 1) / (total - 1)) * 100.0, 2)
        ranked[code] = {"rank": idx, "percentile": percentile}
    return ranked


def _latest_model_row(conn):
    return conn.execute(
        """
        SELECT *
        FROM etf_qlib_model_state
        WHERE status IN ('trained', 'training', 'error')
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()


def _persist_model_state(conn, model_id: str, payload: dict) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO etf_qlib_model_state (
            model_id, status, train_start, train_end, valid_start, valid_end, test_start, test_end,
            etf_count, sample_count, feature_count, hold_ic_mean, grid_ic_mean, excess_ic_mean,
            step_mae, strategy_accuracy, test_top20_hold_return, test_top20_strategy_return,
            model_path, data_dir, train_params_json, error, created_at, finished_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            model_id,
            payload.get("status"),
            payload.get("train_start"),
            payload.get("train_end"),
            payload.get("valid_start"),
            payload.get("valid_end"),
            payload.get("test_start"),
            payload.get("test_end"),
            payload.get("etf_count"),
            payload.get("sample_count"),
            payload.get("feature_count"),
            payload.get("hold_ic_mean"),
            payload.get("grid_ic_mean"),
            payload.get("excess_ic_mean"),
            payload.get("step_mae"),
            payload.get("strategy_accuracy"),
            payload.get("test_top20_hold_return"),
            payload.get("test_top20_strategy_return"),
            payload.get("model_path"),
            payload.get("data_dir"),
            json.dumps(payload.get("train_params") or {}, ensure_ascii=False),
            payload.get("error"),
            payload.get("created_at"),
            payload.get("finished_at"),
        ),
    )
    conn.commit()


def _persist_predictions(conn, model_id: str, prediction_rows: list[dict]) -> None:
    conn.execute("DELETE FROM etf_qlib_predictions WHERE model_id = ?", (model_id,))
    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_qlib_predictions (
            model_id, code, name, category, predict_date, hold_score, hold_rank, hold_percentile,
            grid_score, grid_rank, grid_percentile, excess_score, excess_rank, excess_percentile,
            step_score, predicted_buy_hold_return_pct, predicted_grid_return_pct,
            predicted_grid_excess_pct, predicted_best_step_pct, preferred_strategy,
            recommended_return_pct, comparison_return_pct, strategy_edge_pct, model_status, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        [
            (
                model_id,
                row.get("code"),
                row.get("name"),
                row.get("category"),
                row.get("predict_date"),
                row.get("hold_score"),
                row.get("hold_rank"),
                row.get("hold_percentile"),
                row.get("grid_score"),
                row.get("grid_rank"),
                row.get("grid_percentile"),
                row.get("excess_score"),
                row.get("excess_rank"),
                row.get("excess_percentile"),
                row.get("step_score"),
                row.get("predicted_buy_hold_return_pct"),
                row.get("predicted_grid_return_pct"),
                row.get("predicted_grid_excess_pct"),
                row.get("predicted_best_step_pct"),
                row.get("preferred_strategy"),
                row.get("recommended_return_pct"),
                row.get("comparison_return_pct"),
                row.get("strategy_edge_pct"),
                row.get("model_status"),
                row.get("created_at"),
            )
            for row in prediction_rows
        ],
    )
    conn.commit()


def _persist_live_backtests(conn, model_id: str) -> None:
    latest_feature_rows = {
        row["code"]: dict(row)
        for row in conn.execute(
            f"""
            SELECT code, name, category, market, {', '.join(_FEATURE_COLUMNS)}, setup_state, trend_status
            FROM etf_qlib_feature_store
            WHERE is_latest = 1
            """
        ).fetchall()
    }
    prediction_rows = {
        row["code"]: dict(row)
        for row in conn.execute(
            "SELECT * FROM etf_qlib_predictions WHERE model_id = ?",
            (model_id,),
        ).fetchall()
    }
    conn.execute("DELETE FROM etf_qlib_backtest_result WHERE model_id = ?", (model_id,))
    conn.execute("DELETE FROM etf_qlib_param_search WHERE model_id = ?", (model_id,))
    now = _now_iso()
    backtest_rows = []
    search_rows = []
    assets = conn.execute(
        "SELECT code FROM etf_asset_universe WHERE is_active = 1 ORDER BY code"
    ).fetchall()
    for asset in assets:
        code = asset["code"]
        if not is_supported_exchange_etf_code(code):
            continue
        price_rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT date, open, high, low, close, volume, amount
                FROM etf_price_kline
                WHERE code = ? AND freq = 'daily' AND adjust = 'qfq'
                ORDER BY date ASC
                LIMIT 500
                """,
                (code,),
            ).fetchall()
        ]
        if len(price_rows) < 40:
            continue
        row = dict(latest_feature_rows.get(code) or {})
        prediction = prediction_rows.get(code) or {}
        row.update(
            {
                "qlib_consensus_score": prediction.get("hold_percentile") if prediction.get("preferred_strategy") == "买入持有" else prediction.get("excess_percentile"),
                "qlib_consensus_factor_group": "etf_grid" if prediction.get("preferred_strategy") == "网格交易" else "etf_hold",
                "qlib_model_status": prediction.get("model_status") or "trained",
            }
        )
        buy_hold = _buy_hold_stats(price_rows)
        step_candidates = _build_grid_step_candidates(price_rows, row=row)
        scored_steps = []
        for step in step_candidates:
            result = _run_grid_backtest(price_rows, step, full_curve=False)
            if result:
                scored_steps.append(_score_grid_backtest(result, buy_hold, row=row))
        scored_steps.sort(
            key=lambda item: (
                -(_safe_float(item.get("candidate_score")) or -9999.0),
                -(_safe_float(item.get("backtest_excess_pct")) or -9999.0),
                -(_safe_float(item.get("return_pct")) or -9999.0),
                item.get("step_pct") or 0.0,
            )
        )
        best = _optimize_grid(price_rows, row=row)
        snapshot_date = price_rows[-1]["date"]
        for rank_order, item in enumerate(scored_steps, start=1):
            search_rows.append(
                (
                    model_id,
                    code,
                    snapshot_date,
                    item.get("step_pct"),
                    item.get("candidate_score"),
                    item.get("return_pct"),
                    item.get("backtest_excess_pct"),
                    item.get("sharpe"),
                    item.get("max_drawdown_pct"),
                    item.get("trade_count"),
                    item.get("sell_count"),
                    item.get("win_rate"),
                    1 if item.get("hard_gate_passed") else 0,
                    rank_order,
                    1 if best and item.get("step_pct") == best.get("step_pct") else 0,
                    now,
                )
            )
        buy_hold_ret = _safe_float((buy_hold or {}).get("return_pct"))
        grid_ret = _safe_float((best or {}).get("return_pct"))
        grid_excess = _safe_float((best or {}).get("backtest_excess_pct"))
        if grid_excess is None and buy_hold_ret is not None and grid_ret is not None:
            grid_excess = round(grid_ret - buy_hold_ret, 4)
        strategy_label = "网格交易" if best and grid_excess is not None and grid_excess >= 0 else "买入持有"
        backtest_rows.append(
            (
                model_id,
                code,
                60,
                snapshot_date,
                buy_hold_ret,
                grid_ret,
                grid_excess,
                _safe_float((best or {}).get("step_pct")),
                int((best or {}).get("trade_count") or 0),
                _safe_float((best or {}).get("win_rate")),
                strategy_label,
                json.dumps((best or {}).get("audit") or {}, ensure_ascii=False),
                now,
            )
        )

    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_qlib_backtest_result (
            model_id, code, window_days, snapshot_date, buy_hold_return_pct, grid_return_pct,
            grid_excess_pct, best_step_pct, trade_count, win_rate, strategy_label, audit_json, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        backtest_rows,
    )
    conn.executemany(
        """
        INSERT OR REPLACE INTO etf_qlib_param_search (
            model_id, code, snapshot_date, step_pct, candidate_score, return_pct, excess_pct,
            sharpe, max_drawdown_pct, trade_count, sell_count, win_rate, hard_gate_passed,
            rank_order, is_best, created_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        search_rows,
    )
    conn.commit()


def train_etf_qlib_pipeline(
    conn,
    *,
    force_refresh: bool = False,
    params: Optional[dict] = None,
    snapshot_topk: int = 50,
) -> dict:
    available, error = is_available()
    if not available:
        raise RuntimeError(f"pyqlib 不可用: {error}")

    train_params = dict(_DEFAULT_PARAMS)
    if params:
        train_params.update(params)

    preparation = build_etf_feature_label_store(
        conn,
        force_refresh=force_refresh,
        sample_step=int(train_params.get("sample_step") or 20),
        lookback_days=int(train_params.get("lookback_days") or 60),
        future_window=int(train_params.get("future_window") or 60),
        max_history_days=int(train_params.get("max_history_days") or 720),
    )
    dump_info = dump_etf_bin_from_db(conn)

    training_rows = _load_training_join_rows(conn, future_window=int(train_params.get("future_window") or 60))
    if not training_rows:
        raise RuntimeError("ETF-only 特征/标签样本为空，无法训练")

    static_frames = {
        "hold": _rows_to_static_frame(training_rows, "buy_hold_return_pct"),
        "grid": _rows_to_static_frame(training_rows, "grid_return_pct"),
        "excess": _rows_to_static_frame(training_rows, "grid_excess_pct"),
        "step": _rows_to_static_frame(training_rows, "best_step_pct"),
    }
    base_frame = static_frames["hold"]
    segments = _split_segments(base_frame)

    model_id = f"etf_qlib_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    created_at = _now_iso()
    _persist_model_state(
        conn,
        model_id,
        {
            "status": "training",
            "train_start": segments["train"][0],
            "train_end": segments["train"][1],
            "valid_start": segments["valid"][0],
            "valid_end": segments["valid"][1],
            "test_start": segments["test"][0],
            "test_end": segments["test"][1],
            "etf_count": len({row["code"] for row in training_rows}),
            "sample_count": len(base_frame),
            "feature_count": len(_FEATURE_COLUMNS),
            "train_params": train_params,
            "data_dir": str(_ETF_QLIB_DATA_DIR),
            "created_at": created_at,
        },
    )

    qlib.init(provider_uri=str(_ETF_QLIB_DATA_DIR), region=REG_CN)
    from qlib.contrib.model.gbdt import LGBModel
    from qlib.workflow import R

    _ensure_dir(_ETF_QLIB_MODEL_DIR)
    _ensure_dir(_ETF_QLIB_RUNS_DIR)

    models = {}
    test_predictions = {}
    test_actuals = {}

    recorder_ctx = R.start(
        experiment_name="chunky_monkey_etf_qlib",
        recorder_name=model_id,
        uri=str(_ETF_QLIB_RUNS_DIR),
    )
    with recorder_ctx:
        recorder = R.get_recorder()
        recorder.log_params(model_id=model_id, **train_params)
        for target, frame in static_frames.items():
            dataset = _build_dataset(frame, segments)
            model = LGBModel(
                loss="mse",
                num_boost_round=int(train_params.get("num_boost_round") or 220),
                early_stopping_rounds=int(train_params.get("early_stopping_rounds") or 40),
                num_leaves=int(train_params.get("num_leaves") or 48),
                learning_rate=float(train_params.get("learning_rate") or 0.05),
                subsample=float(train_params.get("subsample") or 0.85),
                colsample_bytree=float(train_params.get("colsample_bytree") or 0.85),
            )
            model.fit(dataset)
            pred_test = model.predict(dataset, segment="test")
            actual_test = dataset.prepare("test", col_set="label").iloc[:, 0]
            pred_test.name = "prediction"
            actual_test.name = "actual"
            models[target] = model
            test_predictions[target] = pred_test
            test_actuals[target] = actual_test

        hold_ic = _safe_corr(test_predictions["hold"], test_actuals["hold"])
        grid_ic = _safe_corr(test_predictions["grid"], test_actuals["grid"])
        excess_ic = _safe_corr(test_predictions["excess"], test_actuals["excess"])
        step_mae = _safe_mae(test_predictions["step"], test_actuals["step"])

        aligned_strategy = pd.concat(
            [test_predictions["hold"], test_predictions["grid"], test_predictions["excess"], test_actuals["hold"], test_actuals["grid"], test_actuals["excess"]],
            axis=1,
            keys=["hold_pred", "grid_pred", "excess_pred", "hold_actual", "grid_actual", "excess_actual"],
        ).dropna()
        if aligned_strategy.empty:
            strategy_accuracy = None
            top20_hold = None
            top20_strategy = None
        else:
            predicted_strategy = np.where(aligned_strategy["excess_pred"] >= 0, "网格交易", "买入持有")
            actual_strategy = np.where(aligned_strategy["excess_actual"] >= 0, "网格交易", "买入持有")
            strategy_accuracy = float(np.mean(predicted_strategy == actual_strategy))

            hold_top = aligned_strategy.sort_values("hold_pred", ascending=False).head(20)
            top20_hold = _safe_round(_mean(hold_top["hold_actual"].tolist()), 4)

            strategy_scores = aligned_strategy[["hold_pred", "grid_pred"]].max(axis=1)
            strategy_table = aligned_strategy.assign(strategy_score=strategy_scores)
            strategy_table = strategy_table.sort_values("strategy_score", ascending=False).head(20)
            actual_strategy_return = np.where(
                strategy_table["excess_actual"] >= 0,
                strategy_table["grid_actual"],
                strategy_table["hold_actual"],
            )
            top20_strategy = _safe_round(_mean(actual_strategy_return.tolist()), 4)

        recorder.log_metrics(
            hold_ic_mean=hold_ic,
            grid_ic_mean=grid_ic,
            excess_ic_mean=excess_ic,
            step_mae=step_mae,
            strategy_accuracy=strategy_accuracy,
            test_top20_hold_return=top20_hold,
            test_top20_strategy_return=top20_strategy,
        )

    latest_feature_rows = _load_latest_feature_rows(conn)
    latest_frame = _latest_rows_to_static_frame(latest_feature_rows)
    predict_segments = {
        "predict": (
            pd.Timestamp(latest_frame.index.get_level_values("datetime").min()).strftime("%Y-%m-%d"),
            pd.Timestamp(latest_frame.index.get_level_values("datetime").max()).strftime("%Y-%m-%d"),
        )
    }
    predict_dataset = _build_dataset(latest_frame, predict_segments)

    pred_series = {
        target: model.predict(predict_dataset, segment="predict")
        for target, model in models.items()
    }
    hold_ranks = _rank_and_percentile({code: _safe_float(pred_series["hold"].get((pd.Timestamp(row["snapshot_date"]), row["qlib_instrument"]))) for code, row in [(item["code"], item) for item in latest_feature_rows]})
    grid_ranks = _rank_and_percentile({code: _safe_float(pred_series["grid"].get((pd.Timestamp(row["snapshot_date"]), row["qlib_instrument"]))) for code, row in [(item["code"], item) for item in latest_feature_rows]})
    excess_ranks = _rank_and_percentile({code: _safe_float(pred_series["excess"].get((pd.Timestamp(row["snapshot_date"]), row["qlib_instrument"]))) for code, row in [(item["code"], item) for item in latest_feature_rows]})

    prediction_rows = []
    for row in latest_feature_rows:
        code = row["code"]
        index_key = (pd.Timestamp(row["snapshot_date"]), row["qlib_instrument"])
        predicted_hold = _safe_float(pred_series["hold"].get(index_key))
        predicted_grid = _safe_float(pred_series["grid"].get(index_key))
        predicted_excess = _safe_float(pred_series["excess"].get(index_key))
        predicted_step = _safe_float(pred_series["step"].get(index_key))
        preferred_strategy = "网格交易" if predicted_excess is not None and predicted_excess >= 0 else "买入持有"
        recommended_return = predicted_grid if preferred_strategy == "网格交易" else predicted_hold
        comparison_return = predicted_hold if preferred_strategy == "网格交易" else predicted_grid
        strategy_edge = None
        if recommended_return is not None and comparison_return is not None:
            strategy_edge = round(recommended_return - comparison_return, 4)
        prediction_rows.append(
            {
                "code": code,
                "name": row.get("name"),
                "category": row.get("category"),
                "predict_date": row.get("snapshot_date"),
                "hold_score": predicted_hold,
                "hold_rank": hold_ranks.get(code, {}).get("rank"),
                "hold_percentile": hold_ranks.get(code, {}).get("percentile"),
                "grid_score": predicted_grid,
                "grid_rank": grid_ranks.get(code, {}).get("rank"),
                "grid_percentile": grid_ranks.get(code, {}).get("percentile"),
                "excess_score": predicted_excess,
                "excess_rank": excess_ranks.get(code, {}).get("rank"),
                "excess_percentile": excess_ranks.get(code, {}).get("percentile"),
                "step_score": predicted_step,
                "predicted_buy_hold_return_pct": predicted_hold,
                "predicted_grid_return_pct": predicted_grid,
                "predicted_grid_excess_pct": predicted_excess,
                "predicted_best_step_pct": round(_clamp(predicted_step or 1.5, 0.8, 4.5), 2) if predicted_step is not None else None,
                "preferred_strategy": preferred_strategy,
                "recommended_return_pct": recommended_return,
                "comparison_return_pct": comparison_return,
                "strategy_edge_pct": strategy_edge,
                "model_status": "trained",
                "created_at": _now_iso(),
            }
        )

    model_path = str(_ETF_QLIB_MODEL_DIR / f"{model_id}.pkl")
    with open(model_path, "wb") as handle:
        pickle.dump(
            {
                "models": models,
                "features": list(_FEATURE_COLUMNS),
                "segments": segments,
                "params": train_params,
                "model_id": model_id,
            },
            handle,
        )

    _persist_predictions(conn, model_id, prediction_rows)
    _persist_live_backtests(conn, model_id)
    _persist_model_state(
        conn,
        model_id,
        {
            "status": "trained",
            "train_start": segments["train"][0],
            "train_end": segments["train"][1],
            "valid_start": segments["valid"][0],
            "valid_end": segments["valid"][1],
            "test_start": segments["test"][0],
            "test_end": segments["test"][1],
            "etf_count": len({row["code"] for row in latest_feature_rows}),
            "sample_count": len(base_frame),
            "feature_count": len(_FEATURE_COLUMNS),
            "hold_ic_mean": hold_ic,
            "grid_ic_mean": grid_ic,
            "excess_ic_mean": excess_ic,
            "step_mae": step_mae,
            "strategy_accuracy": strategy_accuracy,
            "test_top20_hold_return": top20_hold,
            "test_top20_strategy_return": top20_strategy,
            "model_path": model_path,
            "data_dir": dump_info.get("data_dir"),
            "train_params": train_params,
            "created_at": created_at,
            "finished_at": _now_iso(),
        },
    )

    try:
        from services.etf_snapshot_manager import invalidate_etf_snapshot_cache

        invalidate_etf_snapshot_cache()
    except Exception:
        pass

    return get_latest_etf_qlib_signal_snapshot(conn, topk=snapshot_topk)


def get_etf_qlib_pipeline_status(conn) -> Dict[str, Any]:
    existing_names = _existing_table_names(conn)
    feature_count = _table_count(conn, "etf_qlib_feature_store")
    label_count = _table_count(conn, "etf_qlib_label_store")
    model_count = _table_count(conn, "etf_qlib_model_state")
    prediction_count = _table_count(conn, "etf_qlib_predictions")
    backtest_count = _table_count(conn, "etf_qlib_backtest_result")
    param_search_count = _table_count(conn, "etf_qlib_param_search")

    capability_matrix = [
        {
            "key": "data_layer",
            "label": "ETF-only 数据层",
            "status": "ready" if feature_count and label_count else "pending",
            "detail": "仅允许 ETF 自身价格、成交额、波动、回撤、折溢价等原生数据入模。",
        },
        {
            "key": "training",
            "label": "ETF-only 训练",
            "status": "ready" if model_count and prediction_count else "pending",
            "detail": "训练目标拆分为持有收益、网格收益、参数稳定性，不复用股票 universe。",
        },
        {
            "key": "backtest",
            "label": "ETF-only 回测",
            "status": "ready" if backtest_count else "pending",
            "detail": "统一沉淀持有收益、网格收益、回撤、换手和稳定性指标。",
        },
        {
            "key": "param_search",
            "label": "ETF-only 参数寻优",
            "status": "ready" if param_search_count else "pending",
            "detail": "为每只 ETF 记录最优步长、参数置信度和跨窗口稳定性。",
        },
    ]
    for item in capability_matrix:
        item["status_label"] = _status_label(item["status"])

    if all(item["status"] == "ready" for item in capability_matrix):
        pipeline_status = "ready"
    elif any(item["status"] == "ready" for item in capability_matrix):
        pipeline_status = "partial"
    else:
        pipeline_status = "pending"

    model_row = _latest_model_row(conn)
    latest_model = dict(model_row) if model_row else {}
    universe_row = conn.execute("SELECT COUNT(*) AS total FROM etf_asset_universe WHERE is_active = 1").fetchone()
    required_tables = [{"name": name, "label": label} for name, label in _REQUIRED_TABLES]
    existing_tables = [item for item in required_tables if item["name"] in existing_names]
    missing_tables = [item["name"] for item in required_tables if item["name"] not in existing_names or _table_count(conn, item["name"]) == 0]

    model_status = latest_model.get("status") or "none"
    return {
        "available": pipeline_status == "ready",
        "isolation_mode": "etf_only",
        "mixed_with_stock_qlib": False,
        "pipeline_status": pipeline_status,
        "pipeline_status_label": _status_label(pipeline_status),
        "message": "ETF 模块只使用 ETF 自身数据进行 Qlib 特征、训练、预测、参数寻优和回测，不再消费股票侧映射结果。",
        "etf_universe_count": int(universe_row["total"] or 0) if universe_row else 0,
        "required_table_count": len(required_tables),
        "existing_table_count": len(existing_tables),
        "required_tables": required_tables,
        "existing_tables": existing_tables,
        "missing_tables": missing_tables,
        "feature_store_row_count": feature_count,
        "label_store_row_count": label_count,
        "model_row_count": model_count,
        "prediction_row_count": prediction_count,
        "backtest_row_count": backtest_count,
        "param_search_row_count": param_search_count,
        "table_counts": {
            "feature_store": feature_count,
            "label_store": label_count,
            "model_state": model_count,
            "predictions": prediction_count,
            "backtests": backtest_count,
            "param_search": param_search_count,
        },
        "capability_matrix": capability_matrix,
        "model_status": model_status,
        "model_status_label": _status_label(model_status),
        "model_id": latest_model.get("model_id"),
        "model_created_at": latest_model.get("created_at"),
        "model_finished_at": latest_model.get("finished_at"),
        "model_error": latest_model.get("error"),
        "model_sample_count": latest_model.get("sample_count"),
        "model_feature_count": latest_model.get("feature_count"),
        "model_etf_count": latest_model.get("etf_count"),
        "data_dir": str(_ETF_QLIB_DATA_DIR),
        "roadmap": [
            "ETF-only 特征层与标签层已经落库，可持续重算。",
            "ETF-only Qlib 模型直接输出持有收益、网格收益、参数稳定性和推荐策略。",
            "ETF 工作台、机会发现和深度分析统一消费 ETF-only Qlib 结果。",
        ],
        "warnings": [
            "当前推荐策略仍需与 ETF 实盘约束回测一起看，不能只看模型预测分数。",
            "网格收益和持有收益会继续并排展示，Qlib 负责预测与参数寻优，不隐藏基准结果。",
        ],
    }


def get_latest_etf_qlib_signal_snapshot(conn, *, topk: int = 50, force_refresh: bool = False) -> Dict[str, Any]:
    if force_refresh:
        return train_etf_qlib_pipeline(conn, force_refresh=True, snapshot_topk=topk)

    status = get_etf_qlib_pipeline_status(conn)
    model_row = _latest_model_row(conn)
    if not model_row or model_row["status"] != "trained":
        status.update(
            {
                "model_id": model_row["model_id"] if model_row else None,
                "model_status": model_row["status"] if model_row else "none",
                "signal_date": None,
                "prediction_count": 0,
                "categories": [],
                "top_hold_predictions": [],
                "top_grid_predictions": [],
                "top_strategy_predictions": [],
                "prediction_map": {},
            }
        )
        return status

    model = dict(model_row)
    prediction_rows = [
        dict(row)
        for row in conn.execute(
            "SELECT * FROM etf_qlib_predictions WHERE model_id = ? ORDER BY code ASC",
            (model["model_id"],),
        ).fetchall()
    ]
    prediction_map = {}
    for row in prediction_rows:
        preferred_strategy = row.get("preferred_strategy") or "买入持有"
        consensus_score = row.get("hold_percentile") if preferred_strategy == "买入持有" else row.get("excess_percentile")
        prediction_map[row["code"]] = {
            "consensus_score": consensus_score,
            "consensus_percentile": consensus_score,
            "leading_factor_group": "etf_grid" if preferred_strategy == "网格交易" else "etf_hold",
            "high_conviction_count": 1 if consensus_score is not None and consensus_score >= 80 else 0,
            "model_status": row.get("model_status") or model.get("status") or "trained",
            "test_top50_avg_return": _safe_round((model.get("test_top20_strategy_return") or 0.0), 6),
            "preferred_strategy": preferred_strategy,
            "predicted_buy_hold_return_pct": row.get("predicted_buy_hold_return_pct"),
            "predicted_grid_return_pct": row.get("predicted_grid_return_pct"),
            "predicted_grid_excess_pct": row.get("predicted_grid_excess_pct"),
            "predicted_best_step_pct": row.get("predicted_best_step_pct"),
            "recommended_return_pct": row.get("recommended_return_pct"),
            "comparison_return_pct": row.get("comparison_return_pct"),
            "strategy_edge_pct": row.get("strategy_edge_pct"),
        }

    def _sort_rows(rows: list[dict], key_name: str) -> list[dict]:
        return sorted(
            rows,
            key=lambda item: (
                -(_safe_float(item.get(key_name)) or -9999.0),
                item.get("code") or "",
            ),
        )[:topk]

    categories = []
    grouped: dict[str, list[dict]] = {}
    for row in prediction_rows:
        grouped.setdefault(row.get("category") or "未分类", []).append(row)
    for category, items in grouped.items():
        top_items = sorted(
            items,
            key=lambda item: (
                -(_safe_float(item.get("recommended_return_pct")) or -9999.0),
                item.get("code") or "",
            ),
        )[:5]
        categories.append(
            {
                "category": category,
                "avg_consensus_score": _safe_round(_mean([prediction_map[item["code"]].get("consensus_score") for item in items]), 2),
                "avg_hold_return_pct": _safe_round(_mean([_safe_float(item.get("predicted_buy_hold_return_pct")) for item in items]), 2),
                "avg_grid_return_pct": _safe_round(_mean([_safe_float(item.get("predicted_grid_return_pct")) for item in items]), 2),
                "avg_strategy_edge_pct": _safe_round(_mean([_safe_float(item.get("strategy_edge_pct")) for item in items]), 2),
                "grid_count": sum(1 for item in items if item.get("preferred_strategy") == "网格交易"),
                "hold_count": sum(1 for item in items if item.get("preferred_strategy") == "买入持有"),
                "top_etfs": [
                    {
                        "code": item.get("code"),
                        "name": item.get("name"),
                        "preferred_strategy": item.get("preferred_strategy"),
                        "predicted_best_step_pct": item.get("predicted_best_step_pct"),
                        "recommended_return_pct": item.get("recommended_return_pct"),
                    }
                    for item in top_items
                ],
            }
        )
    categories.sort(
        key=lambda item: (
            -(_safe_float(item.get("avg_consensus_score")) or -9999.0),
            -(_safe_float(item.get("avg_strategy_edge_pct")) or -9999.0),
            item.get("category") or "",
        )
    )

    status.update(
        {
            "available": True,
            "pipeline_status": "ready",
            "pipeline_status_label": _status_label("ready"),
            "model_id": model.get("model_id"),
            "model_status": model.get("status"),
            "signal_date": max((row.get("predict_date") for row in prediction_rows if row.get("predict_date")), default=None),
            "prediction_count": len(prediction_rows),
            "sample_count": model.get("sample_count"),
            "feature_count": model.get("feature_count"),
            "hold_ic_mean": model.get("hold_ic_mean"),
            "grid_ic_mean": model.get("grid_ic_mean"),
            "excess_ic_mean": model.get("excess_ic_mean"),
            "step_mae": model.get("step_mae"),
            "strategy_accuracy": model.get("strategy_accuracy"),
            "test_top20_hold_return": model.get("test_top20_hold_return"),
            "test_top20_strategy_return": model.get("test_top20_strategy_return"),
            "categories": categories[:topk],
            "top_hold_predictions": _sort_rows(prediction_rows, "predicted_buy_hold_return_pct"),
            "top_grid_predictions": _sort_rows(prediction_rows, "predicted_grid_return_pct"),
            "top_strategy_predictions": _sort_rows(prediction_rows, "recommended_return_pct"),
            "prediction_map": prediction_map,
        }
    )
    return status
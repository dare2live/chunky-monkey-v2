"""
qlib_full_engine.py — Qlib AI 多因子引擎

基于 pyqlib 0.9.7，使用 Alpha158 + 自定义因子（财务 + 机构）+ LGBModel 标准训练管线。
查询 API（get_model_status/get_predictions/get_factor_importance）不依赖 pyqlib，
仅训练功能需要 pyqlib 安装。
"""

import json
import logging
from contextlib import nullcontext
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("cm-api")

_QLIB_AVAILABLE = True
_QLIB_ERROR = None
try:
    import qlib
    from qlib.config import REG_CN
except ImportError as e:
    _QLIB_AVAILABLE = False
    _QLIB_ERROR = str(e)

_MODEL_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "qlib_models"
_DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "qlib_data"
_RUNS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "qlib_runs"


def is_available() -> tuple[bool, Optional[str]]:
    return _QLIB_AVAILABLE, _QLIB_ERROR


def _safe_round(value, digits: int = 4):
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except Exception:
        return None


# ============================================================
# Schema
# ============================================================

def ensure_tables(conn):
    """创建完整 Qlib 专属表"""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS qlib_model_state (
            model_id        TEXT PRIMARY KEY,
            status          TEXT NOT NULL DEFAULT 'idle',
            train_start     TEXT,
            train_end       TEXT,
            valid_start     TEXT,
            valid_end       TEXT,
            test_start      TEXT,
            test_end        TEXT,
            stock_count     INTEGER,
            factor_count    INTEGER,
            ic_mean         REAL,
            rank_ic_mean    REAL,
            test_top50_avg_return REAL,
            error           TEXT,
            model_path      TEXT,
            train_params_json TEXT,
            created_at      TEXT,
            finished_at     TEXT
        );

        CREATE TABLE IF NOT EXISTS qlib_predictions (
            model_id        TEXT NOT NULL,
            stock_code      TEXT NOT NULL,
            stock_name      TEXT,
            predict_date    TEXT,
            qlib_score      REAL,
            qlib_rank       INTEGER,
            qlib_percentile REAL,
            PRIMARY KEY (model_id, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_qp_rank ON qlib_predictions(model_id, qlib_rank);

        CREATE TABLE IF NOT EXISTS qlib_factor_importance (
            model_id        TEXT NOT NULL,
            factor_name     TEXT NOT NULL,
            importance      REAL,
            factor_group    TEXT,
            PRIMARY KEY (model_id, factor_name)
        );

        CREATE TABLE IF NOT EXISTS qlib_data_state (
            data_dir        TEXT PRIMARY KEY,
            last_dump_date  TEXT,
            stock_count     INTEGER,
            trading_days    INTEGER,
            min_date        TEXT,
            max_date        TEXT,
            format_version  TEXT DEFAULT 'v1',
            updated_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS qlib_backtest_result (
            model_id        TEXT NOT NULL,
            backtest_id     TEXT NOT NULL,
            strategy        TEXT,
            sharpe_ratio    REAL,
            calmar_ratio    REAL,
            max_drawdown    REAL,
            annual_return   REAL,
            turnover        REAL,
            detail_json     TEXT,
            created_at      TEXT,
            PRIMARY KEY (model_id, backtest_id)
        );
    """)
    for ddl in [
        "ALTER TABLE qlib_model_state ADD COLUMN train_params_json TEXT",
    ]:
        try:
            conn.execute(ddl)
        except Exception:
            pass
    conn.commit()


# ============================================================
# 初始化
# ============================================================

def init_qlib(data_dir: str = None) -> bool:
    """初始化 pyqlib 环境"""
    if not _QLIB_AVAILABLE:
        logger.warning(f"[Qlib-Full] pyqlib 不可用: {_QLIB_ERROR}")
        return False

    data_path = data_dir or str(_DATA_DIR)
    try:
        qlib.init(provider_uri=data_path, region=REG_CN)
        logger.info(f"[Qlib-Full] 初始化成功: {data_path}")
        return True
    except Exception as e:
        logger.error(f"[Qlib-Full] 初始化失败: {e}")
        return False


# ============================================================
# 自定义因子（财务 + 机构）
# ============================================================

def _load_financial_factors(smart_conn, codes: list) -> pd.DataFrame:
    """从 fact_financial_derived 加载财务因子"""
    placeholders = ",".join("?" for _ in codes)
    rows = smart_conn.execute(
        f"SELECT stock_code, roe, debt_ratio, current_ratio, gross_margin, "
        f"net_margin, revenue_yoy, profit_yoy, ocf_to_profit "
        f"FROM dim_financial_latest WHERE stock_code IN ({placeholders})",
        codes
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    data = []
    for r in rows:
        code = r["stock_code"]
        prefix = "SH" if str(code).startswith("6") else "SZ"
        data.append({
            "instrument": f"{prefix}{code}",
            "fin_roe": r["roe"],
            "fin_debt_ratio": r["debt_ratio"],
            "fin_current_ratio": r["current_ratio"],
            "fin_gross_margin": r["gross_margin"],
            "fin_net_margin": r["net_margin"],
            "fin_revenue_yoy": r["revenue_yoy"],
            "fin_profit_yoy": r["profit_yoy"],
            "fin_ocf_to_profit": r["ocf_to_profit"],
        })

    return pd.DataFrame(data).set_index("instrument")


def _load_institution_factors(smart_conn, codes: list) -> pd.DataFrame:
    """从 mart_stock_trend 加载机构因子"""
    placeholders = ",".join("?" for _ in codes)
    rows = smart_conn.execute(
        f"SELECT stock_code, inst_count_t0, inst_count_t1, inst_count_t2, "
        f"composite_priority_score, discovery_score "
        f"FROM mart_stock_trend WHERE stock_code IN ({placeholders})",
        codes
    ).fetchall()

    if not rows:
        return pd.DataFrame()

    data = []
    for r in rows:
        code = r["stock_code"]
        prefix = "SH" if str(code).startswith("6") else "SZ"
        data.append({
            "instrument": f"{prefix}{code}",
            "inst_count_t0": r["inst_count_t0"],
            "inst_trend": (1 if (r["inst_count_t0"] or 0) > (r["inst_count_t1"] or 0)
                           else -1 if (r["inst_count_t0"] or 0) < (r["inst_count_t1"] or 0)
                           else 0),
            "inst_composite_priority": r["composite_priority_score"],
            "inst_discovery_score": r["discovery_score"],
        })

    return pd.DataFrame(data).set_index("instrument")


def _normalize_predictions(pred) -> pd.DataFrame:
    """标准化 Qlib 预测输出为 (datetime, instrument, qlib_score) DataFrame"""
    if pred is None:
        return pd.DataFrame(columns=["datetime", "instrument", "qlib_score"])

    if isinstance(pred, pd.Series):
        pred_df = pred.to_frame("qlib_score")
    elif isinstance(pred, pd.DataFrame):
        pred_df = pred.copy()
        if "qlib_score" not in pred_df.columns:
            first_col = pred_df.columns[0]
            pred_df = pred_df.rename(columns={first_col: "qlib_score"})
    else:
        pred_df = pd.DataFrame(pred)
        if pred_df.empty:
            return pd.DataFrame(columns=["datetime", "instrument", "qlib_score"])
        if "qlib_score" not in pred_df.columns:
            first_col = pred_df.columns[0]
            pred_df = pred_df.rename(columns={first_col: "qlib_score"})

    if isinstance(pred_df.index, pd.MultiIndex):
        names = list(pred_df.index.names)
        if "datetime" in names and "instrument" in names:
            pred_df = pred_df.reset_index()[["datetime", "instrument", "qlib_score"]]
        else:
            pred_df = pred_df.reset_index()
            pred_df.columns = ["datetime", "instrument", "qlib_score"]
    else:
        pred_df = pred_df.reset_index()
        if len(pred_df.columns) >= 3:
            pred_df = pred_df.iloc[:, :3]
            pred_df.columns = ["datetime", "instrument", "qlib_score"]
        elif len(pred_df.columns) == 2:
            pred_df.columns = ["instrument", "qlib_score"]
            pred_df["datetime"] = None
        else:
            return pd.DataFrame(columns=["datetime", "instrument", "qlib_score"])

    pred_df["qlib_score"] = pd.to_numeric(pred_df["qlib_score"], errors="coerce")
    pred_df = pred_df.dropna(subset=["qlib_score"])
    return pred_df


def _stock_code_from_instrument(instrument: str) -> str:
    text = str(instrument or "")
    if len(text) >= 8 and text[:2].isalpha():
        return text[2:]
    return text


def _extract_metric(metrics: dict, *suffixes: str):
    if not metrics:
        return None
    for suffix in suffixes:
        for key, value in metrics.items():
            if str(key).endswith(suffix):
                return _safe_round(value)
    return None


def _calc_topk_avg_return(recorder, topk: int = 50) -> Optional[float]:
    try:
        pred = recorder.load_object("pred.pkl")
        label = recorder.load_object("label.pkl")
    except Exception:
        return None
    if pred is None or label is None:
        return None
    try:
        pred_df = pred.iloc[:, [0]].rename(columns={pred.columns[0]: "score"})
        label_df = label.iloc[:, [0]].rename(columns={label.columns[0]: "label"})
        joined = pred_df.join(label_df, how="inner").dropna(subset=["score", "label"])
        if joined.empty:
            return None
        mean_returns = []
        for _, group in joined.groupby(level="datetime"):
            top_group = group.sort_values("score", ascending=False).head(topk)
            if not top_group.empty:
                mean_returns.append(float(top_group["label"].mean()))
        if not mean_returns:
            return None
        return _safe_round(np.mean(mean_returns))
    except Exception:
        return None


def _resolve_backtest_benchmark(data_dir: str, requested: Optional[str]) -> Optional[str]:
    instruments_path = Path(data_dir) / "instruments" / "all.txt"
    if not instruments_path.exists():
        return requested
    try:
        with instruments_path.open() as handle:
            instruments = {line.strip().split("\t")[0] for line in handle if line.strip()}
    except Exception:
        return requested
    if requested and requested in instruments:
        return requested
    fallback_candidates = [
        "SZ159919",  # 沪深300ETF
        "SZ159915",  # 创业板ETF
        "SZ159949",  # 创业板50ETF
        "SZ159918",  # 中小板ETF
    ]
    for code in fallback_candidates:
        if code in instruments:
            return code
    return None


def _extract_feature_meta(lgb_model) -> tuple[list[str], list[float]]:
    model = getattr(lgb_model, "model", None)
    if model is None:
        return [], []
    feature_names = []
    importances = []
    try:
        feature_names = list(model.feature_name())
        importances = list(model.feature_importance(importance_type="gain"))
    except Exception:
        try:
            booster = getattr(model, "booster_", None)
            if booster is not None:
                feature_names = list(booster.feature_name())
                importances = list(booster.feature_importance(importance_type="gain"))
        except Exception:
            logger.warning("[Qlib-Full] 无法提取特征重要性，跳过写入")
    return feature_names, importances


def _persist_training_outputs(smart_conn, *, model_id: str, params: dict,
                              model_path: str, pred, lgb_model) -> dict:
    pred_df = _normalize_predictions(pred)
    if pred_df.empty:
        raise RuntimeError("Qlib 预测结果为空，无法写回排名")

    if pred_df["datetime"].notna().any():
        latest_dt = pred_df["datetime"].dropna().max()
        latest_df = pred_df[pred_df["datetime"] == latest_dt].copy()
    else:
        latest_dt = datetime.now().strftime("%Y-%m-%d")
        latest_df = pred_df.copy()

    latest_df["stock_code"] = latest_df["instrument"].map(_stock_code_from_instrument)
    latest_df = latest_df.sort_values("qlib_score", ascending=False).reset_index(drop=True)
    latest_df["qlib_rank"] = latest_df.index + 1
    total = len(latest_df)
    latest_df["qlib_percentile"] = latest_df["qlib_rank"].map(
        lambda rank: round((1 - ((rank - 1) / total)) * 100, 2) if total else None
    )

    info_rows = smart_conn.execute("""
        SELECT stock_code, stock_name
        FROM dim_active_a_stock
    """).fetchall()
    info_map = {row["stock_code"]: dict(row) for row in info_rows}

    smart_conn.execute("DELETE FROM qlib_predictions WHERE model_id = ?", (model_id,))
    for row in latest_df.itertuples(index=False):
        meta = info_map.get(row.stock_code, {})
        smart_conn.execute("""
            INSERT OR REPLACE INTO qlib_predictions
            (model_id, stock_code, stock_name, predict_date, qlib_score, qlib_rank, qlib_percentile)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            model_id,
            row.stock_code,
            meta.get("stock_name") or row.stock_code,
            str(latest_dt)[:10],
            float(row.qlib_score),
            int(row.qlib_rank),
            float(row.qlib_percentile),
        ))

    feature_names, importances = _extract_feature_meta(lgb_model)
    smart_conn.execute("DELETE FROM qlib_factor_importance WHERE model_id = ?", (model_id,))
    for factor_name, importance in zip(feature_names, importances):
        if importance is None:
            continue
        if str(factor_name).startswith("inst_"):
            factor_group = "institution"
        elif str(factor_name).startswith("fin_"):
            factor_group = "financial"
        else:
            factor_group = "alpha158"
        smart_conn.execute("""
            INSERT OR REPLACE INTO qlib_factor_importance
            (model_id, factor_name, importance, factor_group)
            VALUES (?, ?, ?, ?)
        """, (model_id, str(factor_name), float(importance), factor_group))

    smart_conn.execute("""
        UPDATE qlib_model_state
        SET status = 'trained',
            stock_count = ?,
            factor_count = ?,
            error = NULL,
            model_path = ?,
            train_params_json = ?,
            finished_at = ?
        WHERE model_id = ?
    """, (
        len(latest_df),
        len(feature_names),
        model_path,
        json.dumps(params, ensure_ascii=False),
        datetime.now().isoformat(),
        model_id,
    ))
    smart_conn.commit()
    return {
        "predictions_count": len(latest_df),
        "factor_count": len(feature_names),
        "predict_date": str(latest_dt)[:10],
    }


def _persist_workflow_records(smart_conn, *, model_id: str, dataset, model, params: dict) -> dict:
    from qlib.workflow import R
    from qlib.workflow.record_temp import PortAnaRecord, SigAnaRecord, SignalRecord

    _RUNS_DIR.mkdir(parents=True, exist_ok=True)
    experiment_name = "chunky_monkey_qlib"
    record_summary = {
        "ic_mean": None,
        "rank_ic_mean": None,
        "test_top50_avg_return": None,
        "backtest_id": None,
    }
    benchmark_code = None
    if params.get("use_benchmark"):
        benchmark_code = _resolve_backtest_benchmark(
            str(_DATA_DIR),
            params.get("benchmark", "SH000300"),
        )
    backtest_config = {
        "strategy": {
            "class": "TopkDropoutStrategy",
            "module_path": "qlib.contrib.strategy",
            "kwargs": {
                "signal": "<PRED>",
                "topk": int(params.get("backtest_topk", 50)),
                "n_drop": int(params.get("backtest_n_drop", 5)),
            },
        },
        "backtest": {
            "start_time": params.get("valid_end") or params.get("test_start"),
            "end_time": params.get("test_end"),
            "account": float(params.get("backtest_account", 100000000)),
            "exchange_kwargs": {
                "limit_threshold": 0.095,
                "deal_price": "close",
                "open_cost": 0.0005,
                "close_cost": 0.0015,
                "min_cost": 5,
            },
        },
    }
    if benchmark_code:
        backtest_config["backtest"]["benchmark"] = benchmark_code

    recorder = None
    started_new_exp = False
    try:
        recorder = R.get_recorder()
    except Exception:
        recorder = None

    record_ctx = nullcontext(recorder)
    if recorder is None:
        started_new_exp = True
        record_ctx = R.start(
            experiment_name=experiment_name,
            recorder_name=model_id,
            uri=str(_RUNS_DIR),
        )

    with record_ctx:
        if recorder is None:
            recorder = R.get_recorder()
        recorder.log_params(
            model_id=model_id,
            train_start=params.get("train_start"),
            train_end=params.get("train_end"),
            valid_end=params.get("valid_end"),
            test_end=params.get("test_end"),
            benchmark=benchmark_code,
            use_alpha158=params.get("use_alpha158", True),
            use_financial=params.get("use_financial", True),
            use_institution=params.get("use_institution", True),
        )
        SignalRecord(model=model, dataset=dataset, recorder=recorder).generate()
        SigAnaRecord(recorder, ana_long_short=True).generate()

        metrics = recorder.list_metrics() or {}
        ic_mean = _safe_round(metrics.get("IC"))
        rank_ic_mean = _safe_round(metrics.get("Rank IC"))
        test_top50_avg_return = _calc_topk_avg_return(
            recorder, topk=int(params.get("backtest_topk", 50))
        )
        annual_return = _extract_metric(metrics, "excess_return_with_cost.annualized_return")
        sharpe_ratio = _extract_metric(metrics, "excess_return_with_cost.information_ratio")
        max_drawdown = _extract_metric(metrics, "excess_return_with_cost.max_drawdown")
        turnover = _extract_metric(metrics, "turnover")
        calmar_ratio = None
        if annual_return is not None and max_drawdown not in (None, 0):
            try:
                calmar_ratio = _safe_round(float(annual_return) / abs(float(max_drawdown)))
            except Exception:
                calmar_ratio = None

        smart_conn.execute(
            """
            UPDATE qlib_model_state
            SET ic_mean = ?, rank_ic_mean = ?, test_top50_avg_return = ?
            WHERE model_id = ?
            """,
            (ic_mean, rank_ic_mean, test_top50_avg_return, model_id),
        )

        backtest_id = None
        try:
            PortAnaRecord(
                recorder,
                config=backtest_config,
                risk_analysis_freq="day",
                indicator_analysis_freq="day",
            ).generate()
            metrics = recorder.list_metrics() or metrics
            annual_return = _extract_metric(metrics, "excess_return_with_cost.annualized_return")
            sharpe_ratio = _extract_metric(metrics, "excess_return_with_cost.information_ratio")
            max_drawdown = _extract_metric(metrics, "excess_return_with_cost.max_drawdown")
            turnover = _extract_metric(metrics, "turnover")
            calmar_ratio = None
            if annual_return is not None and max_drawdown not in (None, 0):
                try:
                    calmar_ratio = _safe_round(float(annual_return) / abs(float(max_drawdown)))
                except Exception:
                    calmar_ratio = None
            backtest_id = f"{model_id}_default_day"
            smart_conn.execute(
                """
                INSERT OR REPLACE INTO qlib_backtest_result
                (model_id, backtest_id, strategy, sharpe_ratio, calmar_ratio, max_drawdown, annual_return, turnover, detail_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    model_id,
                    backtest_id,
                    "TopkDropoutStrategy(topk=50,n_drop=5)",
                    sharpe_ratio,
                    calmar_ratio,
                    max_drawdown,
                    annual_return,
                    turnover,
                    json.dumps(metrics, ensure_ascii=False),
                    datetime.now().isoformat(),
                ),
            )
        except Exception as exc:
            record_summary["backtest_error"] = str(exc)
            logger.warning(f"[Qlib-Full] PortAnaRecord 回测失败，保留信号分析结果继续写回: {exc}")
        smart_conn.commit()
        record_summary.update(
            {
                "ic_mean": ic_mean,
                "rank_ic_mean": rank_ic_mean,
                "test_top50_avg_return": test_top50_avg_return,
                "backtest_id": backtest_id,
                "backtest_annual_return": annual_return,
                "backtest_sharpe_ratio": sharpe_ratio,
                "backtest_max_drawdown": max_drawdown,
            }
        )
    if not started_new_exp:
        try:
            R.end_exp()
        except Exception:
            pass
    return record_summary


def sync_latest_predictions_to_stock_trend(smart_conn, model_id: Optional[str] = None) -> int:
    ensure_tables(smart_conn)
    for ddl in [
        "ALTER TABLE mart_stock_trend ADD COLUMN qlib_score REAL",
        "ALTER TABLE mart_stock_trend ADD COLUMN qlib_percentile REAL",
        "ALTER TABLE mart_stock_trend ADD COLUMN qlib_rank INTEGER",
    ]:
        try:
            smart_conn.execute(ddl)
        except Exception:
            pass
    if not model_id:
        row = smart_conn.execute(
            "SELECT model_id FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return 0
        model_id = row["model_id"]

    rows = smart_conn.execute(
        "SELECT stock_code, qlib_rank, qlib_score, qlib_percentile FROM qlib_predictions WHERE model_id = ?",
        (model_id,)
    ).fetchall()
    smart_conn.execute(
        "UPDATE mart_stock_trend SET qlib_rank = NULL, qlib_score = NULL, qlib_percentile = NULL"
    )
    updated = 0
    for row in rows:
        cur = smart_conn.execute("""
            UPDATE mart_stock_trend
            SET qlib_rank = ?, qlib_score = ?, qlib_percentile = ?
            WHERE stock_code = ?
        """, (row["qlib_rank"], row["qlib_score"], row["qlib_percentile"], row["stock_code"]))
        updated += cur.rowcount or 0
    smart_conn.commit()
    return updated


# ============================================================
# 训练
# ============================================================

def train_full_model(smart_conn, data_dir: str = None, *, params: Optional[dict] = None) -> dict:
    """完整 Qlib 训练管线：Alpha158 + 自定义因子 + LGBModel"""
    if not _QLIB_AVAILABLE:
        raise RuntimeError(f"pyqlib 不可用: {_QLIB_ERROR}")

    params = dict(params or {})
    train_start = params.get("train_start") or "2023-01-01"
    train_end = params.get("train_end") or "2025-03-31"
    valid_end = params.get("valid_end") or "2025-09-30"
    test_end = params.get("test_end") or "2026-01-31"
    use_financial = params.get("use_financial", True)
    use_institution = params.get("use_institution", True)
    sample_stock_limit = int(params.get("sample_stock_limit", 0) or 0)
    if params.get("use_alpha158", True) is False:
        logger.warning("[Qlib-Full] 当前版本暂不支持关闭 Alpha158，已自动保留基础量价因子")
        params["use_alpha158"] = True

    data_path = data_dir or str(_DATA_DIR)
    from services.qlib_data_handler import dump_bin_from_db, get_qlib_data_status
    qlib_data_status = get_qlib_data_status(data_path)
    if not qlib_data_status.get("available"):
        logger.info("[Qlib-Full] 检测到 qlib_data 不可用，开始从 market_data.db 自动构建")
        from services.market_db import get_market_conn

        mkt_conn = get_market_conn()
        try:
            dump_result = dump_bin_from_db(mkt_conn, data_dir=data_path)
            logger.info(f"[Qlib-Full] 已自动构建 qlib_data: {dump_result}")
        finally:
            try:
                mkt_conn.close()
            except Exception:
                pass

    if not init_qlib(data_path):
        raise RuntimeError("Qlib 初始化失败")

    from qlib.config import C
    qlib_joblib_backend = params.get("qlib_joblib_backend") or "threading"
    qlib_kernels = int(params.get("qlib_kernels", 1) or 1)
    C["joblib_backend"] = qlib_joblib_backend
    C["kernels"] = max(1, qlib_kernels)
    logger.info(
        f"[Qlib-Full] 使用安全执行模式 backend={C['joblib_backend']} kernels={C['kernels']}"
    )

    ensure_tables(smart_conn)
    _MODEL_DIR.mkdir(parents=True, exist_ok=True)

    model_id = f"lgb_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger.info(f"[Qlib-Full] 开始训练: {model_id}")
    smart_conn.execute("""
        INSERT OR REPLACE INTO qlib_model_state
        (model_id, status, train_start, train_end, valid_start, valid_end, test_start, test_end,
         error, model_path, train_params_json, created_at, finished_at)
        VALUES (?, 'training', ?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, NULL)
    """, (
        model_id,
        train_start,
        train_end,
        train_end,
        valid_end,
        valid_end,
        test_end,
        json.dumps(params, ensure_ascii=False),
        datetime.now().isoformat(),
    ))
    smart_conn.commit()

    try:
        from qlib.contrib.data.handler import Alpha158
        from qlib.contrib.model.gbdt import LGBModel
        from qlib.data.dataset import DatasetH

        stock_sql = (
            "SELECT DISTINCT a.stock_code "
            "FROM dim_active_a_stock a "
            "LEFT JOIN excluded_stocks e ON e.stock_code = a.stock_code "
            "WHERE e.stock_code IS NULL "
            "ORDER BY a.stock_code"
        )
        if sample_stock_limit > 0:
            stock_sql += f" LIMIT {sample_stock_limit}"
        stock_rows = smart_conn.execute(stock_sql).fetchall()
        all_codes = [r["stock_code"] for r in stock_rows]
        qlib_instruments = [
            ("SH" if str(code).startswith("6") else "SZ") + str(code)
            for code in all_codes
        ]

        # Alpha158 标准因子处理器
        handler_config = {
            "start_time": train_start,
            "end_time": test_end,
            "instruments": qlib_instruments if qlib_instruments else "all",
        }

        handler = Alpha158(**handler_config)
        dataset = DatasetH(
            handler=handler,
            segments={
                "train": (train_start, train_end),
                "valid": (train_end, valid_end),
                "test": (valid_end, test_end),
            }
        )

        # ============================================================
        # 注入自定义因子（财务 + 机构）
        # ============================================================
        fin_factors = _load_financial_factors(smart_conn, all_codes) if (all_codes and use_financial) else pd.DataFrame()
        inst_factors = _load_institution_factors(smart_conn, all_codes) if (all_codes and use_institution) else pd.DataFrame()

        custom_factors = pd.DataFrame()
        if not fin_factors.empty and not inst_factors.empty:
            custom_factors = fin_factors.join(inst_factors, how="outer")
        elif not fin_factors.empty:
            custom_factors = fin_factors
        elif not inst_factors.empty:
            custom_factors = inst_factors

        custom_factor_count = len(custom_factors.columns) if not custom_factors.empty else 0
        logger.info(f"[Qlib-Full] 自定义因子: {custom_factor_count} 个, 覆盖 {len(custom_factors)} 只股票")

        # 如果有自定义因子，注入到 dataset 中
        # Qlib DatasetH 内部用 handler.fetch() 获取特征 DataFrame
        # 我们在 handler._data 层面直接 concat（社区通用做法）
        if not custom_factors.empty:
            try:
                # 获取 handler 已有的数据，并拼接自定义因子
                existing_df = handler.fetch(col_set="feature")
                if existing_df is not None and not existing_df.empty:
                    # existing_df 的 index 是 (datetime, instrument)
                    # custom_factors 的 index 是 instrument
                    # 需要按 instrument 维度广播到所有日期
                    instruments_in_data = existing_df.index.get_level_values("instrument").unique()
                    custom_matched = custom_factors.reindex(instruments_in_data)
                    # 广播到 MultiIndex
                    for col in custom_matched.columns:
                        instrument_values = custom_matched[col]
                        existing_df[col] = existing_df.index.get_level_values("instrument").map(
                            instrument_values.to_dict()
                        ).values
                    logger.info(f"[Qlib-Full] 已注入 {custom_factor_count} 个自定义因子到 Alpha158 数据集")
                else:
                    logger.warning("[Qlib-Full] Alpha158 handler.fetch 返回空数据，跳过因子注入")
            except Exception as e:
                logger.warning(f"[Qlib-Full] 自定义因子注入失败（回退到纯 Alpha158）: {e}")

        # LGBModel
        lgb_model = LGBModel(
            loss="mse",
            num_boost_round=int(params.get("num_boost_round", 500)),
            early_stopping_rounds=int(params.get("early_stopping_rounds", 50)),
            num_leaves=int(params.get("num_leaves", 64)),
            learning_rate=float(params.get("learning_rate", 0.05)),
            subsample=float(params.get("subsample", 0.8)),
            colsample_bytree=float(params.get("colsample_bytree", 0.8)),
        )

        lgb_model.fit(dataset)

        # 预测
        pred = lgb_model.predict(dataset)
        logger.info(f"[Qlib-Full] 预测完成: {len(pred)} 条")

        # 保存模型
        import pickle
        model_path = str(_MODEL_DIR / f"{model_id}.pkl")
        with open(model_path, "wb") as f:
            pickle.dump({"model": lgb_model, "handler_config": handler_config,
                         "custom_factor_count": custom_factor_count}, f)

        persisted = _persist_training_outputs(
            smart_conn,
            model_id=model_id,
            params=params,
            model_path=model_path,
            pred=pred,
            lgb_model=lgb_model,
        )
        workflow_summary = {}
        try:
            workflow_summary = _persist_workflow_records(
                smart_conn,
                model_id=model_id,
                dataset=dataset,
                model=lgb_model,
                params=params,
            )
            logger.info(
                "[Qlib-Full] Workflow records 完成: "
                f"IC={workflow_summary.get('ic_mean')} "
                f"RankIC={workflow_summary.get('rank_ic_mean')} "
                f"Backtest={workflow_summary.get('backtest_id')}"
            )
        except Exception as exc:
            workflow_summary = {
                "workflow_error": str(exc),
                "ic_mean": None,
                "rank_ic_mean": None,
                "test_top50_avg_return": None,
                "backtest_id": None,
            }
            logger.warning(f"[Qlib-Full] Workflow records 生成失败，保留训练结果继续回流: {exc}")
        sync_latest_predictions_to_stock_trend(smart_conn, model_id=model_id)

        result = {
            "model_id": model_id,
            "status": "trained",
            "model_path": model_path,
            "predictions_count": persisted["predictions_count"],
            "custom_factors": custom_factor_count,
            "factor_count": persisted["factor_count"],
            "predict_date": persisted["predict_date"],
            "ic_mean": workflow_summary.get("ic_mean"),
            "rank_ic_mean": workflow_summary.get("rank_ic_mean"),
            "test_top50_avg_return": workflow_summary.get("test_top50_avg_return"),
            "backtest_id": workflow_summary.get("backtest_id"),
            "workflow_error": workflow_summary.get("workflow_error") or workflow_summary.get("backtest_error"),
        }
        logger.info(f"[Qlib-Full] 训练完成: {result}")
        return result

    except Exception as e:
        smart_conn.execute("""
            UPDATE qlib_model_state
            SET status = 'failed', error = ?, finished_at = ?
            WHERE model_id = ?
        """, (str(e), datetime.now().isoformat(), model_id))
        smart_conn.commit()
        logger.error(f"[Qlib-Full] 训练失败: {e}")
        raise


def run_backtest(smart_conn, model_id: str, data_dir: str = None) -> dict:
    """返回最新 Qlib 回测摘要；训练主流程已自动生成标准回测记录。"""
    ensure_tables(smart_conn)
    row = smart_conn.execute(
        """
        SELECT model_id, backtest_id, strategy, sharpe_ratio, calmar_ratio,
               max_drawdown, annual_return, turnover, created_at
        FROM qlib_backtest_result
        WHERE model_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()
    if not row:
        return {"model_id": model_id, "status": "not_ready"}
    return {
        "model_id": row["model_id"],
        "status": "ok",
        "backtest_id": row["backtest_id"],
        "strategy": row["strategy"],
        "sharpe_ratio": _safe_round(row["sharpe_ratio"]),
        "calmar_ratio": _safe_round(row["calmar_ratio"]),
        "max_drawdown": _safe_round(row["max_drawdown"]),
        "annual_return": _safe_round(row["annual_return"]),
        "turnover": _safe_round(row["turnover"]),
        "created_at": row["created_at"],
    }


# ============================================================
# 查询 API（不依赖 pyqlib，只读 SQLite）
# ============================================================

def get_model_status(conn) -> Optional[dict]:
    """返回最新模型状态"""
    ensure_tables(conn)
    row = conn.execute(
        "SELECT * FROM qlib_model_state ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def get_factor_importance(conn, model_id: Optional[str] = None) -> list:
    """返回因子重要性"""
    ensure_tables(conn)
    if not model_id:
        row = conn.execute(
            "SELECT model_id FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return []
        model_id = row["model_id"]

    rows = conn.execute(
        "SELECT * FROM qlib_factor_importance WHERE model_id = ? ORDER BY importance DESC",
        (model_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_model_summary(conn, model_id: Optional[str] = None) -> dict:
    """返回最新训练模型的摘要，供评分卡/验证页直接复用。"""
    ensure_tables(conn)
    if not model_id:
        row = conn.execute(
            "SELECT model_id FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if not row:
            return {}
        model_id = row["model_id"]

    model_row = conn.execute(
        "SELECT * FROM qlib_model_state WHERE model_id = ?",
        (model_id,),
    ).fetchone()
    if not model_row:
        return {}
    model = dict(model_row)

    predict_row = conn.execute(
        """
        SELECT COUNT(*) AS prediction_count,
               MAX(predict_date) AS predict_date,
               AVG(qlib_percentile) AS avg_percentile
        FROM qlib_predictions
        WHERE model_id = ?
        """,
        (model_id,),
    ).fetchone()

    group_rows = conn.execute(
        """
        SELECT COALESCE(factor_group, 'unknown') AS factor_group,
               COUNT(*) AS factor_count,
               SUM(importance) AS total_importance,
               AVG(importance) AS avg_importance
        FROM qlib_factor_importance
        WHERE model_id = ?
        GROUP BY COALESCE(factor_group, 'unknown')
        ORDER BY SUM(importance) DESC, factor_group
        """,
        (model_id,),
    ).fetchall()
    top_factor_rows = conn.execute(
        """
        SELECT factor_name, importance, factor_group
        FROM qlib_factor_importance
        WHERE model_id = ?
        ORDER BY importance DESC, factor_name
        LIMIT 5
        """,
        (model_id,),
    ).fetchall()
    backtest_row = conn.execute(
        """
        SELECT backtest_id, strategy, sharpe_ratio, calmar_ratio, max_drawdown, annual_return, turnover, created_at
        FROM qlib_backtest_result
        WHERE model_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (model_id,),
    ).fetchone()

    params = {}
    try:
        params = json.loads(model.get("train_params_json") or "{}")
    except Exception:
        params = {}

    return {
        "model_id": model_id,
        "status": model.get("status"),
        "train_start": model.get("train_start"),
        "train_end": model.get("train_end"),
        "valid_start": model.get("valid_start"),
        "valid_end": model.get("valid_end"),
        "test_start": model.get("test_start"),
        "test_end": model.get("test_end"),
        "stock_count": int(model.get("stock_count") or 0),
        "factor_count": int(model.get("factor_count") or 0),
        "prediction_count": int((predict_row["prediction_count"] if predict_row else 0) or 0),
        "predict_date": predict_row["predict_date"] if predict_row else None,
        "avg_percentile": _safe_round(predict_row["avg_percentile"] if predict_row else None, 2),
        "ic_mean": _safe_round(model.get("ic_mean")),
        "rank_ic_mean": _safe_round(model.get("rank_ic_mean")),
        "test_top50_avg_return": _safe_round(model.get("test_top50_avg_return")),
        "created_at": model.get("created_at"),
        "finished_at": model.get("finished_at"),
        "train_params": {
            "use_alpha158": bool(params.get("use_alpha158", True)),
            "use_financial": bool(params.get("use_financial", True)),
            "use_institution": bool(params.get("use_institution", True)),
        },
        "factor_groups": [
            {
                "factor_group": row["factor_group"],
                "factor_count": int(row["factor_count"] or 0),
                "total_importance": _safe_round(row["total_importance"], 2),
                "avg_importance": _safe_round(row["avg_importance"], 2),
            }
            for row in group_rows
        ],
        "top_factors": [
            {
                "factor_name": row["factor_name"],
                "factor_group": row["factor_group"],
                "importance": _safe_round(row["importance"], 2),
            }
            for row in top_factor_rows
        ],
        "latest_backtest": (
            {
                "backtest_id": backtest_row["backtest_id"],
                "strategy": backtest_row["strategy"],
                "sharpe_ratio": _safe_round(backtest_row["sharpe_ratio"]),
                "calmar_ratio": _safe_round(backtest_row["calmar_ratio"]),
                "max_drawdown": _safe_round(backtest_row["max_drawdown"]),
                "annual_return": _safe_round(backtest_row["annual_return"]),
                "turnover": _safe_round(backtest_row["turnover"]),
                "created_at": backtest_row["created_at"],
            }
            if backtest_row else None
        ),
    }

"""
stock_forecast_engine.py — 股票预测特征中间事实层

把 Qlib 预测结果拆成结构化预测分：
- 20 日收益概率分
- 60 日相对行业超额分
- 波动收益性价比分

供评分、解释页和训练后回流统一复用。
"""

import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Optional

from services.qlib_full_engine import ensure_tables as ensure_qlib_tables
from services.qlib_full_engine import sync_latest_predictions_to_stock_trend
from services.utils import safe_float as _safe_float, percentile_ranks as _percentile_ranks

logger = logging.getLogger("cm-api")


def _clamp_score(value: Optional[float], lo: float = 0.0, hi: float = 100.0) -> float:
    if value is None:
        return lo
    return round(max(lo, min(hi, float(value))), 2)


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_stock_forecast_features (
            snapshot_date                    TEXT NOT NULL,
            model_id                         TEXT NOT NULL,
            predict_date                     TEXT,
            stock_code                       TEXT NOT NULL,
            stock_name                       TEXT,
            sw_level1                        TEXT,
            sw_level2                        TEXT,
            qlib_score                       REAL,
            qlib_rank                        INTEGER,
            qlib_percentile                  REAL,
            industry_qlib_percentile         REAL,
            industry_relative_group          TEXT,
            volatility_20d                   REAL,
            max_drawdown_60d                 REAL,
            volatility_rank                  REAL,
            drawdown_rank                    REAL,
            forecast_20d_score               REAL,
            forecast_60d_excess_score        REAL,
            forecast_risk_adjusted_score     REAL,
            forecast_score_v1                REAL,
            forecast_reason                  TEXT,
            updated_at                       TEXT,
            PRIMARY KEY (snapshot_date, model_id, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_fsff_model ON fact_stock_forecast_features(model_id, stock_code);

        CREATE TABLE IF NOT EXISTS dim_stock_forecast_latest (
            stock_code                       TEXT PRIMARY KEY,
            snapshot_date                    TEXT,
            model_id                         TEXT,
            predict_date                     TEXT,
            stock_name                       TEXT,
            sw_level1                        TEXT,
            sw_level2                        TEXT,
            qlib_score                       REAL,
            qlib_rank                        INTEGER,
            qlib_percentile                  REAL,
            industry_qlib_percentile         REAL,
            industry_relative_group          TEXT,
            volatility_20d                   REAL,
            max_drawdown_60d                 REAL,
            volatility_rank                  REAL,
            drawdown_rank                    REAL,
            forecast_20d_score               REAL,
            forecast_60d_excess_score        REAL,
            forecast_risk_adjusted_score     REAL,
            forecast_score_v1                REAL,
            forecast_reason                  TEXT,
            updated_at                       TEXT
        );
    """)
    conn.commit()


def build_stock_forecast_features(conn, snapshot_date: Optional[str] = None) -> int:
    ensure_qlib_tables(conn)
    ensure_tables(conn)
    snapshot_date = snapshot_date or date.today().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()

    model_row = conn.execute(
        "SELECT model_id FROM qlib_model_state WHERE status='trained' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not model_row:
        conn.execute("DELETE FROM dim_stock_forecast_latest")
        conn.commit()
        logger.info("[预测特征] 无可用 Qlib 模型，跳过构建")
        return 0

    model_id = model_row["model_id"]
    sync_latest_predictions_to_stock_trend(conn, model_id=model_id)

    pred_rows = conn.execute("""
        SELECT p.model_id, p.stock_code, p.stock_name, p.predict_date,
               p.qlib_score, p.qlib_rank, p.qlib_percentile,
               i.sw_level1, i.sw_level2,
               s.volatility_20d, s.max_drawdown_60d
        FROM qlib_predictions p
        LEFT JOIN dim_stock_industry i ON i.stock_code = p.stock_code
        LEFT JOIN dim_stock_stage_latest s ON s.stock_code = p.stock_code
        WHERE p.model_id = ?
    """, (model_id,)).fetchall()
    if not pred_rows:
        conn.execute("DELETE FROM dim_stock_forecast_latest")
        conn.commit()
        logger.info(f"[预测特征] 模型 {model_id} 无预测结果，跳过构建")
        return 0

    rows = [dict(row) for row in pred_rows]
    by_group = {("all", "all"): list(rows)}
    for row in rows:
        if row.get("sw_level2"):
            by_group.setdefault(("l2", row["sw_level2"]), []).append(row)
        if row.get("sw_level1"):
            by_group.setdefault(("l1", row["sw_level1"]), []).append(row)

    group_sizes = {key: len(group_rows) for key, group_rows in by_group.items()}
    group_rank_map = {}
    for (level, name), group_rows in by_group.items():
        scores = [_safe_float(row.get("qlib_score")) for row in group_rows]
        ranks = _percentile_ranks(scores)
        for row, rank in zip(group_rows, ranks):
            if rank is not None:
                group_rank_map[(level, name, row["stock_code"])] = rank

    vol_ranks = _percentile_ranks([(-_safe_float(row.get("volatility_20d")) if _safe_float(row.get("volatility_20d")) is not None else None) for row in rows])
    dd_ranks = _percentile_ranks([(-_safe_float(row.get("max_drawdown_60d")) if _safe_float(row.get("max_drawdown_60d")) is not None else None) for row in rows])

    conn.execute("DELETE FROM fact_stock_forecast_features WHERE snapshot_date = ? OR model_id = ?", (snapshot_date, model_id))
    inserted = 0
    for idx, row in enumerate(rows):
        stock_code = row["stock_code"]
        sw2 = row.get("sw_level2")
        sw1 = row.get("sw_level1")
        if sw2 and group_sizes.get(("l2", sw2), 0) >= 15:
            industry_pct = group_rank_map.get(("l2", sw2, stock_code))
            rel_group = f"SW2:{sw2}"
        elif sw1 and group_sizes.get(("l1", sw1), 0) >= 20:
            industry_pct = group_rank_map.get(("l1", sw1, stock_code))
            rel_group = f"SW1:{sw1}"
        else:
            industry_pct = group_rank_map.get(("all", "all", stock_code))
            rel_group = "ALL"

        qlib_pct = _safe_float(row.get("qlib_percentile"))
        vol_rank = vol_ranks[idx]
        dd_rank = dd_ranks[idx]
        forecast_20d_score = _clamp_score(qlib_pct if qlib_pct is not None else 50.0)
        forecast_60d_excess_score = _clamp_score(industry_pct if industry_pct is not None else forecast_20d_score)
        risk_adjusted = _clamp_score(
            forecast_20d_score * 0.55
            + (vol_rank if vol_rank is not None else 50.0) * 0.25
            + (dd_rank if dd_rank is not None else 50.0) * 0.20
        )
        forecast_score_v1 = _clamp_score(
            forecast_20d_score * 0.40
            + forecast_60d_excess_score * 0.40
            + risk_adjusted * 0.20
        )

        reasons = []
        if forecast_20d_score >= 75:
            reasons.append("Qlib短期预测较强")
        if forecast_60d_excess_score >= 70:
            reasons.append("行业内相对预测靠前")
        if risk_adjusted >= 70:
            reasons.append("波动收益性价比较好")
        if not reasons:
            reasons.append("预测结构中性")
        forecast_reason = "；".join(reasons[:2])

        conn.execute("""
            INSERT OR REPLACE INTO fact_stock_forecast_features (
                snapshot_date, model_id, predict_date, stock_code, stock_name,
                sw_level1, sw_level2, qlib_score, qlib_rank, qlib_percentile,
                industry_qlib_percentile, industry_relative_group,
                volatility_20d, max_drawdown_60d, volatility_rank, drawdown_rank,
                forecast_20d_score, forecast_60d_excess_score,
                forecast_risk_adjusted_score, forecast_score_v1, forecast_reason, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_date,
            model_id,
            row.get("predict_date"),
            stock_code,
            row.get("stock_name"),
            sw1,
            sw2,
            row.get("qlib_score"),
            row.get("qlib_rank"),
            qlib_pct,
            industry_pct,
            rel_group,
            _safe_float(row.get("volatility_20d")),
            _safe_float(row.get("max_drawdown_60d")),
            vol_rank,
            dd_rank,
            forecast_20d_score,
            forecast_60d_excess_score,
            risk_adjusted,
            forecast_score_v1,
            forecast_reason,
            now,
        ))
        inserted += 1

    conn.execute("DELETE FROM dim_stock_forecast_latest")
    conn.execute("""
        INSERT INTO dim_stock_forecast_latest (
            stock_code, snapshot_date, model_id, predict_date, stock_name,
            sw_level1, sw_level2, qlib_score, qlib_rank, qlib_percentile,
            industry_qlib_percentile, industry_relative_group,
            volatility_20d, max_drawdown_60d, volatility_rank, drawdown_rank,
            forecast_20d_score, forecast_60d_excess_score,
            forecast_risk_adjusted_score, forecast_score_v1, forecast_reason, updated_at
        )
        SELECT stock_code, snapshot_date, model_id, predict_date, stock_name,
               sw_level1, sw_level2, qlib_score, qlib_rank, qlib_percentile,
               industry_qlib_percentile, industry_relative_group,
               volatility_20d, max_drawdown_60d, volatility_rank, drawdown_rank,
               forecast_20d_score, forecast_60d_excess_score,
               forecast_risk_adjusted_score, forecast_score_v1, forecast_reason, updated_at
        FROM fact_stock_forecast_features
        WHERE snapshot_date = ? AND model_id = ?
    """, (snapshot_date, model_id))
    conn.commit()
    logger.info(f"[预测特征] 构建完成: {inserted} 只股票, 模型 {model_id}")
    return inserted

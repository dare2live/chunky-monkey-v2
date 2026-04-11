"""
etf_db.py — ETF 独立数据库 (etf.db)

职责：ETF 资产池、ETF 行情、ETF 快照与同步状态。
ETF 运行时只通过本模块读写，不再复用股票侧业务库与行情库。
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


logger = logging.getLogger("cm-api")

_DB_DIR = Path(__file__).resolve().parent.parent.parent / "data"
_DB_PATH = _DB_DIR / "etf.db"
_BOOTSTRAP_ATTEMPTED = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def get_etf_conn(timeout: int = 30) -> sqlite3.Connection:
    """获取 etf.db 连接，并确保 ETF 专用 schema 已准备好。"""
    _DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(_DB_PATH), timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    _maybe_bootstrap_legacy(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS etf_asset_universe (
            code        TEXT PRIMARY KEY,
            name        TEXT,
            market      TEXT,
            category    TEXT,
            is_active   INTEGER DEFAULT 1,
            updated_at  TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_etf_asset_active
            ON etf_asset_universe(is_active, category);

        CREATE TABLE IF NOT EXISTS etf_price_kline (
            code        TEXT    NOT NULL,
            date        TEXT    NOT NULL,
            freq        TEXT    NOT NULL DEFAULT 'daily',
            adjust      TEXT    NOT NULL DEFAULT 'qfq',
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      REAL,
            amount      REAL,
            source      TEXT,
            batch_id    TEXT,
            ingested_at TEXT,
            PRIMARY KEY (code, date, freq, adjust)
        );
        CREATE INDEX IF NOT EXISTS idx_epk_code_freq
            ON etf_price_kline(code, freq);
        CREATE INDEX IF NOT EXISTS idx_epk_date
            ON etf_price_kline(date);

        CREATE TABLE IF NOT EXISTS etf_sync_state (
            dataset         TEXT NOT NULL DEFAULT 'price_kline',
            code            TEXT NOT NULL,
            freq            TEXT NOT NULL DEFAULT 'daily',
            adjust          TEXT NOT NULL DEFAULT 'qfq',
            source          TEXT,
            min_date        TEXT,
            max_date        TEXT,
            row_count       INTEGER DEFAULT 0,
            last_success_at TEXT,
            last_attempt_at TEXT,
            last_error      TEXT,
            PRIMARY KEY (dataset, code, freq, adjust)
        );

        CREATE TABLE IF NOT EXISTS etf_import_batch (
            batch_id        TEXT PRIMARY KEY,
            dataset         TEXT,
            source          TEXT,
            rows_imported   INTEGER DEFAULT 0,
            min_date        TEXT,
            max_date        TEXT,
            started_at      TEXT,
            finished_at     TEXT,
            status          TEXT DEFAULT 'running',
            error           TEXT,
            detail          TEXT
        );

        CREATE TABLE IF NOT EXISTS mart_etf_snapshot_latest (
            code            TEXT PRIMARY KEY,
            snapshot_id     TEXT NOT NULL,
            category        TEXT,
            factor_rank     INTEGER,
            factor_score    REAL,
            rotation_score  REAL,
            strategy_type   TEXT,
            payload_json    TEXT NOT NULL,
            updated_at      TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_metf_snapshot
            ON mart_etf_snapshot_latest(snapshot_id);

        CREATE TABLE IF NOT EXISTS mart_etf_snapshot_state (
            state_key               TEXT PRIMARY KEY,
            snapshot_id             TEXT,
            schema_version          INTEGER DEFAULT 1,
            computed_at             TEXT,
            etf_count               INTEGER DEFAULT 0,
            history_start           TEXT,
            history_end             TEXT,
            overview_json           TEXT,
            factor_snapshot_json    TEXT,
            mining_snapshot_json    TEXT,
            source_status_json      TEXT
        );

        CREATE TABLE IF NOT EXISTS etf_qlib_feature_store (
            snapshot_date        TEXT NOT NULL,
            code                 TEXT NOT NULL,
            name                 TEXT,
            category             TEXT,
            market               TEXT,
            qlib_instrument      TEXT NOT NULL,
            is_latest            INTEGER DEFAULT 0,
            sample_tag           TEXT DEFAULT 'train',
            close                REAL,
            amount               REAL,
            momentum_5d          REAL,
            momentum_20d         REAL,
            momentum_60d         REAL,
            volatility_20d       REAL,
            drawdown_60d         REAL,
            amplitude_5d         REAL,
            amplitude_20d        REAL,
            amount_ratio_5_20    REAL,
            ma_gap_10            REAL,
            ma_gap_20            REAL,
            ma_gap_50            REAL,
            range_position_20    REAL,
            range_position_60    REAL,
            trend_score          REAL,
            mean_reversion_score REAL,
            setup_state          TEXT,
            trend_status         TEXT,
            created_at           TEXT,
            PRIMARY KEY (snapshot_date, code)
        );
        CREATE INDEX IF NOT EXISTS idx_eqfs_code_date
            ON etf_qlib_feature_store(code, snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_eqfs_latest
            ON etf_qlib_feature_store(is_latest, sample_tag, snapshot_date);

        CREATE TABLE IF NOT EXISTS etf_qlib_label_store (
            snapshot_date        TEXT NOT NULL,
            code                 TEXT NOT NULL,
            future_window        INTEGER NOT NULL DEFAULT 60,
            buy_hold_return_pct  REAL,
            grid_return_pct      REAL,
            grid_excess_pct      REAL,
            best_step_pct        REAL,
            grid_trade_count     INTEGER,
            strategy_label       TEXT,
            strategy_flag        INTEGER DEFAULT 0,
            created_at           TEXT,
            PRIMARY KEY (snapshot_date, code, future_window)
        );
        CREATE INDEX IF NOT EXISTS idx_eqls_code_window
            ON etf_qlib_label_store(code, future_window, snapshot_date);

        CREATE TABLE IF NOT EXISTS etf_qlib_model_state (
            model_id                  TEXT PRIMARY KEY,
            status                    TEXT NOT NULL DEFAULT 'idle',
            train_start               TEXT,
            train_end                 TEXT,
            valid_start               TEXT,
            valid_end                 TEXT,
            test_start                TEXT,
            test_end                  TEXT,
            etf_count                 INTEGER,
            sample_count              INTEGER,
            feature_count             INTEGER,
            hold_ic_mean              REAL,
            grid_ic_mean              REAL,
            excess_ic_mean            REAL,
            step_mae                  REAL,
            strategy_accuracy         REAL,
            test_top20_hold_return    REAL,
            test_top20_strategy_return REAL,
            model_path                TEXT,
            data_dir                  TEXT,
            train_params_json         TEXT,
            error                     TEXT,
            created_at                TEXT,
            finished_at               TEXT
        );

        CREATE TABLE IF NOT EXISTS etf_qlib_predictions (
            model_id                        TEXT NOT NULL,
            code                            TEXT NOT NULL,
            name                            TEXT,
            category                        TEXT,
            predict_date                    TEXT,
            hold_score                      REAL,
            hold_rank                       INTEGER,
            hold_percentile                 REAL,
            grid_score                      REAL,
            grid_rank                       INTEGER,
            grid_percentile                 REAL,
            excess_score                    REAL,
            excess_rank                     INTEGER,
            excess_percentile               REAL,
            step_score                      REAL,
            predicted_buy_hold_return_pct   REAL,
            predicted_grid_return_pct       REAL,
            predicted_grid_excess_pct       REAL,
            predicted_best_step_pct         REAL,
            preferred_strategy              TEXT,
            recommended_return_pct          REAL,
            comparison_return_pct           REAL,
            strategy_edge_pct               REAL,
            model_status                    TEXT,
            created_at                      TEXT,
            PRIMARY KEY (model_id, code)
        );
        CREATE INDEX IF NOT EXISTS idx_eqp_model_hold
            ON etf_qlib_predictions(model_id, hold_rank);
        CREATE INDEX IF NOT EXISTS idx_eqp_model_grid
            ON etf_qlib_predictions(model_id, grid_rank);
        CREATE INDEX IF NOT EXISTS idx_eqp_model_excess
            ON etf_qlib_predictions(model_id, excess_rank);

        CREATE TABLE IF NOT EXISTS etf_qlib_backtest_result (
            model_id              TEXT NOT NULL,
            code                  TEXT NOT NULL,
            window_days           INTEGER NOT NULL DEFAULT 60,
            snapshot_date         TEXT,
            buy_hold_return_pct   REAL,
            grid_return_pct       REAL,
            grid_excess_pct       REAL,
            best_step_pct         REAL,
            trade_count           INTEGER,
            win_rate              REAL,
            strategy_label        TEXT,
            audit_json            TEXT,
            created_at            TEXT,
            PRIMARY KEY (model_id, code, window_days)
        );
        CREATE INDEX IF NOT EXISTS idx_eqbr_model_strategy
            ON etf_qlib_backtest_result(model_id, strategy_label);

        CREATE TABLE IF NOT EXISTS etf_qlib_param_search (
            model_id              TEXT NOT NULL,
            code                  TEXT NOT NULL,
            snapshot_date         TEXT NOT NULL,
            step_pct              REAL NOT NULL,
            candidate_score       REAL,
            return_pct            REAL,
            excess_pct            REAL,
            sharpe                REAL,
            max_drawdown_pct      REAL,
            trade_count           INTEGER,
            sell_count            INTEGER,
            win_rate              REAL,
            hard_gate_passed      INTEGER DEFAULT 0,
            rank_order            INTEGER,
            is_best               INTEGER DEFAULT 0,
            created_at            TEXT,
            PRIMARY KEY (model_id, code, snapshot_date, step_pct)
        );
        CREATE INDEX IF NOT EXISTS idx_eqps_model_code
            ON etf_qlib_param_search(model_id, code, snapshot_date);
        """
    )
    conn.commit()


def upsert_price_rows(conn: sqlite3.Connection, rows: list[dict], source: str,
                      batch_id: str | None = None) -> int:
    if not rows:
        return 0
    now = _now_iso()
    conn.executemany(
        "INSERT OR REPLACE INTO etf_price_kline "
        "(code, date, freq, adjust, open, high, low, close, volume, amount, "
        " source, batch_id, ingested_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            (
                row["code"], row["date"], row.get("freq", "daily"), row.get("adjust", "qfq"),
                row.get("open"), row.get("high"), row.get("low"), row.get("close"),
                row.get("volume"), row.get("amount"), source, batch_id, now,
            )
            for row in rows
        ],
    )
    conn.commit()
    return len(rows)


def update_sync_state(conn: sqlite3.Connection, code: str, freq: str, *,
                      source: str | None = None,
                      min_date: str | None = None,
                      max_date: str | None = None,
                      row_count: int | None = None,
                      error: str | None = None) -> None:
    now = _now_iso()
    conn.execute(
        "INSERT INTO etf_sync_state "
        "(dataset, code, freq, adjust, source, min_date, max_date, "
        " row_count, last_success_at, last_attempt_at, last_error) "
        "VALUES ('price_kline',?,?,'qfq',?,?,?,?,?,?,?) "
        "ON CONFLICT(dataset, code, freq, adjust) DO UPDATE SET "
        " source=COALESCE(excluded.source, source), "
        " min_date=COALESCE(excluded.min_date, min_date), "
        " max_date=COALESCE(excluded.max_date, max_date), "
        " row_count=COALESCE(excluded.row_count, row_count), "
        " last_success_at=CASE WHEN excluded.last_error IS NULL "
        "   THEN excluded.last_success_at ELSE last_success_at END, "
        " last_attempt_at=excluded.last_attempt_at, "
        " last_error=excluded.last_error",
        (
            code,
            freq,
            source,
            min_date,
            max_date,
            row_count,
            now if error is None else None,
            now,
            error,
        ),
    )
    conn.commit()


def _target_has_data(conn: sqlite3.Connection) -> bool:
    checks = [
        "SELECT 1 FROM etf_asset_universe LIMIT 1",
        "SELECT 1 FROM etf_price_kline LIMIT 1",
        "SELECT 1 FROM mart_etf_snapshot_state WHERE snapshot_id IS NOT NULL LIMIT 1",
    ]
    return any(conn.execute(sql).fetchone() is not None for sql in checks)


def _copy_rows(conn: sqlite3.Connection, table: str, columns: tuple[str, ...], rows) -> None:
    if not rows:
        return
    placeholders = ", ".join(["?"] * len(columns))
    col_sql = ", ".join(columns)
    conn.executemany(
        f"INSERT OR REPLACE INTO {table} ({col_sql}) VALUES ({placeholders})",
        [tuple(row[column] for column in columns) for row in rows],
    )


def _maybe_bootstrap_legacy(conn: sqlite3.Connection) -> None:
    global _BOOTSTRAP_ATTEMPTED
    if _BOOTSTRAP_ATTEMPTED or _target_has_data(conn):
        _BOOTSTRAP_ATTEMPTED = True
        return

    _BOOTSTRAP_ATTEMPTED = True

    try:
        from services.db import get_conn as get_business_conn
        from services.market_db import get_market_conn
    except Exception as exc:
        logger.warning("[ETF_DB] 无法加载 legacy 数据源，跳过 bootstrap: %s", exc)
        return

    business_conn = get_business_conn()
    market_conn = get_market_conn()
    try:
        legacy_assets = business_conn.execute(
            """
            SELECT code, name, market, category, is_active, updated_at
            FROM dim_asset_universe
            WHERE asset_type = 'etf'
            ORDER BY code
            """
        ).fetchall()
        if not legacy_assets:
            return

        asset_rows = [dict(row) for row in legacy_assets]
        codes = [row["code"] for row in asset_rows if row.get("code")]

        conn.execute("BEGIN IMMEDIATE")
        _copy_rows(
            conn,
            "etf_asset_universe",
            ("code", "name", "market", "category", "is_active", "updated_at"),
            asset_rows,
        )

        for start in range(0, len(codes), 50):
            chunk = codes[start:start + 50]
            placeholders = ", ".join(["?"] * len(chunk))
            price_rows = market_conn.execute(
                f"""
                SELECT code, date, freq, adjust, open, high, low, close,
                       volume, amount, source, batch_id, ingested_at
                FROM price_kline
                WHERE code IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            _copy_rows(
                conn,
                "etf_price_kline",
                (
                    "code", "date", "freq", "adjust", "open", "high", "low", "close",
                    "volume", "amount", "source", "batch_id", "ingested_at",
                ),
                price_rows,
            )

            sync_rows = market_conn.execute(
                f"""
                SELECT dataset, code, freq, adjust, source, min_date, max_date,
                       row_count, last_success_at, last_attempt_at, last_error
                FROM market_sync_state
                WHERE dataset = 'price_kline'
                  AND code IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            _copy_rows(
                conn,
                "etf_sync_state",
                (
                    "dataset", "code", "freq", "adjust", "source", "min_date", "max_date",
                    "row_count", "last_success_at", "last_attempt_at", "last_error",
                ),
                sync_rows,
            )

        latest_rows = business_conn.execute(
            """
            SELECT code, snapshot_id, category, factor_rank, factor_score,
                   rotation_score, strategy_type, payload_json, updated_at
            FROM mart_etf_snapshot_latest
            """
        ).fetchall()
        _copy_rows(
            conn,
            "mart_etf_snapshot_latest",
            (
                "code", "snapshot_id", "category", "factor_rank", "factor_score",
                "rotation_score", "strategy_type", "payload_json", "updated_at",
            ),
            latest_rows,
        )

        state_rows = business_conn.execute(
            """
            SELECT state_key, snapshot_id, schema_version, computed_at, etf_count,
                   history_start, history_end, overview_json,
                   factor_snapshot_json, mining_snapshot_json, source_status_json
            FROM mart_etf_snapshot_state
            """
        ).fetchall()
        _copy_rows(
            conn,
            "mart_etf_snapshot_state",
            (
                "state_key", "snapshot_id", "schema_version", "computed_at", "etf_count",
                "history_start", "history_end", "overview_json", "factor_snapshot_json",
                "mining_snapshot_json", "source_status_json",
            ),
            state_rows,
        )

        conn.commit()
        logger.info(
            "[ETF_DB] 已从 legacy 共享库迁移 ETF 数据: universe=%s, codes=%s",
            len(asset_rows),
            len(codes),
        )
    except Exception as exc:
        conn.rollback()
        logger.exception("[ETF_DB] legacy bootstrap 失败: %s", exc)
    finally:
        business_conn.close()
        market_conn.close()
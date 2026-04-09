"""
数据库服务 — Chunky Monkey v2

数据分层：
  原始层（只追加）: market_raw_holdings, raw_fetch_batch
  维度层: dim_active_a_stock, dim_stock_industry, dim_trading_calendar, inst_institutions, inst_name_aliases
  事实层: inst_holdings, fact_institution_event, fact_northbound_daily, stock_watchlist
  集市层（可重算）: mart_institution_profile, mart_institution_industry_stat, mart_stock_trend
  系统层: sys_schema_version, excluded_stocks, exclusion_categories, app_settings
"""

import sqlite3
import os
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("cm-api")

DB_DIR = Path(__file__).resolve().parent.parent.parent / "data"
DB_PATH = DB_DIR / "smartmoney.db"


def get_conn(timeout: int = 30) -> sqlite3.Connection:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db():
    conn = get_conn()
    try:
        conn.executescript("""
            -- ============================================================
            -- 原始层（只追加不改）
            -- ============================================================

            CREATE TABLE IF NOT EXISTS market_raw_holdings (
                holder_name     TEXT NOT NULL,
                stock_code      TEXT NOT NULL,
                stock_name      TEXT,
                report_date     TEXT NOT NULL,
                notice_date     TEXT,
                holder_rank     INTEGER,
                hold_amount     REAL,
                hold_market_cap REAL,
                hold_ratio      REAL,
                holder_type     TEXT,
                hold_change     TEXT,
                hold_change_num REAL,
                raw_json        TEXT,
                created_at      TEXT,
                UNIQUE(holder_name, stock_code, report_date, holder_rank)
            );
            CREATE INDEX IF NOT EXISTS idx_mrh_stock ON market_raw_holdings(stock_code);
            CREATE INDEX IF NOT EXISTS idx_mrh_report ON market_raw_holdings(report_date);
            CREATE INDEX IF NOT EXISTS idx_mrh_notice ON market_raw_holdings(notice_date);
            CREATE INDEX IF NOT EXISTS idx_mrh_holder ON market_raw_holdings(holder_name);
            CREATE INDEX IF NOT EXISTS idx_mrh_stock_report ON market_raw_holdings(stock_code, report_date);

            -- K线已迁移到独立的 market_data.db.price_kline

            CREATE TABLE IF NOT EXISTS raw_fetch_batch (
                batch_id        TEXT PRIMARY KEY,
                source          TEXT,
                fetch_type      TEXT,
                status          TEXT DEFAULT 'running',
                started_at      TEXT,
                finished_at     TEXT,
                rows_fetched    INTEGER DEFAULT 0,
                data_range_from TEXT,
                data_range_to   TEXT,
                error           TEXT
            );

            -- ============================================================
            -- 维度层
            -- ============================================================

            -- dim_stock 已退役（2026-04-08）：曾因从未被任何 sync 步骤填充导致
            -- sync_financial / calc_financial_derived / calc_screening 静默 0 行；
            -- 当前可交易 A 股主数据统一走 dim_active_a_stock（security_master 维护）。

            CREATE TABLE IF NOT EXISTS dim_active_a_stock (
                stock_code       TEXT PRIMARY KEY,
                stock_name       TEXT,
                market           TEXT,
                source           TEXT,
                updated_at       TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_daas_updated ON dim_active_a_stock(updated_at);

            CREATE TABLE IF NOT EXISTS dim_stock_industry (
                stock_code  TEXT PRIMARY KEY,
                sw_level1   TEXT,
                sw_level2   TEXT,
                sw_level3   TEXT,
                sw_code     TEXT,
                updated_at  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_dsi_l1 ON dim_stock_industry(sw_level1);
            CREATE INDEX IF NOT EXISTS idx_dsi_l2 ON dim_stock_industry(sw_level2);

            CREATE TABLE IF NOT EXISTS dim_trading_calendar (
                trade_date  TEXT PRIMARY KEY,
                is_trading  INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS inst_institutions (
                id           TEXT PRIMARY KEY,
                name         TEXT NOT NULL,
                display_name TEXT,
                type         TEXT DEFAULT 'other',
                enabled      INTEGER DEFAULT 1,
                blacklisted  INTEGER DEFAULT 0,
                aliases      TEXT DEFAULT '[]',
                manual_type  TEXT,
                merged_into  TEXT,
                created_at   TEXT,
                updated_at   TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_inst_type ON inst_institutions(type);
            CREATE INDEX IF NOT EXISTS idx_inst_enabled ON inst_institutions(enabled);

            CREATE TABLE IF NOT EXISTS inst_name_aliases (
                institution_id TEXT PRIMARY KEY,
                aliases        TEXT,
                created_at     TEXT,
                updated_at     TEXT
            );

            -- ============================================================
            -- 事实层
            -- ============================================================

            CREATE TABLE IF NOT EXISTS inst_holdings (
                institution_id  TEXT,
                holder_name     TEXT,
                holder_type     TEXT,
                stock_code      TEXT NOT NULL,
                stock_name      TEXT,
                report_date     TEXT NOT NULL,
                notice_date     TEXT,
                holder_rank     INTEGER,
                hold_amount     REAL,
                hold_market_cap REAL,
                hold_ratio      REAL,
                hold_change     TEXT,
                hold_change_num REAL,
                created_at      TEXT,
                UNIQUE(holder_name, stock_code, report_date)
            );
            CREATE INDEX IF NOT EXISTS idx_ih_inst ON inst_holdings(institution_id);
            CREATE INDEX IF NOT EXISTS idx_ih_stock ON inst_holdings(stock_code);
            CREATE INDEX IF NOT EXISTS idx_ih_report ON inst_holdings(report_date);

            CREATE TABLE IF NOT EXISTS fact_institution_event (
                institution_id    TEXT NOT NULL,
                holder_name       TEXT,
                stock_code        TEXT NOT NULL,
                stock_name        TEXT,
                report_date       TEXT NOT NULL,
                notice_date       TEXT,
                event_type        TEXT NOT NULL,
                hold_amount       REAL,
                prev_hold_amount  REAL,
                change_amount     REAL,
                change_pct        REAL,
                report_season     TEXT,
                cost_window_start TEXT,
                cost_window_end   TEXT,
                inst_ref_cost     REAL,
                inst_cost_method  TEXT,
                premium_pct       REAL,
                premium_bucket    TEXT,
                follow_gate       TEXT,
                follow_gate_reason TEXT,
                created_at        TEXT,
                PRIMARY KEY (institution_id, stock_code, report_date)
            );
            CREATE INDEX IF NOT EXISTS idx_event_type ON fact_institution_event(event_type);
            CREATE INDEX IF NOT EXISTS idx_event_date ON fact_institution_event(report_date);

            -- 收益字段已合并入 fact_institution_event

            CREATE TABLE IF NOT EXISTS fact_northbound_daily (
                stock_code      TEXT NOT NULL,
                stock_name      TEXT,
                hold_shares     REAL,
                hold_market_cap REAL,
                hold_ratio      REAL,
                change_shares   REAL,
                trade_date      TEXT NOT NULL,
                updated_at      TEXT,
                PRIMARY KEY (stock_code, trade_date)
            );

            CREATE TABLE IF NOT EXISTS stock_watchlist (
                stock_code          TEXT NOT NULL,
                stock_name          TEXT,
                added_date          TEXT NOT NULL,
                added_price         REAL,
                added_reason        TEXT,
                source_institution  TEXT,
                source_event_type   TEXT,
                gain_since_added    REAL,
                max_gain            REAL,
                max_drawdown        REAL,
                current_price       REAL,
                status              TEXT DEFAULT 'active',
                closed_date         TEXT,
                closed_price        REAL,
                closed_reason       TEXT,
                updated_at          TEXT,
                PRIMARY KEY (stock_code, added_date)
            );

            CREATE TABLE IF NOT EXISTS fact_setup_snapshot (
                snapshot_date         TEXT NOT NULL,
                stock_code            TEXT NOT NULL,
                stock_name            TEXT,
                setup_tag             TEXT NOT NULL,
                setup_priority        INTEGER,
                setup_reason          TEXT,
                setup_confidence      TEXT,
                setup_level           TEXT,
                setup_inst_id         TEXT,
                setup_inst_name       TEXT,
                setup_event_type      TEXT,
                setup_industry_name   TEXT,
                snapshot_sw_level1    TEXT,
                snapshot_sw_level2    TEXT,
                snapshot_sw_level3    TEXT,
                action_score          REAL,
                discovery_score       REAL,
                company_quality_score REAL,
                stage_score           REAL,
                forecast_score        REAL,
                forecast_score_effective REAL,
                raw_composite_priority_score REAL,
                composite_priority_score REAL,
                composite_cap_score   REAL,
                composite_cap_reason  TEXT,
                stock_archetype       TEXT,
                priority_pool         TEXT,
                priority_pool_reason  TEXT,
                score_highlights      TEXT,
                score_risks           TEXT,
                latest_report_date    TEXT,
                latest_notice_date    TEXT,
                report_age_days       INTEGER,
                setup_score_raw       REAL,
                setup_execution_gate  TEXT,
                setup_execution_reason TEXT,
                industry_skill_raw    REAL,
                industry_skill_grade  INTEGER,
                followability_grade   INTEGER,
                premium_grade         INTEGER,
                report_recency_grade  INTEGER,
                reliability_grade     INTEGER,
                crowding_bucket       TEXT,
                crowding_yield_raw    REAL,
                crowding_yield_grade  INTEGER,
                crowding_stability_raw REAL,
                crowding_stability_grade INTEGER,
                crowding_fit_raw      REAL,
                crowding_fit_grade    INTEGER,
                crowding_fit_sample   INTEGER,
                crowding_fit_source   TEXT,
                entry_trade_date      TEXT,
                entry_price           REAL,
                current_trade_date    TEXT,
                current_price         REAL,
                gain_to_now           REAL,
                gain_10d              REAL,
                gain_30d              REAL,
                gain_60d              REAL,
                max_drawdown_10d      REAL,
                max_drawdown_30d      REAL,
                max_drawdown_60d      REAL,
                matured_10d           INTEGER DEFAULT 0,
                matured_30d           INTEGER DEFAULT 0,
                matured_60d           INTEGER DEFAULT 0,
                updated_at            TEXT,
                PRIMARY KEY (snapshot_date, stock_code, setup_tag, setup_inst_id)
            );
            CREATE INDEX IF NOT EXISTS idx_setup_snapshot_date
                ON fact_setup_snapshot(snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_setup_snapshot_tag
                ON fact_setup_snapshot(setup_tag, snapshot_date);
            CREATE INDEX IF NOT EXISTS idx_setup_snapshot_stock
                ON fact_setup_snapshot(stock_code);
            CREATE INDEX IF NOT EXISTS idx_setup_snapshot_sw1_date
                ON fact_setup_snapshot(snapshot_sw_level1, snapshot_date);

            -- ============================================================
            -- 集市层（派生，可重算）
            -- ============================================================

            CREATE TABLE IF NOT EXISTS mart_institution_profile (
                institution_id          TEXT PRIMARY KEY,
                institution_name        TEXT,
                display_name            TEXT,
                inst_type               TEXT,
                total_events            INTEGER,
                total_stocks            INTEGER,
                total_periods           INTEGER,
                avg_gain_10d            REAL,
                avg_gain_30d            REAL,
                avg_gain_60d            REAL,
                avg_gain_120d           REAL,
                avg_excess_30d          REAL,
                avg_excess_60d          REAL,
                win_rate_30d            REAL,
                win_rate_60d            REAL,
                win_rate_90d            REAL,
                total_win_rate          REAL,
                median_gain_30d         REAL,
                median_gain_60d         REAL,
                median_max_drawdown_30d REAL,
                median_max_drawdown_60d REAL,
                top_industry_1          TEXT,
                top_industry_2          TEXT,
                top_industry_3          TEXT,
                main_industry_1         TEXT,
                main_industry_2         TEXT,
                main_industry_3         TEXT,
                best_industry_1         TEXT,
                best_industry_2         TEXT,
                best_industry_3         TEXT,
                concentration           REAL,
                current_stock_count     INTEGER,
                current_total_cap       REAL,
                latest_notice_date      TEXT,
                recent_new_entry_count  INTEGER,
                recent_increase_count   INTEGER,
                recent_exit_count       INTEGER DEFAULT 0,
                northbound_overlap_rate REAL,
                quality_score           REAL,
                score_basis             TEXT,
                score_confidence        TEXT,
                historical_median_holding_days INTEGER,
                current_avg_held_days   INTEGER,
                buy_event_count         INTEGER,
                buy_avg_gain_30d        REAL,
                buy_avg_gain_60d        REAL,
                buy_avg_gain_120d       REAL,
                buy_win_rate_30d        REAL,
                buy_win_rate_60d        REAL,
                buy_win_rate_120d       REAL,
                buy_median_max_drawdown_30d REAL,
                buy_median_max_drawdown_60d REAL,
                avg_premium_pct         REAL,
                safe_follow_event_count INTEGER,
                safe_follow_win_rate_30d REAL,
                safe_follow_avg_gain_30d REAL,
                safe_follow_avg_drawdown_30d REAL,
                premium_discount_event_count INTEGER,
                premium_discount_win_rate_30d REAL,
                premium_near_cost_event_count INTEGER,
                premium_near_cost_win_rate_30d REAL,
                premium_premium_event_count INTEGER,
                premium_premium_win_rate_30d REAL,
                premium_high_event_count INTEGER,
                premium_high_win_rate_30d REAL,
                signal_transfer_efficiency_30d REAL,
                followability_hint      TEXT,
                followability_score     REAL,
                followability_confidence TEXT,
                data_completeness       TEXT DEFAULT 'complete',
                updated_at              TEXT
            );

            CREATE TABLE IF NOT EXISTS mart_institution_industry_stat (
                institution_id TEXT NOT NULL,
                sw_level       TEXT NOT NULL,
                industry_name  TEXT NOT NULL,
                sample_events  INTEGER DEFAULT 0,
                avg_gain_30d   REAL,
                avg_gain_60d   REAL,
                avg_gain_90d   REAL,
                avg_gain_120d  REAL,
                win_rate_30d   REAL,
                win_rate_60d   REAL,
                win_rate_90d   REAL,
                total_win_rate REAL,
                max_drawdown_30d REAL,
                max_drawdown_60d REAL,
                updated_at     TEXT,
                PRIMARY KEY (institution_id, sw_level, industry_name)
            );

            CREATE TABLE IF NOT EXISTS mart_stock_trend (
                stock_code         TEXT PRIMARY KEY,
                stock_name         TEXT,
                inst_count_t0      INTEGER,
                inst_count_t1      INTEGER,
                inst_count_t2      INTEGER,
                inst_cap_t0        REAL,
                inst_cap_t1        REAL,
                inst_cap_t2        REAL,
                inst_trend         TEXT,
                cap_trend          TEXT,
                latest_events      TEXT,
                latest_report_date TEXT,
                latest_notice_date TEXT,
                price_1m_pct       REAL,
                price_20d_pct      REAL,
                price_trend        TEXT,
                setup_tag          TEXT,
                setup_priority     INTEGER,
                setup_reason       TEXT,
                setup_confidence   TEXT,
                setup_level        TEXT,
                setup_inst_id      TEXT,
                setup_inst_name    TEXT,
                setup_event_type   TEXT,
                setup_industry_name TEXT,
                setup_score_raw    REAL,
                setup_execution_gate TEXT,
                setup_execution_reason TEXT,
                industry_skill_raw REAL,
                industry_skill_grade INTEGER,
                followability_grade INTEGER,
                premium_grade      INTEGER,
                report_recency_grade INTEGER,
                reliability_grade  INTEGER,
                crowding_bucket    TEXT,
                crowding_yield_raw REAL,
                crowding_yield_grade INTEGER,
                crowding_stability_raw REAL,
                crowding_stability_grade INTEGER,
                crowding_fit_raw   REAL,
                crowding_fit_grade INTEGER,
                crowding_fit_sample INTEGER,
                crowding_fit_source TEXT,
                report_age_days    INTEGER,
                qlib_rank          INTEGER,
                qlib_score         REAL,
                qlib_percentile    REAL,
                discovery_score    REAL,
                company_quality_score REAL,
                stage_score        REAL,
                forecast_score     REAL,
                forecast_score_effective REAL,
                raw_composite_priority_score REAL,
                composite_priority_score REAL,
                composite_cap_score REAL,
                composite_cap_reason TEXT,
                stock_archetype    TEXT,
                priority_pool      TEXT,
                priority_pool_reason TEXT,
                score_highlights   TEXT,
                score_risks        TEXT,
                updated_at         TEXT
            );

            -- ============================================================
            -- 系统层
            -- ============================================================

            CREATE TABLE IF NOT EXISTS sys_schema_version (
                layer       TEXT PRIMARY KEY,
                version     TEXT,
                updated_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS excluded_stocks (
                stock_code TEXT NOT NULL,
                category   TEXT NOT NULL,
                stock_name TEXT,
                reason     TEXT,
                created_at TEXT,
                PRIMARY KEY (stock_code, category)
            );

            CREATE TABLE IF NOT EXISTS exclusion_categories (
                category    TEXT PRIMARY KEY,
                label       TEXT NOT NULL,
                enabled     INTEGER DEFAULT 1,
                updated_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS app_settings (
                key        TEXT PRIMARY KEY,
                value      TEXT,
                updated_at TEXT
            );

            -- 更新管线状态
            CREATE TABLE IF NOT EXISTS step_status (
                step_id       TEXT PRIMARY KEY,
                group_name    TEXT,
                step_name     TEXT,
                step_order    INTEGER,
                status        TEXT DEFAULT 'idle',
                started_at    TEXT,
                finished_at   TEXT,
                error         TEXT,
                records       INTEGER DEFAULT 0
            );

            -- 抓取日志
            CREATE TABLE IF NOT EXISTS scan_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_type        TEXT,
                update_date_from TEXT,
                update_date_to   TEXT,
                rows_fetched     INTEGER DEFAULT 0,
                rows_inserted    INTEGER DEFAULT 0,
                rows_updated     INTEGER DEFAULT 0,
                duration_sec     REAL,
                status           TEXT DEFAULT 'running',
                error            TEXT,
                created_at       TEXT
            );

            CREATE TABLE IF NOT EXISTS market_gap_queue (
                dataset         TEXT NOT NULL,
                stock_code      TEXT NOT NULL,
                stock_name      TEXT,
                status          TEXT DEFAULT 'pending',
                reason          TEXT,
                last_error      TEXT,
                source_attempts INTEGER DEFAULT 0,
                first_seen_at   TEXT,
                last_attempt_at TEXT,
                resolved_at     TEXT,
                updated_at      TEXT,
                PRIMARY KEY (dataset, stock_code)
            );
            CREATE INDEX IF NOT EXISTS idx_gap_queue_dataset_status
                ON market_gap_queue(dataset, status);
            CREATE INDEX IF NOT EXISTS idx_gap_queue_status_updated
                ON market_gap_queue(status, updated_at DESC);
        """)
        conn.commit()

        # 增量添加新列（SQLite 不支持 ADD COLUMN IF NOT EXISTS）
        # 收益字段已直接维护在 fact_institution_event 上
        try:
            conn.execute("ALTER TABLE mart_institution_profile ADD COLUMN win_rate_90d REAL")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mart_institution_profile ADD COLUMN total_win_rate REAL")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mart_institution_profile ADD COLUMN quality_score REAL")
        except Exception:
            pass
        try:
            conn.execute("ALTER TABLE mart_institution_profile ADD COLUMN recent_exit_count INTEGER DEFAULT 0")
        except Exception:
            pass
        for col in [
            "stock_name TEXT",
            "reason TEXT",
            "last_error TEXT",
            "source_attempts INTEGER DEFAULT 0",
            "first_seen_at TEXT",
            "last_attempt_at TEXT",
            "resolved_at TEXT",
            "updated_at TEXT",
        ]:
            try:
                conn.execute(f"ALTER TABLE market_gap_queue ADD COLUMN {col}")
            except Exception:
                pass
        # mart_stock_trend 新列（评分系统）
        for col in ["action_score REAL", "leader_inst TEXT",
                     "leader_score REAL", "consensus_count INTEGER", "path_state TEXT",
                     "setup_tag TEXT", "setup_priority INTEGER", "setup_reason TEXT",
                     "setup_confidence TEXT", "setup_level TEXT", "setup_inst_id TEXT",
                     "setup_inst_name TEXT", "setup_event_type TEXT", "setup_industry_name TEXT",
                     "setup_score_raw REAL", "setup_execution_gate TEXT", "setup_execution_reason TEXT",
                     "industry_skill_raw REAL",
                     "industry_skill_grade INTEGER", "followability_grade INTEGER",
                     "premium_grade INTEGER", "report_recency_grade INTEGER",
                     "reliability_grade INTEGER", "crowding_bucket TEXT",
                     "crowding_yield_raw REAL", "crowding_yield_grade INTEGER",
                     "crowding_stability_raw REAL", "crowding_stability_grade INTEGER",
                     "crowding_fit_raw REAL", "crowding_fit_grade INTEGER",
                     "crowding_fit_sample INTEGER", "crowding_fit_source TEXT",
                     "report_age_days INTEGER", "qlib_rank INTEGER",
                     "qlib_score REAL", "qlib_percentile REAL",
                     "discovery_score REAL", "company_quality_score REAL",
                     "stage_score REAL", "forecast_score REAL",
                     "forecast_score_effective REAL", "raw_composite_priority_score REAL",
                     "composite_priority_score REAL", "composite_cap_score REAL",
                     "composite_cap_reason TEXT", "stock_archetype TEXT",
                     "priority_pool TEXT", "priority_pool_reason TEXT",
                     "score_highlights TEXT", "score_risks TEXT"]:
            try:
                conn.execute(f"ALTER TABLE mart_stock_trend ADD COLUMN {col}")
            except Exception:
                pass

        # fact_institution_event 增强：承载事件收益与路径分析字段
        for col in [
            "report_season TEXT",
            "cost_window_start TEXT",
            "cost_window_end TEXT",
            "inst_ref_cost REAL",
            "inst_cost_method TEXT",
            "premium_pct REAL",
            "premium_bucket TEXT",
            "follow_gate TEXT",
            "follow_gate_reason TEXT",
            "tradable_date TEXT",
            "price_entry REAL",
            "price_entry_status TEXT",
            "gain_10d REAL", "gain_30d REAL", "gain_60d REAL",
            "gain_90d REAL", "gain_120d REAL",
            "excess_30d REAL", "excess_60d REAL", "excess_120d REAL",
            "max_drawdown_30d REAL", "max_drawdown_60d REAL",
            "return_to_now REAL",
            "max_rally_to_now REAL",
            "max_drawdown_to_now REAL",
            "path_state TEXT",
            "date_quality TEXT",
            "calc_version TEXT",
            "calc_ref_price_mode TEXT",
            "calc_completed_at TEXT",
        ]:
            try:
                conn.execute(f"ALTER TABLE fact_institution_event ADD COLUMN {col}")
            except Exception:
                pass

        for col in [
            "stock_name TEXT",
            "setup_priority INTEGER",
            "setup_reason TEXT",
            "setup_confidence TEXT",
            "setup_level TEXT",
            "setup_inst_name TEXT",
            "setup_event_type TEXT",
            "setup_industry_name TEXT",
            "snapshot_sw_level1 TEXT",
            "snapshot_sw_level2 TEXT",
            "snapshot_sw_level3 TEXT",
            "action_score REAL",
            "discovery_score REAL",
            "company_quality_score REAL",
            "stage_score REAL",
            "forecast_score REAL",
            "forecast_score_effective REAL",
            "raw_composite_priority_score REAL",
            "composite_priority_score REAL",
            "composite_cap_score REAL",
            "composite_cap_reason TEXT",
            "stock_archetype TEXT",
            "priority_pool TEXT",
            "priority_pool_reason TEXT",
            "score_highlights TEXT",
            "score_risks TEXT",
            "latest_report_date TEXT",
            "latest_notice_date TEXT",
            "report_age_days INTEGER",
            "setup_score_raw REAL",
            "setup_execution_gate TEXT",
            "setup_execution_reason TEXT",
            "industry_skill_raw REAL",
            "industry_skill_grade INTEGER",
            "followability_grade INTEGER",
            "premium_grade INTEGER",
            "report_recency_grade INTEGER",
            "reliability_grade INTEGER",
            "crowding_bucket TEXT",
            "crowding_yield_raw REAL",
            "crowding_yield_grade INTEGER",
            "crowding_stability_raw REAL",
            "crowding_stability_grade INTEGER",
            "crowding_fit_raw REAL",
            "crowding_fit_grade INTEGER",
            "crowding_fit_sample INTEGER",
            "crowding_fit_source TEXT",
            "entry_trade_date TEXT",
            "entry_price REAL",
            "current_trade_date TEXT",
            "current_price REAL",
            "gain_to_now REAL",
            "gain_10d REAL",
            "gain_30d REAL",
            "gain_60d REAL",
            "max_drawdown_10d REAL",
            "max_drawdown_30d REAL",
            "max_drawdown_60d REAL",
            "matured_10d INTEGER DEFAULT 0",
            "matured_30d INTEGER DEFAULT 0",
            "matured_60d INTEGER DEFAULT 0",
        ]:
            try:
                conn.execute(f"ALTER TABLE fact_setup_snapshot ADD COLUMN {col}")
            except Exception:
                pass

        # Phase 0: mart 表增加 data_completeness 列
        for tbl in ["mart_institution_profile", "mart_institution_industry_stat",
                     "mart_stock_trend"]:
            try:
                conn.execute(
                    f"ALTER TABLE {tbl} ADD COLUMN data_completeness TEXT DEFAULT 'complete'"
                )
            except Exception:
                pass

        # Phase 1: mart_institution_profile 买入类评分字段 + 评分元数据
        for col in ["score_basis TEXT", "score_confidence TEXT",
                     "historical_median_holding_days INTEGER",
                     "current_avg_held_days INTEGER"]:
            try:
                conn.execute(f"ALTER TABLE mart_institution_profile ADD COLUMN {col}")
            except Exception:
                pass
        for col in [
            "buy_event_count INTEGER",
            "buy_avg_gain_30d REAL", "buy_avg_gain_60d REAL", "buy_avg_gain_120d REAL",
            "buy_win_rate_30d REAL", "buy_win_rate_60d REAL", "buy_win_rate_120d REAL",
            "buy_median_max_drawdown_30d REAL", "buy_median_max_drawdown_60d REAL",
            "avg_premium_pct REAL",
            "safe_follow_event_count INTEGER",
            "safe_follow_win_rate_30d REAL",
            "safe_follow_avg_gain_30d REAL",
            "safe_follow_avg_drawdown_30d REAL",
            "premium_discount_event_count INTEGER",
            "premium_discount_win_rate_30d REAL",
            "premium_near_cost_event_count INTEGER",
            "premium_near_cost_win_rate_30d REAL",
            "premium_premium_event_count INTEGER",
            "premium_premium_win_rate_30d REAL",
            "premium_high_event_count INTEGER",
            "premium_high_win_rate_30d REAL",
            "signal_transfer_efficiency_30d REAL",
            "followability_hint TEXT",
            "followability_score REAL",
            "followability_confidence TEXT",
            "main_industry_1 TEXT", "main_industry_2 TEXT", "main_industry_3 TEXT",
            "best_industry_1 TEXT", "best_industry_2 TEXT", "best_industry_3 TEXT",
        ]:
            try:
                conn.execute(f"ALTER TABLE mart_institution_profile ADD COLUMN {col}")
            except Exception:
                pass

        # Phase 0: mart_current_relationship 物化表
        conn.execute("""
            CREATE TABLE IF NOT EXISTS mart_current_relationship (
                institution_id    TEXT NOT NULL,
                institution_name  TEXT,
                display_name      TEXT,
                inst_type         TEXT,
                stock_code        TEXT NOT NULL,
                stock_name        TEXT,
                report_date       TEXT NOT NULL,
                notice_date       TEXT,
                holder_rank       INTEGER,
                hold_amount       REAL,
                hold_market_cap   REAL,
                hold_ratio        REAL,
                hold_change       TEXT,
                event_type        TEXT,
                change_pct        REAL,
                gain_10d          REAL,
                gain_30d          REAL,
                gain_60d          REAL,
                gain_90d          REAL,
                gain_120d         REAL,
                max_drawdown_30d  REAL,
                max_drawdown_60d  REAL,
                report_season     TEXT,
                inst_ref_cost     REAL,
                inst_cost_method  TEXT,
                premium_pct       REAL,
                premium_bucket    TEXT,
                follow_gate       TEXT,
                follow_gate_reason TEXT,
                price_entry       REAL,
                return_to_now     REAL,
                path_state        TEXT,
                entry_report_date TEXT,
                entry_notice_date TEXT,
                notice_age_days   INTEGER,
                disclosure_lag_days INTEGER,
                current_held_days INTEGER,
                sw_level1         TEXT,
                sw_level2         TEXT,
                sw_level3         TEXT,
                has_return_data   INTEGER DEFAULT 0,
                has_industry_data INTEGER DEFAULT 0,
                updated_at        TEXT,
                PRIMARY KEY (institution_id, stock_code)
            )
        """)
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mcr_inst "
            "ON mart_current_relationship(institution_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mcr_stock "
            "ON mart_current_relationship(stock_code)"
        )

        for col in [
            "report_season TEXT",
            "inst_ref_cost REAL",
            "inst_cost_method TEXT",
            "premium_pct REAL",
            "premium_bucket TEXT",
            "follow_gate TEXT",
            "follow_gate_reason TEXT",
        ]:
            try:
                conn.execute(f"ALTER TABLE mart_current_relationship ADD COLUMN {col}")
            except Exception:
                pass

        conn.commit()

        # 初始化排除类别（如果为空）
        existing = conn.execute("SELECT COUNT(*) FROM exclusion_categories").fetchone()[0]
        if existing == 0:
            now = datetime.now().isoformat()
            categories = [
                ("ST", "ST/*ST 股票", 1),
                ("BSE", "北交所 (8/9开头)", 1),
                ("NEEQ", "新三板 (4开头)", 1),
                ("OTC", "老三板 (400开头)", 1),
                ("B_SHARE", "B股 (200/900开头)", 1),
                ("CDR", "CDR 存托凭证", 1),
            ]
            for cat, label, enabled in categories:
                conn.execute(
                    "INSERT OR IGNORE INTO exclusion_categories (category, label, enabled, updated_at) VALUES (?, ?, ?, ?)",
                    (cat, label, enabled, now)
                )
            conn.commit()
            logger.info(f"[DB] 初始化 {len(categories)} 个排除类别")

        # ============================================================
        # Phase 2: 新增表 — 财务/选股/资产/Qlib
        # ============================================================

        # 财务数据表（由 financial_client.py ensure_tables 管理，此处确保存在）
        from services.financial_client import ensure_tables as _ensure_fin_tables
        _ensure_fin_tables(conn)

        # 财务指标增强表（由 financial_indicator_client.py ensure_tables 管理）
        from services.financial_indicator_client import ensure_tables as _ensure_fin_indicator_tables
        _ensure_fin_indicator_tables(conn)

        # 资本行为增强表（由 capital_client.py ensure_tables 管理）
        from services.capital_client import ensure_tables as _ensure_capital_tables
        _ensure_capital_tables(conn)

        # 质量特征中间层（由 quality_feature_engine.py ensure_tables 管理）
        from services.quality_feature_engine import ensure_tables as _ensure_quality_feature_tables
        _ensure_quality_feature_tables(conn)

        # 行业上下文中间层（由 industry_context_engine.py ensure_tables 管理）
        from services.industry_context_engine import ensure_tables as _ensure_industry_context_tables
        _ensure_industry_context_tables(conn)

        # 选股结果表（由 screening_engine.py ensure_tables 管理）
        from services.screening_engine import ensure_tables as _ensure_screen_tables
        _ensure_screen_tables(conn)

        # Qlib 完整版表（由 qlib_full_engine.py ensure_tables 管理）
        from services.qlib_full_engine import ensure_tables as _ensure_qlib_full_tables
        _ensure_qlib_full_tables(conn)

        # 板块动量表（由 sector_momentum.py ensure_tables 管理）
        from services.sector_momentum import ensure_tables as _ensure_sector_tables
        _ensure_sector_tables(conn)

        # 资产池维度表（ETF / 指数 / 股票统一管理）
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_asset_universe (
                code        TEXT PRIMARY KEY,
                name        TEXT,
                asset_type  TEXT DEFAULT 'stock',
                market      TEXT,
                category    TEXT,
                list_date   TEXT,
                is_active   INTEGER DEFAULT 1,
                updated_at  TEXT
            )
        """)
        try:
            conn.execute("ALTER TABLE dim_asset_universe ADD COLUMN category TEXT")
        except Exception:
            pass
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_dau_type ON dim_asset_universe(asset_type)"
        )
        conn.commit()
        conn.execute("""
            INSERT OR IGNORE INTO app_settings (key, value, updated_at)
            VALUES ('module_qlib_enabled', '1', CURRENT_TIMESTAMP),
                   ('module_etf_enabled', '1', CURRENT_TIMESTAMP),
                   ('module_akquant_enabled', '0', CURRENT_TIMESTAMP)
        """)
        # 一次性迁移：qlib 模块默认从 0 升到 1（仅当历史默认值未被用户显式改过时）
        cur = conn.execute(
            "SELECT value FROM app_settings WHERE key='_migration_qlib_default_v1'"
        ).fetchone()
        if not cur:
            conn.execute(
                "UPDATE app_settings SET value='1', updated_at=CURRENT_TIMESTAMP "
                "WHERE key='module_qlib_enabled' AND value='0'"
            )
            conn.execute(
                "INSERT OR IGNORE INTO app_settings (key, value, updated_at) "
                "VALUES ('_migration_qlib_default_v1', 'done', CURRENT_TIMESTAMP)"
            )

        conn.execute("DELETE FROM app_settings WHERE key LIKE 'scoring.stock.%'")
        conn.execute("DELETE FROM app_settings WHERE key LIKE 'scoring.timing.%'")
        conn.execute("DELETE FROM app_settings WHERE key LIKE 'scoring.path.%'")
        conn.execute("DELETE FROM app_settings WHERE key LIKE 'scoring.event_type.%'")

        conn.commit()

        logger.info("[DB] 数据库初始化完成")
    finally:
        conn.close()

def get_enabled_modules(conn) -> dict:
    rows = conn.execute("SELECT key, value FROM app_settings WHERE key LIKE 'module_%_enabled'").fetchall()
    modules = {"qlib": True, "etf": True, "akquant": False}
    for r in rows:
        key = r["key"].replace("module_", "").replace("_enabled", "")
        modules[key] = str(r["value"]) == "1"
    return modules

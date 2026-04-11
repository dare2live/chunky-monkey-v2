import json
import sqlite3
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services import etf_snapshot_manager  # noqa: E402
from services.etf_engine import _classify_etf_strategy  # noqa: E402
from services.etf_snapshot_manager import invalidate_etf_snapshot_cache, load_cached_etf_row  # noqa: E402
from services.qlib_full_engine import ensure_tables, get_qlib_etf_consensus  # noqa: E402


class EtfQlibConsensusTests(unittest.TestCase):
    def setUp(self):
        invalidate_etf_snapshot_cache()

    def tearDown(self):
        invalidate_etf_snapshot_cache()

    def _make_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        ensure_tables(conn)
        conn.execute(
            """
            CREATE TABLE dim_stock_industry_context_latest (
                stock_code TEXT PRIMARY KEY,
                sw_level1 TEXT
            )
            """
        )
        return conn

    def _make_snapshot_state_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE mart_etf_snapshot_latest (
                code TEXT PRIMARY KEY,
                snapshot_id TEXT,
                category TEXT,
                factor_rank INTEGER,
                factor_score REAL,
                rotation_score REAL,
                strategy_type TEXT,
                payload_json TEXT NOT NULL,
                updated_at TEXT
            );
            CREATE TABLE mart_etf_snapshot_state (
                state_key TEXT PRIMARY KEY,
                snapshot_id TEXT,
                schema_version INTEGER,
                computed_at TEXT,
                etf_count INTEGER,
                history_start TEXT,
                history_end TEXT,
                overview_json TEXT,
                factor_snapshot_json TEXT,
                mining_snapshot_json TEXT,
                source_status_json TEXT
            );
            CREATE TABLE etf_asset_universe (
                code TEXT PRIMARY KEY,
                is_active INTEGER,
                updated_at TEXT
            );
            """
        )
        return conn

    def _make_snapshot_market_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE etf_sync_state (
                code TEXT,
                dataset TEXT,
                freq TEXT,
                adjust TEXT,
                source TEXT,
                min_date TEXT,
                max_date TEXT,
                row_count INTEGER,
                last_success_at TEXT,
                last_attempt_at TEXT,
                last_error TEXT
            );
            CREATE TABLE etf_price_kline (
                code TEXT,
                date TEXT,
                freq TEXT,
                adjust TEXT
            );
            """
        )
        return conn

    def _make_snapshot_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute(
            """
            CREATE TABLE mart_etf_snapshot_latest (
                code TEXT PRIMARY KEY,
                payload_json TEXT NOT NULL
            )
            """
        )
        return conn

    def _current_snapshot_payload(self, code: str, *, name: str, category: str, strategy_type: str) -> str:
        return json.dumps(
            {
                "code": code,
                "name": name,
                "category": category,
                "strategy_type": strategy_type,
                "strategy_reason": "测试",
                "qlib_consensus_score": 72.5,
                "qlib_model_status": "trained",
                "qlib_consensus_factor_group": "institution",
                "qlib_preferred_strategy": "网格交易",
                "qlib_predicted_best_step_pct": 1.8,
                "qlib_predicted_buy_hold_return_pct": 1.2,
                "qlib_predicted_grid_return_pct": 2.4,
                "backtest_hard_gate_passed": True,
                "tradeability_status": "ok",
                "tradeability_reason": "",
            },
            ensure_ascii=False,
        )

    def test_get_qlib_etf_consensus_groups_predictions_by_etf_category(self):
        conn = self._make_conn()
        try:
            conn.execute(
                """
                INSERT INTO qlib_model_state (
                    model_id, status, train_start, train_end, test_start, test_end,
                    stock_count, factor_count, ic_mean, rank_ic_mean,
                    test_top50_avg_return, created_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "model-1",
                    "trained",
                    "2024-01-01",
                    "2024-12-31",
                    "2025-01-01",
                    "2025-03-31",
                    4,
                    6,
                    0.041,
                    0.056,
                    0.123,
                    "2025-04-01T10:00:00",
                    "2025-04-01T12:00:00",
                ),
            )
            conn.executemany(
                "INSERT INTO qlib_factor_importance (model_id, factor_name, importance, factor_group) VALUES (?, ?, ?, ?)",
                [
                    ("model-1", "inst_score", 0.6, "institution"),
                    ("model-1", "alpha_signal", 0.4, "alpha158"),
                ],
            )
            conn.executemany(
                "INSERT INTO qlib_predictions (model_id, stock_code, stock_name, predict_date, qlib_score, qlib_rank, qlib_percentile) VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    ("model-1", "000001", "药企A", "2025-04-01", 0.91, 1, 91.0),
                    ("model-1", "000002", "软件B", "2025-04-01", 0.84, 2, 84.0),
                    ("model-1", "000003", "银行C", "2025-04-01", 0.82, 3, 82.0),
                    ("model-1", "000004", "药企D", "2025-04-01", 0.86, 4, 86.0),
                ],
            )
            conn.executemany(
                "INSERT INTO dim_stock_industry_context_latest (stock_code, sw_level1) VALUES (?, ?)",
                [
                    ("000001", "医药生物"),
                    ("000002", "计算机"),
                    ("000003", "银行"),
                    ("000004", "医药生物"),
                ],
            )
            conn.commit()

            consensus = get_qlib_etf_consensus(conn, topk=10)

            self.assertTrue(consensus["available"])
            self.assertEqual(consensus["model_status"], "trained")
            self.assertEqual(consensus["leading_factor_group"], "institution")
            self.assertEqual(consensus["factor_consensus"]["institution"], 0.6)
            self.assertEqual(consensus["mapped_stock_count"], 4)
            self.assertIn("医疗健康", consensus["category_signal_map"])
            self.assertEqual(consensus["category_signal_map"]["医疗健康"]["stock_count"], 2)
            self.assertGreater(
                consensus["category_signal_map"]["医疗健康"]["consensus_score"],
                consensus["category_signal_map"]["金融"]["consensus_score"],
            )
        finally:
            conn.close()

    def test_qlib_support_promotes_borderline_etf_to_grid_candidate(self):
        base_row = {
            "category": "数字科技",
            "trend_status": "多头",
            "setup_state": "震荡观察",
            "rotation_bucket": "watch",
            "momentum_20d": 13.0,
            "relative_strength_12w": 0.0,
            "volatility_20d": 10.0,
            "amplitude_20d": 8.0,
            "max_drawdown_60d": -3.0,
        }

        strategy, _, _, score = _classify_etf_strategy(base_row)
        self.assertEqual(strategy, "观察池")
        self.assertEqual(score, 56.0)

        qlib_row = dict(base_row)
        qlib_row.update({
            "qlib_consensus_score": 72.0,
            "qlib_model_status": "trained",
            "qlib_consensus_factor_group": "institution",
        })
        strategy, reason, step, score = _classify_etf_strategy(qlib_row)

        self.assertEqual(strategy, "网格候选")
        self.assertIn("Qlib 共识", reason)
        self.assertEqual(step, 1.3)
        self.assertGreater(score, 56.0)

    def test_load_cached_etf_row_rejects_stale_snapshot_payload(self):
        conn = self._make_snapshot_conn()
        try:
            conn.execute(
                "INSERT INTO mart_etf_snapshot_latest (code, payload_json) VALUES (?, ?)",
                (
                    "159001",
                    '{"code":"159001","name":"旧快照ETF","category":"宽基","strategy_type":"买入持有"}',
                ),
            )
            conn.commit()

            self.assertIsNone(load_cached_etf_row(conn, "159001"))
        finally:
            conn.close()

    def test_load_cached_etf_row_accepts_current_snapshot_payload(self):
        conn = self._make_snapshot_conn()
        try:
            conn.execute(
                "INSERT INTO mart_etf_snapshot_latest (code, payload_json) VALUES (?, ?)",
                (
                    "159002",
                    self._current_snapshot_payload(
                        "159002",
                        name="当前快照ETF",
                        category="医疗健康",
                        strategy_type="网格交易",
                    ),
                ),
            )
            conn.commit()

            payload = load_cached_etf_row(conn, "159002")
            self.assertIsNotNone(payload)
            self.assertEqual(payload["code"], "159002")
            self.assertEqual(payload["qlib_model_status"], "trained")
        finally:
            conn.close()

    def test_invalidate_etf_snapshot_cache_clears_memory_bundle(self):
        etf_snapshot_manager._ETF_SNAPSHOT_MEMORY_CACHE["snapshot_id"] = "snap-test"
        etf_snapshot_manager._ETF_SNAPSHOT_MEMORY_CACHE["bundle"] = {"snapshot_id": "snap-test"}

        invalidate_etf_snapshot_cache()

        self.assertIsNone(etf_snapshot_manager._ETF_SNAPSHOT_MEMORY_CACHE["snapshot_id"])
        self.assertIsNone(etf_snapshot_manager._ETF_SNAPSHOT_MEMORY_CACHE["bundle"])

    def test_get_latest_snapshot_refreshes_when_live_source_status_changes(self):
        conn = self._make_snapshot_state_conn()
        mkt_conn = self._make_snapshot_market_conn()
        try:
            conn.execute(
                "INSERT INTO etf_asset_universe (code, is_active, updated_at) VALUES (?, ?, ?)",
                ("159001", 1, "2026-04-10T09:00:00"),
            )
            conn.execute(
                "INSERT INTO mart_etf_snapshot_latest (code, snapshot_id, category, strategy_type, payload_json, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "159001",
                    "snap-old",
                    "宽基",
                    "买入持有",
                    self._current_snapshot_payload(
                        "159001",
                        name="快照ETF",
                        category="宽基",
                        strategy_type="买入持有",
                    ),
                    "2026-04-09T09:00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO mart_etf_snapshot_state (
                    state_key, snapshot_id, schema_version, computed_at, etf_count,
                    history_start, history_end, overview_json,
                    factor_snapshot_json, mining_snapshot_json, source_status_json
                ) VALUES ('latest', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "snap-old",
                    etf_snapshot_manager.ETF_SNAPSHOT_SCHEMA_VERSION,
                    "2026-04-09T09:00:00",
                    1,
                    "2023-01-03",
                    "2026-04-09",
                    "{}",
                    "{}",
                    "{}",
                    '{"universe_count":1,"universe_updated_at":"2026-04-10T09:00:00","kline_etf_count":1,"history_start":"2023-01-03","history_end":"2026-04-09","coverage_2023_count":1,"recent_only_count":0,"no_kline_count":0,"latest_kline_success_at":"2026-04-09T09:30:00","latest_kline_attempt_at":"2026-04-09T09:31:00","source_breakdown":[{"source":"mock","count":1}],"snapshot_is_stale":true,"snapshot_lag_minutes":30,"connectivity":{}}',
                ),
            )
            mkt_conn.execute(
                """
                INSERT INTO etf_sync_state (
                    code, dataset, freq, adjust, source, min_date, max_date,
                    row_count, last_success_at, last_attempt_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "159001",
                    "price_kline",
                    "daily",
                    "qfq",
                    "mock",
                    "2023-01-03",
                    "2026-04-10",
                    300,
                    "2026-04-10T09:30:00",
                    "2026-04-10T09:31:00",
                    None,
                ),
            )
            mkt_conn.execute(
                "INSERT INTO etf_price_kline (code, date, freq, adjust) VALUES (?, ?, ?, ?)",
                ("159001", "2023-01-03", "daily", "qfq"),
            )
            mkt_conn.execute(
                "INSERT INTO etf_price_kline (code, date, freq, adjust) VALUES (?, ?, ?, ?)",
                ("159001", "2026-04-10", "daily", "qfq"),
            )
            conn.commit()
            mkt_conn.commit()

            refreshed_bundle = {
                "snapshot_id": "snap-new",
                "computed_at": "2026-04-10T10:00:00",
                "etf_count": 1,
                "rows": [{"code": "159001"}],
                "overview": {},
                "factor_snapshot": {},
                "mining_snapshot": {},
                "source_status": {"snapshot_is_stale": False},
                "is_stale": False,
            }
            with mock.patch.object(etf_snapshot_manager, "persist_latest_etf_snapshot", return_value=refreshed_bundle) as persist_mock:
                bundle = etf_snapshot_manager.get_latest_etf_snapshot_bundle(conn, mkt_conn)

            persist_mock.assert_called_once()
            self.assertEqual(bundle["snapshot_id"], "snap-new")
        finally:
            conn.close()
            mkt_conn.close()

    def test_get_latest_snapshot_reuses_cached_rows_when_source_status_matches(self):
        conn = self._make_snapshot_state_conn()
        mkt_conn = self._make_snapshot_market_conn()
        try:
            conn.execute(
                "INSERT INTO etf_asset_universe (code, is_active, updated_at) VALUES (?, ?, ?)",
                ("159001", 1, "2026-04-10T09:00:00"),
            )
            conn.execute(
                "INSERT INTO mart_etf_snapshot_latest (code, snapshot_id, category, strategy_type, payload_json, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    "159001",
                    "snap-stable",
                    "宽基",
                    "买入持有",
                    self._current_snapshot_payload(
                        "159001",
                        name="快照ETF",
                        category="宽基",
                        strategy_type="买入持有",
                    ),
                    "2026-04-10T09:35:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO mart_etf_snapshot_state (
                    state_key, snapshot_id, schema_version, computed_at, etf_count,
                    history_start, history_end, overview_json,
                    factor_snapshot_json, mining_snapshot_json, source_status_json
                ) VALUES ('latest', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "snap-stable",
                    etf_snapshot_manager.ETF_SNAPSHOT_SCHEMA_VERSION,
                    "2026-04-10T09:35:00",
                    1,
                    "2023-01-03",
                    "2026-04-10",
                    "{}",
                    "{}",
                    "{}",
                    '{"universe_count":1,"universe_updated_at":"2026-04-10T09:00:00","kline_etf_count":1,"history_start":"2023-01-03","history_end":"2026-04-10","coverage_2023_count":1,"recent_only_count":0,"no_kline_count":0,"latest_kline_success_at":"2026-04-10T09:30:00","latest_kline_attempt_at":"2026-04-10T09:31:00","source_breakdown":[{"source":"mock","count":1}],"snapshot_is_stale":false,"snapshot_lag_minutes":0,"connectivity":{}}',
                ),
            )
            mkt_conn.execute(
                """
                INSERT INTO etf_sync_state (
                    code, dataset, freq, adjust, source, min_date, max_date,
                    row_count, last_success_at, last_attempt_at, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "159001",
                    "price_kline",
                    "daily",
                    "qfq",
                    "mock",
                    "2023-01-03",
                    "2026-04-10",
                    300,
                    "2026-04-10T09:30:00",
                    "2026-04-10T09:31:00",
                    None,
                ),
            )
            mkt_conn.execute(
                "INSERT INTO etf_price_kline (code, date, freq, adjust) VALUES (?, ?, ?, ?)",
                ("159001", "2023-01-03", "daily", "qfq"),
            )
            mkt_conn.execute(
                "INSERT INTO etf_price_kline (code, date, freq, adjust) VALUES (?, ?, ?, ?)",
                ("159001", "2026-04-10", "daily", "qfq"),
            )
            conn.commit()
            mkt_conn.commit()

            with mock.patch.object(etf_snapshot_manager, "persist_latest_etf_snapshot") as persist_mock:
                bundle = etf_snapshot_manager.get_latest_etf_snapshot_bundle(conn, mkt_conn)

            persist_mock.assert_not_called()
            self.assertEqual(bundle["snapshot_id"], "snap-stable")
            self.assertEqual(bundle["rows"][0]["code"], "159001")
            self.assertFalse(bundle["is_stale"])
        finally:
            conn.close()
            mkt_conn.close()


if __name__ == "__main__":
    unittest.main()

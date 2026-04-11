import sqlite3
import sys
import unittest
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services import etf_qlib_engine  # noqa: E402
from services.etf_qlib_engine import (  # noqa: E402
    get_etf_qlib_pipeline_status,
    get_latest_etf_qlib_signal_snapshot,
)


class EtfQlibEngineTests(unittest.TestCase):
    def _make_conn(self):
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE etf_asset_universe (
                code TEXT PRIMARY KEY,
                is_active INTEGER DEFAULT 1
            );
            CREATE TABLE etf_qlib_feature_store (
                snapshot_date TEXT,
                code TEXT
            );
            CREATE TABLE etf_qlib_label_store (
                snapshot_date TEXT,
                code TEXT
            );
            CREATE TABLE etf_qlib_model_state (
                model_id TEXT PRIMARY KEY,
                status TEXT,
                etf_count INTEGER,
                sample_count INTEGER,
                feature_count INTEGER,
                error TEXT,
                created_at TEXT,
                finished_at TEXT
            );
            CREATE TABLE etf_qlib_predictions (
                model_id TEXT,
                code TEXT
            );
            CREATE TABLE etf_qlib_backtest_result (
                model_id TEXT,
                code TEXT
            );
            CREATE TABLE etf_qlib_param_search (
                model_id TEXT,
                code TEXT
            );
            """
        )
        return conn

    def test_pipeline_status_reports_model_timing_and_table_counts(self):
        conn = self._make_conn()
        try:
            conn.executemany(
                "INSERT INTO etf_asset_universe (code, is_active) VALUES (?, ?)",
                [("159001", 1), ("159002", 1)],
            )
            conn.executemany(
                "INSERT INTO etf_qlib_feature_store (snapshot_date, code) VALUES (?, ?)",
                [("2026-04-10", "159001"), ("2026-04-10", "159002")],
            )
            conn.executemany(
                "INSERT INTO etf_qlib_label_store (snapshot_date, code) VALUES (?, ?)",
                [("2026-04-10", "159001"), ("2026-04-10", "159002")],
            )
            conn.execute(
                """
                INSERT INTO etf_qlib_model_state (
                    model_id, status, etf_count, sample_count, feature_count,
                    error, created_at, finished_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "etf_qlib_20260411_120000",
                    "trained",
                    2,
                    128,
                    17,
                    None,
                    "2026-04-11 12:00:00",
                    "2026-04-11 12:05:00",
                ),
            )
            conn.executemany(
                "INSERT INTO etf_qlib_predictions (model_id, code) VALUES (?, ?)",
                [("etf_qlib_20260411_120000", "159001"), ("etf_qlib_20260411_120000", "159002")],
            )
            conn.executemany(
                "INSERT INTO etf_qlib_backtest_result (model_id, code) VALUES (?, ?)",
                [("etf_qlib_20260411_120000", "159001"), ("etf_qlib_20260411_120000", "159002")],
            )
            conn.executemany(
                "INSERT INTO etf_qlib_param_search (model_id, code) VALUES (?, ?)",
                [("etf_qlib_20260411_120000", "159001"), ("etf_qlib_20260411_120000", "159002")],
            )
            conn.commit()

            status = get_etf_qlib_pipeline_status(conn)

            self.assertTrue(status["available"])
            self.assertEqual(status["pipeline_status"], "ready")
            self.assertEqual(status["model_status"], "trained")
            self.assertEqual(status["model_status_label"], "已训练")
            self.assertEqual(status["model_id"], "etf_qlib_20260411_120000")
            self.assertEqual(status["model_finished_at"], "2026-04-11 12:05:00")
            self.assertEqual(status["model_sample_count"], 128)
            self.assertEqual(status["model_feature_count"], 17)
            self.assertEqual(status["model_etf_count"], 2)
            self.assertEqual(status["feature_store_row_count"], 2)
            self.assertEqual(status["label_store_row_count"], 2)
            self.assertEqual(status["prediction_row_count"], 2)
            self.assertEqual(status["backtest_row_count"], 2)
            self.assertEqual(status["param_search_row_count"], 2)
            self.assertEqual(status["table_counts"]["predictions"], 2)
        finally:
            conn.close()

    def test_force_refresh_forwards_requested_topk_to_training(self):
        dummy_conn = object()
        sentinel = {"status": "ok", "topk": 12}

        with mock.patch.object(etf_qlib_engine, "train_etf_qlib_pipeline", return_value=sentinel) as train_mock:
            result = get_latest_etf_qlib_signal_snapshot(dummy_conn, topk=12, force_refresh=True)

        self.assertEqual(result, sentinel)
        train_mock.assert_called_once_with(dummy_conn, force_refresh=True, snapshot_topk=12)


if __name__ == "__main__":
    unittest.main()
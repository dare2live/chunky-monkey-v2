import sqlite3
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from routers.updater import _collect_downstream_steps  # noqa: E402
from services.audit import _external_attention_plan_reason, _summarize_external_attention  # noqa: E402


def _make_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE dim_stock_attention_latest (
            stock_code TEXT,
            snapshot_date TEXT,
            comment_available INTEGER DEFAULT 0,
            survey_available INTEGER DEFAULT 0,
            comment_trade_date TEXT,
            last_survey_date TEXT
        );

        CREATE TABLE mart_current_relationship (
            stock_code TEXT
        );

        CREATE TABLE mart_stock_trend (
            stock_code TEXT,
            external_attention_score REAL,
            attention_comment_trade_date TEXT,
            external_attention_signal TEXT
        );
        """
    )
    return conn


class ExternalAttentionSyncPlanTests(unittest.TestCase):
    def test_summary_marks_stale_snapshot_before_market_refresh(self):
        conn = _make_conn()
        today = date.today()
        snapshot_day = today - timedelta(days=2)

        conn.executemany(
            """
            INSERT INTO dim_stock_attention_latest (
                stock_code, snapshot_date, comment_available, survey_available,
                comment_trade_date, last_survey_date
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("000001", snapshot_day.isoformat(), 1, 0, (today - timedelta(days=3)).isoformat(), (today - timedelta(days=2)).isoformat()),
                ("000002", snapshot_day.isoformat(), 1, 1, (today - timedelta(days=2)).isoformat(), (today - timedelta(days=1)).isoformat()),
            ],
        )
        conn.executemany(
            "INSERT INTO mart_current_relationship (stock_code) VALUES (?)",
            [("000001",), ("000002",), ("000003",)],
        )
        conn.executemany(
            """
            INSERT INTO mart_stock_trend (
                stock_code, external_attention_score, attention_comment_trade_date, external_attention_signal
            ) VALUES (?, ?, ?, ?)
            """,
            [
                ("000001", 72.0, snapshot_day.isoformat(), "外部确认增强"),
                ("000003", None, None, ""),
            ],
        )

        summary = _summarize_external_attention(conn, today.isoformat(), expected_stocks=3)

        self.assertEqual(summary["latest_snapshot_date"], snapshot_day.isoformat())
        self.assertEqual(summary["snapshot_rows"], 2)
        self.assertEqual(summary["covered_stocks"], 2)
        self.assertEqual(summary["missing_stocks"], 1)
        self.assertEqual(summary["snapshot_lag_days"], 2)
        self.assertEqual(summary["trend_scored_stocks"], 1)
        self.assertEqual(_external_attention_plan_reason(summary), "外部关注快照滞后2天")

    def test_summary_marks_missing_current_stock_coverage_even_when_snapshot_is_fresh(self):
        conn = _make_conn()
        today = date.today()

        conn.executemany(
            """
            INSERT INTO dim_stock_attention_latest (
                stock_code, snapshot_date, comment_available, survey_available,
                comment_trade_date, last_survey_date
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("000001", today.isoformat(), 1, 0, today.isoformat(), today.isoformat()),
                ("000002", today.isoformat(), 1, 1, today.isoformat(), today.isoformat()),
            ],
        )
        conn.executemany(
            "INSERT INTO mart_current_relationship (stock_code) VALUES (?)",
            [("000001",), ("000002",), ("000003",)],
        )

        summary = _summarize_external_attention(conn, today.isoformat(), expected_stocks=3)

        self.assertEqual(summary["snapshot_lag_days"], 0)
        self.assertEqual(summary["missing_stocks"], 1)
        self.assertEqual(_external_attention_plan_reason(summary), "1只当前股票缺外部关注覆盖")

    def test_build_external_attention_cascades_to_stock_scores(self):
        step_ids = _collect_downstream_steps("build_external_attention")

        self.assertIn("build_external_attention", step_ids)
        self.assertIn("calc_stock_scores", step_ids)


if __name__ == "__main__":
    unittest.main()
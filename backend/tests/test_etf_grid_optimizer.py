import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.etf_grid_engine import (  # noqa: E402
    _buy_hold_stats,
    _build_grid_step_candidates,
    _optimize_grid,
    _run_grid_backtest,
    _score_grid_backtest,
)


def _price_rows(prices):
    start = datetime(2025, 1, 1)
    return [
        {"date": (start + timedelta(days=index)).strftime("%Y-%m-%d"), "close": float(price)}
        for index, price in enumerate(prices)
    ]


def _grid_row(trend="震荡"):
    return {
        "strategy_type": "网格候选",
        "trend_status": trend,
        "setup_state": "震荡观察",
        "rotation_bucket": "watch",
    }


class EtfGridOptimizerTests(unittest.TestCase):
    def test_monotonic_downtrend_produces_no_feasible_grid_step(self):
        prices = [100.0 - index * 0.45 for index in range(90)]
        rows = _price_rows(prices)
        row = _grid_row(trend="空头")
        buy_hold = _buy_hold_stats(rows)
        self.assertIsNotNone(buy_hold)

        candidates = _build_grid_step_candidates(rows, row=row)
        scored_candidates = []
        for step in candidates:
            backtest = _run_grid_backtest(rows, step)
            self.assertIsNotNone(backtest)
            scored = _score_grid_backtest(backtest, buy_hold, row=row)
            scored_candidates.append(scored)

        self.assertTrue(scored_candidates)
        self.assertTrue(all(not item["hard_gate_passed"] for item in scored_candidates))
        self.assertTrue(all(item["candidate_score"] == 0.0 for item in scored_candidates))
        self.assertTrue(any("有效卖出回笼" in item["hard_gate_reason"] for item in scored_candidates))
        self.assertIsNone(_optimize_grid(rows, row=row))

    def test_range_bound_prices_keep_feasible_grid_candidates(self):
        pattern = [100.0, 98.6, 97.2, 98.4, 100.1, 101.8, 103.1, 101.5]
        prices = [pattern[index % len(pattern)] for index in range(144)]
        rows = _price_rows(prices)
        row = _grid_row(trend="震荡")

        best = _optimize_grid(rows, row=row)
        self.assertIsNotNone(best)
        self.assertTrue(best["hard_gate_passed"])
        self.assertTrue(best["audit"]["audit_passed"])
        self.assertGreater(best["sell_count"], 0)
        self.assertGreater(best["valid_candidate_count"], 0)
        self.assertAlmostEqual(best["cash_ledger_gap"], 0.0, delta=0.05)
        self.assertAlmostEqual(best["pnl_ledger_gap"], 0.0, delta=0.05)


if __name__ == "__main__":
    unittest.main()

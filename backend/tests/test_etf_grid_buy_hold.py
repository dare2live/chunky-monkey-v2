import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.etf_grid_engine import _buy_hold_stats

def test_buy_hold_stats_insufficient_data():
    rows = [{"close": 100.0, "date": f"2026-01-{i+1:02d}"} for i in range(5)]
    assert _buy_hold_stats(rows) is None

def test_buy_hold_stats_valid():
    rows = []
    # simulate prices 10.0 -> 20.0
    for i in range(10):
        rows.append({"close": 10.0 + i, "date": f"2026-01-{i+1:02d}"})
    stats = _buy_hold_stats(rows)
    assert stats is not None
    assert stats["return_pct"] > 0
    assert stats["max_drawdown_pct"] == 0.0

def test_buy_hold_stats_drawdown():
    rows = []
    prices = [10.0, 15.0, 12.0, 8.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0]
    for i, p in enumerate(prices):
        rows.append({"close": p, "date": f"2026-01-{i+1:02d}"})
    stats = _buy_hold_stats(rows)
    assert stats is not None
    assert stats["max_drawdown_pct"] > 0.0

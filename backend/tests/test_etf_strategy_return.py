import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.etf_mining_engine import _strategy_return_snapshot

def test_strategy_return_snapshot():
    res = _strategy_return_snapshot("网格交易", 15.0, 10.0)
    assert res["recommended_strategy_label"] == "最优网格"
    assert res["recommended_strategy_return_pct"] == 15.0
    assert res["comparison_strategy_label"] == "买入持有"
    assert res["comparison_strategy_return_pct"] == 10.0
    assert res["strategy_edge_pct"] == 5.0
    
    res2 = _strategy_return_snapshot("买入持有", 15.0, 20.0)
    assert res2["recommended_strategy_label"] == "买入持有"
    assert res2["recommended_strategy_return_pct"] == 20.0
    assert res2["comparison_strategy_label"] == "最优网格"
    assert res2["comparison_strategy_return_pct"] == 15.0
    assert res2["strategy_edge_pct"] == 5.0

    res3 = _strategy_return_snapshot(None, None, None)
    assert res3["recommended_strategy_return_pct"] is None

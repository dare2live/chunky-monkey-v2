import sys
from pathlib import Path
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.etf_grid_engine import _max_drawdown, _trade_fee, _max_affordable_units, _open_units, _open_cost_basis

def test_max_drawdown():
    assert _max_drawdown([]) is None
    assert _max_drawdown([100.0]) is None
    assert _max_drawdown([100.0, 110.0, 120.0]) == 0.0
    assert _max_drawdown([100.0, 90.0, 80.0]) == 20.0  # (100-80)/100
    assert _max_drawdown([100.0, 120.0, 60.0, 150.0]) == 50.0  # (120-60)/120

def test_trade_fee():
    assert _trade_fee(100.0, 0.0001, 5.0) == 5.0
    assert _trade_fee(100000.0, 0.0001, 5.0) == 10.0  # 100000 * 0.0001
    assert _trade_fee(0.0, 0.0, 0.0) == 0.0

def test_max_affordable_units():
    assert _max_affordable_units(10000.0, 10.0, 100, 0.0001, 5.0) == 900
    assert _max_affordable_units(900.0, 10.0, 100, 0.0001, 5.0) == 0
    assert _max_affordable_units(1020.0, 10.0, 100, 0.0001, 5.0) == 100

def test_open_units():
    assert _open_units([]) == 0
    assert _open_units([{"units": 100}, {"units": 200}]) == 300

def test_open_cost_basis():
    assert _open_cost_basis([]) == 0.0
    assert _open_cost_basis([{"cost_total": 100.0}]) == 100.0
    assert _open_cost_basis([{"cost_total": 100.0}, {"cost_total": 250.0}]) == 350.0

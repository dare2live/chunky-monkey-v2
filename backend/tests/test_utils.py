import sys
from pathlib import Path

# Add backend directory to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.utils import safe_float, percentile_ranks, normalize_ymd, clamp, clamp_score, parse_any_date

def test_safe_float():
    assert safe_float(1.5) == 1.5
    assert safe_float("2.3") == 2.3
    assert safe_float(None) is None
    assert safe_float("abc") is None
    assert safe_float(float('nan')) is None

def test_percentile_ranks():
    assert percentile_ranks([]) == []
    assert percentile_ranks([5.0]) == [50.0]
    assert percentile_ranks([None, None]) == [None, None]
    assert percentile_ranks([10.0, 30.0, 20.0, None]) == [0.0, 100.0, 50.0, None]
    assert percentile_ranks([10.0, 10.0, 20.0]) == [25.0, 25.0, 100.0]

def test_normalize_ymd():
    assert normalize_ymd("2026-04-12") == "2026-04-12"
    assert normalize_ymd("20260412") == "2026-04-12"
    assert normalize_ymd("2026/04/12") == "2026-04-12"
    assert normalize_ymd(None) is None
    assert normalize_ymd("abc") is None
    assert normalize_ymd("2026-04") is None

def test_clamp():
    assert clamp(5.0, 0.0, 10.0) == 5.0
    assert clamp(-5.0, 0.0, 10.0) == 0.0
    assert clamp(15.0, 0.0, 10.0) == 10.0

def test_clamp_score():
    assert clamp_score(50.123, 0.0, 100.0) == 50.12
    assert clamp_score(None, 20.0, 100.0) == 20.0
    assert clamp_score(150.0, 0.0, 100.0) == 100.0

def test_parse_any_date():
    from datetime import datetime
    assert parse_any_date("2025-01-15") == datetime(2025, 1, 15)
    assert parse_any_date("20250115") == datetime(2025, 1, 15)
    assert parse_any_date(None) is None
    assert parse_any_date("") is None
    assert parse_any_date("abc") is None
    assert parse_any_date("  2025-01-15  ") == datetime(2025, 1, 15)


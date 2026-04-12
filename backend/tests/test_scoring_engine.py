import pytest
from services.scoring import _report_recency_grade, _premium_grade, _days_since, INST_SCORE_DEFAULTS

def test_report_recency_grade():
    assert _report_recency_grade(None) == 5
    assert _report_recency_grade(15) == 2   # 0-30 days
    assert _report_recency_grade(40) == 4   # 31-45 days
    assert _report_recency_grade(50) == 1   # 46-60 days (best)
    assert _report_recency_grade(75) == 3   # 61-90 days
    assert _report_recency_grade(100) == 4  # 91-120 days
    assert _report_recency_grade(150) == 5  # >120 days

def test_premium_grade():
    assert _premium_grade(None) == 5
    assert _premium_grade(-5.0) == 1
    assert _premium_grade(0) == 1
    assert _premium_grade(3.0) == 2
    assert _premium_grade(8.0) == 3
    assert _premium_grade(15.0) == 4
    assert _premium_grade(25.0) == 5

def test_inst_score_defaults_exist():
    # Verify the schema exists for the algorithm
    assert "win_rate_30d_weight" in INST_SCORE_DEFAULTS
    assert "gain_60d_weight" in INST_SCORE_DEFAULTS

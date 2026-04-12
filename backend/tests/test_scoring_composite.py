"""测试 scoring.py 中提取的 composite / pool / discovery 纯函数。"""
import pytest
from services.scoring import (
    compute_composite_priority,
    apply_composite_ceiling,
    assign_priority_pool,
    _score_discovery,
)


class TestCompositeComputation:
    def test_basic_weights(self):
        raw, final = compute_composite_priority(100, 100, 100, 100)
        assert raw == 100.0
        assert final == 100.0

    def test_weights_sum_to_expected(self):
        raw, final = compute_composite_priority(50, 50, 50, 50)
        assert raw == 50.0
        assert final == 50.0

    def test_attention_boost(self):
        raw, final = compute_composite_priority(70, 70, 70, 70, attention_boost=10)
        assert final == raw + 10

    def test_crowding_penalty(self):
        raw, final = compute_composite_priority(70, 70, 70, 70, crowding_penalty=5)
        assert final == raw - 5

    def test_clamped_to_0_100(self):
        raw, final = compute_composite_priority(0, 0, 0, 0, crowding_penalty=50)
        assert final == 0.0
        raw2, final2 = compute_composite_priority(100, 100, 100, 100, attention_boost=50)
        assert final2 == 100.0


class TestCompositeCeiling:
    def test_no_cap_when_healthy(self):
        score, cap, reason = apply_composite_ceiling(80, 60, 70, "高质量稳健型", 0)
        assert cap is None
        assert reason is None
        assert score == 80

    def test_stage_below_40_caps_69(self):
        score, cap, reason = apply_composite_ceiling(85, 35, 70, "高质量稳健型", 0)
        assert score == 69.0
        assert cap == 69.0
        assert "阶段分低于40" in reason

    def test_quality_below_45_caps_64(self):
        score, cap, reason = apply_composite_ceiling(85, 60, 40, "高质量稳健型", 0)
        assert score == 64.0

    def test_quality_below_45_no_cap_for_cycle(self):
        score, cap, reason = apply_composite_ceiling(85, 60, 40, "周期/事件驱动型", 0)
        assert cap is None

    def test_crowding_caps_69(self):
        score, cap, reason = apply_composite_ceiling(85, 60, 70, "高质量稳健型", 9)
        assert score == 69.0

    def test_moderate_crowding_caps_74_when_stage_low(self):
        score, cap, reason = apply_composite_ceiling(85, 55, 70, "高质量稳健型", 6.5)
        assert score == 74.0


class TestPoolAssignment:
    def test_d_pool_low_stage(self):
        pool, reason = assign_priority_pool(80, 80, 60, 70, 35, None, 0, False, False)
        assert pool == "D池"

    def test_d_pool_low_composite(self):
        pool, reason = assign_priority_pool(40, 40, 60, 70, 60, None, 0, False, False)
        assert pool == "D池"

    def test_a_pool_all_gates_pass(self):
        pool, reason = assign_priority_pool(80, 80, 60, 70, 55, None, 0, False, False)
        assert pool == "A池"

    def test_b_pool_composite_60_to_74(self):
        pool, reason = assign_priority_pool(65, 65, 60, 70, 55, None, 0, False, False)
        assert pool == "B池"

    def test_c_pool(self):
        pool, reason = assign_priority_pool(50, 50, 60, 70, 55, None, 0, False, False)
        assert pool == "C池"

    def test_a_pool_blocked_by_low_discovery(self):
        pool, reason = assign_priority_pool(80, 80, 40, 70, 55, None, 0, False, False)
        assert pool == "B池"
        assert "发现分不足" in reason


class TestDiscoveryScore:
    def _make_holder(self, **kw):
        base = {
            "institution_id": "inst_1",
            "event_type": "new_entry",
            "hold_ratio": 5.0,
            "hold_market_cap": 1e8,
            "holder_rank": "3",
            "change_pct": 15.0,
        }
        base.update(kw)
        return base

    def test_strong_discovery(self):
        holders = [self._make_holder()]
        profiles = {"inst_1": {"buy_event_count": 25, "buy_win_rate_30d": 65, "buy_avg_gain_30d": 18}}
        score, skill, fresh, strength = _score_discovery(
            holders, "inst_1", profiles, {}, {}, 10,
            "2026-04-01", None,
            (1.0, 3.0, 5.0), (5e7, 1e8, 2e8),
        )
        assert score > 0
        assert skill > 0

    def test_no_leader(self):
        holders = [self._make_holder(institution_id="inst_x")]
        score, skill, fresh, strength = _score_discovery(
            holders, None, {}, {}, {}, 200,
            None, None,
            (None, None, None), (None, None, None),
        )
        assert score >= 0
        assert skill <= 24  # ref_sample < 5 cap

    def test_old_notice_low_fresh(self):
        holders = [self._make_holder()]
        profiles = {"inst_1": {"buy_event_count": 10, "buy_win_rate_30d": 55, "buy_avg_gain_30d": 10}}
        score_new, _, fresh_new, _ = _score_discovery(
            holders, "inst_1", profiles, {}, {}, 10,
            "2026-04-01", None,
            (None, None, None), (None, None, None),
        )
        score_old, _, fresh_old, _ = _score_discovery(
            holders, "inst_1", profiles, {}, {}, 90,
            "2026-01-01", None,
            (None, None, None), (None, None, None),
        )
        assert fresh_new > fresh_old

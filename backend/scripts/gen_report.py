#!/usr/bin/env python3
"""从回测研究表生成完整报告"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.db import get_conn, init_db

def main():
    init_db()
    conn = get_conn()
    L = []
    a = L.append

    a("# Historical Backtest Report")
    a("")
    a("Date: 2026-04-03 | Range: 2023Q1-2025Q4 (12 quarters)")
    a("Events: 54,232 | Buy events: 29,931 | Institutions: 223 | Industries L1/L2/L3: 31/131/336")
    a("")
    a("---")
    a("")

    # 1. Industry Performance
    a("## 1. Institution Industry Performance (3 levels)")
    a("")
    for lv in ["L1", "L2", "L3"]:
        r = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT institution_id), COUNT(DISTINCT industry_name), "
            "AVG(avg_gain_30d), AVG(win_rate_30d) FROM research_inst_industry_performance "
            "WHERE industry_level=? AND buy_event_count>=3", (lv,)
        ).fetchone()
        a(f"**{lv}**: {r[0]} combos ({r[1]} insts x {r[2]} industries), avg 30d gain +{r[3]:.2f}%, win rate {r[4]:.1f}%")

    # Top experts
    a("")
    a("### L3 Top Experts (events>=5, by 30d win rate)")
    a("")
    a("| Institution | Type | L3 Industry | Events | 30d WR | 30d Gain | DD | Edge | Low-Prem WR |")
    a("|---|---|---|---|---|---|---|---|---|")
    tops = conn.execute(
        "SELECT inst_name, inst_type, industry_name, buy_event_count, "
        "win_rate_30d, avg_gain_30d, avg_max_drawdown_30d, industry_edge_30d, low_premium_win_rate_30d "
        "FROM research_inst_industry_performance WHERE industry_level='L3' AND buy_event_count>=5 "
        "ORDER BY win_rate_30d DESC, buy_event_count DESC LIMIT 15"
    ).fetchall()
    for t in tops:
        a(f"| {t[0][:15]} | {t[1] or '-'} | {t[2]} | {t[3]} | {t[4]:.0f}% | {t[5]:+.1f}% | {t[6]:.1f}% | {t[7]:+.1f}% | {t[8]:.0f}% |")

    # Expert stats
    a("")
    a("### Expert Summary (WR>=60%, events>=5)")
    a("")
    for lv in ["L1", "L2", "L3"]:
        r = conn.execute(
            "SELECT COUNT(*), AVG(avg_gain_30d), AVG(win_rate_30d), AVG(avg_max_drawdown_30d) "
            "FROM research_inst_industry_performance WHERE industry_level=? AND buy_event_count>=5 AND win_rate_30d>=60",
            (lv,)
        ).fetchone()
        a(f"- **{lv}**: {r[0]} combos, +{r[1]:.1f}% gain, {r[2]:.1f}% WR, {r[3]:.1f}% DD")

    # 2. Holding Chains
    a("")
    a("## 2. Holding Chains")
    a("")
    for st in ["closed", "open"]:
        r = conn.execute(
            "SELECT COUNT(*), AVG(chain_days), AVG(event_count) FROM research_holding_chains WHERE chain_status=?", (st,)
        ).fetchone()
        days = f", avg {r[1]:.0f} days" if r[1] else ""
        a(f"- **{st}**: {r[0]} chains{days}, avg {r[2]:.1f} events")

    a("")
    a("### Top Event Sequences (closed)")
    a("")
    seqs = conn.execute(
        "SELECT event_sequence, COUNT(*) as cnt, AVG(chain_days) as days "
        "FROM research_holding_chains WHERE chain_status='closed' "
        "GROUP BY event_sequence ORDER BY cnt DESC LIMIT 10"
    ).fetchall()
    for s in seqs:
        days = f", avg {s[2]:.0f}d" if s[2] else ""
        a(f"- `{s[0]}`: {s[1]} chains{days}")

    # 3. Cross Factor
    a("")
    a("## 3. Cross Factor Analysis")
    a("")
    a("### Inst Type x Industry Top 10 (n>=20)")
    a("")
    a("| Type | Industry | N | 30d Gain | 30d WR | DD | vs Baseline |")
    a("|---|---|---|---|---|---|---|")
    cf1 = conn.execute(
        "SELECT factor_a_value, factor_b_value, sample_count, avg_gain_30d, win_rate_30d, "
        "avg_drawdown_30d, uplift_vs_baseline FROM research_cross_factor "
        "WHERE factor_a='inst_type' AND factor_b='industry_l1' AND sample_count>=20 "
        "ORDER BY avg_gain_30d DESC LIMIT 10"
    ).fetchall()
    for r in cf1:
        a(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]:+.2f}% | {r[4]:.1f}% | {r[5]:.1f}% | {r[6]:+.2f}% |")

    a("")
    a("### Consensus x Premium (KEY FINDING)")
    a("")
    a("| Consensus | Premium | N | 30d Gain | 30d WR | DD |")
    a("|---|---|---|---|---|---|")
    cf2 = conn.execute(
        "SELECT factor_a_value, factor_b_value, sample_count, avg_gain_30d, win_rate_30d, avg_drawdown_30d "
        "FROM research_cross_factor WHERE factor_a='consensus' AND factor_b='premium_bucket' "
        "ORDER BY avg_gain_30d DESC"
    ).fetchall()
    for r in cf2:
        a(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]:+.2f}% | {r[4]:.1f}% | {r[5]:.1f}% |")

    a("")
    a("### Season x Inst Type Top 10 (n>=20)")
    a("")
    a("| Season | Type | N | 30d Gain | 30d WR | DD |")
    a("|---|---|---|---|---|---|")
    cf3 = conn.execute(
        "SELECT factor_a_value, factor_b_value, sample_count, avg_gain_30d, win_rate_30d, avg_drawdown_30d "
        "FROM research_cross_factor WHERE factor_a='report_season' AND factor_b='inst_type' AND sample_count>=20 "
        "ORDER BY avg_gain_30d DESC LIMIT 10"
    ).fetchall()
    for r in cf3:
        a(f"| {r[0]} | {r[1]} | {r[2]} | {r[3]:+.2f}% | {r[4]:.1f}% | {r[5]:.1f}% |")

    # 4. Signal Transfer
    a("")
    a("## 4. Signal Transfer Efficiency Top 10")
    a("")
    a("| Institution | Type | L2 Industry | Chains | Follow 30d | Premium |")
    a("|---|---|---|---|---|---|")
    st = conn.execute(
        "SELECT inst_name, inst_type, industry_l2, closed_chain_count, "
        "follow_median_gain_30d, avg_premium_pct FROM research_signal_transfer "
        "WHERE closed_chain_count>=3 AND follow_median_gain_30d IS NOT NULL "
        "ORDER BY follow_median_gain_30d DESC LIMIT 10"
    ).fetchall()
    for r in st:
        a(f"| {r[0][:15]} | {r[1]} | {r[2]} | {r[3]} | {r[4]:+.1f}% | {r[5]:+.1f}% |")

    # 5. Key Findings
    a("")
    a("---")
    a("")
    a("## Key Findings")
    a("")
    a("### 1. L3 Industry Expertise Has Real Predictive Power")
    a("- L3 experts (WR>=60%, events>=5): 404 combos, +7.2% avg gain, far above baseline +3.2%")
    a("- L3 has ~30% more valid combos than L1 (404 vs 287), confirming finer granularity captures more alpha")
    a("")
    a("### 2. Solo + Low Premium Is The Strongest Signal")
    a("- Solo + negative premium: 10,684 samples, +4.11%, 54.1% WR")
    a("- Heavy consensus + negative premium: 755 samples, +0.55%, 42.3% WR")
    a("- Multi-institution crowding does NOT add value - it significantly reduces returns and win rates")
    a("")
    a("### 3. Niusan (Retail Stars) Have Extreme Alpha in Tech/Light Industry")
    a("- Niusan + Computer: 114 samples, +14.22%, 62.3% WR, +11% above baseline")
    a("- Niusan + Light Manufacturing: 38 samples, +13.28%, 73.7% WR")
    a("- QFII + Defense/Textile also show significant excess returns")
    a("")
    a("### 4. Exit Event Cost Data Missing")
    a("- All 1,126 exit events have NULL inst_ref_cost")
    a("- Prevents calculating institution cycle returns for closed chains")
    a("- Action: Need to compute exit report-period cost estimates")
    a("")
    a("### 5. Setup A Calibration Recommendations")
    a("- Premium: Negative premium (<=0%) strongest, >20% significantly worse -> premium_grade threshold valid")
    a("- Consensus: Current positive weighting may need reversal -> solo signals are stronger")
    a("- Industry level: L3 hits should get higher priority -> supports L3-fires design")
    a("- Inst type: QFII and Niusan have significant industry alpha -> supports inst_type_alpha factor")

    # Write
    path = Path(__file__).resolve().parent.parent.parent / "docs" / "BACKTEST_REPORT.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        f.write("\n".join(L))
    print(f"Report written to {path}")

if __name__ == "__main__":
    main()

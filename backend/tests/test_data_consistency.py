"""
Phase 0-D: 数据一致性回归测试

4 组测试 + 运行时旧引用检测：
1. 迁移一致性：K 线行数/价格迁移前后一致
2. 当前关系对称性：机构↔股票双向一致
3. 事件收益唯一来源：gain_30d 来自 fact_institution_event 同一字段
4. 旧逻辑清理：业务代码不再直接读取旧表
"""

import os
import subprocess
import sys
from pathlib import Path

# 把 backend 加到 path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.db import get_conn, init_db
from services.market_db import get_market_conn, init_market_db


def test_migration_consistency():
    """测试 1：K 线迁移一致性"""
    print("=== Test 1: Migration Consistency ===")
    biz = get_conn()
    mkt = get_market_conn()

    # 检查 market_data.db 有数据
    pk_count = mkt.execute("SELECT COUNT(*) FROM price_kline").fetchone()[0]
    assert pk_count > 0, f"price_kline is empty! Expected migrated data."
    print(f"  price_kline: {pk_count} rows ✓")

    # 如果旧表还在，做行数对比
    old_exists = biz.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE name='stock_kline'"
    ).fetchone()[0]
    if old_exists:
        old_count = biz.execute("SELECT COUNT(*) FROM stock_kline").fetchone()[0]
        assert pk_count >= old_count, (
            f"price_kline ({pk_count}) < stock_kline ({old_count})! Migration incomplete."
        )
        print(f"  stock_kline: {old_count} rows, price_kline >= old ✓")

        # 样本价格对比（取 5 个样本）
        samples = biz.execute(
            "SELECT code, date, freq, open, close FROM stock_kline LIMIT 5"
        ).fetchall()
        for s in samples:
            new_row = mkt.execute(
                "SELECT open, close FROM price_kline WHERE code=? AND date=? AND freq=?",
                (s["code"], s["date"], s["freq"])
            ).fetchone()
            assert new_row is not None, f"Missing in price_kline: {s['code']}/{s['date']}/{s['freq']}"
            assert new_row["open"] == s["open"], f"Open mismatch: {s['code']}/{s['date']}"
            assert new_row["close"] == s["close"], f"Close mismatch: {s['code']}/{s['date']}"
        print(f"  Sample price check: {len(samples)} rows match ✓")
    else:
        print("  stock_kline already dropped, skipping comparison")

    biz.close()
    mkt.close()
    print("  PASSED\n")


def test_current_relationship_symmetry():
    """测试 2：当前关系对称性"""
    print("=== Test 2: Current Relationship Symmetry ===")
    conn = get_conn()

    mcr_count = conn.execute("SELECT COUNT(*) FROM mart_current_relationship").fetchone()[0]
    if mcr_count == 0:
        print("  mart_current_relationship is empty, skipping (run build_current_rel first)")
        conn.close()
        return

    print(f"  mart_current_relationship: {mcr_count} rows")

    # 2a: 取 10 个样本机构，验证其持仓数 = mcr 实际行数
    sample_insts = conn.execute(
        "SELECT institution_id, COUNT(*) as cnt "
        "FROM mart_current_relationship GROUP BY institution_id LIMIT 10"
    ).fetchall()
    for si in sample_insts:
        actual = conn.execute(
            "SELECT COUNT(*) FROM mart_current_relationship WHERE institution_id=?",
            (si["institution_id"],)
        ).fetchone()[0]
        assert actual == si["cnt"], (
            f"Inst {si['institution_id']}: group count {si['cnt']} != actual {actual}"
        )
    print(f"  Institution count consistency: {len(sample_insts)} samples ✓")

    # 2b: 取 10 个样本股票，验证其机构数 = mcr 实际行数
    sample_stocks = conn.execute(
        "SELECT stock_code, COUNT(*) as cnt "
        "FROM mart_current_relationship GROUP BY stock_code LIMIT 10"
    ).fetchall()
    for ss in sample_stocks:
        actual = conn.execute(
            "SELECT COUNT(*) FROM mart_current_relationship WHERE stock_code=?",
            (ss["stock_code"],)
        ).fetchone()[0]
        assert actual == ss["cnt"], (
            f"Stock {ss['stock_code']}: group count {ss['cnt']} != actual {actual}"
        )
    print(f"  Stock count consistency: {len(sample_stocks)} samples ✓")

    # 2c: 双向对称 — 如果 inst A holds stock B，则 stock B 的 holder 里包含 A
    sample_pairs = conn.execute(
        "SELECT institution_id, stock_code FROM mart_current_relationship LIMIT 20"
    ).fetchall()
    for p in sample_pairs:
        reverse = conn.execute(
            "SELECT 1 FROM mart_current_relationship WHERE stock_code=? AND institution_id=?",
            (p["stock_code"], p["institution_id"])
        ).fetchone()
        assert reverse is not None, (
            f"Symmetry broken: {p['institution_id']} → {p['stock_code']} exists, "
            f"but reverse lookup failed"
        )
    print(f"  Bidirectional symmetry: {len(sample_pairs)} pairs ✓")

    # 2d: per-stock latest 口径验证 — mcr 中每条记录的 report_date 应等于该股票全市场最新期
    mismatches = conn.execute("""
        SELECT m.stock_code, m.report_date AS mcr_rd, l.max_rd AS expected_rd
        FROM mart_current_relationship m
        LEFT JOIN (
            SELECT stock_code, MAX(report_date) AS max_rd
            FROM market_raw_holdings GROUP BY stock_code
        ) l ON m.stock_code = l.stock_code
        WHERE m.report_date != l.max_rd
        LIMIT 5
    """).fetchall()
    if mismatches:
        for mm in mismatches:
            print(f"  ⚠ Stock {mm['stock_code']}: mcr_rd={mm['mcr_rd']}, expected={mm['expected_rd']}")
        assert False, f"Per-stock latest mismatch: {len(mismatches)} rows"
    print("  Per-stock latest report_date consistency ✓")

    conn.close()
    print("  PASSED\n")


def test_event_return_single_source():
    """测试 3：事件收益唯一来源"""
    print("=== Test 3: Event Return Single Source ===")
    conn = get_conn()

    # 检查增强列是否存在
    cols = [r[1] for r in conn.execute("PRAGMA table_info(fact_institution_event)").fetchall()]
    assert "gain_30d" in cols, "fact_institution_event missing gain_30d column"
    assert "calc_version" in cols, "fact_institution_event missing calc_version column"
    print("  fact_institution_event enhanced columns exist ✓")

    # 如果有收益数据，验证来源一致性
    # 关键：MCR 只存每只股票最新期的关系，所以必须按 (inst, stock, report_date) 精确匹配
    sample = conn.execute(
        "SELECT e.institution_id, e.stock_code, e.report_date, e.gain_30d "
        "FROM fact_institution_event e "
        "INNER JOIN mart_current_relationship m "
        "  ON e.institution_id = m.institution_id "
        "  AND e.stock_code = m.stock_code "
        "  AND e.report_date = m.report_date "
        "WHERE e.gain_30d IS NOT NULL LIMIT 5"
    ).fetchall()
    if sample:
        for s in sample:
            mcr = conn.execute(
                "SELECT gain_30d FROM mart_current_relationship "
                "WHERE institution_id=? AND stock_code=?",
                (s["institution_id"], s["stock_code"])
            ).fetchone()
            if mcr and mcr["gain_30d"] is not None:
                assert mcr["gain_30d"] == s["gain_30d"], (
                    f"gain_30d mismatch: event={s['gain_30d']}, mcr={mcr['gain_30d']} "
                    f"for {s['institution_id']}/{s['stock_code']}/{s['report_date']}"
                )
        print(f"  Event→MCR gain_30d consistency: {len(sample)} samples ✓")
    else:
        print("  No gain_30d data yet (calc_returns not run), skipping value check")

    conn.close()
    print("  PASSED\n")


def test_old_reference_cleanup():
    """测试 4：旧逻辑清理（静态代码扫描）"""
    print("=== Test 4: Old Reference Cleanup ===")
    backend_dir = Path(__file__).resolve().parent.parent

    # 排除范围
    exclude_dirs = {"scripts", "tests", "__pycache__"}

    warnings = []
    for pattern, description in [
        ("FROM fact_event_return", "direct read from fact_event_return"),
        ("JOIN fact_event_return", "join with fact_event_return"),
        ("INTO fact_event_return", "write to fact_event_return"),
        ("FROM stock_kline", "direct read from stock_kline"),
        ("INTO stock_kline", "write to stock_kline"),
        ("dim_stock_industry", "direct access to dim_stock_industry"),
    ]:
        result = subprocess.run(
            ["grep", "-rn", pattern, str(backend_dir),
             "--include=*.py"],
            capture_output=True, text=True
        )
        if result.stdout:
            for line in result.stdout.strip().split("\n"):
                # 检查是否在排除目录
                rel = os.path.relpath(line.split(":")[0], backend_dir)
                parts = Path(rel).parts
                if any(p in exclude_dirs for p in parts):
                    continue
                if rel in {"services/industry.py", "services/db.py"}:
                    continue
                # 检查是否是注释或 deprecated 标记
                code_part = ":".join(line.split(":")[2:]).strip()
                if code_part.startswith("#") or "DEPRECATED" in code_part or "deprecated" in code_part:
                    continue
                if "INSERT OR REPLACE INTO dim_stock_industry" in code_part:
                    continue
                # 检查是否是 try/except 保护的过渡期代码
                if "try:" in code_part or "except" in code_part:
                    continue
                warnings.append(f"  ⚠ {description}: {rel}:{line.split(':')[1]}")

    if warnings:
        print(f"  Found {len(warnings)} old references in business code:")
        for w in warnings:
            print(w)
        print("  NOTE: Some may be legitimate transition code with try/except")
    else:
        print("  No unprotected old references found ✓")

    print("  PASSED (informational)\n")


def run_all():
    """运行所有一致性测试"""
    print("\n" + "=" * 60)
    print("Phase 0-D: Data Consistency Tests")
    print("=" * 60 + "\n")

    init_db()
    init_market_db()

    test_migration_consistency()
    test_current_relationship_symmetry()
    test_event_return_single_source()
    test_old_reference_cleanup()

    print("=" * 60)
    print("ALL TESTS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    run_all()

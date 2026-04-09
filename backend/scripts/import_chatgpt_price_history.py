#!/usr/bin/env python3
"""
import_chatgpt_price_history.py — 从 chatGPT 项目导入日 K 线数据到 claude 的 market_data.db

策略：missing-only（只补 claude 缺的，不覆盖已有的）
同时从导入的日 K 派生月 K。
"""

import json
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT / "backend"))

from services.market_db import (
    get_market_conn, init_market_db,
    upsert_price_rows, update_sync_state,
    start_import_batch, finish_import_batch,
)
from services.db import get_conn

CHATGPT_DB = _ROOT.parent / "chatGPT" / "data" / "smartmoney_research.db"


def get_missing_codes():
    """找出 claude tracked 但缺日 K 的股票"""
    biz = get_conn()
    mkt = get_market_conn()

    tracked = set(r[0] for r in biz.execute(
        "SELECT DISTINCT stock_code FROM inst_holdings"
    ).fetchall())

    has_daily = set(r[0] for r in mkt.execute(
        "SELECT DISTINCT code FROM price_kline WHERE freq='daily'"
    ).fetchall())

    biz.close()
    mkt.close()
    return tracked - has_daily


def import_daily(missing_codes):
    """从 chatGPT 导入日 K"""
    if not CHATGPT_DB.exists():
        print(f"  chatGPT 数据库不存在: {CHATGPT_DB}")
        return 0

    src = sqlite3.connect(str(CHATGPT_DB))
    src.row_factory = sqlite3.Row
    mkt = get_market_conn()

    batch_id = start_import_batch(
        mkt, source_type="chatgpt_import",
        source_name=str(CHATGPT_DB.name),
        freq="daily", adjust="qfq"
    )

    total = 0
    imported_codes = set()
    source_stats = {}

    for code in sorted(missing_codes):
        rows = src.execute(
            "SELECT stock_code, trade_date, open, high, low, close, volume, amount, source "
            "FROM fact_price_daily WHERE stock_code = ?",
            (code,)
        ).fetchall()
        if not rows:
            continue

        kline_rows = []
        for r in rows:
            td = r["trade_date"]
            # 转换日期格式 YYYYMMDD → YYYY-MM-DD
            if len(td) == 8:
                date_str = f"{td[:4]}-{td[4:6]}-{td[6:8]}"
            else:
                date_str = td
            kline_rows.append({
                "code": r["stock_code"], "date": date_str,
                "freq": "daily", "adjust": "qfq",
                "open": r["open"], "high": r["high"],
                "low": r["low"], "close": r["close"],
                "volume": r["volume"], "amount": r["amount"],
            })
            src_name = r["source"] or "unknown"
            source_stats[src_name] = source_stats.get(src_name, 0) + 1

        n = upsert_price_rows(mkt, kline_rows, source="chatgpt_import", batch_id=batch_id)
        total += n
        imported_codes.add(code)

        # 更新 sync_state
        dates = [r["date"] for r in kline_rows]
        update_sync_state(mkt, code, "daily", source="chatgpt_import",
                          min_date=min(dates), max_date=max(dates),
                          row_count=len(kline_rows))

    # 完成批次
    finish_import_batch(
        mkt, batch_id, rows_imported=total,
        min_date=None, max_date=None,
        status="completed",
        detail=json.dumps({
            "codes_imported": len(imported_codes),
            "total_rows": total,
            "source_distribution": source_stats,
        }, ensure_ascii=False),
    )

    src.close()
    mkt.close()
    return len(imported_codes)


def derive_monthly():
    """从已导入的日 K 派生月 K"""
    mkt = get_market_conn()

    # 找所有有日 K 但缺月 K 的股票
    codes_need_monthly = mkt.execute("""
        SELECT DISTINCT d.code
        FROM price_kline d
        LEFT JOIN (SELECT DISTINCT code FROM price_kline WHERE freq='monthly') m
            ON d.code = m.code
        WHERE d.freq = 'daily' AND m.code IS NULL
    """).fetchall()
    codes = [r[0] for r in codes_need_monthly]

    if not codes:
        print("  无需派生月 K（所有股票已有月 K）")
        return 0

    batch_id = start_import_batch(
        mkt, source_type="derived_from_daily",
        source_name="price_kline.daily → monthly",
        freq="monthly", adjust="qfq"
    )

    total = 0
    for code in codes:
        # 按月聚合
        monthly_rows = mkt.execute("""
            SELECT
                code,
                substr(date, 1, 7) || '-01' as month_date,
                -- 月开盘：该月首日 open
                (SELECT open FROM price_kline p2
                 WHERE p2.code = p1.code AND substr(p2.date,1,7) = substr(p1.date,1,7)
                 AND p2.freq='daily' ORDER BY p2.date LIMIT 1) as open,
                MAX(high) as high,
                MIN(low) as low,
                -- 月收盘：该月末日 close
                (SELECT close FROM price_kline p3
                 WHERE p3.code = p1.code AND substr(p3.date,1,7) = substr(p1.date,1,7)
                 AND p3.freq='daily' ORDER BY p3.date DESC LIMIT 1) as close,
                SUM(volume) as volume,
                SUM(amount) as amount
            FROM price_kline p1
            WHERE code = ? AND freq = 'daily'
            GROUP BY code, substr(date, 1, 7)
            ORDER BY month_date
        """, (code,)).fetchall()

        if not monthly_rows:
            continue

        rows = [{
            "code": code, "date": r["month_date"],
            "freq": "monthly", "adjust": "qfq",
            "open": r["open"], "high": r["high"],
            "low": r["low"], "close": r["close"],
            "volume": r["volume"], "amount": r["amount"],
        } for r in monthly_rows]

        upsert_price_rows(mkt, rows, source="derived_from_daily", batch_id=batch_id)
        total += len(rows)

        dates = [r["date"] for r in rows]
        update_sync_state(mkt, code, "monthly", source="derived_from_daily",
                          min_date=min(dates), max_date=max(dates),
                          row_count=len(rows))

    finish_import_batch(
        mkt, batch_id, rows_imported=total,
        status="completed",
        detail=json.dumps({"codes": len(codes), "monthly_rows": total}, ensure_ascii=False),
    )

    mkt.close()
    return len(codes)


def main():
    print("=" * 60)
    print("从 chatGPT 项目导入 K 线数据")
    print("=" * 60)

    init_market_db()

    # 1. 找缺口
    missing = get_missing_codes()
    print(f"\n  claude tracked 股票缺日 K: {len(missing)} 只")

    if not missing:
        print("  无缺口，无需导入")
        return

    # 2. 检查 chatGPT 数据库
    if not CHATGPT_DB.exists():
        print(f"  chatGPT 数据库不存在: {CHATGPT_DB}")
        return

    src = sqlite3.connect(str(CHATGPT_DB))
    chatgpt_codes = set(r[0] for r in src.execute(
        "SELECT DISTINCT stock_code FROM fact_price_daily"
    ).fetchall())
    src.close()

    can_import = missing & chatgpt_codes
    print(f"  chatGPT 有日 K 的股票: {len(chatgpt_codes)} 只")
    print(f"  可导入（缺口 ∩ chatGPT 有）: {len(can_import)} 只")

    if not can_import:
        print("  chatGPT 没有 claude 需要的日 K 数据")
        return

    # 3. 导入日 K
    print(f"\n--- 导入日 K ---")
    imported = import_daily(can_import)
    print(f"  导入完成: {imported} 只股票")

    # 4. 派生月 K
    print(f"\n--- 派生月 K ---")
    derived = derive_monthly()
    print(f"  派生完成: {derived} 只股票")

    # 5. 统计
    mkt = get_market_conn()
    daily_count = mkt.execute("SELECT COUNT(DISTINCT code) FROM price_kline WHERE freq='daily'").fetchone()[0]
    monthly_count = mkt.execute("SELECT COUNT(DISTINCT code) FROM price_kline WHERE freq='monthly'").fetchone()[0]
    total_rows = mkt.execute("SELECT COUNT(*) FROM price_kline").fetchone()[0]
    mkt.close()

    print(f"\n✓ 导入完成")
    print(f"  日 K 覆盖: {daily_count} 只股票")
    print(f"  月 K 覆盖: {monthly_count} 只股票")
    print(f"  总行数: {total_rows}")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
migrate_market_data.py — 将 smartmoney.db.stock_kline 迁移到 market_data.db.price_kline

独立运行：python backend/scripts/migrate_market_data.py
幂等设计：UPSERT 不重复插入，失败后可重跑。
"""

import json
import sqlite3
import sys
from pathlib import Path

# 把项目根目录加到 path
_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT / "backend"))

from services.market_db import (
    get_market_conn, init_market_db,
    upsert_price_rows, update_sync_state,
    start_import_batch, finish_import_batch,
)
from services.db import get_conn


def migrate():
    print("=" * 60)
    print("K 线迁移：smartmoney.db.stock_kline → market_data.db.price_kline")
    print("=" * 60)

    # 确保行情库已建表
    init_market_db()

    biz_conn = get_conn()
    mkt_conn = get_market_conn()

    # 检查旧表是否存在
    table_check = biz_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='stock_kline'"
    ).fetchone()
    if not table_check:
        print("⚠ smartmoney.db 中没有 stock_kline 表，无需迁移。")
        return

    # 读取旧数据
    rows = biz_conn.execute(
        "SELECT code, date, freq, open, high, low, close, volume, amount "
        "FROM stock_kline"
    ).fetchall()
    total = len(rows)
    if total == 0:
        print("⚠ stock_kline 表为空，无需迁移。")
        return

    print(f"  读取旧 K 线: {total} 行")

    # 按 freq 分组统计
    daily_rows = []
    monthly_rows = []
    for r in rows:
        entry = {
            "code": r["code"], "date": r["date"],
            "freq": r["freq"], "adjust": "qfq",
            "open": r["open"], "high": r["high"],
            "low": r["low"], "close": r["close"],
            "volume": r["volume"], "amount": r["amount"],
        }
        if r["freq"] == "daily":
            daily_rows.append(entry)
        else:
            monthly_rows.append(entry)

    daily_codes = set(r["code"] for r in daily_rows)
    monthly_codes = set(r["code"] for r in monthly_rows)

    print(f"  日K: {len(daily_rows)} 行, {len(daily_codes)} 只股票")
    print(f"  月K: {len(monthly_rows)} 行, {len(monthly_codes)} 只股票")

    # 开始批次
    batch_id = start_import_batch(
        mkt_conn, source_type="migration_v1",
        source_name="smartmoney.db.stock_kline",
        freq="mixed", adjust="qfq"
    )
    print(f"  批次 ID: {batch_id}")

    # 写入
    all_rows = daily_rows + monthly_rows
    written = 0
    batch_size = 5000
    for i in range(0, len(all_rows), batch_size):
        chunk = all_rows[i:i + batch_size]
        n = upsert_price_rows(mkt_conn, chunk, source="eastmoney_direct",
                               batch_id=batch_id)
        written += n
        print(f"  写入进度: {written}/{total}")

    # 更新 sync_state
    print("  更新 market_sync_state ...")
    sync_rows = mkt_conn.execute(
        "SELECT code, freq, MIN(date) as min_d, MAX(date) as max_d, COUNT(*) as cnt "
        "FROM price_kline WHERE batch_id=? GROUP BY code, freq",
        (batch_id,)
    ).fetchall()
    for sr in sync_rows:
        update_sync_state(
            mkt_conn, sr["code"], sr["freq"],
            source="eastmoney_direct",
            min_date=sr["min_d"], max_date=sr["max_d"],
            row_count=sr["cnt"],
        )

    # 迁移摘要
    daily_dates = mkt_conn.execute(
        "SELECT MIN(date), MAX(date) FROM price_kline "
        "WHERE batch_id=? AND freq='daily'", (batch_id,)
    ).fetchone()
    monthly_dates = mkt_conn.execute(
        "SELECT MIN(date), MAX(date) FROM price_kline "
        "WHERE batch_id=? AND freq='monthly'", (batch_id,)
    ).fetchone()

    summary = {
        "total_rows": written,
        "daily_stocks": len(daily_codes),
        "monthly_stocks": len(monthly_codes),
        "daily_min_date": daily_dates[0] if daily_dates else None,
        "daily_max_date": daily_dates[1] if daily_dates else None,
        "monthly_min_date": monthly_dates[0] if monthly_dates else None,
        "monthly_max_date": monthly_dates[1] if monthly_dates else None,
    }

    # 完成批次
    all_dates = [r["date"] for r in all_rows if r["date"]]
    finish_import_batch(
        mkt_conn, batch_id,
        rows_imported=written,
        min_date=min(all_dates) if all_dates else None,
        max_date=max(all_dates) if all_dates else None,
        status="completed",
        detail=json.dumps(summary, ensure_ascii=False),
    )

    # 标记迁移完成
    biz_conn.execute(
        "INSERT OR REPLACE INTO app_settings (key, value) "
        "VALUES ('market_data.migrated', 'true')"
    )
    biz_conn.commit()

    print()
    print("✓ 迁移完成")
    print(f"  总行数: {written}")
    print(f"  日K: {len(daily_codes)} 只, "
          f"{daily_dates[0]}~{daily_dates[1]}" if daily_dates[0] else "无")
    print(f"  月K: {len(monthly_codes)} 只, "
          f"{monthly_dates[0]}~{monthly_dates[1]}" if monthly_dates[0] else "无")
    print(f"  批次: {batch_id}")
    print(f"  app_settings.market_data.migrated = true")

    biz_conn.close()
    mkt_conn.close()


if __name__ == "__main__":
    migrate()

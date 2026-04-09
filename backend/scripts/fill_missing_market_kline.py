#!/usr/bin/env python3
"""
fill_missing_market_kline.py

补齐 claude 项目当前 tracked 股票缺失的 K 线数据。

策略：
- missing-only：仅处理 market_data.db 中缺日 K 的 tracked 股票
- 日 K：从 2023-01-01 开始拉全历史，优先东财，失败回退新浪 / 腾讯
- 月 K：从日 K 聚合派生
"""

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT / "backend"))

from services.akshare_client import fetch_stock_kline_daily
from services.db import get_conn
from services.market_db import (
    get_market_conn,
    init_market_db,
    start_import_batch,
    finish_import_batch,
    upsert_price_rows,
    update_sync_state,
)


def get_missing_codes() -> list[str]:
    biz = get_conn()
    mkt = get_market_conn()
    try:
        tracked = {
            r[0]
            for r in biz.execute("SELECT DISTINCT stock_code FROM inst_holdings").fetchall()
        }
        has_daily = {
            r[0]
            for r in mkt.execute(
                "SELECT DISTINCT code FROM price_kline WHERE freq='daily'"
            ).fetchall()
        }
        return sorted(tracked - has_daily)
    finally:
        biz.close()
        mkt.close()


def derive_monthly_from_daily(mkt_conn, codes: list[str]) -> tuple[int, int]:
    batch_id = start_import_batch(
        mkt_conn,
        source_type="derived_from_daily",
        source_name="price_kline.daily → monthly (gap fill)",
        freq="monthly",
        adjust="qfq",
    )
    code_count = 0
    row_count = 0

    for code in codes:
        rows = mkt_conn.execute(
            """
            SELECT date, open, high, low, close, volume, amount
            FROM price_kline
            WHERE code=? AND freq='daily' AND adjust='qfq'
            ORDER BY date
            """,
            (code,),
        ).fetchall()
        if not rows:
            continue

        frame = pd.DataFrame([dict(r) for r in rows])
        frame["date"] = pd.to_datetime(frame["date"])
        frame["month"] = frame["date"].dt.to_period("M")
        monthly_rows = []
        for _, group in frame.groupby("month", sort=True):
            group = group.sort_values("date")
            monthly_rows.append(
                {
                    "code": code,
                    "date": group.iloc[0]["date"].strftime("%Y-%m-01"),
                    "freq": "monthly",
                    "adjust": "qfq",
                    "open": group.iloc[0]["open"],
                    "high": group["high"].max(),
                    "low": group["low"].min(),
                    "close": group.iloc[-1]["close"],
                    "volume": group["volume"].sum(min_count=1),
                    "amount": group["amount"].sum(min_count=1),
                }
            )

        if not monthly_rows:
            continue

        upsert_price_rows(mkt_conn, monthly_rows, source="derived_from_daily", batch_id=batch_id)
        dates = [r["date"] for r in monthly_rows]
        update_sync_state(
            mkt_conn,
            code,
            "monthly",
            source="derived_from_daily",
            min_date=min(dates),
            max_date=max(dates),
            row_count=len(monthly_rows),
        )
        code_count += 1
        row_count += len(monthly_rows)

    finish_import_batch(
        mkt_conn,
        batch_id,
        rows_imported=row_count,
        status="completed",
        detail=json.dumps(
            {"codes": code_count, "rows": row_count, "mode": "derived_from_daily"},
            ensure_ascii=False,
        ),
    )
    return code_count, row_count


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--start-date", default="20230101")
    args = parser.parse_args()

    init_market_db()
    missing_codes = get_missing_codes()
    print(f"tracked 缺日K: {len(missing_codes)} 只")
    if not missing_codes:
        print("无缺口，无需补数")
        return 0

    mkt_conn = get_market_conn()
    batch_id = start_import_batch(
        mkt_conn,
        source_type="gap_fill",
        source_name="akshare_multi_source",
        freq="daily",
        adjust="qfq",
    )

    written_rows = 0
    imported_codes = 0
    failed_codes = []
    source_counter = defaultdict(int)
    sem = asyncio.Semaphore(max(1, args.concurrency))

    async def fetch_one(code: str):
        async with sem:
            try:
                df, source = await fetch_stock_kline_daily(
                    code,
                    start_date=args.start_date,
                    end_date=datetime.now().strftime("%Y%m%d"),
                )
                return code, df, source, None
            except Exception as exc:
                return code, None, "", str(exc)

    tasks = [asyncio.create_task(fetch_one(code)) for code in missing_codes]
    total = len(tasks)

    for idx, task in enumerate(asyncio.as_completed(tasks), start=1):
        code, df, source, error = await task
        if df is None or df.empty:
            failed_codes.append({"code": code, "error": error or "empty"})
        else:
            rows = [
                {
                    "code": code,
                    "date": str(r["date"])[:10],
                    "freq": "daily",
                    "adjust": "qfq",
                    "open": r["open"],
                    "high": r["high"],
                    "low": r["low"],
                    "close": r["close"],
                    "volume": r.get("volume"),
                    "amount": r.get("amount"),
                }
                for _, r in df.iterrows()
            ]
            write_source = f"akshare_{source}" if source else "akshare_unknown"
            upsert_price_rows(mkt_conn, rows, source=write_source, batch_id=batch_id)
            dates = [r["date"] for r in rows]
            update_sync_state(
                mkt_conn,
                code,
                "daily",
                source=write_source,
                min_date=min(dates),
                max_date=max(dates),
                row_count=len(rows),
            )
            imported_codes += 1
            written_rows += len(rows)
            source_counter[source or "unknown"] += 1

        if idx % 25 == 0 or idx == total:
            print(
                f"[{idx}/{total}] imported={imported_codes} failed={len(failed_codes)} rows={written_rows}",
                flush=True,
            )

    finish_import_batch(
        mkt_conn,
        batch_id,
        rows_imported=written_rows,
        status="completed" if not failed_codes else "partial",
        error=None if not failed_codes else f"{len(failed_codes)} codes failed",
        detail=json.dumps(
            {
                "imported_codes": imported_codes,
                "failed_codes": failed_codes[:100],
                "source_distribution": dict(source_counter),
                "start_date": args.start_date,
            },
            ensure_ascii=False,
        ),
    )

    missing_monthly = [
        r[0]
        for r in mkt_conn.execute(
            """
            SELECT DISTINCT d.code
            FROM price_kline d
            LEFT JOIN (
                SELECT DISTINCT code FROM price_kline WHERE freq='monthly'
            ) m ON d.code = m.code
            WHERE d.freq='daily' AND m.code IS NULL
            """
        ).fetchall()
    ]
    monthly_codes, monthly_rows = derive_monthly_from_daily(mkt_conn, missing_monthly)
    mkt_conn.close()

    print("\n=== K线补数完成 ===")
    print(f"导入日K股票: {imported_codes}")
    print(f"导入日K行数: {written_rows}")
    print(f"失败股票数: {len(failed_codes)}")
    print(f"派生月K股票: {monthly_codes}")
    print(f"派生月K行数: {monthly_rows}")
    return 0 if not failed_codes else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))

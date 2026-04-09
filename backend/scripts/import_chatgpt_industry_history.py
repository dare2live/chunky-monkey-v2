#!/usr/bin/env python3
"""
import_chatgpt_industry_history.py

从 chatGPT 项目导入缺失的申万行业到 claude.dim_stock_industry。

策略：
- missing-only：只补 claude 当前 tracked 股票里缺行业的股票
- 取 chatGPT.dim_stock_industry_history(source='sw_main') 每只股票最新一条
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT / "backend"))

from services.db import get_conn

CHATGPT_DB = _ROOT.parent / "chatGPT" / "data" / "smartmoney_research.db"


def get_missing_codes(conn) -> list[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT h.stock_code
        FROM inst_holdings h
        LEFT JOIN dim_stock_industry d ON h.stock_code = d.stock_code
        WHERE d.stock_code IS NULL
        ORDER BY h.stock_code
        """
    ).fetchall()
    return [r[0] for r in rows]


def main():
    print("=" * 60)
    print("从 chatGPT 项目导入申万行业数据")
    print("=" * 60)

    if not CHATGPT_DB.exists():
        print(f"chatGPT 数据库不存在: {CHATGPT_DB}")
        return 1

    conn = get_conn()
    src = sqlite3.connect(str(CHATGPT_DB))
    src.row_factory = sqlite3.Row
    now = datetime.now().isoformat()

    try:
        missing_codes = get_missing_codes(conn)
        print(f"\nclaude tracked 股票缺行业: {len(missing_codes)} 只")
        if not missing_codes:
            print("无需导入，行业已完整")
            return 0

        placeholders = ",".join("?" for _ in missing_codes)
        rows = src.execute(
            f"""
            WITH latest AS (
                SELECT stock_code, MAX(effective_date) AS max_effective_date
                FROM dim_stock_industry_history
                WHERE source = 'sw_main' AND stock_code IN ({placeholders})
                GROUP BY stock_code
            )
            SELECT h.stock_code, h.level1, h.level2, h.level3
            FROM dim_stock_industry_history h
            JOIN latest l
              ON h.stock_code = l.stock_code
             AND h.effective_date = l.max_effective_date
            WHERE h.source = 'sw_main'
            """,
            missing_codes,
        ).fetchall()

        print(f"chatGPT 可提供行业: {len(rows)} 只")
        if not rows:
            print("chatGPT 中没有可导入的缺失行业")
            return 0

        written = 0
        for row in rows:
            conn.execute(
                """
                INSERT OR REPLACE INTO dim_stock_industry
                    (stock_code, sw_level1, sw_level2, sw_level3, sw_code, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    row["stock_code"],
                    row["level1"] or "",
                    row["level2"] or "",
                    row["level3"] or "",
                    "",
                    now,
                ),
            )
            written += 1

        conn.commit()
        remaining = len(get_missing_codes(conn))
        print(f"导入完成: {written} 只")
        print(f"剩余缺行业: {remaining} 只")
        return 0
    finally:
        conn.close()
        src.close()


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""
fill_missing_industry_from_sw_hist.py

对 claude 当前 tracked 股票中缺失申万行业的股票，
直接使用 ak.stock_industry_clf_hist_sw() 最新归属进行补齐。
"""

import sqlite3
import sys
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_ROOT / "backend"))

from services.db import get_conn


def build_path_map(tree_df: pd.DataFrame):
    rows = {}
    for _, row in tree_df.iterrows():
        code = str(row.get("类目编码") or "").strip()
        if not code:
            continue
        rows[code] = {
            "name": str(row.get("类目名称") or "").strip(),
            "parent": str(row.get("父类编码") or "").strip(),
            "level": int(row.get("分级") or 0),
        }
    path_map = {}
    for code in rows:
        current = code
        path = []
        seen = set()
        while current and current in rows and current not in seen:
            seen.add(current)
            item = rows[current]
            if item["level"] > 0 and item["name"]:
                path.append(item["name"])
            current = item["parent"]
        if path:
            path_map[code] = list(reversed(path))
            if code.startswith("S"):
                path_map[code[1:]] = path_map[code]
    return path_map


def main():
    conn = get_conn()
    now = datetime.now().isoformat()
    try:
        missing_codes = [
            r[0]
            for r in conn.execute(
                """
                SELECT DISTINCT h.stock_code
                FROM inst_holdings h
                LEFT JOIN dim_stock_industry d ON h.stock_code = d.stock_code
                WHERE d.stock_code IS NULL
                ORDER BY h.stock_code
                """
            ).fetchall()
        ]
        print(f"tracked 缺行业: {len(missing_codes)} 只")
        if not missing_codes:
            print("无需补齐")
            return 0

        tree = ak.stock_industry_category_cninfo(symbol="申银万国行业分类标准")
        hist = ak.stock_industry_clf_hist_sw()
        path_map = build_path_map(tree)

        hist = hist.copy()
        hist["symbol6"] = hist["symbol"].astype(str).str.zfill(6)
        hist["start_date"] = hist["start_date"].astype(str)
        hist["update_time"] = hist["update_time"].astype(str)
        hist = hist.sort_values(["symbol6", "start_date", "update_time"])
        latest = hist.groupby("symbol6", as_index=False).tail(1)
        latest_map = {str(r["symbol6"]).strip(): r for _, r in latest.iterrows()}

        written = 0
        unresolved = []
        for code in missing_codes:
            row = latest_map.get(code)
            if row is None:
                unresolved.append(code)
                continue
            industry_code = str(row["industry_code"]).strip()
            path = path_map.get(industry_code) or path_map.get(f"S{industry_code}") or []
            if len(path) < 3:
                unresolved.append(code)
                continue
            l1, l2, l3 = path[0], path[1], path[2]
            conn.execute(
                """
                INSERT OR REPLACE INTO dim_stock_industry
                    (stock_code, sw_level1, sw_level2, sw_level3, sw_code, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (code, l1, l2, l3, industry_code, now),
            )
            written += 1

        conn.commit()
        remaining = conn.execute(
            """
            SELECT COUNT(DISTINCT h.stock_code)
            FROM inst_holdings h
            LEFT JOIN dim_stock_industry d ON h.stock_code = d.stock_code
            WHERE d.stock_code IS NULL
            """
        ).fetchone()[0]
        print(f"补齐成功: {written} 只")
        print(f"剩余缺行业: {remaining} 只")
        print(f"仍无法解析: {unresolved}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

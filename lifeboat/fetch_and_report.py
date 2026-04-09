#!/usr/bin/env python3
"""
救生艇脚本 — Chunky Monkey 核心功能独立备份

功能：查找跟踪机构最新新进了哪些股票
依赖：pip install akshare pandas
运行：python fetch_and_report.py
输出：report.html（浏览器直接打开）

完全独立，不依赖主项目任何代码、数据库或配置。
"""

import json
import os
import sys
from datetime import datetime

import akshare as ak
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INST_FILE = os.path.join(SCRIPT_DIR, "institutions.json")
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "report.html")

# 最近可能的报告期（按时间倒序尝试）
def _candidate_report_dates():
    now = datetime.now()
    dates = []
    for year in range(now.year, now.year - 2, -1):
        for mmdd in ["1231", "0930", "0630", "0331"]:
            d = f"{year}{mmdd}"
            if d <= now.strftime("%Y%m%d"):
                dates.append(d)
    return dates


def load_institutions():
    if not os.path.exists(INST_FILE):
        print(f"错误：找不到机构列表文件 {INST_FILE}")
        print("请创建 institutions.json，内容为机构名称的 JSON 数组。")
        sys.exit(1)
    with open(INST_FILE, "r", encoding="utf-8") as f:
        names = json.load(f)
    print(f"已加载 {len(names)} 个跟踪机构")
    return set(names)


def fetch_latest_holdings():
    """用 AKShare 获取最近几个报告期的数据，按每只股票取最新报告期。

    公司随时可能公告，不同股票的最新报告期可能不同。
    拉最近 3 个报告期合并后，按股票去重保留最新一条。
    """
    candidates = _candidate_report_dates()
    all_frames = []
    fetched_dates = []

    # 拉最近 3 个有数据的报告期
    for date_str in candidates:
        if len(fetched_dates) >= 3:
            break
        fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        print(f"拉取报告期 {fmt} ...")
        try:
            df = ak.stock_gdfx_free_holding_analyse_em(date=date_str)
            if df is not None and not df.empty:
                print(f"  获取 {len(df)} 条记录")
                df["_report_period"] = date_str
                all_frames.append(df)
                fetched_dates.append(fmt)
            else:
                print(f"  无数据")
        except Exception as e:
            print(f"  失败: {e}")

    if not all_frames:
        print("所有报告期均无数据，请检查网络连接。")
        sys.exit(1)

    # 合并所有报告期，按每只股票取最新报告期的记录
    combined = pd.concat(all_frames, ignore_index=True)
    combined["_report_period"] = combined["_report_period"].astype(str)

    stock_col = "股票代码"
    period_col = "_report_period"
    latest_per_stock = combined.groupby(stock_col)[period_col].max().reset_index()
    latest_per_stock.columns = [stock_col, "_latest_period"]
    merged = combined.merge(latest_per_stock, on=stock_col)
    result = merged[merged[period_col] == merged["_latest_period"]].drop(
        columns=["_latest_period"]
    )

    report_label = "、".join(fetched_dates)
    print(f"\n合并 {len(fetched_dates)} 个报告期，去重后 {len(result)} 条（每只股票取最新）")
    return result, report_label


def find_new_entries(df, institutions):
    """筛选跟踪机构的新进持股"""
    # 字段名参考：股东名称, 股票代码, 股票简称, 期末持股-持股变动, 期末持股-数量, ...
    results = []
    for _, row in df.iterrows():
        name = str(row.get("股东名称", "")).strip()
        if name not in institutions:
            continue
        change = str(row.get("期末持股-持股变动", "")).strip()
        if change != "新进":
            continue
        results.append({
            "stock_code": str(row.get("股票代码", "")).strip(),
            "stock_name": str(row.get("股票简称", "")).strip(),
            "holder_name": name,
            "holder_type": str(row.get("股东类型", "")).strip(),
            "hold_amount": row.get("期末持股-数量"),
            "hold_ratio": row.get("期末持股-流通市值"),
            "report_date": str(row.get("报告期", "")).strip(),
        })
    return results


def generate_html(results, report_date):
    """生成简洁的 HTML 报告"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    rows_html = ""
    if results:
        for i, r in enumerate(results, 1):
            amount = r["hold_amount"]
            if amount is not None:
                try:
                    amount = f"{float(amount):,.0f}"
                except (ValueError, TypeError):
                    amount = str(amount)
            else:
                amount = "-"
            rows_html += f"""
            <tr>
                <td>{i}</td>
                <td>{r['stock_code']}</td>
                <td>{r['stock_name']}</td>
                <td>{r['holder_name']}</td>
                <td>{r['holder_type']}</td>
                <td>{amount}</td>
                <td>{r['report_date']}</td>
            </tr>"""
    else:
        rows_html = '<tr><td colspan="7" style="text-align:center;color:#999">本期无跟踪机构新进记录</td></tr>'

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>救生艇报告 — 机构新进股票</title>
<style>
  body {{ font-family: -apple-system, sans-serif; max-width: 960px; margin: 40px auto; padding: 0 20px; color: #1e293b; }}
  h1 {{ font-size: 20px; margin-bottom: 4px; }}
  .meta {{ color: #94a3b8; font-size: 13px; margin-bottom: 20px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  th {{ background: #f1f5f9; text-align: left; padding: 8px 10px; font-weight: 600; border-bottom: 2px solid #e2e8f0; }}
  td {{ padding: 7px 10px; border-bottom: 1px solid #f1f5f9; }}
  tr:hover td {{ background: #f8fafc; }}
  .count {{ font-size: 14px; font-weight: 600; color: #3b82f6; margin-bottom: 14px; }}
</style>
</head>
<body>
<h1>救生艇报告 — 跟踪机构新进股票</h1>
<div class="meta">报告期：{report_date} | 生成时间：{now} | 跟踪机构数：{len(results)} 条新进记录</div>
<div class="count">共发现 {len(results)} 条新进记录</div>
<table>
<thead><tr><th>#</th><th>代码</th><th>名称</th><th>新进机构</th><th>类型</th><th>持股数</th><th>报告期</th></tr></thead>
<tbody>{rows_html}</tbody>
</table>
</body>
</html>"""
    return html


def main():
    print("=" * 50)
    print("  救生艇 — 机构新进股票查询")
    print("=" * 50)

    institutions = load_institutions()
    df, report_date = fetch_latest_holdings()
    results = find_new_entries(df, institutions)

    print(f"\n找到 {len(results)} 条新进记录")
    for r in results[:10]:
        print(f"  {r['stock_code']} {r['stock_name']} ← {r['holder_name']}")
    if len(results) > 10:
        print(f"  ... 还有 {len(results) - 10} 条")

    html = generate_html(results, report_date)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n报告已保存: {OUTPUT_FILE}")
    print("用浏览器打开即可查看。")


if __name__ == "__main__":
    main()

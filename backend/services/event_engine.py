"""
事件生成引擎

优先使用东财原始数据中的 hold_change 字段（新进/加仓/减仓），
退出事件通过对比每只股票最新两个报告期推算。
"""

import logging
from datetime import datetime

logger = logging.getLogger("cm-api")

# 东财 hold_change 字段映射
_CHANGE_MAP = {
    "新进": "new_entry",
    "加仓": "increase",
    "增加": "increase",
    "减仓": "decrease",
    "减少": "decrease",
    "不变": "unchanged",
}



def generate_events(conn) -> int:
    """从 inst_holdings 生成事件（优先用东财原始标记，回退到持仓量对比）"""
    logger.info("[事件] 开始生成...")

    rows = conn.execute("""
        SELECT institution_id, holder_name, stock_code, stock_name,
               report_date, notice_date, hold_amount, hold_change, hold_change_num
        FROM inst_holdings
        WHERE institution_id IS NOT NULL AND stock_code IS NOT NULL
        ORDER BY institution_id, stock_code, report_date
    """).fetchall()

    if not rows:
        logger.warning("[事件] 无持仓数据")
        return 0

    groups = {}
    for r in rows:
        key = (r["institution_id"], r["stock_code"])
        if key not in groups:
            groups[key] = []
        groups[key].append(dict(r))

    now = datetime.now().isoformat()
    events = []

    for (inst_id, stock_code), records in groups.items():
        records.sort(key=lambda x: x["report_date"])

        for i, rec in enumerate(records):
            cur = float(rec["hold_amount"] or 0)

            # 优先使用东财原始标记
            raw_change = (rec.get("hold_change") or "").strip()
            event_type = _CHANGE_MAP.get(raw_change)

            if i == 0:
                prev = 0
                if not event_type:
                    event_type = "new_entry"
            else:
                prev = float(records[i-1]["hold_amount"] or 0)
                # 东财没给标记时，自己算
                if not event_type:
                    if prev == 0 and cur > 0:
                        event_type = "new_entry"
                    elif cur > prev:
                        event_type = "increase"
                    elif cur < prev:
                        event_type = "decrease"
                    else:
                        event_type = "unchanged"

            change = cur - prev
            pct = (change / prev * 100) if prev > 0 else 0

            events.append((
                inst_id, rec["holder_name"], stock_code, rec["stock_name"],
                rec["report_date"], rec["notice_date"], event_type,
                cur, prev, change, round(pct, 2), now
            ))

    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("DELETE FROM fact_institution_event")
        conn.executemany("""
            INSERT OR REPLACE INTO fact_institution_event
            (institution_id, holder_name, stock_code, stock_name,
             report_date, notice_date, event_type,
             hold_amount, prev_hold_amount, change_amount, change_pct, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, events)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    counts = {}
    for e in events:
        t = e[6]
        counts[t] = counts.get(t, 0) + 1
    logger.info(f"[事件] 生成 {len(events)} 条: {counts}")
    return len(events)


def generate_exit_events(conn) -> int:
    """检测退出事件：每只股票取自己最新的报告期和上一期对比，上期有该机构、最新期没有 → 退出"""

    # 每只股票最新的两个报告期
    stock_periods = conn.execute("""
        SELECT stock_code, report_date,
               ROW_NUMBER() OVER (PARTITION BY stock_code ORDER BY report_date DESC) as rn
        FROM (SELECT DISTINCT stock_code, report_date FROM market_raw_holdings
              WHERE stock_code IS NOT NULL)
    """).fetchall()

    # 按股票分组，取最新两期
    latest_two = {}  # stock_code -> [最新, 次新]
    for r in stock_periods:
        code = r["stock_code"]
        rn = r["rn"]
        if rn <= 2:
            if code not in latest_two:
                latest_two[code] = [None, None]
            latest_two[code][rn - 1] = r["report_date"]

    # 获取所有跟踪机构
    inst_ids = set()
    for r in conn.execute("SELECT id FROM inst_institutions WHERE enabled=1 AND blacklisted=0 AND merged_into IS NULL").fetchall():
        inst_ids.add(r["id"])

    # 获取所有 inst_holdings 的 (institution_id, stock_code, report_date) 索引
    holdings_index = set()
    holdings_detail = {}
    for r in conn.execute("""
        SELECT institution_id, stock_code, report_date, holder_name, stock_name, hold_amount
        FROM inst_holdings WHERE institution_id IS NOT NULL
    """).fetchall():
        key = (r["institution_id"], r["stock_code"], r["report_date"])
        holdings_index.add(key)
        holdings_detail[key] = r

    # 批量查出每个 (stock_code, report_date) 的公告日，供 exit 事件使用
    notice_map = {}  # (stock_code, report_date) -> notice_date
    for r in conn.execute("""
        SELECT stock_code, report_date, MAX(notice_date) AS notice_date
        FROM market_raw_holdings
        WHERE stock_code IS NOT NULL AND notice_date IS NOT NULL AND notice_date != ''
        GROUP BY stock_code, report_date
    """).fetchall():
        notice_map[(r["stock_code"], r["report_date"])] = r["notice_date"]

    now = datetime.now().isoformat()
    exits = []

    for stock_code, periods in latest_two.items():
        latest_rd = periods[0]
        prev_rd = periods[1]
        if not latest_rd or not prev_rd:
            continue

        # exit 的公告日 = 该股票最新报告期在原始数据中的公告日
        exit_notice = notice_map.get((stock_code, latest_rd))

        for inst_id in inst_ids:
            prev_key = (inst_id, stock_code, prev_rd)
            latest_key = (inst_id, stock_code, latest_rd)

            # 上期有、最新期没有 → 退出
            if prev_key in holdings_index and latest_key not in holdings_index:
                prev_rec = holdings_detail[prev_key]
                prev_amt = float(prev_rec["hold_amount"] or 0)
                exits.append((
                    inst_id, prev_rec["holder_name"],
                    stock_code, prev_rec["stock_name"],
                    latest_rd, exit_notice, "exit",
                    0, prev_amt, -prev_amt, -100.0, now
                ))

    if exits:
        conn.executemany("""
            INSERT OR REPLACE INTO fact_institution_event
            (institution_id, holder_name, stock_code, stock_name,
             report_date, notice_date, event_type,
             hold_amount, prev_hold_amount, change_amount, change_pct, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, exits)
        conn.commit()

    logger.info(f"[事件] 退出: {len(exits)} 条")
    return len(exits)

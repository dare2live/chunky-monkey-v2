"""
backtest_engine.py — 历史回测引擎

对历史数据做全量研究回放，产出 5 张研究表：
1. 机构三层行业真实表现
2. 持仓链条全景
3. Setup A 历史回放
4. 多维交叉分析
5. 信号传递效率

独立执行，不进 DAG。
"""

import logging
import json
from datetime import datetime
from collections import defaultdict

from services.industry import industry_join_clause, industry_select_clause
from services.setup_replay import build_setup_replay

logger = logging.getLogger("cm-api")


def run_full_backtest(conn, mkt_conn) -> dict:
    """执行全量回测，返回摘要"""
    results = {}

    logger.info("[回测] 开始全量历史回测...")

    # 表 1
    r1 = build_inst_industry_performance(conn)
    results["inst_industry"] = r1

    # 表 2
    r2 = build_holding_chains(conn)
    results["holding_chains"] = r2

    # 表 3
    r3 = build_setup_replay(conn)
    results["setup_replay"] = r3

    # 表 4
    r4 = build_cross_factor_analysis(conn)
    results["cross_factor"] = r4

    # 表 5
    r5 = build_signal_transfer(conn)
    results["signal_transfer"] = r5

    logger.info("[回测] 全量回测完成")
    return results


def build_inst_industry_performance(conn) -> dict:
    """研究表 1：机构三层行业真实表现"""
    logger.info("[回测-表1] 机构行业表现...")

    conn.execute("DROP TABLE IF EXISTS research_inst_industry_performance")
    conn.execute("""
        CREATE TABLE research_inst_industry_performance (
            institution_id TEXT,
            inst_name TEXT,
            inst_type TEXT,
            industry_level TEXT,
            industry_name TEXT,
            buy_event_count INTEGER,
            total_event_count INTEGER,
            avg_gain_10d REAL, avg_gain_30d REAL, avg_gain_60d REAL, avg_gain_120d REAL,
            win_rate_10d REAL, win_rate_30d REAL, win_rate_60d REAL, win_rate_120d REAL,
            median_gain_30d REAL, median_gain_60d REAL,
            avg_max_drawdown_30d REAL, avg_max_drawdown_60d REAL,
            win_loss_ratio_30d REAL,
            avg_inst_ref_cost REAL,
            avg_premium_pct REAL,
            low_premium_win_rate_30d REAL,
            high_premium_win_rate_30d REAL,
            industry_edge_30d REAL,
            PRIMARY KEY (institution_id, industry_level, industry_name)
        )
    """)

    # 获取每个机构的整体买入表现（用于计算 edge）
    inst_baseline = {}
    for r in conn.execute("""
        SELECT institution_id, AVG(gain_30d) as avg30
        FROM fact_institution_event
        WHERE event_type IN ('new_entry','increase') AND gain_30d IS NOT NULL
        GROUP BY institution_id
    """).fetchall():
        inst_baseline[r["institution_id"]] = r["avg30"] or 0

    count = 0
    industry_join = industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER")
    for level_col, level_name in [("sw_level1", "L1"), ("sw_level2", "L2"), ("sw_level3", "L3")]:
        rows = conn.execute(f"""
            SELECT
                e.institution_id,
                COALESCE(NULLIF(i.display_name,''), i.name) as inst_name,
                i.type as inst_type,
                industry_dim.{level_col} as industry,
                -- 买入事件数
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') THEN 1 ELSE 0 END) as buy_cnt,
                COUNT(*) as total_cnt,
                -- 收益（仅买入事件）
                AVG(CASE WHEN e.event_type IN ('new_entry','increase') THEN e.gain_10d END) as ag10,
                AVG(CASE WHEN e.event_type IN ('new_entry','increase') THEN e.gain_30d END) as ag30,
                AVG(CASE WHEN e.event_type IN ('new_entry','increase') THEN e.gain_60d END) as ag60,
                AVG(CASE WHEN e.event_type IN ('new_entry','increase') THEN e.gain_120d END) as ag120,
                -- 胜率
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_10d>0 THEN 1 ELSE 0 END)*100.0/
                    MAX(SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_10d IS NOT NULL THEN 1 ELSE 0 END),1) as wr10,
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/
                    MAX(SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_30d IS NOT NULL THEN 1 ELSE 0 END),1) as wr30,
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_60d>0 THEN 1 ELSE 0 END)*100.0/
                    MAX(SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_60d IS NOT NULL THEN 1 ELSE 0 END),1) as wr60,
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_120d>0 THEN 1 ELSE 0 END)*100.0/
                    MAX(SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.gain_120d IS NOT NULL THEN 1 ELSE 0 END),1) as wr120,
                -- 回撤
                AVG(CASE WHEN e.event_type IN ('new_entry','increase') THEN e.max_drawdown_30d END) as dd30,
                AVG(CASE WHEN e.event_type IN ('new_entry','increase') THEN e.max_drawdown_60d END) as dd60,
                -- 成本和溢价
                AVG(e.inst_ref_cost) as avg_cost,
                AVG(e.premium_pct) as avg_prem,
                -- 低溢价胜率
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.premium_pct<=5 AND e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/
                    MAX(SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.premium_pct<=5 AND e.gain_30d IS NOT NULL THEN 1 ELSE 0 END),1) as lp_wr30,
                -- 高溢价胜率
                SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.premium_pct>20 AND e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/
                    MAX(SUM(CASE WHEN e.event_type IN ('new_entry','increase') AND e.premium_pct>20 AND e.gain_30d IS NOT NULL THEN 1 ELSE 0 END),1) as hp_wr30
            FROM fact_institution_event e
            JOIN inst_institutions i ON e.institution_id = i.id
            {industry_join}
            WHERE industry_dim.{level_col} IS NOT NULL AND industry_dim.{level_col} != ''
                AND e.gain_30d IS NOT NULL
            GROUP BY e.institution_id, industry_dim.{level_col}
            HAVING buy_cnt >= 1
        """).fetchall()

        for r in rows:
            baseline = inst_baseline.get(r["institution_id"], 0)
            edge = (r["ag30"] or 0) - baseline

            conn.execute("""
                INSERT OR REPLACE INTO research_inst_industry_performance
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r["institution_id"], r["inst_name"], r["inst_type"],
                level_name, r["industry"],
                r["buy_cnt"], r["total_cnt"],
                r["ag10"], r["ag30"], r["ag60"], r["ag120"],
                r["wr10"], r["wr30"], r["wr60"], r["wr120"],
                None, None,  # median
                r["dd30"], r["dd60"],
                None,  # win_loss_ratio
                r["avg_cost"], r["avg_prem"],
                r["lp_wr30"], r["hp_wr30"],
                edge,
            ))
            count += 1

    conn.commit()
    logger.info(f"[回测-表1] 完成: {count} 条记录")
    return {"rows": count}


def build_holding_chains(conn) -> dict:
    """研究表 2：持仓链条全景"""
    logger.info("[回测-表2] 持仓链条...")

    conn.execute("DROP TABLE IF EXISTS research_holding_chains")
    conn.execute("""
        CREATE TABLE research_holding_chains (
            institution_id TEXT,
            stock_code TEXT,
            chain_id INTEGER,
            chain_start_date TEXT,
            chain_end_date TEXT,
            chain_status TEXT,
            chain_days INTEGER,
            event_sequence TEXT,
            event_count INTEGER,
            entry_inst_cost REAL,
            exit_inst_cost REAL,
            chain_inst_gain_pct REAL,
            entry_follow_price REAL,
            entry_premium_pct REAL,
            follow_gain_30d REAL,
            follow_gain_60d REAL,
            follow_gain_120d REAL,
            max_drawdown_30d REAL,
            industry_l1 TEXT,
            industry_l2 TEXT,
            industry_l3 TEXT,
            PRIMARY KEY (institution_id, stock_code, chain_id)
        )
    """)

    # 获取所有事件按 (inst, stock, report_date) 排序
    events = conn.execute("""
        SELECT e.institution_id, e.stock_code, e.report_date, e.notice_date,
               e.event_type, e.inst_ref_cost, e.price_entry, e.premium_pct,
               e.gain_30d, e.gain_60d, e.gain_120d, e.max_drawdown_30d,
               {industry_columns}
        FROM fact_institution_event e
        {industry_join}
        ORDER BY e.institution_id, e.stock_code, e.report_date
    """.format(
        industry_columns=industry_select_clause(alias="industry_dim"),
        industry_join=industry_join_clause("e.stock_code", alias="industry_dim", join_type="LEFT"),
    )).fetchall()

    chains = []
    current_chain = None
    prev_key = None

    for ev in events:
        key = (ev["institution_id"], ev["stock_code"])

        if key != prev_key:
            # 新的机构-股票对，关闭之前的链
            if current_chain:
                chains.append(current_chain)
            current_chain = None
            prev_key = key

        et = ev["event_type"]

        if et == "new_entry":
            # 如果已有开放链，先关闭它（可能丢失了 exit）
            if current_chain and current_chain["status"] == "open":
                chains.append(current_chain)
            # 开新链
            current_chain = {
                "institution_id": ev["institution_id"],
                "stock_code": ev["stock_code"],
                "start_date": ev["notice_date"] or ev["report_date"],
                "end_date": None,
                "status": "open",
                "events": [et],
                "entry_cost": ev["inst_ref_cost"],
                "exit_cost": None,
                "entry_follow": ev["price_entry"],
                "entry_premium": ev["premium_pct"],
                "follow_g30": ev["gain_30d"],
                "follow_g60": ev["gain_60d"],
                "follow_g120": ev["gain_120d"],
                "dd30": ev["max_drawdown_30d"],
                "l1": ev["sw_level1"], "l2": ev["sw_level2"], "l3": ev["sw_level3"],
            }
        elif et == "exit":
            if current_chain and current_chain["status"] == "open":
                current_chain["end_date"] = ev["notice_date"] or ev["report_date"]
                current_chain["status"] = "closed"
                current_chain["exit_cost"] = ev["inst_ref_cost"]
                current_chain["events"].append(et)
                chains.append(current_chain)
                current_chain = None
            # else: orphan exit, ignore
        else:
            # increase / unchanged / decrease — extend chain
            if current_chain and current_chain["status"] == "open":
                current_chain["events"].append(et)

    # Don't forget last chain
    if current_chain:
        chains.append(current_chain)

    # Write to table
    count = 0
    chain_counter = defaultdict(int)
    for c in chains:
        k = (c["institution_id"], c["stock_code"])
        chain_counter[k] += 1
        cid = chain_counter[k]

        start = c["start_date"]
        end = c["end_date"]
        days = None
        if start and end:
            try:
                d1 = datetime.strptime(str(start)[:10], "%Y-%m-%d")
                d2 = datetime.strptime(str(end)[:10], "%Y-%m-%d")
                days = (d2 - d1).days
            except (ValueError, TypeError):
                pass

        inst_gain = None
        if c["entry_cost"] and c["exit_cost"] and c["entry_cost"] > 0:
            inst_gain = round((c["exit_cost"] - c["entry_cost"]) / c["entry_cost"] * 100, 2)

        conn.execute("""
            INSERT OR REPLACE INTO research_holding_chains
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            c["institution_id"], c["stock_code"], cid,
            start, end, c["status"], days,
            "→".join(c["events"]), len(c["events"]),
            c["entry_cost"], c["exit_cost"], inst_gain,
            c["entry_follow"], c["entry_premium"],
            c["follow_g30"], c["follow_g60"], c["follow_g120"], c["dd30"],
            c["l1"], c["l2"], c["l3"],
        ))
        count += 1

    conn.commit()
    logger.info(f"[回测-表2] 完成: {count} 条链")
    return {"total_chains": count, "closed": sum(1 for c in chains if c["status"] == "closed")}


def build_cross_factor_analysis(conn) -> dict:
    """研究表 3：多维交叉分析"""
    logger.info("[回测-表3] 交叉分析...")

    conn.execute("DROP TABLE IF EXISTS research_cross_factor")
    conn.execute("""
        CREATE TABLE research_cross_factor (
            factor_a TEXT,
            factor_a_value TEXT,
            factor_b TEXT,
            factor_b_value TEXT,
            sample_count INTEGER,
            avg_gain_30d REAL,
            avg_gain_60d REAL,
            avg_gain_120d REAL,
            win_rate_30d REAL,
            win_rate_60d REAL,
            avg_drawdown_30d REAL,
            win_loss_ratio_30d REAL,
            uplift_vs_baseline REAL
        )
    """)

    # Baseline
    bl = conn.execute("""
        SELECT AVG(gain_30d) FROM fact_institution_event
        WHERE event_type IN ('new_entry','increase') AND gain_30d IS NOT NULL
    """).fetchone()[0] or 0

    analyses = [
        # (factor_a, factor_b, SQL)
        ("inst_type", "industry_l1", """
            SELECT i.type as fa, industry_dim.sw_level1 as fb,
                COUNT(*) as n, AVG(e.gain_30d) as g30, AVG(e.gain_60d) as g60, AVG(e.gain_120d) as g120,
                SUM(CASE WHEN e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) as wr30,
                SUM(CASE WHEN e.gain_60d>0 THEN 1 ELSE 0 END)*100.0/MAX(SUM(CASE WHEN e.gain_60d IS NOT NULL THEN 1 ELSE 0 END),1) as wr60,
                AVG(e.max_drawdown_30d) as dd30
            FROM fact_institution_event e
            JOIN inst_institutions i ON e.institution_id=i.id
            {industry_join}
            WHERE e.event_type IN ('new_entry','increase') AND e.gain_30d IS NOT NULL
                AND industry_dim.sw_level1 IS NOT NULL
            GROUP BY i.type, industry_dim.sw_level1 HAVING n>=10
        """.format(industry_join=industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER"))),
        ("change_magnitude", "industry_l1", """
            SELECT CASE
                WHEN e.change_pct>100 THEN '翻倍加仓'
                WHEN e.change_pct>50 THEN '大幅加仓'
                WHEN e.change_pct>0 THEN '小幅加仓'
                WHEN e.change_pct IS NULL THEN '新进'
                ELSE '其他'
            END as fa, industry_dim.sw_level1 as fb,
                COUNT(*) as n, AVG(e.gain_30d) as g30, AVG(e.gain_60d) as g60, AVG(e.gain_120d) as g120,
                SUM(CASE WHEN e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) as wr30,
                SUM(CASE WHEN e.gain_60d>0 THEN 1 ELSE 0 END)*100.0/MAX(SUM(CASE WHEN e.gain_60d IS NOT NULL THEN 1 ELSE 0 END),1) as wr60,
                AVG(e.max_drawdown_30d) as dd30
            FROM fact_institution_event e
            {industry_join}
            WHERE e.event_type IN ('new_entry','increase') AND e.gain_30d IS NOT NULL
                AND industry_dim.sw_level1 IS NOT NULL
            GROUP BY fa, industry_dim.sw_level1 HAVING n>=10
        """.format(industry_join=industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER"))),
        ("report_season", "inst_type", """
            SELECT CASE
                WHEN e.report_date LIKE '%0331' THEN 'Q1'
                WHEN e.report_date LIKE '%0630' THEN 'H1'
                WHEN e.report_date LIKE '%0930' THEN 'Q3'
                WHEN e.report_date LIKE '%1231' THEN '年报'
                ELSE '其他'
            END as fa, i.type as fb,
                COUNT(*) as n, AVG(e.gain_30d) as g30, AVG(e.gain_60d) as g60, AVG(e.gain_120d) as g120,
                SUM(CASE WHEN e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) as wr30,
                SUM(CASE WHEN e.gain_60d>0 THEN 1 ELSE 0 END)*100.0/MAX(SUM(CASE WHEN e.gain_60d IS NOT NULL THEN 1 ELSE 0 END),1) as wr60,
                AVG(e.max_drawdown_30d) as dd30
            FROM fact_institution_event e
            JOIN inst_institutions i ON e.institution_id=i.id
            WHERE e.event_type IN ('new_entry','increase') AND e.gain_30d IS NOT NULL
            GROUP BY fa, i.type HAVING n>=10
        """),
        ("premium_bucket", "industry_l1", """
            SELECT CASE
                WHEN e.premium_pct<=0 THEN '负溢价'
                WHEN e.premium_pct<=10 THEN '0-10%'
                WHEN e.premium_pct<=20 THEN '10-20%'
                ELSE '>20%'
            END as fa, industry_dim.sw_level1 as fb,
                COUNT(*) as n, AVG(e.gain_30d) as g30, AVG(e.gain_60d) as g60, AVG(e.gain_120d) as g120,
                SUM(CASE WHEN e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) as wr30,
                SUM(CASE WHEN e.gain_60d>0 THEN 1 ELSE 0 END)*100.0/MAX(SUM(CASE WHEN e.gain_60d IS NOT NULL THEN 1 ELSE 0 END),1) as wr60,
                AVG(e.max_drawdown_30d) as dd30
            FROM fact_institution_event e
            {industry_join}
            WHERE e.event_type IN ('new_entry','increase') AND e.gain_30d IS NOT NULL
                AND e.premium_pct IS NOT NULL AND industry_dim.sw_level1 IS NOT NULL
            GROUP BY fa, industry_dim.sw_level1 HAVING n>=10
        """.format(industry_join=industry_join_clause("e.stock_code", alias="industry_dim", join_type="INNER"))),
        ("consensus", "premium_bucket", """
            WITH stock_inst AS (
                SELECT stock_code, report_date, COUNT(DISTINCT institution_id) as inst_cnt
                FROM fact_institution_event WHERE event_type IN ('new_entry','increase')
                GROUP BY stock_code, report_date
            )
            SELECT CASE
                WHEN s.inst_cnt=1 THEN '独行侠'
                WHEN s.inst_cnt<=3 THEN '轻共识'
                ELSE '重共识'
            END as fa,
            CASE
                WHEN e.premium_pct<=0 THEN '负溢价'
                WHEN e.premium_pct<=10 THEN '0-10%'
                ELSE '>10%'
            END as fb,
                COUNT(*) as n, AVG(e.gain_30d) as g30, AVG(e.gain_60d) as g60, AVG(e.gain_120d) as g120,
                SUM(CASE WHEN e.gain_30d>0 THEN 1 ELSE 0 END)*100.0/COUNT(*) as wr30,
                SUM(CASE WHEN e.gain_60d>0 THEN 1 ELSE 0 END)*100.0/MAX(SUM(CASE WHEN e.gain_60d IS NOT NULL THEN 1 ELSE 0 END),1) as wr60,
                AVG(e.max_drawdown_30d) as dd30
            FROM fact_institution_event e
            JOIN stock_inst s ON e.stock_code=s.stock_code AND e.report_date=s.report_date
            WHERE e.event_type IN ('new_entry','increase') AND e.gain_30d IS NOT NULL
                AND e.premium_pct IS NOT NULL
            GROUP BY fa, fb HAVING n>=20
        """),
    ]

    count = 0
    for fa_name, fb_name, sql in analyses:
        rows = conn.execute(sql).fetchall()
        for r in rows:
            # win_loss_ratio
            wl = None
            conn.execute("""
                INSERT INTO research_cross_factor
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                fa_name, r["fa"], fb_name, r["fb"],
                r["n"], r["g30"], r["g60"], r["g120"],
                r["wr30"], r["wr60"], r["dd30"],
                wl, (r["g30"] or 0) - bl,
            ))
            count += 1

    conn.commit()
    logger.info(f"[回测-表3] 完成: {count} 条交叉记录")
    return {"rows": count}


def build_signal_transfer(conn) -> dict:
    """研究表 4：信号传递效率"""
    logger.info("[回测-表4] 信号传递效率...")

    conn.execute("DROP TABLE IF EXISTS research_signal_transfer")
    conn.execute("""
        CREATE TABLE research_signal_transfer (
            institution_id TEXT,
            inst_name TEXT,
            inst_type TEXT,
            industry_l2 TEXT,
            closed_chain_count INTEGER,
            inst_cycle_median_gain REAL,
            follow_median_gain_30d REAL,
            follow_median_gain_60d REAL,
            avg_premium_pct REAL,
            signal_capture_30d REAL,
            PRIMARY KEY (institution_id, industry_l2)
        )
    """)

    # 从链条表聚合
    rows = conn.execute("""
        SELECT c.institution_id,
               COALESCE(NULLIF(i.display_name,''), i.name) as inst_name,
               i.type as inst_type,
               c.industry_l2,
               COUNT(*) as chain_cnt,
               AVG(c.chain_inst_gain_pct) as avg_inst_gain,
               AVG(c.follow_gain_30d) as avg_fg30,
               AVG(c.follow_gain_60d) as avg_fg60,
               AVG(c.entry_premium_pct) as avg_prem
        FROM research_holding_chains c
        JOIN inst_institutions i ON c.institution_id = i.id
        WHERE c.industry_l2 IS NOT NULL AND c.follow_gain_30d IS NOT NULL
        GROUP BY c.institution_id, c.industry_l2
        HAVING chain_cnt >= 3
    """).fetchall()

    count = 0
    for r in rows:
        capture = None
        if r["avg_inst_gain"] and r["avg_inst_gain"] != 0 and r["avg_fg30"]:
            capture = round(r["avg_fg30"] / r["avg_inst_gain"], 3)

        conn.execute("""
            INSERT OR REPLACE INTO research_signal_transfer
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, (
            r["institution_id"], r["inst_name"], r["inst_type"],
            r["industry_l2"], r["chain_cnt"],
            r["avg_inst_gain"], r["avg_fg30"], r["avg_fg60"],
            r["avg_prem"], capture,
        ))
        count += 1

    conn.commit()
    logger.info(f"[回测-表4] 完成: {count} 条记录")
    return {"rows": count}

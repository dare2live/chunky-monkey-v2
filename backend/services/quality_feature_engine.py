"""
quality_feature_engine.py — 质量分中间事实层

把财务快照、资本行为、扩展财务指标统一沉到一张质量特征表里，
供评分、详情页、验证与回放复用。
"""

import logging
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger("cm-api")


def _table_columns(conn, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row[1] for row in rows}


def _ensure_columns(conn, table_name: str, columns: dict[str, str]) -> None:
    existing = _table_columns(conn, table_name)
    for col, ddl in columns.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {ddl}")


def _safe_float(value):
    try:
        if value is None:
            return None
        value = float(value)
        if value != value:
            return None
        return value
    except Exception:
        return None


def _clamp_score(value: Optional[float], lo: float = 0.0, hi: float = 100.0) -> float:
    if value is None:
        return lo
    return round(max(lo, min(hi, float(value))), 2)


def _score_ge(value: Optional[float], rules, default: float = 0.0) -> float:
    if value is None:
        return default
    for threshold, score in rules:
        if value >= threshold:
            return score
    return default


def _score_le(value: Optional[float], rules, default: float = 0.0) -> float:
    if value is None:
        return default
    for threshold, score in rules:
        if value <= threshold:
            return score
    return default


def _rank_score(rank: Optional[float], max_score: float) -> float:
    if rank is None:
        return round(max_score * 0.5, 2)
    return round(max_score * max(min(rank, 100.0), 0.0) / 100.0, 2)


def _percentile_ranks(values: list[Optional[float]]) -> list[Optional[float]]:
    pairs = [(idx, val) for idx, val in enumerate(values) if val is not None]
    if not pairs:
        return [None] * len(values)
    pairs.sort(key=lambda item: item[1])
    total = len(pairs)
    result = [None] * len(values)
    if total == 1:
        result[pairs[0][0]] = 100.0
        return result
    for pos, (idx, _) in enumerate(pairs):
        result[idx] = round(pos / (total - 1) * 100.0, 2)
    return result


def _days_since(date_str: Optional[str]) -> Optional[int]:
    if not date_str:
        return None
    try:
        dt = datetime.strptime(str(date_str), "%Y-%m-%d").date()
        return (date.today() - dt).days
    except Exception:
        return None


def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fact_stock_quality_features (
            snapshot_date              TEXT NOT NULL,
            stock_code                 TEXT NOT NULL,
            latest_financial_report_date TEXT,
            latest_indicator_report_date TEXT,
            sw_level1                  TEXT,
            sw_level2                  TEXT,
            roe                        REAL,
            roa_ak                     REAL,
            gross_margin               REAL,
            ocf_to_profit              REAL,
            debt_ratio                 REAL,
            current_ratio              REAL,
            contract_to_revenue        REAL,
            revenue_growth_yoy_ak      REAL,
            net_profit_growth_yoy_ak   REAL,
            dividend_financing_ratio   REAL,
            repurchase_count_3y        INTEGER,
            active_repurchase_count    INTEGER,
            future_unlock_ratio_180d   REAL,
            holder_count_change_pct    REAL,
            total_shares_growth_3y     REAL,
            net_profit_positive_8q     INTEGER,
            operating_cashflow_positive_8q INTEGER,
            revenue_yoy_positive_4q    INTEGER,
            profit_yoy_positive_4q     INTEGER,
            roe_rank                   REAL,
            gross_margin_rank          REAL,
            ocf_rank                   REAL,
            debt_rank                  REAL,
            current_ratio_rank         REAL,
            contract_rank              REAL,
            roa_rank                   REAL,
            asset_turnover_rank        REAL,
            inventory_turnover_rank    REAL,
            receivables_turnover_rank  REAL,
            quality_profit_raw         REAL,
            quality_cash_raw           REAL,
            quality_balance_raw        REAL,
            quality_margin_raw         REAL,
            quality_contract_raw       REAL,
            quality_freshness_raw      REAL,
            quality_capital_raw        REAL,
            quality_efficiency_raw     REAL,
            quality_growth_raw         REAL,
            quality_score_v1           REAL,
            updated_at                 TEXT,
            PRIMARY KEY (snapshot_date, stock_code)
        );
        CREATE INDEX IF NOT EXISTS idx_fsqf_stock ON fact_stock_quality_features(stock_code);

        CREATE TABLE IF NOT EXISTS dim_stock_quality_latest (
            stock_code                 TEXT PRIMARY KEY,
            snapshot_date              TEXT,
            latest_financial_report_date TEXT,
            latest_indicator_report_date TEXT,
            sw_level1                  TEXT,
            sw_level2                  TEXT,
            roe                        REAL,
            roa_ak                     REAL,
            gross_margin               REAL,
            ocf_to_profit              REAL,
            debt_ratio                 REAL,
            current_ratio              REAL,
            contract_to_revenue        REAL,
            revenue_growth_yoy_ak      REAL,
            net_profit_growth_yoy_ak   REAL,
            dividend_financing_ratio   REAL,
            repurchase_count_3y        INTEGER,
            active_repurchase_count    INTEGER,
            future_unlock_ratio_180d   REAL,
            holder_count_change_pct    REAL,
            total_shares_growth_3y     REAL,
            net_profit_positive_8q     INTEGER,
            operating_cashflow_positive_8q INTEGER,
            revenue_yoy_positive_4q    INTEGER,
            profit_yoy_positive_4q     INTEGER,
            roe_rank                   REAL,
            gross_margin_rank          REAL,
            ocf_rank                   REAL,
            debt_rank                  REAL,
            current_ratio_rank         REAL,
            contract_rank              REAL,
            roa_rank                   REAL,
            asset_turnover_rank        REAL,
            inventory_turnover_rank    REAL,
            receivables_turnover_rank  REAL,
            quality_profit_raw         REAL,
            quality_cash_raw           REAL,
            quality_balance_raw        REAL,
            quality_margin_raw         REAL,
            quality_contract_raw       REAL,
            quality_freshness_raw      REAL,
            quality_capital_raw        REAL,
            quality_efficiency_raw     REAL,
            quality_growth_raw         REAL,
            quality_score_v1           REAL,
            updated_at                 TEXT
        );
    """)
    _ensure_columns(conn, "fact_stock_quality_features", {
        "holder_count_change_pct": "REAL",
        "total_shares_growth_3y": "REAL",
        "net_profit_positive_8q": "INTEGER",
        "operating_cashflow_positive_8q": "INTEGER",
        "revenue_yoy_positive_4q": "INTEGER",
        "profit_yoy_positive_4q": "INTEGER",
    })
    _ensure_columns(conn, "dim_stock_quality_latest", {
        "holder_count_change_pct": "REAL",
        "total_shares_growth_3y": "REAL",
        "net_profit_positive_8q": "INTEGER",
        "operating_cashflow_positive_8q": "INTEGER",
        "revenue_yoy_positive_4q": "INTEGER",
        "profit_yoy_positive_4q": "INTEGER",
    })
    conn.commit()


def _load_financial_groups(conn):
    financial_by_stock = {}
    fin_groups = {("all", "all"): []}
    fin_pct_map = {}
    fin_group_sizes = {}
    rows = conn.execute("""
        SELECT f.stock_code, f.latest_report_date, f.roe, f.debt_ratio, f.current_ratio,
               f.gross_margin, f.ocf_to_profit, f.contract_to_revenue,
               i.sw_level1, i.sw_level2
        FROM dim_financial_latest f
        LEFT JOIN dim_stock_industry i ON i.stock_code = f.stock_code
    """).fetchall()
    for row in rows:
        d = dict(row)
        financial_by_stock[d["stock_code"]] = d
        fin_groups[("all", "all")].append(d)
        if d.get("sw_level2"):
            fin_groups.setdefault(("l2", d["sw_level2"]), []).append(d)
        if d.get("sw_level1"):
            fin_groups.setdefault(("l1", d["sw_level1"]), []).append(d)

    fin_metrics = {
        "roe": False,
        "gross_margin": False,
        "ocf_to_profit": False,
        "debt_ratio": True,
        "current_ratio": False,
        "contract_to_revenue": True,
    }
    for (level, group_name), group_rows in fin_groups.items():
        fin_group_sizes[(level, group_name)] = len(group_rows)
        for metric, reverse in fin_metrics.items():
            values = [_safe_float(r.get(metric)) for r in group_rows]
            if reverse:
                values = [(-v if v is not None else None) for v in values]
            ranks = _percentile_ranks(values)
            for r, rank in zip(group_rows, ranks):
                if rank is not None:
                    fin_pct_map[(level, group_name, metric, r["stock_code"])] = rank
    return financial_by_stock, fin_pct_map, fin_group_sizes


def _load_indicator_groups(conn):
    indicator_by_stock = {}
    indicator_groups = {("all", "all"): []}
    indicator_pct_map = {}
    indicator_group_sizes = {}
    rows = conn.execute("""
        SELECT f.stock_code, f.latest_report_date, f.roe_ak, f.roa_ak, f.gross_margin_ak,
               f.net_margin_ak, f.current_ratio_ak, f.quick_ratio_ak, f.debt_ratio_ak,
               f.asset_turnover_ak, f.inventory_turnover_ak, f.receivables_turnover_ak,
               f.revenue_growth_yoy_ak, f.net_profit_growth_yoy_ak, i.sw_level1, i.sw_level2
        FROM dim_financial_indicator_latest f
        LEFT JOIN dim_stock_industry i ON i.stock_code = f.stock_code
    """).fetchall()
    for row in rows:
        d = dict(row)
        indicator_by_stock[d["stock_code"]] = d
        indicator_groups[("all", "all")].append(d)
        if d.get("sw_level2"):
            indicator_groups.setdefault(("l2", d["sw_level2"]), []).append(d)
        if d.get("sw_level1"):
            indicator_groups.setdefault(("l1", d["sw_level1"]), []).append(d)
    indicator_metrics = {
        "roa_ak": False,
        "asset_turnover_ak": False,
        "inventory_turnover_ak": False,
        "receivables_turnover_ak": False,
        "quick_ratio_ak": False,
        "debt_ratio_ak": True,
    }
    for (level, group_name), group_rows in indicator_groups.items():
        indicator_group_sizes[(level, group_name)] = len(group_rows)
        for metric, reverse in indicator_metrics.items():
            values = [_safe_float(r.get(metric)) for r in group_rows]
            if reverse:
                values = [(-v if v is not None else None) for v in values]
            ranks = _percentile_ranks(values)
            for r, rank in zip(group_rows, ranks):
                if rank is not None:
                    indicator_pct_map[(level, group_name, metric, r["stock_code"])] = rank
    return indicator_by_stock, indicator_pct_map, indicator_group_sizes


def _pick_rank(stock_code: str, metric: str, sw_level2: Optional[str], sw_level1: Optional[str], pct_map: dict, group_sizes: dict) -> Optional[float]:
    if sw_level2 and group_sizes.get(("l2", sw_level2), 0) >= 15:
        rank = pct_map.get(("l2", sw_level2, metric, stock_code))
        if rank is not None:
            return rank
    if sw_level1 and group_sizes.get(("l1", sw_level1), 0) >= 20:
        rank = pct_map.get(("l1", sw_level1, metric, stock_code))
        if rank is not None:
            return rank
    return pct_map.get(("all", "all", metric, stock_code))


def build_quality_features(conn, snapshot_date: Optional[str] = None) -> int:
    ensure_tables(conn)
    snapshot_date = snapshot_date or date.today().strftime("%Y-%m-%d")
    now = datetime.now().isoformat()

    financial_by_stock, fin_pct_map, fin_group_sizes = _load_financial_groups(conn)
    indicator_by_stock, indicator_pct_map, indicator_group_sizes = _load_indicator_groups(conn)
    capital_by_stock = {}
    try:
        rows = conn.execute("""
            SELECT stock_code, listed_days, cumulative_dividend, avg_annual_dividend,
                   dividend_count, financing_total, financing_count, dividend_financing_ratio,
                   repurchase_count_3y, repurchase_amount_3y, repurchase_ratio_sum_3y,
                   active_repurchase_count, future_unlock_count_180d, future_unlock_ratio_180d,
                   last_dividend_notice_date, dividend_cash_sum_5y, dividend_event_count_5y,
                   dividend_implemented_count_5y, last_allotment_notice_date,
                   allotment_count_5y, allotment_ratio_sum_5y, allotment_raised_funds_5y
            FROM dim_capital_behavior_latest
        """).fetchall()
        for row in rows:
            capital_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        capital_by_stock = {}
    archetype_by_stock = {}
    try:
        rows = conn.execute("""
            SELECT stock_code, total_shares_growth_3y, net_profit_positive_8q,
                   operating_cashflow_positive_8q, revenue_yoy_positive_4q,
                   profit_yoy_positive_4q
            FROM dim_stock_archetype_latest
        """).fetchall()
        for row in rows:
            archetype_by_stock[row["stock_code"]] = dict(row)
    except Exception:
        archetype_by_stock = {}

    stock_rows = conn.execute("""
        SELECT stock_code, sw_level1, sw_level2
        FROM dim_stock_industry
        UNION
        SELECT stock_code, NULL AS sw_level1, NULL AS sw_level2
        FROM dim_financial_latest
        WHERE stock_code NOT IN (SELECT stock_code FROM dim_stock_industry)
    """).fetchall()
    stock_meta = {row["stock_code"]: dict(row) for row in stock_rows}
    all_codes = sorted(set(stock_meta) | set(financial_by_stock) | set(indicator_by_stock) | set(capital_by_stock))

    conn.execute("DELETE FROM fact_stock_quality_features WHERE snapshot_date = ?", (snapshot_date,))
    inserted = 0
    for sc in all_codes:
        meta = stock_meta.get(sc) or {}
        sw_level1 = meta.get("sw_level1")
        sw_level2 = meta.get("sw_level2")
        fin = financial_by_stock.get(sc) or {}
        indicator = indicator_by_stock.get(sc) or {}
        capital = capital_by_stock.get(sc) or {}
        archetype = archetype_by_stock.get(sc) or {}

        fin_report_days = _days_since(fin.get("latest_report_date"))
        roe = _safe_float(fin.get("roe"))
        debt_ratio = _safe_float(fin.get("debt_ratio"))
        current_ratio = _safe_float(fin.get("current_ratio"))
        gross_margin = _safe_float(fin.get("gross_margin"))
        ocf_to_profit = _safe_float(fin.get("ocf_to_profit"))
        contract_to_revenue = _safe_float(fin.get("contract_to_revenue"))
        holder_count_change_pct = _safe_float(fin.get("holder_count_change_pct"))
        total_shares_growth_3y = _safe_float(archetype.get("total_shares_growth_3y"))
        net_profit_positive_8q = int(_safe_float(archetype.get("net_profit_positive_8q")) or 0)
        operating_cashflow_positive_8q = int(_safe_float(archetype.get("operating_cashflow_positive_8q")) or 0)
        revenue_yoy_positive_4q = int(_safe_float(archetype.get("revenue_yoy_positive_4q")) or 0)
        profit_yoy_positive_4q = int(_safe_float(archetype.get("profit_yoy_positive_4q")) or 0)

        roa_ak = _safe_float(indicator.get("roa_ak"))
        revenue_growth_yoy_ak = _safe_float(indicator.get("revenue_growth_yoy_ak"))
        net_profit_growth_yoy_ak = _safe_float(indicator.get("net_profit_growth_yoy_ak"))
        dividend_financing_ratio = _safe_float(capital.get("dividend_financing_ratio"))
        repurchase_count_3y = int(_safe_float(capital.get("repurchase_count_3y")) or 0)
        active_repurchase_count = int(_safe_float(capital.get("active_repurchase_count")) or 0)
        future_unlock_ratio_180d = _safe_float(capital.get("future_unlock_ratio_180d"))
        dividend_count = _safe_float(capital.get("dividend_count"))
        financing_count = _safe_float(capital.get("financing_count"))
        repurchase_amount_3y = _safe_float(capital.get("repurchase_amount_3y"))
        unlock_count_180d = _safe_float(capital.get("future_unlock_count_180d"))
        dividend_event_count_5y = _safe_float(capital.get("dividend_event_count_5y"))
        dividend_implemented_count_5y = _safe_float(capital.get("dividend_implemented_count_5y"))
        allotment_count_5y = _safe_float(capital.get("allotment_count_5y"))
        allotment_ratio_sum_5y = _safe_float(capital.get("allotment_ratio_sum_5y"))
        inventory_turnover_ak = _safe_float(indicator.get("inventory_turnover_ak"))
        receivables_turnover_ak = _safe_float(indicator.get("receivables_turnover_ak"))

        roe_rank = _pick_rank(sc, "roe", sw_level2, sw_level1, fin_pct_map, fin_group_sizes)
        gross_margin_rank = _pick_rank(sc, "gross_margin", sw_level2, sw_level1, fin_pct_map, fin_group_sizes)
        ocf_rank = _pick_rank(sc, "ocf_to_profit", sw_level2, sw_level1, fin_pct_map, fin_group_sizes)
        debt_rank = _pick_rank(sc, "debt_ratio", sw_level2, sw_level1, fin_pct_map, fin_group_sizes)
        current_ratio_rank = _pick_rank(sc, "current_ratio", sw_level2, sw_level1, fin_pct_map, fin_group_sizes)
        contract_rank = _pick_rank(sc, "contract_to_revenue", sw_level2, sw_level1, fin_pct_map, fin_group_sizes)
        roa_rank = _pick_rank(sc, "roa_ak", sw_level2, sw_level1, indicator_pct_map, indicator_group_sizes)
        asset_turnover_rank = _pick_rank(sc, "asset_turnover_ak", sw_level2, sw_level1, indicator_pct_map, indicator_group_sizes)
        inventory_turnover_rank = _pick_rank(sc, "inventory_turnover_ak", sw_level2, sw_level1, indicator_pct_map, indicator_group_sizes)
        receivables_turnover_rank = _pick_rank(sc, "receivables_turnover_ak", sw_level2, sw_level1, indicator_pct_map, indicator_group_sizes)

        quality_profit_raw = (
            _rank_score(roe_rank, 18)
            + _score_ge(roe, ((0.18, 12), (0.10, 9), (0.05, 6), (0.0, 3)), 0)
            + (6.0 if net_profit_positive_8q >= 7 else 4.0 if net_profit_positive_8q >= 6 else 2.0 if net_profit_positive_8q >= 4 else 0.0)
            + (4.0 if profit_yoy_positive_4q >= 4 else 3.0 if profit_yoy_positive_4q >= 3 else 1.0 if profit_yoy_positive_4q >= 2 else 0.0)
        )
        quality_cash_raw = (
            _rank_score(ocf_rank, 12)
            + _score_ge(ocf_to_profit, ((1.2, 13), (0.9, 10), (0.7, 7), (0.4, 4), (0.0, 2)), 0)
            + (5.0 if operating_cashflow_positive_8q >= 7 else 3.0 if operating_cashflow_positive_8q >= 6 else 1.5 if operating_cashflow_positive_8q >= 4 else 0.0)
        )
        quality_balance_raw = (
            _rank_score(debt_rank, 10)
            + _score_le(debt_ratio, ((0.30, 5), (0.50, 4), (0.70, 2)), 0)
            + _score_ge(current_ratio, ((2.0, 10), (1.5, 8), (1.2, 6), (1.0, 3)), 0)
        )
        quality_margin_raw = _rank_score(gross_margin_rank, 10)
        quality_contract_raw = (
            _rank_score(contract_rank, 3)
            + _score_le(contract_to_revenue, ((0.10, 2), (0.20, 1)), 0)
        ) if contract_to_revenue is not None else 2.5
        quality_freshness_raw = _score_le(fin_report_days, ((120, 5), (210, 4), (330, 2)), 0)

        quality_capital_raw = 0.0
        if dividend_count is not None:
            quality_capital_raw += (
                2.0 if dividend_count >= 10 else
                1.0 if dividend_count >= 5 else
                0.5 if dividend_count >= 2 else 0.0
            )
        if dividend_implemented_count_5y is not None:
            quality_capital_raw += (
                1.5 if dividend_implemented_count_5y >= 5 else
                1.0 if dividend_implemented_count_5y >= 3 else
                0.5 if dividend_implemented_count_5y >= 1 else 0.0
            )
        if dividend_financing_ratio is None:
            if (financing_count or 0) == 0 and (dividend_count or 0) >= 3:
                quality_capital_raw += 2.0
        else:
            quality_capital_raw += (
                3.0 if dividend_financing_ratio >= 1.0 else
                2.0 if dividend_financing_ratio >= 0.5 else
                1.0 if dividend_financing_ratio >= 0.2 else
                -1.5
            )
        if repurchase_count_3y >= 2 or (repurchase_amount_3y or 0) >= 1e8:
            quality_capital_raw += 2.5
        elif repurchase_count_3y >= 1 or active_repurchase_count >= 1:
            quality_capital_raw += 1.0
        if future_unlock_ratio_180d is not None:
            quality_capital_raw += (
                -4.0 if future_unlock_ratio_180d > 0.10 else
                -2.0 if future_unlock_ratio_180d > 0.05 else
                -1.0 if future_unlock_ratio_180d > 0.02 else
                0.5
            )
        elif (unlock_count_180d or 0) == 0:
            quality_capital_raw += 0.5
        if allotment_count_5y is not None and allotment_count_5y > 0:
            quality_capital_raw += (
                -2.0 if (allotment_ratio_sum_5y or 0) >= 1.5 else
                -1.0
            )
        if total_shares_growth_3y is not None:
            quality_capital_raw += (
                1.5 if total_shares_growth_3y <= 0.05 else
                0.5 if total_shares_growth_3y <= 0.15 else
                -1.0 if total_shares_growth_3y <= 0.30 else
                -2.5
            )
        if holder_count_change_pct is not None:
            quality_capital_raw += (
                1.0 if holder_count_change_pct <= -0.05 else
                0.5 if holder_count_change_pct <= 0.02 else
                -0.5 if holder_count_change_pct <= 0.10 else
                -1.5
            )

        quality_efficiency_raw = (
            _rank_score(roa_rank, 8)
            + _rank_score(asset_turnover_rank, 4)
            + (_rank_score(inventory_turnover_rank, 2) if inventory_turnover_ak is not None else 1.0)
            + (_rank_score(receivables_turnover_rank, 2) if receivables_turnover_ak is not None else 1.0)
        )
        quality_growth_raw = (
            _score_ge(revenue_growth_yoy_ak, ((20.0, 4), (10.0, 3), (0.0, 1)), 0)
            + _score_ge(net_profit_growth_yoy_ak, ((20.0, 4), (10.0, 3), (0.0, 1)), 0)
            + (4.0 if revenue_yoy_positive_4q >= 4 else 3.0 if revenue_yoy_positive_4q >= 3 else 1.0 if revenue_yoy_positive_4q >= 2 else 0.0)
            + (4.0 if profit_yoy_positive_4q >= 4 else 3.0 if profit_yoy_positive_4q >= 3 else 1.0 if profit_yoy_positive_4q >= 2 else 0.0)
        )

        quality_score_v1 = _clamp_score(
            quality_profit_raw + quality_cash_raw + quality_balance_raw
            + quality_margin_raw + quality_contract_raw + quality_freshness_raw
            + quality_capital_raw + quality_efficiency_raw + quality_growth_raw
        )

        conn.execute("""
            INSERT OR REPLACE INTO fact_stock_quality_features
            (snapshot_date, stock_code, latest_financial_report_date, latest_indicator_report_date,
             sw_level1, sw_level2, roe, roa_ak, gross_margin, ocf_to_profit, debt_ratio,
             current_ratio, contract_to_revenue, revenue_growth_yoy_ak, net_profit_growth_yoy_ak,
             dividend_financing_ratio, repurchase_count_3y, active_repurchase_count,
             future_unlock_ratio_180d, holder_count_change_pct, total_shares_growth_3y,
             net_profit_positive_8q, operating_cashflow_positive_8q, revenue_yoy_positive_4q,
             profit_yoy_positive_4q,
             roe_rank, gross_margin_rank, ocf_rank, debt_rank,
             current_ratio_rank, contract_rank, roa_rank, asset_turnover_rank,
             inventory_turnover_rank, receivables_turnover_rank, quality_profit_raw,
             quality_cash_raw, quality_balance_raw, quality_margin_raw, quality_contract_raw,
             quality_freshness_raw, quality_capital_raw, quality_efficiency_raw,
             quality_growth_raw, quality_score_v1, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot_date, sc, fin.get("latest_report_date"), indicator.get("latest_report_date"),
            sw_level1, sw_level2, roe, roa_ak, gross_margin, ocf_to_profit, debt_ratio,
            current_ratio, contract_to_revenue, revenue_growth_yoy_ak, net_profit_growth_yoy_ak,
            dividend_financing_ratio, repurchase_count_3y, active_repurchase_count,
            future_unlock_ratio_180d, holder_count_change_pct, total_shares_growth_3y,
            net_profit_positive_8q, operating_cashflow_positive_8q, revenue_yoy_positive_4q,
            profit_yoy_positive_4q,
            roe_rank, gross_margin_rank, ocf_rank, debt_rank,
            current_ratio_rank, contract_rank, roa_rank, asset_turnover_rank,
            inventory_turnover_rank, receivables_turnover_rank, quality_profit_raw,
            quality_cash_raw, quality_balance_raw, quality_margin_raw, quality_contract_raw,
            quality_freshness_raw, quality_capital_raw, quality_efficiency_raw,
            quality_growth_raw, quality_score_v1, now,
        ))
        inserted += 1

    conn.execute("DELETE FROM dim_stock_quality_latest")
    conn.execute("""
        INSERT INTO dim_stock_quality_latest
        (stock_code, snapshot_date, latest_financial_report_date, latest_indicator_report_date,
         sw_level1, sw_level2, roe, roa_ak, gross_margin, ocf_to_profit, debt_ratio,
         current_ratio, contract_to_revenue, revenue_growth_yoy_ak, net_profit_growth_yoy_ak,
         dividend_financing_ratio, repurchase_count_3y, active_repurchase_count,
         future_unlock_ratio_180d, holder_count_change_pct, total_shares_growth_3y,
         net_profit_positive_8q, operating_cashflow_positive_8q, revenue_yoy_positive_4q,
         profit_yoy_positive_4q,
         roe_rank, gross_margin_rank, ocf_rank, debt_rank,
         current_ratio_rank, contract_rank, roa_rank, asset_turnover_rank,
         inventory_turnover_rank, receivables_turnover_rank, quality_profit_raw,
         quality_cash_raw, quality_balance_raw, quality_margin_raw, quality_contract_raw,
         quality_freshness_raw, quality_capital_raw, quality_efficiency_raw,
         quality_growth_raw, quality_score_v1, updated_at)
        SELECT stock_code, snapshot_date, latest_financial_report_date, latest_indicator_report_date,
               sw_level1, sw_level2, roe, roa_ak, gross_margin, ocf_to_profit, debt_ratio,
               current_ratio, contract_to_revenue, revenue_growth_yoy_ak, net_profit_growth_yoy_ak,
               dividend_financing_ratio, repurchase_count_3y, active_repurchase_count,
               future_unlock_ratio_180d, holder_count_change_pct, total_shares_growth_3y,
               net_profit_positive_8q, operating_cashflow_positive_8q, revenue_yoy_positive_4q,
               profit_yoy_positive_4q,
               roe_rank, gross_margin_rank, ocf_rank, debt_rank,
               current_ratio_rank, contract_rank, roa_rank, asset_turnover_rank,
               inventory_turnover_rank, receivables_turnover_rank, quality_profit_raw,
               quality_cash_raw, quality_balance_raw, quality_margin_raw, quality_contract_raw,
               quality_freshness_raw, quality_capital_raw, quality_efficiency_raw,
               quality_growth_raw, quality_score_v1, updated_at
        FROM fact_stock_quality_features
        WHERE snapshot_date = ?
    """, (snapshot_date,))
    conn.commit()
    logger.info(f"[质量特征] 构建完成: {inserted} 只股票, 快照 {snapshot_date}")
    return inserted

"""
industry.py — 行业解析单点实现

所有需要行业信息的地方统一通过本模块访问，禁止直接查 dim_stock_industry。

用法约定：
- 物化表构建（build_current_relationship）→ load_industry_map() 批量装载
- 页面/服务层 → resolve_industry() 单点查询
- 两者底层读同一张 dim_stock_industry，口径统一
"""

import logging

logger = logging.getLogger(__name__)

# 模块级缓存，避免同一进程内重复全表扫描
_industry_cache: dict = None
INDUSTRY_LEVEL_COLUMNS = ("sw_level1", "sw_level2", "sw_level3")


def load_industry_map(conn) -> dict[str, dict]:
    """
    批量加载行业映射，用于物化表构建（避免 N+1 查询）。

    Returns:
        {stock_code: {"sw_level1": ..., "sw_level2": ..., "sw_level3": ...}}
    """
    global _industry_cache
    rows = conn.execute(
        "SELECT stock_code, sw_level1, sw_level2, sw_level3 "
        "FROM dim_stock_industry"
    ).fetchall()
    result = {}
    for r in rows:
        result[r["stock_code"]] = {
            "sw_level1": r["sw_level1"],
            "sw_level2": r["sw_level2"],
            "sw_level3": r["sw_level3"],
        }
    _industry_cache = result
    logger.debug(f"[Industry] Loaded industry map: {len(result)} stocks")
    return result


def industry_join_clause(stock_expr: str, *, alias: str = "industry_dim", join_type: str = "LEFT") -> str:
    """生成统一的行业关联 SQL 片段，避免业务代码散落直连表名。"""
    mode = join_type.strip().upper()
    if mode not in {"LEFT", "INNER"}:
        raise ValueError(f"Unsupported industry join type: {join_type}")
    return f"{mode} JOIN dim_stock_industry {alias} ON {stock_expr} = {alias}.stock_code"


def industry_select_clause(*, alias: str = "industry_dim", prefix: str = "") -> str:
    """生成统一的行业字段 SELECT 片段。"""
    return ", ".join(f"{alias}.{col} AS {prefix}{col}" for col in INDUSTRY_LEVEL_COLUMNS)


def industry_complete_condition(*, alias: str = "industry_dim") -> str:
    """生成三级行业完整条件。"""
    return " AND ".join(f"{alias}.{col} IS NOT NULL AND {alias}.{col} != ''" for col in INDUSTRY_LEVEL_COLUMNS)


def count_industry_rows(conn) -> int:
    """统一读取行业维表行数。"""
    row = conn.execute("SELECT COUNT(*) FROM dim_stock_industry").fetchone()
    return (row[0] if row else 0) or 0


def summarize_industry_coverage(conn, stock_scope_sql: str, *, stock_code_column: str = "stock_code") -> dict:
    """
    统计给定股票集合的行业覆盖情况。

    stock_scope_sql 必须返回一列股票代码，列名默认为 stock_code。
    """
    alias = "industry_dim"
    row = conn.execute(
        f"""
        WITH stock_scope AS (
            {stock_scope_sql}
        )
        SELECT
            COUNT(*) AS total_codes,
            SUM(CASE WHEN {alias}.sw_level1 IS NOT NULL AND {alias}.sw_level1 != '' THEN 1 ELSE 0 END) AS level1_codes,
            SUM(CASE WHEN {alias}.sw_level2 IS NOT NULL AND {alias}.sw_level2 != '' THEN 1 ELSE 0 END) AS level2_codes,
            SUM(CASE WHEN {alias}.sw_level3 IS NOT NULL AND {alias}.sw_level3 != '' THEN 1 ELSE 0 END) AS level3_codes,
            SUM(CASE
                WHEN {industry_complete_condition(alias=alias)}
                THEN 1 ELSE 0 END
            ) AS complete_codes
        FROM stock_scope scope
        {industry_join_clause(f"scope.{stock_code_column}", alias=alias, join_type="LEFT")}
        """
    ).fetchone()
    if not row:
        return {
            "total_codes": 0,
            "level1_codes": 0,
            "level2_codes": 0,
            "level3_codes": 0,
            "complete_codes": 0,
        }
    return {
        "total_codes": (row["total_codes"] if row["total_codes"] is not None else 0),
        "level1_codes": (row["level1_codes"] if row["level1_codes"] is not None else 0),
        "level2_codes": (row["level2_codes"] if row["level2_codes"] is not None else 0),
        "level3_codes": (row["level3_codes"] if row["level3_codes"] is not None else 0),
        "complete_codes": (row["complete_codes"] if row["complete_codes"] is not None else 0),
    }


def resolve_industry(conn, stock_code: str, ref_date=None) -> dict:
    """
    单点查询股票行业分类。

    Args:
        conn: 数据库连接
        stock_code: 股票代码
        ref_date: 预留参数，未来支持按时点查历史行业。当前忽略。

    Returns:
        {"sw_level1": ..., "sw_level2": ..., "sw_level3": ...} or None
    """
    # 优先从缓存读
    global _industry_cache
    if _industry_cache is not None and stock_code in _industry_cache:
        return _industry_cache[stock_code]

    row = conn.execute(
        "SELECT sw_level1, sw_level2, sw_level3 "
        "FROM dim_stock_industry WHERE stock_code=?",
        (stock_code,)
    ).fetchone()
    if row:
        return {
            "sw_level1": row["sw_level1"],
            "sw_level2": row["sw_level2"],
            "sw_level3": row["sw_level3"],
        }
    return None


def invalidate_cache():
    """清除缓存，下次调用 load_industry_map/resolve_industry 时重新加载"""
    global _industry_cache
    _industry_cache = None

"""
utils.py — 全局共享工具函数

所有模块共用的纯函数放在这里，消除跨文件重复定义。
"""

from typing import Optional


def safe_float(value) -> Optional[float]:
    """安全转换为 float，None / NaN / 异常值统一返回 None。"""
    try:
        if value is None:
            return None
        value = float(value)
        if value != value:  # NaN check
            return None
        return value
    except Exception:
        return None


def percentile_ranks(values: list[Optional[float]]) -> list[Optional[float]]:
    """
    对一组可含 None 的数值做百分位排名（0-100）。
    None 值保持 None，相同值取平均排名，单元素返回 50.0。
    """
    indexed = [(i, v) for i, v in enumerate(values) if v is not None]
    if not indexed:
        return [None] * len(values)

    indexed.sort(key=lambda x: x[1])
    n = len(indexed)
    ranks: list[Optional[float]] = [None] * len(values)

    i = 0
    while i < n:
        # 处理相同值（取平均排名）
        j = i
        while j < n and indexed[j][1] == indexed[i][1]:
            j += 1
        avg_rank = (i + j - 1) / 2.0
        pctile = (avg_rank / (n - 1) * 100) if n > 1 else 50.0
        for k in range(i, j):
            ranks[indexed[k][0]] = round(pctile, 2)
        i = j

    return ranks


def normalize_ymd(date_str: Optional[str]) -> Optional[str]:
    """归一化日期到 YYYY-MM-DD 格式。支持 YYYYMMDD / YYYY-MM-DD / YYYY/MM/DD。"""
    if not date_str:
        return None
    raw = str(date_str).strip()
    digits = raw.replace("-", "").replace("/", "")
    if len(digits) != 8 or not digits.isdigit():
        return None
    return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"


def clamp(value: float, lo: float, hi: float) -> float:
    """限制值在 [lo, hi] 范围内。"""
    return max(lo, min(hi, value))

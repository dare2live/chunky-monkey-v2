import sys
from pathlib import Path

# Add backend directory to Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.constants import ETF_INDUSTRY_MAP, ETF_CATEGORY_SORT_ORDER, ETF_NON_INDUSTRY_CATS

def test_etf_industry_map_unique():
    names = [name for _, name in ETF_INDUSTRY_MAP]
    # No duplicate industries in map
    assert len(names) == len(set(names))
    
    # Sort order contains all mapped categories
    for name in names:
        assert name in ETF_CATEGORY_SORT_ORDER

def test_etf_non_industry_categories():
    for cat in ETF_NON_INDUSTRY_CATS:
        assert cat in ETF_CATEGORY_SORT_ORDER

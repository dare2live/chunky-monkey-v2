from typing import List, Optional, Any, Dict
from pydantic import BaseModel, Field


# ==========================================
# EastMoney API Data Schemas
# ==========================================

class EastMoneyHoldingsItem(BaseModel):
    """EastMoney RPT_F10_EH_FREEHOLDERS endpoint row"""
    # The API might change types, so we try to be lenient with input strings but validate key fields exist
    SECUCODE: str
    SECURITY_CODE: Optional[str] = None
    SECURITY_NAME_ABBR: Optional[str] = None
    HOLDER_NAME: str
    END_DATE: Optional[str] = None
    REPORT_DATE: Optional[str] = None
    UPDATE_DATE: Optional[str] = None
    NOTICE_DATE: Optional[str] = None
    
    # Ranks and numbers logic will be converted downstream, but we validate presence of keys
    HOLDER_RANK: Optional[Any] = None
    HOLDER_RANKN: Optional[Any] = None
    FREE_HOLDNUM: Optional[Any] = None
    HOLD_NUM: Optional[Any] = None
    HOLDER_MARKET_CAP: Optional[Any] = None
    HOLD_MARKET_CAP: Optional[Any] = None
    
    HOLDER_NEWTYPE: Optional[str] = None
    HOLDER_TYPE: Optional[str] = None
    HOLDER_STATEE: Optional[str] = None
    HOLDSTATE: Optional[str] = None

    class Config:
        extra = "allow"  # Allow extra keys, we just want to protect against fundamental changes


class EastMoneyHoldingsResponse(BaseModel):
    """EastMoney Data Wrapper Result"""
    success: bool
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None

    def get_data_items(self) -> List[Dict[str, Any]]:
        """Safely extract data items or return empty list"""
        if not self.result or "data" not in self.result or not self.result["data"]:
            return []
        
        # Pydantic validation for internal child items
        validated_items = []
        for item in self.result["data"]:
            # If an item doesn't map correctly, it will throw a ValidationError
            valid_item = EastMoneyHoldingsItem(**item)
            validated_items.append(valid_item.model_dump(exclude_unset=False, by_alias=True) | item)
        return validated_items


# ==========================================
# AKShare DataFrame Schemas (Row Level Validations)
# ==========================================

class KLineDailyRow(BaseModel):
    """Daily K-Line structure from various sources"""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[float] = None
    amount: Optional[float] = None


class SWIndustryRow(BaseModel):
    """AKShare SW Industry tree structure row parsing"""
    股票代码: str
    
    class Config:
        extra = "allow"


class SWIndustryTreeRow(BaseModel):
    """AKShare SW Industry Tree category row"""
    类目编码: str
    
    class Config:
        extra = "allow"

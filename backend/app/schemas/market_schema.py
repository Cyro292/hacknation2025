from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class MarketBase(BaseModel):
    """Base schema for a market"""
    polymarket_id: str = Field(..., description="Unique identifier from Polymarket")
    question: str = Field(..., description="Market question")
    description: Optional[str] = Field(None, description="Detailed market description")
    outcomes: List[str] = Field(default_factory=list, description="Possible outcomes")
    outcome_prices: List[str] = Field(default_factory=list, description="Current prices for outcomes")
    end_date: Optional[datetime] = Field(None, description="Market close date")
    volume: float = Field(default=0.0, description="Total trading volume")
    is_active: bool = Field(default=True, description="Whether market is currently active")
    slug: Optional[str] = Field(None, description="URL slug from Polymarket")
    one_day_price_change: Optional[float] = Field(None, description="24h price change")
    one_week_price_change: Optional[float] = Field(None, description="7-day price change")
    one_month_price_change: Optional[float] = Field(None, description="30-day price change")
    tags: List[str] = Field(default_factory=list, description="Event tags (e.g., Politics, Economy)")

class MarketCreate(MarketBase):
    """Schema for creating a market"""
    pass

class MarketUpdate(BaseModel):
    """Schema for updating a market"""
    question: Optional[str] = None
    description: Optional[str] = None
    outcomes: Optional[List[str]] = None
    outcome_prices: Optional[List[str]] = None
    end_date: Optional[datetime] = None
    volume: Optional[float] = None
    is_active: Optional[bool] = None
    slug: Optional[str] = None
    one_day_price_change: Optional[float] = None
    one_week_price_change: Optional[float] = None
    one_month_price_change: Optional[float] = None

class Market(MarketBase):
    """Schema for a complete market with database fields"""
    id: int = Field(..., description="Database primary key")
    created_at: datetime = Field(..., description="When the market was first added")
    updated_at: datetime = Field(..., description="When the market was last updated")
    last_scraped_at: Optional[datetime] = Field(None, description="When the market was last scraped")
    
    # Volatility fields from market_volatility table (via join)
    real_volatility_24h: Optional[float] = Field(None, description="Real volatility score calculated from price changes")
    proxy_volatility_24h: Optional[float] = Field(None, description="Proxy volatility score based on market characteristics")
    volatility_calculation_method: Optional[str] = Field(None, description="Method used to calculate volatility")
    volatility_data_points: Optional[int] = Field(None, description="Number of data points used in volatility calculation")
    volatility_calculated_at: Optional[datetime] = Field(None, description="When the volatility was last calculated")

    class Config:
        from_attributes = True

class MarketResponse(BaseModel):
    """Schema for API market response"""
    market: Market

class MarketListResponse(BaseModel):
    """Schema for API market list response"""
    markets: List[Market]
    total: int
    page: int
    page_size: int


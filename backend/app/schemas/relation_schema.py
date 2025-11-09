from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from app.schemas.market_schema import Market

class RelatedMarket(BaseModel):
    """Schema for a related market result"""
    market_id: int = Field(..., description="Related market ID")
    similarity: float = Field(..., description="Similarity score (0.0-1.0)")
    correlation: float = Field(0.0, description="Correlation score")
    pressure: float = Field(0.0, description="Pressure score")
    ai_correlation_score: Optional[float] = Field(None, description="AI-generated correlation score (0.0-1.0)")
    ai_explanation: Optional[str] = Field(None, description="AI-generated explanation of relationship")
    investment_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Arbitrage opportunity score (0.0-1.0). Higher = better price differential")
    investment_rationale: Optional[str] = Field(None, description="Arbitrage opportunity explanation focusing on price differentials")
    risk_level: Optional[str] = Field(None, description="Risk level: low, medium, high")
    expected_values: Optional[Dict[str, Any]] = Field(None, description="Expected value calculations for all 4 scenarios")
    best_strategy: Optional[str] = Field(None, description="Recommended betting strategy based on EV analysis")

class RelationSearchResponse(BaseModel):
    """Response for relation searches"""
    source_market_id: int = Field(..., description="The source market ID")
    related_markets: List[RelatedMarket] = Field(..., description="List of related markets")
    count: int = Field(..., description="Number of related markets found")

class MarketRelation(BaseModel):
    """Schema for a stored market relation"""
    id: int = Field(..., description="Relation ID")
    market_id_1: int = Field(..., description="First market ID")
    market_id_2: int = Field(..., description="Second market ID")
    similarity: float = Field(..., description="Similarity score (0.0-1.0)")
    correlation: float = Field(0.0, description="Correlation score")
    pressure: float = Field(0.0, description="Pressure score")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Update timestamp")
    
    class Config:
        from_attributes = True

class MarketRelationCreate(BaseModel):
    """Schema for creating a market relation"""
    market_id_1: int = Field(..., description="First market ID")
    market_id_2: int = Field(..., description="Second market ID")
    similarity: float = Field(..., ge=0.0, le=1.0, description="Similarity score (0.0-1.0)")
    correlation: Optional[float] = Field(0.0, description="Correlation score")
    pressure: Optional[float] = Field(0.0, description="Pressure score")

class MarketRelationBatchCreate(BaseModel):
    """Schema for batch creating market relations"""
    relations: List[MarketRelationCreate] = Field(..., description="List of relations to create")

class EnrichedRelatedMarket(BaseModel):
    """Schema for a related market with full market details"""
    market_id: int = Field(..., description="Related market ID")
    similarity: float = Field(..., description="Similarity score (0.0-1.0)")
    correlation: float = Field(0.0, description="Correlation score")
    pressure: float = Field(0.0, description="Pressure score")
    market: Market = Field(..., description="Full market details")
    ai_correlation_score: Optional[float] = Field(None, description="AI-generated correlation score (0.0-1.0)")
    ai_explanation: Optional[str] = Field(None, description="AI-generated explanation of relationship")
    investment_score: Optional[float] = Field(None, ge=0.0, le=1.0, description="Arbitrage opportunity score (0.0-1.0). Higher = better price differential")
    investment_rationale: Optional[str] = Field(None, description="Arbitrage opportunity explanation focusing on price differentials")
    risk_level: Optional[str] = Field(None, description="Risk level: low, medium, high")
    expected_values: Optional[Dict[str, Any]] = Field(None, description="Expected value calculations for all 4 scenarios")
    best_strategy: Optional[str] = Field(None, description="Recommended betting strategy based on EV analysis")

class EnrichedRelationResponse(BaseModel):
    """Response for enriched relation searches with full market data"""
    source_market_id: int = Field(..., description="The source market ID")
    source_market: Optional[Market] = Field(None, description="Full source market details")
    related_markets: List[EnrichedRelatedMarket] = Field(..., description="List of related markets with full details")
    count: int = Field(..., description="Number of related markets found")
"""
Market Routes - API endpoints for market CRUD operations
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from datetime import datetime
from app.schemas.market_schema import (
    Market,
    MarketCreate,
    MarketUpdate,
    MarketResponse,
    MarketListResponse
)
from app.services.database_service import get_database_service

router = APIRouter(prefix="/markets", tags=["Markets"])


@router.post("/", response_model=MarketResponse, status_code=201)
async def create_market(market_data: MarketCreate):
    """
    Create a new market.
    
    Example:
    ```json
    {
        "polymarket_id": "btc-100k-2025",
        "question": "Will Bitcoin reach $100k by 2025?",
        "description": "Market about Bitcoin price prediction",
        "outcomes": ["Yes", "No"],
        "outcome_prices": ["0.65", "0.35"],
        "end_date": "2025-12-31T00:00:00Z",
        "volume": 1000000.0,
        "is_active": true
    }
    ```
    """
    try:
        db = get_database_service()
        market = await db.create_market(market_data)
        return MarketResponse(market=market)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{market_id}", response_model=MarketResponse)
async def get_market(market_id: int):
    """
    Get a market by its database ID.
    """
    try:
        db = get_database_service()
        market = await db.get_market_by_id(market_id)
        
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return MarketResponse(market=market)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/polymarket/{polymarket_id}", response_model=MarketResponse)
async def get_market_by_polymarket_id(polymarket_id: str):
    """
    Get a market by its Polymarket ID.
    """
    try:
        db = get_database_service()
        market = await db.get_market_by_polymarket_id(polymarket_id)
        
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return MarketResponse(market=market)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=MarketListResponse)
async def get_markets(
    limit: int = Query(100, ge=1, le=2000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    is_active: Optional[bool] = Query(None, description="Filter by active status"),
    order_by: str = Query("created_at", description="Field to order by"),
    ascending: bool = Query(False, description="Sort order")
):
    """
    Get a list of markets with pagination and filtering.
    """
    try:
        db = get_database_service()
        
        markets = await db.get_markets(
            limit=limit,
            offset=offset,
            is_active=is_active,
            order_by=order_by,
            ascending=ascending
        )
        
        total = await db.count_markets(is_active=is_active)
        
        page = offset // limit if limit > 0 else 0
        
        return MarketListResponse(
            markets=markets,
            total=total,
            page=page,
            page_size=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{market_id}", response_model=MarketResponse)
async def update_market(market_id: int, update_data: MarketUpdate):
    """
    Update an existing market.
    
    Example:
    ```json
    {
        "outcome_prices": ["0.70", "0.30"],
        "volume": 1500000.0,
        "is_active": true
    }
    ```
    """
    try:
        db = get_database_service()
        market = await db.update_market(market_id, update_data)
        
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return MarketResponse(market=market)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{market_id}", status_code=204)
async def delete_market(market_id: int):
    """
    Delete a market by ID.
    """
    try:
        db = get_database_service()
        deleted = await db.delete_market(market_id)
        
        if not deleted:
            raise HTTPException(status_code=404, detail="Market not found")
        
        return None
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/query", response_model=MarketListResponse)
async def search_markets(
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results")
):
    """
    Search markets by question or description.
    
    Example: `/markets/search/query?q=bitcoin&limit=10`
    """
    try:
        db = get_database_service()
        markets = await db.search_markets(query=q, limit=limit)
        
        return MarketListResponse(
            markets=markets,
            total=len(markets),
            page=0,
            page_size=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/filter/active", response_model=MarketListResponse)
async def get_active_markets(
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """
    Get all active markets.
    """
    try:
        db = get_database_service()
        markets = await db.get_active_markets(limit=limit)
        total = await db.count_markets(is_active=True)
        
        return MarketListResponse(
            markets=markets,
            total=total,
            page=0,
            page_size=limit
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch/upsert")
async def batch_upsert_markets(markets: List[MarketCreate]):
    """
    Batch upsert multiple markets.
    
    Example:
    ```json
    [
        {
            "polymarket_id": "market-1",
            "question": "Question 1?",
            ...
        },
        {
            "polymarket_id": "market-2",
            "question": "Question 2?",
            ...
        }
    ]
    ```
    """
    try:
        db = get_database_service()
        result = await db.batch_upsert_markets(markets)
        
        return {
            "message": "Batch upsert completed",
            "successful": result["successful"],
            "failed": result["failed"],
            "total": result["total"]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/stats/overview")
async def get_market_stats():
    """
    Get overall market statistics.
    """
    try:
        db = get_database_service()
        
        total_markets = await db.count_markets()
        active_markets = await db.count_markets(is_active=True)
        inactive_markets = await db.count_markets(is_active=False)
        
        return {
            "total_markets": total_markets,
            "active_markets": active_markets,
            "inactive_markets": inactive_markets
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


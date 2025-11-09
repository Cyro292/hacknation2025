"""
Relation Routes - API endpoints for stored market relationships
"""
from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from app.schemas.relation_schema import (
    RelatedMarket,
    RelationSearchResponse,
    MarketRelation,
    MarketRelationCreate,
    MarketRelationBatchCreate,
    EnrichedRelatedMarket,
    EnrichedRelationResponse,
)
from app.services.relation_service import get_relation_service

router = APIRouter(prefix="/relations", tags=["Relations"])


@router.get("/{market_id}/enriched", response_model=EnrichedRelationResponse)
async def get_related_markets_enriched(
    market_id: int,
    limit: int = Query(10, ge=1, le=1000, description="Maximum number of related markets"),
    min_similarity: float = Query(0.7, ge=0.0, le=1.0, description="Minimum similarity threshold"),
    min_volume: Optional[float] = Query(None, ge=0.0, description="Minimum market volume filter"),
    ai_analysis: bool = Query(False, description="Include AI-generated correlation analysis (slower)"),
    ai_model: str = Query("gemini-flash", description="AI model: 'gemini-flash' (fast) or 'gemini-pro' (quality)")
):
    """
    Get related markets with full market details (enriched data).
    Returns markets with similarity >= min_similarity threshold, including full market objects.
    
    Example: `/relations/123/enriched?limit=100&min_similarity=0.7&min_volume=10000&ai_analysis=true&ai_model=gemini-pro`
    
    Args:
        market_id: Source market ID
        limit: Maximum number of results (default: 10, max: 1000)
        min_similarity: Minimum similarity score (default: 0.7)
        min_volume: Minimum market volume (optional, filters out low-volume markets)
        ai_analysis: Include AI correlation analysis (default: False for speed)
        ai_model: AI model to use - 'gemini-flash' (faster) or 'gemini-pro' (higher quality)
    
    Returns:
        Enriched response with full market details, AI correlation scores, and arbitrage analysis
    
    Note:
        AI analysis adds 1-3 seconds per market. Use sparingly for large result sets.
        Includes arbitrage scores (0.0-1.0 scale) and risk levels when AI analysis is enabled.
        Arbitrage scores focus on price differentials - same prices = low score.
    """
    try:
        service = get_relation_service()
        
        # Get enriched results with full market data (service handles all DB calls)
        result = await service.get_related_markets_enriched(
            market_id=market_id,
            limit=limit,
            min_similarity=min_similarity,
            min_volume=min_volume,
            include_source=True,
            include_ai_analysis=ai_analysis,
            ai_model=ai_model
        )
        
        return EnrichedRelationResponse(
            source_market_id=market_id,
            source_market=result["source_market"],
            related_markets=[
                EnrichedRelatedMarket(
                    market_id=mid,
                    similarity=sim,
                    correlation=corr,
                    pressure=press,
                    market=market,
                    ai_correlation_score=ai_score,
                    ai_explanation=ai_explanation,
                    investment_score=inv_score,
                    investment_rationale=inv_rationale,
                    risk_level=risk,
                    expected_values=exp_values,
                    best_strategy=best_strat
                )
                for mid, sim, corr, press, market, ai_score, ai_explanation, inv_score, inv_rationale, risk, exp_values, best_strat in result["related_markets"]
            ],
            count=len(result["related_markets"])
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{market_id}", response_model=RelationSearchResponse)
async def get_related_markets(
    market_id: int,
    limit: int = Query(10, ge=1, le=1000, description="Maximum number of related markets"),
    min_similarity: float = Query(0.7, ge=0.0, le=1.0, description="Minimum similarity threshold"),
    min_volume: Optional[float] = Query(None, ge=0.0, description="Minimum market volume filter"),
    ai_analysis: bool = Query(False, description="Include AI-generated correlation analysis (slower)"),
    ai_model: str = Query("gemini-flash", description="AI model: 'gemini-flash' (fast) or 'gemini-pro' (quality)")
):
    """
    Get related markets from stored relations in database (lightweight version without full market objects).
    Returns markets with similarity >= min_similarity threshold.
    
    Example: `/relations/123?limit=100&min_similarity=0.7&min_volume=10000&ai_analysis=true&ai_model=gemini-pro`
    
    Args:
        market_id: Source market ID
        limit: Maximum number of results (default: 10, max: 1000)
        min_similarity: Minimum similarity score (default: 0.7)
        min_volume: Minimum market volume (optional, filters out low-volume markets)
        ai_analysis: Include AI correlation analysis (default: False for speed)
        ai_model: AI model to use - 'gemini-flash' (faster) or 'gemini-pro' (higher quality)
    
    Note:
        For full market details, use `/relations/{market_id}/enriched` instead.
        AI analysis adds 1-3 seconds per market and includes arbitrage scoring (0.0-1.0) when enabled.
        Arbitrage scores focus on price differentials - same prices = low score.
    """
    try:
        service = get_relation_service()
        results = await service.get_related_markets(
            market_id=market_id,
            limit=limit,
            min_similarity=min_similarity,
            min_volume=min_volume,
            include_ai_analysis=ai_analysis,
            ai_model=ai_model
        )
        
        return RelationSearchResponse(
            source_market_id=market_id,
            related_markets=[
                RelatedMarket(
                    market_id=mid, 
                    similarity=sim, 
                    correlation=corr, 
                    pressure=press,
                    ai_correlation_score=ai_score,
                    ai_explanation=ai_explanation,
                    investment_score=inv_score,
                    investment_rationale=inv_rationale,
                    risk_level=risk,
                    expected_values=exp_values,
                    best_strategy=best_strat
                )
                for mid, sim, corr, press, ai_score, ai_explanation, inv_score, inv_rationale, risk, exp_values, best_strat in results
            ],
            count=len(results)
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/between/{market_id_1}/{market_id_2}", response_model=MarketRelation)
async def get_relation_between_markets(market_id_1: int, market_id_2: int):
    """
    Get the relation between two specific markets.
    
    Example: `/relations/between/123/456`
    """
    try:
        service = get_relation_service()
        relation = await service.get_relation_between(market_id_1, market_id_2)
        
        if not relation:
            raise HTTPException(
                status_code=404,
                detail=f"No relation found between markets {market_id_1} and {market_id_2}"
            )
        
        return relation
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=MarketRelation, status_code=201)
async def create_relation(request: MarketRelationCreate):
    """
    Create or update a relation between two markets.
    
    Example request body:
    ```json
    {
        "market_id_1": 123,
        "market_id_2": 456,
        "similarity": 0.85,
        "correlation": 0.72,
        "pressure": 0.45
    }
    ```
    """
    try:
        service = get_relation_service()
        relation = await service.create_relation(
            market_id_1=request.market_id_1,
            market_id_2=request.market_id_2,
            similarity=request.similarity,
            correlation=request.correlation or 0.0,
            pressure=request.pressure or 0.0
        )
        return relation
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/batch", status_code=201)
async def create_relations_batch(request: MarketRelationBatchCreate):
    """
    Create multiple relations in batch.
    
    Example request body:
    ```json
    {
        "relations": [
            {"market_id_1": 123, "market_id_2": 456, "similarity": 0.85, "correlation": 0.72, "pressure": 0.45},
            {"market_id_1": 123, "market_id_2": 789, "similarity": 0.92, "correlation": 0.68, "pressure": 0.52}
        ]
    }
    ```
    """
    try:
        service = get_relation_service()
        result = await service.create_relations_batch(request.relations)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/between/{market_id_1}/{market_id_2}", status_code=204)
async def delete_relation(market_id_1: int, market_id_2: int):
    """
    Delete a relation between two markets.
    
    Example: `/relations/between/123/456`
    """
    try:
        service = get_relation_service()
        deleted = await service.delete_relation(market_id_1, market_id_2)
        
        if not deleted:
            raise HTTPException(
                status_code=404,
                detail=f"No relation found between markets {market_id_1} and {market_id_2}"
            )
        
        return None
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{market_id}/all", status_code=200)
async def delete_all_relations_for_market(market_id: int):
    """
    Delete all relations involving a specific market.
    
    Example: `/relations/123/all`
    """
    try:
        service = get_relation_service()
        count = await service.delete_all_relations_for_market(market_id)
        return {"deleted": count, "market_id": market_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/count")
async def count_relations(market_id: Optional[int] = Query(None, description="Optional market ID to count relations for")):
    """
    Count total relations in database, optionally for a specific market.
    
    Example: `/relations/count` or `/relations/count?market_id=123`
    """
    try:
        service = get_relation_service()
        count = await service.count_relations(market_id=market_id)
        
        if market_id:
            return {"market_id": market_id, "count": count}
        else:
            return {"total_relations": count}
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/statistics/{market_id}")
async def get_relation_statistics(market_id: int):
    """
    Get statistics about a market's relationships from stored relations.
    
    Example: `/relations/statistics/123`
    """
    try:
        service = get_relation_service()
        
        # Get all related markets with different thresholds
        high_similarity = await service.get_related_markets(
            market_id=market_id, 
            limit=1000, 
            min_similarity=0.9
        )
        medium_similarity = await service.get_related_markets(
            market_id=market_id, 
            limit=1000, 
            min_similarity=0.7
        )
        low_similarity = await service.get_related_markets(
            market_id=market_id, 
            limit=1000, 
            min_similarity=0.5
        )
        
        # Calculate statistics
        return {
            "market_id": market_id,
            "total_related_markets": len(low_similarity),
            "high_similarity_count": len(high_similarity),  # >= 0.9
            "medium_similarity_count": len(medium_similarity),  # >= 0.7
            "low_similarity_count": len(low_similarity),  # >= 0.5
            "average_similarity": (
                sum(score for _, score in low_similarity) / len(low_similarity)
                if low_similarity else 0.0
            ),
            "max_similarity": (
                max(score for _, score in low_similarity)
                if low_similarity else 0.0
            )
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


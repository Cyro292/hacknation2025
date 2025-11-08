"""
Relation Service - Manages stored market relationships in database
"""
from typing import List, Optional, Tuple
from app.schemas.relation_schema import MarketRelation, MarketRelationCreate
from app.schemas.market_schema import Market
from app.services.database_service import get_database_service
from app.services.vector_service import get_vector_service
import logging
import numpy as np

logger = logging.getLogger(__name__)


class RelationService:
    """Manages stored market relationships in database."""
    
    def __init__(self):
        self.db = get_database_service()
        self._vector_service = None
    
    @property
    def vector_service(self):
        """Lazy initialization of vector service - only when needed."""
        if self._vector_service is None:
            self._vector_service = get_vector_service()
        return self._vector_service
    
    async def get_related_markets(
        self,
        market_id: int,
        limit: int = 10,
        min_similarity: float = 0.7
    ) -> List[Tuple[int, float, float, float]]:
        """
        Get related markets from stored relations.
        
        Args:
            market_id: Source market ID
            limit: Maximum number of results
            min_similarity: Minimum similarity threshold
            
        Returns:
            List of (related_market_id, similarity, correlation, pressure) tuples
        """
        try:
            # Query relations where this market is involved
            response = self.db.client.table('market_relations')\
                .select('*')\
                .or_(f"market_id_1.eq.{market_id},market_id_2.eq.{market_id}")\
                .gte('similarity', min_similarity)\
                .order('similarity', desc=True)\
                .limit(limit)\
                .execute()
            
            results = []
            for relation in response.data:
                # Return the OTHER market ID
                related_id = (
                    relation['market_id_2'] 
                    if relation['market_id_1'] == market_id 
                    else relation['market_id_1']
                )
                results.append((
                    related_id,
                    float(relation['similarity']),
                    float(relation.get('correlation', 0.0)),
                    float(relation.get('pressure', 0.0))
                ))
            
            return results
            
        except Exception as e:
            logger.error(f"Error getting related markets: {e}")
            raise
    
    async def get_relation_between(
        self,
        market_id_1: int,
        market_id_2: int
    ) -> Optional[MarketRelation]:
        """Get relation between two specific markets."""
        try:
            # Ensure market_id_1 < market_id_2 for query
            min_id = min(market_id_1, market_id_2)
            max_id = max(market_id_1, market_id_2)
            
            response = self.db.client.table('market_relations')\
                .select('*')\
                .eq('market_id_1', min_id)\
                .eq('market_id_2', max_id)\
                .execute()
            
            if response.data:
                return MarketRelation(**response.data[0])
            return None
            
        except Exception as e:
            logger.error(f"Error getting relation between markets: {e}")
            raise
    
    async def create_relation(
        self,
        market_id_1: int,
        market_id_2: int,
        similarity: float,
        correlation: float = 0.0,
        pressure: float = 0.0
    ) -> MarketRelation:
        """
        Create or update a relation between two markets.
        
        Args:
            market_id_1: First market ID
            market_id_2: Second market ID
            similarity: Similarity score (0.0-1.0)
            correlation: Correlation score (default: 0.0)
            pressure: Pressure score (default: 0.0)
            
        Returns:
            Created MarketRelation
        """
        try:
            # Ensure market_id_1 < market_id_2
            min_id = min(market_id_1, market_id_2)
            max_id = max(market_id_1, market_id_2)
            
            data = {
                'market_id_1': min_id,
                'market_id_2': max_id,
                'similarity': similarity,
                'correlation': correlation,
                'pressure': pressure
            }
            
            # Upsert: update if exists, insert if not
            response = self.db.client.table('market_relations').upsert(
                data,
                on_conflict='market_id_1,market_id_2'
            ).execute()
            
            if response.data:
                return MarketRelation(**response.data[0])
            raise Exception("Failed to create relation")
            
        except Exception as e:
            logger.error(f"Error creating relation: {e}")
            raise
    
    async def create_relations_batch(
        self,
        relations: List[MarketRelationCreate]
    ) -> dict:
        """
        Create multiple relations in batch.
        
        Args:
            relations: List of relations to create
            
        Returns:
            Dictionary with success/failure counts
        """
        created = 0
        failed = 0
        
        for relation in relations:
            try:
                await self.create_relation(
                    relation.market_id_1,
                    relation.market_id_2,
                    relation.similarity,
                    relation.correlation or 0.0,
                    relation.pressure or 0.0
                )
                created += 1
            except Exception as e:
                failed += 1
                logger.error(f"Failed to create relation: {e}")
        
        return {
            "created": created,
            "failed": failed,
            "total": len(relations)
        }
    
    async def delete_relation(
        self,
        market_id_1: int,
        market_id_2: int
    ) -> bool:
        """Delete a relation between two markets."""
        try:
            min_id = min(market_id_1, market_id_2)
            max_id = max(market_id_1, market_id_2)
            
            response = self.db.client.table('market_relations')\
                .delete()\
                .eq('market_id_1', min_id)\
                .eq('market_id_2', max_id)\
                .execute()
            
            return len(response.data) > 0
            
        except Exception as e:
            logger.error(f"Error deleting relation: {e}")
            raise
    
    async def delete_all_relations_for_market(
        self,
        market_id: int
    ) -> int:
        """Delete all relations involving a specific market."""
        try:
            response = self.db.client.table('market_relations')\
                .delete()\
                .or_(f"market_id_1.eq.{market_id},market_id_2.eq.{market_id}")\
                .execute()
            
            return len(response.data)
            
        except Exception as e:
            logger.error(f"Error deleting relations for market: {e}")
            raise
    
    async def count_relations(
        self,
        market_id: Optional[int] = None
    ) -> int:
        """Count total relations, optionally for a specific market."""
        try:
            query = self.db.client.table('market_relations').select('id', count='exact')
            
            if market_id is not None:
                query = query.or_(f"market_id_1.eq.{market_id},market_id_2.eq.{market_id}")
            
            response = query.execute()
            return response.count if response.count is not None else 0
            
        except Exception as e:
            logger.error(f"Error counting relations: {e}")
            raise
    
    # ==================== CALCULATION METHODS ====================
    
    def calculate_correlation(self, market1: Market, market2: Market) -> float:
        """
        Calculate correlation between two markets based on outcome prices.
        
        If both markets have outcome prices, calculate correlation.
        Otherwise, return 0.0 (no price data available).
        
        Args:
            market1: First market
            market2: Second market
            
        Returns:
            Correlation score (0.0-1.0)
        """
        # If either market doesn't have outcome prices, return 0
        if not market1.outcome_prices or not market2.outcome_prices:
            return 0.0
        
        try:
            # Convert price strings to floats
            prices1 = [float(p) for p in market1.outcome_prices if p]
            prices2 = [float(p) for p in market2.outcome_prices if p]
            
            # Need at least 2 prices to calculate correlation
            if len(prices1) < 2 or len(prices2) < 2:
                return 0.0
            
            # Normalize prices to 0-1 range if needed
            # Take the first outcome price as a proxy for market sentiment
            # For binary markets, use the "Yes" outcome price
            price1 = prices1[0] if prices1 else 0.0
            price2 = prices2[0] if prices2 else 0.0
            
            # Simple correlation: how similar are the prices?
            # If both markets are bullish (high prices) or bearish (low prices), correlation is high
            price_diff = abs(price1 - price2)
            correlation = 1.0 - min(price_diff, 1.0)  # Inverse of price difference
            
            return max(0.0, min(1.0, correlation))
            
        except (ValueError, IndexError) as e:
            logger.debug(f"Error calculating correlation: {e}")
            return 0.0
    
    def calculate_pressure(
        self,
        similarity: float,
        correlation: float,
        volume1: float,
        volume2: float
    ) -> float:
        """
        Calculate pressure score based on similarity, correlation, and volumes.
        
        Pressure represents the "strength" or "intensity" of the relationship.
        Higher pressure = stronger relationship.
        
        Formula: pressure = similarity * correlation * volume_factor
        
        Args:
            similarity: Similarity score (0.0-1.0)
            correlation: Correlation score (0.0-1.0)
            volume1: Volume of first market
            volume2: Volume of second market
            
        Returns:
            Pressure score (0.0-1.0)
        """
        # Normalize volumes (log scale to handle large differences)
        # Use average volume as a proxy
        avg_volume = (volume1 + volume2) / 2.0
        
        # Normalize volume to 0-1 range using log scale
        # Assuming max volume is around 1M, adjust as needed
        if avg_volume <= 0:
            volume_factor = 0.1  # Minimum factor for zero volume
        else:
            # Log normalization: log(1 + volume) / log(1 + max_volume)
            # Using 1M as max volume reference
            max_volume_ref = 1_000_000.0
            volume_factor = min(1.0, np.log1p(avg_volume) / np.log1p(max_volume_ref))
            # Ensure minimum of 0.1 to avoid zero pressure
            volume_factor = max(0.1, volume_factor)
        
        # Pressure = similarity * correlation * volume_factor
        # This ensures pressure is high only when all factors are high
        pressure = similarity * correlation * volume_factor
        
        # Normalize to 0-1 range
        return max(0.0, min(1.0, pressure))
    
    # ==================== RELATION DISCOVERY METHODS ====================
    
    async def find_similar_markets_for_relation(
        self,
        market_id: int,
        similarity_threshold: float,
        limit: int = 100
    ) -> List[Tuple[int, float]]:
        """
        Find similar markets using vector embeddings.
        
        Args:
            market_id: Source market ID
            similarity_threshold: Minimum similarity threshold
            limit: Maximum number of results
            
        Returns:
            List of (market_id, similarity) tuples
        """
        try:
            results = await self.vector_service.find_similar_to_market(market_id, limit=limit)
            
            # Filter by threshold
            filtered = [(mid, sim) for mid, sim in results if sim >= similarity_threshold]
            return filtered
            
        except Exception as e:
            logger.error(f"Error finding similar markets for {market_id}: {e}")
            return []
    
    async def create_relations_for_market(
        self,
        market_id: int,
        similarity_threshold: float,
        correlation_threshold: float,
        limit: int = 100
    ) -> Tuple[int, int]:
        """
        Create relations for a single market based on similarity and correlation thresholds.
        
        Args:
            market_id: Market ID to create relations for
            similarity_threshold: Minimum similarity threshold (0.0-1.0)
            correlation_threshold: Minimum correlation threshold (0.0-1.0)
            limit: Maximum number of similar markets to consider
            
        Returns:
            Tuple of (created_count, skipped_count)
        """
        created = 0
        skipped = 0
        
        try:
            # Get the market
            market = await self.db.get_market_by_id(market_id)
            if not market:
                logger.warning(f"Market {market_id} not found")
                return (0, 0)
            
            # Find similar markets
            similar_markets = await self.find_similar_markets_for_relation(
                market_id,
                similarity_threshold,
                limit=limit
            )
            
            if not similar_markets:
                return (0, 0)
            
            # Check existing relations to avoid duplicates
            existing_relations = await self.get_related_markets(market_id, limit=1000, min_similarity=0.0)
            existing_market_ids = {mid for mid, _, _, _ in existing_relations}
            
            # Process each similar market
            relations_to_create = []
            
            for similar_market_id, similarity in similar_markets:
                # Skip if relation already exists
                if similar_market_id in existing_market_ids:
                    skipped += 1
                    continue
                
                # Skip self
                if similar_market_id == market_id:
                    continue
                
                # Get the similar market
                similar_market = await self.db.get_market_by_id(similar_market_id)
                if not similar_market:
                    continue
                
                # Calculate correlation
                correlation = self.calculate_correlation(market, similar_market)
                
                # Skip if correlation is below threshold
                if correlation < correlation_threshold:
                    skipped += 1
                    continue
                
                # Calculate pressure
                pressure = self.calculate_pressure(
                    similarity=similarity,
                    correlation=correlation,
                    volume1=market.volume or 0.0,
                    volume2=similar_market.volume or 0.0
                )
                
                # Create relation
                relations_to_create.append(
                    MarketRelationCreate(
                        market_id_1=market_id,
                        market_id_2=similar_market_id,
                        similarity=similarity,
                        correlation=correlation,
                        pressure=pressure
                    )
                )
            
            # Batch create relations
            if relations_to_create:
                result = await self.create_relations_batch(relations_to_create)
                created = result.get('created', 0)
                skipped += result.get('failed', 0)
            
            return (created, skipped)
            
        except Exception as e:
            logger.error(f"Error creating relations for market {market_id}: {e}")
            return (0, 0)
    
    async def estimate_relations_count(
        self,
        market_ids: List[int],
        similarity_threshold: float,
        correlation_threshold: float,
        sample_size: Optional[int] = None
    ) -> dict:
        """
        Estimate how many relations would be created for given markets.
        Uses sampling if sample_size is provided for faster estimation.
        
        Args:
            market_ids: List of market IDs to estimate for
            similarity_threshold: Minimum similarity threshold
            correlation_threshold: Minimum correlation threshold
            sample_size: If provided, only sample this many markets for estimation
            
        Returns:
            Dictionary with estimation results:
            - estimated_total: Estimated total relations
            - sampled_markets: Number of markets sampled
            - relations_per_market: Average relations per market
            - sample_relations: Actual relations found in sample
        """
        try:
            # If sample_size is provided, use sampling
            markets_to_sample = market_ids
            if sample_size and sample_size < len(market_ids):
                import random
                markets_to_sample = random.sample(market_ids, min(sample_size, len(market_ids)))
            
            total_relations = 0
            markets_processed = 0
            
            for market_id in markets_to_sample:
                try:
                    # Get the market
                    market = await self.db.get_market_by_id(market_id)
                    if not market:
                        continue
                    
                    # Find similar markets
                    similar_markets = await self.find_similar_markets_for_relation(
                        market_id,
                        similarity_threshold,
                        limit=100
                    )
                    
                    if not similar_markets:
                        continue
                    
                    # Check existing relations to avoid duplicates
                    existing_relations = await self.get_related_markets(market_id, limit=1000, min_similarity=0.0)
                    existing_market_ids = {mid for mid, _, _, _ in existing_relations}
                    
                    # Count potential relations
                    potential_relations = 0
                    
                    for similar_market_id, similarity in similar_markets:
                        # Skip if relation already exists
                        if similar_market_id in existing_market_ids:
                            continue
                        
                        # Skip self
                        if similar_market_id == market_id:
                            continue
                        
                        # Get the similar market
                        similar_market = await self.db.get_market_by_id(similar_market_id)
                        if not similar_market:
                            continue
                        
                        # Calculate correlation
                        correlation = self.calculate_correlation(market, similar_market)
                        
                        # Count if correlation meets threshold
                        if correlation >= correlation_threshold:
                            potential_relations += 1
                    
                    total_relations += potential_relations
                    markets_processed += 1
                    
                except Exception as e:
                    logger.debug(f"Error estimating for market {market_id}: {e}")
                    continue
            
            # Calculate average and extrapolate
            if markets_processed > 0:
                avg_relations_per_market = total_relations / markets_processed
                estimated_total = int(avg_relations_per_market * len(market_ids))
            else:
                avg_relations_per_market = 0.0
                estimated_total = 0
            
            return {
                "estimated_total": estimated_total,
                "sampled_markets": markets_processed,
                "total_markets": len(market_ids),
                "relations_per_market": round(avg_relations_per_market, 2),
                "sample_relations": total_relations,
                "is_sampled": sample_size is not None and sample_size < len(market_ids)
            }
            
        except Exception as e:
            logger.error(f"Error estimating relations count: {e}")
            return {
                "estimated_total": 0,
                "sampled_markets": 0,
                "total_markets": len(market_ids),
                "relations_per_market": 0.0,
                "sample_relations": 0,
                "is_sampled": False,
                "error": str(e)
            }


_relation_service: Optional[RelationService] = None


def get_relation_service() -> RelationService:
    """Get or create the relation service singleton."""
    global _relation_service
    if _relation_service is None:
        _relation_service = RelationService()
    return _relation_service
